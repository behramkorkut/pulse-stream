package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"

	"github.com/behramkorkut/pulse-stream/internal/dedupe"
	"github.com/behramkorkut/pulse-stream/internal/event"
)

// callLog enregistre l'ordre des appels entre la source, le magasin et la mémoire des doublons.
type callLog struct {
	mu      sync.Mutex
	entries []string
}

func (c *callLog) add(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, s)
}

func (c *callLog) get() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.entries...)
}

// fakeSource délivre des messages puis attend l'annulation, comme un topic sans nouveau message.
type fakeSource struct {
	mu         sync.Mutex
	pending    []kafka.Message
	commits    []kafka.Message
	calls      *callLog
	failCommit bool // simule un crash juste avant la validation des offsets
}

func (s *fakeSource) FetchMessage(ctx context.Context) (kafka.Message, error) {
	s.mu.Lock()
	if len(s.pending) > 0 {
		m := s.pending[0]
		s.pending = s.pending[1:]
		s.mu.Unlock()
		return m, nil
	}
	s.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (s *fakeSource) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failCommit {
		return errors.New("arrêt simulé avant la validation des offsets")
	}
	s.commits = append(s.commits, msgs...)
	s.calls.add("commit")
	return nil
}

func (s *fakeSource) committed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commits)
}

// fakeStore se comporte comme le magasin MongoDB : chaque Apply est atomique (le verrou joue le rôle de la
// transaction) et ne compte que ce que les positions ne couvrent pas encore. failFirst échoue les N premiers appels
// (tous si < 0), AVANT toute écriture.
type fakeStore struct {
	mu        sync.Mutex
	applied   [][]Bucket
	positions map[Partition]int64
	calls     int
	failFirst int
	log       *callLog
}

func (s *fakeStore) Apply(_ context.Context, events []Counted, upTo map[Partition]int64) (Applied, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failFirst < 0 || s.calls <= s.failFirst {
		return Applied{}, errors.New("mongo indisponible")
	}
	if s.positions == nil {
		s.positions = map[Partition]int64{}
	}
	fresh := NotYetApplied(events, s.positions)
	for p, offset := range Advanced(upTo, s.positions) {
		s.positions[p] = offset
	}
	buckets := Aggregate(fresh)
	s.applied = append(s.applied, buckets)
	s.log.add("apply")
	return Applied{Events: fresh, Buckets: len(buckets)}, nil
}

// totals additionne les compteurs de tous les buckets appliqués, tous appels confondus.
func (s *fakeStore) totals() (pageviews, clicks, bots, sessions int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, call := range s.applied {
		for _, b := range call {
			pageviews += b.Pageviews
			clicks += b.Clicks
			bots += b.BotEvents
			sessions += b.Sessions
		}
	}
	return
}

func (s *fakeStore) applyCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.applied)
}

// recordingSeen décore la mémoire des doublons pour tracer les réservations et simuler une panne de Redis.
type recordingSeen struct {
	dedupe.Store
	log       *callLog
	failClaim bool
}

func (r *recordingSeen) Claim(ctx context.Context, keys []dedupe.Key, owners []string) ([]string, error) {
	if r.failClaim {
		return nil, errors.New("redis indisponible")
	}
	r.log.add("claim")
	return r.Store.Claim(ctx, keys, owners)
}

func msgFor(t *testing.T, offset int64, e event.Enriched) kafka.Message {
	t.Helper()
	value, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "enriched-events", Offset: offset, Key: []byte(e.SiteID + "/" + e.VisitorID), Value: value}
}

func testConfig() Config {
	return Config{BatchSize: 50, BatchWait: 20 * time.Millisecond, MaxAttempts: 3, RetryBackoff: time.Millisecond}
}

type rig struct {
	runner *Runner
	src    *fakeSource
	store  *fakeStore
	seen   *recordingSeen
	calls  *callLog
}

func newRig(msgs []kafka.Message, cfg Config) *rig {
	calls := &callLog{}
	src := &fakeSource{pending: msgs, calls: calls}
	store := &fakeStore{log: calls}
	seen := &recordingSeen{Store: dedupe.NewMemory(), log: calls}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &rig{runner: NewRunner(src, store, seen, cfg, log), src: src, store: store, seen: seen, calls: calls}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout en attendant : %s", what)
}

func (r *rig) start(t *testing.T) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.runner.Run(ctx) }()

	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("Run ne s'est pas arrêté après l'annulation")
			return nil
		}
	}
}

func TestRunnerCountsEventsAndCommits(t *testing.T) {
	msgs := []kafka.Message{
		msgFor(t, 0, human("e1", "site-42", event.TypePageview, "mobile", "safari", 0, true)),
		msgFor(t, 1, human("e2", "site-42", event.TypeClick, "desktop", "chrome", time.Second, false)),
		msgFor(t, 2, bot("e3", "site-42", 2*time.Second)),
	}
	r := newRig(msgs, testConfig())
	stop := r.start(t)

	waitFor(t, "les 3 offsets validés", func() bool { return r.src.committed() == 3 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v, want nil après un arrêt demandé", err)
	}

	pv, clicks, bots, sessions := r.store.totals()
	if pv != 1 || clicks != 1 || bots != 1 || sessions != 1 {
		t.Errorf("totaux = pv %d clicks %d bots %d sessions %d, want 1 1 1 1", pv, clicks, bots, sessions)
	}
}

// Ordre des étapes : réserver, PUIS écrire (compteurs et positions ensemble), PUIS valider les offsets.
func TestRunnerClaimsThenAppliesThenCommits(t *testing.T) {
	r := newRig([]kafka.Message{msgFor(t, 0, human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true))}, testConfig())
	stop := r.start(t)

	waitFor(t, "le commit", func() bool { return r.src.committed() == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	got := r.calls.get()
	want := []string{"claim", "apply", "commit"}
	if len(got) < 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("ordre des appels = %v, want %v", got, want)
	}
}

func TestRunnerCountsADuplicatedEventOnlyOnce(t *testing.T) {
	same := human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true)
	other := human("e2", "site-42", event.TypePageview, "desktop", "chrome", time.Second, false)

	cfg := testConfig()
	cfg.BatchSize = 3 // le lot 1 contient e1, e1 (doublon dans le lot), e2 ; le lot 2 contient e1 (déjà compté)
	r := newRig([]kafka.Message{
		msgFor(t, 0, same), msgFor(t, 1, same), msgFor(t, 2, other),
		msgFor(t, 3, same),
	}, cfg)
	stop := r.start(t)

	waitFor(t, "les 4 offsets validés", func() bool { return r.src.committed() == 4 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if pv, _, _, sessions := r.store.totals(); pv != 2 || sessions != 1 {
		t.Errorf("pageviews = %d, sessions = %d, want 2 et 1 (e1 compté une seule fois malgré 3 livraisons)", pv, sessions)
	}
}

// Régression : l'identifiant vient du client et n'est unique qu'au sein d'un site. Le même identifiant envoyé par
// plusieurs sites désigne des événements différents : tous doivent être comptés, dans un même lot (dédoublonnage
// interne au lot) comme dans des lots différents (mémoire des doublons dans Redis).
func TestRunnerCountsTheSameIDOnDifferentSites(t *testing.T) {
	cfg := testConfig()
	cfg.BatchSize = 2 // lot 1 : e1 de site-a et e1 de site-b ; lot 2 : e1 de site-c
	r := newRig([]kafka.Message{
		msgFor(t, 0, human("e1", "site-a", event.TypePageview, "desktop", "chrome", 0, true)),
		msgFor(t, 1, human("e1", "site-b", event.TypePageview, "desktop", "chrome", 0, true)),
		msgFor(t, 2, human("e1", "site-c", event.TypePageview, "desktop", "chrome", 0, true)),
	}, cfg)
	stop := r.start(t)

	waitFor(t, "les 3 offsets validés", func() bool { return r.src.committed() == 3 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if pv, _, _, _ := r.store.totals(); pv != 3 {
		t.Errorf("pageviews = %d, want 3 : un même identifiant sur trois sites, ce sont trois événements", pv)
	}
}

func TestRunnerSkipsPoisonMessagesButStillCommitsThem(t *testing.T) {
	msgs := []kafka.Message{
		{Topic: "enriched-events", Offset: 0, Value: []byte(`{oops`)},
		msgFor(t, 1, human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true)),
		{Topic: "enriched-events", Offset: 2, Value: []byte(`{}`)},
	}
	r := newRig(msgs, testConfig())
	stop := r.start(t)

	// Les 3 offsets doivent être validés : un message poison ne doit jamais bloquer le flux.
	waitFor(t, "les 3 offsets validés", func() bool { return r.src.committed() == 3 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if pv, _, _, _ := r.store.totals(); pv != 1 {
		t.Errorf("pageviews = %d, want 1 (l'événement valide est compté malgré les messages poison)", pv)
	}
}

func TestRunnerRetriesTransientStoreFailure(t *testing.T) {
	r := newRig([]kafka.Message{msgFor(t, 0, human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true))}, testConfig())
	r.store.failFirst = 2
	stop := r.start(t)

	waitFor(t, "le commit après les nouveaux essais", func() bool { return r.src.committed() == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if pv, _, _, _ := r.store.totals(); pv != 1 {
		t.Errorf("pageviews = %d, want 1", pv)
	}
}

// Les événements sont réservés AVANT l'écriture. Si l'écriture échoue, rien n'est validé, et à la relecture le
// message retrouve sa propre réservation : l'événement est compté, une fois, y compris quand une copie plus loin
// dans la partition (renvoi du client) arrive entre-temps.
func TestRunnerCountsAfterAFailedWriteWhenTheBatchIsReplayed(t *testing.T) {
	view := human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true)
	calls := &callLog{}
	store := &fakeStore{log: calls, failFirst: -1}
	memory := dedupe.NewMemory() // le Redis partagé par les deux "vies"
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Première vie : réserve e1, puis le magasin reste indisponible jusqu'à l'abandon.
	first := &fakeSource{pending: []kafka.Message{msgFor(t, 0, view)}, calls: calls}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := NewRunner(first, store, memory, testConfig(), log).Run(ctx); err == nil || ctx.Err() != nil {
		t.Fatalf("Run() = %v, want une erreur remontée avant le délai (le magasin est indisponible)", err)
	}
	if n := first.committed(); n != 0 {
		t.Fatalf("%d offsets validés : les messages seraient perdus", n)
	}

	// Seconde vie : le magasin répond. Kafka redonne le message 0, suivi d'une copie de e1 (offset 1).
	store.mu.Lock()
	store.failFirst = 0
	store.mu.Unlock()
	second := &fakeSource{pending: []kafka.Message{msgFor(t, 0, view), msgFor(t, 1, view)}, calls: calls}
	stop := startRunner(t, NewRunner(second, store, memory, testConfig(), log))
	waitFor(t, "les 2 offsets validés", func() bool { return second.committed() == 2 })
	if err := stop(); err != nil {
		t.Fatalf("seconde vie : Run() error = %v", err)
	}

	if pv, _, _, _ := store.totals(); pv != 1 {
		t.Errorf("pageviews = %d, want 1 : ni perdu (réservé avant l'échec), ni compté deux fois (copie)", pv)
	}
}

func TestRunnerDoesNotCountWhenTheClaimFails(t *testing.T) {
	r := newRig([]kafka.Message{msgFor(t, 0, human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true))}, testConfig())
	r.seen.failClaim = true

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.runner.Run(ctx); err == nil || ctx.Err() != nil {
		t.Fatalf("Run() = %v, want une erreur remontée avant le délai (la mémoire des doublons est indisponible)", err)
	}
	if n := r.store.applyCalls(); n != 0 {
		t.Errorf("%d écritures de compteurs sans avoir pu dédoublonner : risque de compter deux fois", n)
	}
	if n := r.src.committed(); n != 0 {
		t.Errorf("%d offsets validés", n)
	}
}

func TestRunnerCountsEventsByOutcome(t *testing.T) {
	same := human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true)
	cfg := testConfig()
	m := NewMetrics(prometheus.NewRegistry())
	cfg.Metrics = m
	r := newRig([]kafka.Message{
		msgFor(t, 0, same), msgFor(t, 1, same), // un doublon dans le lot
		{Topic: "enriched-events", Offset: 2, Value: []byte(`{oops`)}, // inexploitable
		msgFor(t, 3, human("e2", "site-42", event.TypeClick, "desktop", "chrome", time.Second, false)),
	}, cfg)
	stop := r.start(t)

	waitFor(t, "les 4 offsets validés", func() bool { return r.src.committed() == 4 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	for outcome, want := range map[string]float64{"counted": 2, "duplicate": 1, "replayed": 0, "skipped": 1} {
		if got := testutil.ToFloat64(m.events.WithLabelValues(outcome)); got != want {
			t.Errorf("issue %q = %v, want %v", outcome, got, want)
		}
	}
	if got := testutil.ToFloat64(m.buckets); got != 1 {
		t.Errorf("documents écrits = %v, want 1 (une seule minute, un seul site)", got)
	}
}

// histogramOf lit le nombre d'observations et leur somme d'un histogramme du registre.
func histogramOf(t *testing.T, reg *prometheus.Registry, name string) (count uint64, sum float64) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather : %v", err)
	}
	for _, mf := range families {
		if mf.GetName() == name {
			h := mf.GetMetric()[0].GetHistogram()
			return h.GetSampleCount(), h.GetSampleSum()
		}
	}
	t.Fatalf("métrique %s absente du registre", name)
	return 0, 0
}

// La fraîcheur se mesure sur chaque événement compté, depuis sa réception par le collector. Un doublon n'est pas
// mesuré une seconde fois ; un événement sans heure de réception (autre producteur) ne l'est pas du tout.
func TestRunnerObservesEndToEndLatency(t *testing.T) {
	received := time.Now().Add(-2 * time.Second)
	e1 := human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true)
	e1.ReceivedAt = received
	e2 := human("e2", "site-42", event.TypeClick, "desktop", "chrome", time.Second, false)
	e2.ReceivedAt = received
	foreign := human("e3", "site-42", event.TypePageview, "desktop", "chrome", 2*time.Second, false)

	reg := prometheus.NewRegistry()
	cfg := testConfig()
	cfg.Metrics = NewMetrics(reg)
	r := newRig([]kafka.Message{msgFor(t, 0, e1), msgFor(t, 1, e2), msgFor(t, 2, e1), msgFor(t, 3, foreign)}, cfg)
	stop := r.start(t)

	waitFor(t, "les 4 offsets validés", func() bool { return r.src.committed() == 4 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	count, sum := histogramOf(t, reg, "pulse_end_to_end_latency_seconds")
	if count != 2 {
		t.Errorf("%d mesures, want 2 (e1 et e2 ; ni le doublon de e1, ni e3 sans heure de réception)", count)
	}
	if sum < 4 {
		t.Errorf("somme = %.2f s, want au moins 4 s (deux événements reçus il y a 2 s)", sum)
	}
}

func TestMetricsSeriesExistAtZeroFromTheStart(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	if got := testutil.CollectAndCount(m.events); got != 4 {
		t.Errorf("séries pulse_aggregator_events_total = %d, want 4 (counted, duplicate, replayed, skipped)", got)
	}
}

// startRunner lance un runner en arrière-plan ; stop l'arrête et retourne son erreur.
func startRunner(t *testing.T, runner *Runner) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("Run ne s'est pas arrêté après l'annulation")
			return nil
		}
	}
}

// Ancienne limite connue, désormais corrigée : un crash APRÈS l'écriture des compteurs mais AVANT la validation
// des offsets faisait recompter le lot à la relecture. Les positions, écrites dans la même transaction que les
// compteurs, couvrent maintenant le lot rejoué : il n'ajoute rien.
func TestCrashAfterWritingDoesNotCountTheBatchTwice(t *testing.T) {
	msgs := []kafka.Message{
		msgFor(t, 0, human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true)),
		msgFor(t, 1, human("e2", "site-42", event.TypeClick, "desktop", "chrome", time.Second, false)),
	}
	calls := &callLog{}
	store := &fakeStore{log: calls} // le MongoDB partagé par les deux "vies" du programme
	memory := dedupe.NewMemory()    // le Redis partagé
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.BatchSize = 2 // les deux messages dans le même lot

	// Première vie : écrit les compteurs, puis "meurt" avant de valider les offsets.
	first := &fakeSource{pending: append([]kafka.Message(nil), msgs...), calls: calls, failCommit: true}
	if err := NewRunner(first, store, memory, cfg, log).Run(context.Background()); err == nil {
		t.Fatal("la première vie aurait dû s'arrêter sur l'échec de la validation")
	}
	if pv, clicks, _, _ := store.totals(); pv != 1 || clicks != 1 {
		t.Fatalf("après la première vie : pv %d clics %d, want 1 1", pv, clicks)
	}

	// Seconde vie : Kafka redonne le même lot (rien n'a été validé).
	second := &fakeSource{pending: append([]kafka.Message(nil), msgs...), calls: calls}
	m := NewMetrics(prometheus.NewRegistry())
	cfg.Metrics = m
	stop := startRunner(t, NewRunner(second, store, memory, cfg, log))
	waitFor(t, "le lot relu est validé", func() bool { return second.committed() == 2 })
	if err := stop(); err != nil {
		t.Fatalf("seconde vie : Run() error = %v", err)
	}

	if pv, clicks, _, _ := store.totals(); pv != 1 || clicks != 1 {
		t.Errorf("pv %d clics %d, want 1 1 : le lot rejoué a été compté une seconde fois", pv, clicks)
	}
	if got := testutil.ToFloat64(m.events.WithLabelValues("replayed")); got != 2 {
		t.Errorf("issue replayed = %v, want 2 (les deux messages étaient déjà couverts par la position)", got)
	}
}

// La course mesurée pendant les tests de panne : pendant un rééquilibrage, deux instances traitent les mêmes
// messages en même temps. Toutes deux se reconnaissent propriétaires (même message) ; l'écriture atomique avec
// les positions n'en laisse compter qu'une.
func TestTwoInstancesOnTheSameMessagesCountThemOnce(t *testing.T) {
	var msgs []kafka.Message
	for i := 0; i < 40; i++ {
		e := human(fmt.Sprintf("e%02d", i), "site-42", event.TypePageview, "desktop", "chrome", time.Duration(i)*time.Second, i == 0)
		msgs = append(msgs, msgFor(t, int64(i), e))
	}
	calls := &callLog{}
	store := &fakeStore{log: calls}
	memory := dedupe.NewMemory()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := testConfig()
	cfg.BatchSize = 7 // des lots qui ne s'alignent pas : les deux instances se chevauchent de façon variée

	a := &fakeSource{pending: append([]kafka.Message(nil), msgs...), calls: calls}
	b := &fakeSource{pending: append([]kafka.Message(nil), msgs...), calls: calls}
	stopA := startRunner(t, NewRunner(a, store, memory, cfg, log))
	stopB := startRunner(t, NewRunner(b, store, memory, cfg, log))
	waitFor(t, "les deux instances ont tout validé", func() bool { return a.committed() == 40 && b.committed() == 40 })
	if err := stopA(); err != nil {
		t.Fatal(err)
	}
	if err := stopB(); err != nil {
		t.Fatal(err)
	}

	if pv, _, _, sessions := store.totals(); pv != 40 || sessions != 1 {
		t.Errorf("pageviews = %d, sessions = %d, want 40 et 1 : des messages ont été comptés par les deux instances", pv, sessions)
	}
}
