package aggregator

import (
	"context"
	"encoding/json"
	"errors"
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
	mu      sync.Mutex
	pending []kafka.Message
	commits []kafka.Message
	calls   *callLog
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
	s.commits = append(s.commits, msgs...)
	s.calls.add("commit")
	return nil
}

func (s *fakeSource) committed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commits)
}

// fakeStore enregistre les buckets reçus. failFirst échoue les N premiers appels (tous si < 0).
type fakeStore struct {
	mu        sync.Mutex
	applied   [][]Bucket
	calls     int
	failFirst int
	log       *callLog
}

func (s *fakeStore) Apply(_ context.Context, buckets []Bucket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failFirst < 0 || s.calls <= s.failFirst {
		return errors.New("mongo indisponible")
	}
	s.applied = append(s.applied, buckets)
	s.log.add("apply")
	return nil
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

// recordingSeen décore une mémoire des doublons pour tracer les marquages et simuler des pannes.
type recordingSeen struct {
	dedupe.Store
	log     *callLog
	failSee bool
	marks   int
}

func (r *recordingSeen) Seen(ctx context.Context, ids []string) ([]bool, error) {
	if r.failSee {
		return nil, errors.New("redis indisponible")
	}
	return r.Store.Seen(ctx, ids)
}

func (r *recordingSeen) Mark(ctx context.Context, ids []string) error {
	r.marks++
	r.log.add("mark")
	return r.Store.Mark(ctx, ids)
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

// Garantie centrale : compter, PUIS mémoriser les identifiants, PUIS valider les offsets.
func TestRunnerAppliesThenMarksThenCommits(t *testing.T) {
	r := newRig([]kafka.Message{msgFor(t, 0, human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true))}, testConfig())
	stop := r.start(t)

	waitFor(t, "le commit", func() bool { return r.src.committed() == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	got := r.calls.get()
	want := []string{"apply", "mark", "commit"}
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

// Si les compteurs ne sont pas écrits, on ne doit ni mémoriser les identifiants (sinon la relecture
// les ignorerait : événements perdus) ni valider les offsets.
func TestRunnerDoesNotMarkNorCommitWhenTheStoreKeepsFailing(t *testing.T) {
	r := newRig([]kafka.Message{msgFor(t, 0, human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true))}, testConfig())
	r.store.failFirst = -1

	// Le délai évite qu'un Run() fautif (qui masquerait l'erreur) fasse attendre le test indéfiniment.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.runner.Run(ctx); err == nil || ctx.Err() != nil {
		t.Fatalf("Run() = %v, want une erreur remontée avant le délai (le magasin est indisponible)", err)
	}
	if r.seen.marks != 0 {
		t.Errorf("%d marquages : les identifiants ne doivent pas être mémorisés si rien n'a été écrit", r.seen.marks)
	}
	if n := r.src.committed(); n != 0 {
		t.Errorf("%d offsets validés : les messages seraient perdus", n)
	}
}

func TestRunnerDoesNotCountWhenTheSeenCheckFails(t *testing.T) {
	r := newRig([]kafka.Message{msgFor(t, 0, human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true))}, testConfig())
	r.seen.failSee = true

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

	for outcome, want := range map[string]float64{"counted": 2, "duplicate": 1, "skipped": 1} {
		if got := testutil.ToFloat64(m.events.WithLabelValues(outcome)); got != want {
			t.Errorf("issue %q = %v, want %v", outcome, got, want)
		}
	}
	if got := testutil.ToFloat64(m.buckets); got != 1 {
		t.Errorf("documents écrits = %v, want 1 (une seule minute, un seul site)", got)
	}
}
