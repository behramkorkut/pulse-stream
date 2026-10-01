package processor

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

	"github.com/behramkorkut/pulse-stream/internal/event"
	"github.com/behramkorkut/pulse-stream/internal/sessions"
)

const (
	testEnriched = "enriched-events"
	testDead     = "dead-letter"
)

// callLog enregistre l'ordre des appels entre la source et la destination.
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

func (s *fakeSource) committed() []kafka.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]kafka.Message(nil), s.commits...)
}

// fakeSink enregistre les messages écrits ; failFirst échoue les N premiers appels, failAlways tous.
type fakeSink struct {
	mu         sync.Mutex
	written    []kafka.Message
	attempts   int
	failFirst  int
	failAlways bool
	calls      *callLog
}

func (s *fakeSink) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.failAlways || s.attempts <= s.failFirst {
		return errors.New("kafka indisponible")
	}
	s.written = append(s.written, msgs...)
	s.calls.add("write")
	return nil
}

func (s *fakeSink) messages() []kafka.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]kafka.Message(nil), s.written...)
}

func (s *fakeSink) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func testConfig() Config {
	return Config{
		EnrichedTopic:   testEnriched,
		DeadLetterTopic: testDead,
		Workers:         4,
		BatchSize:       50,
		BatchWait:       20 * time.Millisecond,
		MaxAttempts:     3,
		RetryBackoff:    time.Millisecond,
	}
}

func newTestRunner(msgs []kafka.Message, cfg Config) (*Runner, *fakeSource, *fakeSink) {
	calls := &callLog{}
	src := &fakeSource{pending: msgs, calls: calls}
	dst := &fakeSink{calls: calls}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return newRunner(src, dst, cfg, log, func() time.Time { return testNow }), src, dst
}

func kmsg(offset int64, key string, value []byte) kafka.Message {
	return kafka.Message{Topic: "raw-events", Partition: 0, Offset: offset, Key: []byte(key), Value: value}
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

// startRunner lance Run dans une goroutine ; stop annule le contexte et retourne l'erreur de Run.
func startRunner(t *testing.T, r *Runner) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

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

func TestRunnerRoutesWritesThenCommits(t *testing.T) {
	msgs := []kafka.Message{
		kmsg(0, "site-42/v-1", rawEvent("evt-0", chromeUA)),
		kmsg(1, "site-42/v-2", rawEvent("evt-1", "Googlebot/2.1")),
		kmsg(2, "site-42/v-3", []byte(`{oops`)),
		kmsg(3, "site-42/v-1", rawEvent("evt-3", chromeUA)),
		kmsg(4, "site-42/v-4", []byte(`{"id":"incomplet"}`)),
	}
	r, src, dst := newTestRunner(msgs, testConfig())
	stop := startRunner(t, r)

	waitFor(t, "les 5 offsets validés", func() bool { return len(src.committed()) == 5 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v, want nil après un arrêt demandé", err)
	}

	// Routage : l'ordre de sortie suit l'ordre d'entrée, chaque message vers le bon topic.
	wantTopics := []string{testEnriched, testEnriched, testDead, testEnriched, testDead}
	written := dst.messages()
	if len(written) != len(wantTopics) {
		t.Fatalf("%d messages écrits, want %d", len(written), len(wantTopics))
	}
	for i, want := range wantTopics {
		if written[i].Topic != want {
			t.Errorf("message %d : topic %q, want %q", i, written[i].Topic, want)
		}
	}

	// Le robot est conservé et marqué.
	var bot event.Enriched
	if err := json.Unmarshal(written[1].Value, &bot); err != nil || !bot.IsBot {
		t.Errorf("robot mal traité : err=%v event=%+v", err, bot)
	}

	// La clé de partitionnement est conservée : même visiteur, même partition en sortie.
	if string(written[0].Key) != "site-42/v-1" || string(written[3].Key) != "site-42/v-1" {
		t.Errorf("clés non conservées : %q, %q", written[0].Key, written[3].Key)
	}

	// Garantie centrale : l'écriture précède le commit.
	calls := src.calls.get()
	if len(calls) < 2 || calls[0] != "write" || calls[1] != "commit" {
		t.Errorf("ordre des appels = %v, want write puis commit", calls)
	}
}

func TestRunnerPreservesInputOrderAcrossWorkers(t *testing.T) {
	const total = 100
	msgs := make([]kafka.Message, total)
	for i := range msgs {
		key := fmt.Sprintf("site-42/v-%d", i%10)
		msgs[i] = kmsg(int64(i), key, rawEvent(fmt.Sprintf("evt-%03d", i), chromeUA))
	}
	cfg := testConfig()
	cfg.Workers = 8
	cfg.BatchSize = 30 // plusieurs lots
	r, src, dst := newTestRunner(msgs, cfg)
	stop := startRunner(t, r)

	waitFor(t, "les 100 offsets validés", func() bool { return len(src.committed()) == total })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	written := dst.messages()
	if len(written) != total {
		t.Fatalf("%d messages écrits, want %d", len(written), total)
	}
	for i, m := range written {
		var e event.Enriched
		if err := json.Unmarshal(m.Value, &e); err != nil {
			t.Fatalf("message %d illisible : %v", i, err)
		}
		if want := fmt.Sprintf("evt-%03d", i); e.ID != want {
			t.Fatalf("position %d : id %q, want %q (l'ordre a été perturbé)", i, e.ID, want)
		}
	}
}

func TestRunnerRetriesTransientWriteFailure(t *testing.T) {
	r, src, dst := newTestRunner([]kafka.Message{kmsg(0, "k", rawEvent("evt-0", chromeUA))}, testConfig())
	dst.failFirst = 2
	stop := startRunner(t, r)

	waitFor(t, "le commit après les nouveaux essais", func() bool { return len(src.committed()) == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := dst.attemptCount(); got != 3 {
		t.Errorf("%d tentatives d'écriture, want 3 (2 échecs puis 1 succès)", got)
	}
}

func TestRunnerDoesNotCommitWhenWriteKeepsFailing(t *testing.T) {
	r, src, dst := newTestRunner([]kafka.Message{kmsg(0, "k", rawEvent("evt-0", chromeUA))}, testConfig())
	dst.failAlways = true

	err := r.Run(context.Background())

	if err == nil {
		t.Fatal("Run() a réussi alors que l'écriture échoue toujours")
	}
	if got := dst.attemptCount(); got != 3 {
		t.Errorf("%d tentatives, want MaxAttempts = 3", got)
	}
	if n := len(src.committed()); n != 0 {
		t.Errorf("%d offsets validés malgré l'échec : des messages seraient perdus", n)
	}
}

func TestShardOf(t *testing.T) {
	const shards = 8
	seen := map[int]bool{}

	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("site-42/v-%d", i))
		s := shardOf(key, shards)
		if s < 0 || s >= shards {
			t.Fatalf("shardOf() = %d, hors de [0,%d[", s, shards)
		}
		if again := shardOf(key, shards); again != s {
			t.Fatalf("shardOf n'est pas stable : %d puis %d", s, again)
		}
		seen[s] = true
	}
	if len(seen) < shards {
		t.Errorf("seulement %d shards utilisés sur %d : mauvaise répartition", len(seen), shards)
	}
}

// flakyStore simule un Redis instable : les failFirst premiers appels échouent (tous si failFirst < 0).
type flakyStore struct {
	mu        sync.Mutex
	inner     sessions.Store
	failFirst int
	calls     int
}

func (f *flakyStore) Touch(ctx context.Context, e event.Event) (sessions.Session, error) {
	f.mu.Lock()
	f.calls++
	fail := f.failFirst < 0 || f.calls <= f.failFirst
	f.mu.Unlock()

	if fail {
		return sessions.Session{}, errors.New("redis indisponible")
	}
	return f.inner.Touch(ctx, e)
}

func decodeEnriched(t *testing.T, m kafka.Message) event.Enriched {
	t.Helper()
	var e event.Enriched
	if err := json.Unmarshal(m.Value, &e); err != nil {
		t.Fatalf("événement enrichi illisible : %v", err)
	}
	return e
}

func TestRunnerAssignsSessions(t *testing.T) {
	at := func(hh, mm int) time.Time { return time.Date(2026, 9, 30, hh, mm, 0, 0, time.UTC) }
	msgs := []kafka.Message{
		kmsg(0, "site-42/v-1", rawEventAt("a", chromeUA, at(11, 0))),
		kmsg(1, "site-42/v-1", rawEventAt("b", chromeUA, at(11, 10))),
		kmsg(2, "site-42/v-2", rawEventAt("c", "curl/8.7.1", at(11, 20))), // robot
		kmsg(3, "site-42/v-1", rawEventAt("d", chromeUA, at(11, 50))),     // 40 min d'inactivité
	}
	cfg := testConfig()
	cfg.Sessions = sessions.NewMemory(sessions.DefaultTimeout)
	r, src, dst := newTestRunner(msgs, cfg)
	stop := startRunner(t, r)

	waitFor(t, "les 4 offsets validés", func() bool { return len(src.committed()) == 4 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	got := dst.messages()
	a, b, c, d := decodeEnriched(t, got[0]), decodeEnriched(t, got[1]), decodeEnriched(t, got[2]), decodeEnriched(t, got[3])

	if a.SessionID == "" || !a.NewSession {
		t.Errorf("a = %+v, want une session neuve", a.SessionID)
	}
	if b.SessionID != a.SessionID || b.NewSession {
		t.Errorf("b : session %q new=%v, want la session de a (%q), non neuve", b.SessionID, b.NewSession, a.SessionID)
	}
	if c.SessionID != "" || c.NewSession || !c.IsBot {
		t.Errorf("c : un robot n'a pas de session, got %q new=%v bot=%v", c.SessionID, c.NewSession, c.IsBot)
	}
	if d.SessionID == a.SessionID || !d.NewSession {
		t.Errorf("d : session %q new=%v, want une nouvelle session après 40 min d'inactivité", d.SessionID, d.NewSession)
	}
}

// Ce test échouerait si deux événements d'un même visiteur étaient traités dans le désordre ou
// en même temps : un seul événement doit ouvrir la session, tous les autres la rejoindre.
func TestRunnerKeepsVisitorEventsOrderedForSessions(t *testing.T) {
	const perVisitor = 40
	var msgs []kafka.Message
	for i := 0; i < perVisitor; i++ {
		for _, visitor := range []string{"v-1", "v-2", "v-3", "v-4", "v-5"} {
			ts := time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute)
			id := fmt.Sprintf("%s-%02d", visitor, i)
			msgs = append(msgs, kmsg(int64(len(msgs)), "site-42/"+visitor, rawEventFor(id, visitor, chromeUA, ts)))
		}
	}
	cfg := testConfig()
	cfg.Workers = 8
	cfg.BatchSize = len(msgs) // un seul gros lot : le pire cas pour le parallélisme
	cfg.Sessions = sessions.NewMemory(sessions.DefaultTimeout)
	r, src, dst := newTestRunner(msgs, cfg)
	stop := startRunner(t, r)

	waitFor(t, "tous les offsets validés", func() bool { return len(src.committed()) == len(msgs) })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	newSessions := map[string]int{}
	sessionOf := map[string]string{}
	for _, m := range dst.messages() {
		e := decodeEnriched(t, m)
		if e.NewSession {
			newSessions[e.VisitorID]++
		}
		if prev, ok := sessionOf[e.VisitorID]; ok && prev != e.SessionID {
			t.Fatalf("visiteur %s : plusieurs sessions (%s et %s) alors que les événements sont espacés de 1 min",
				e.VisitorID, prev, e.SessionID)
		}
		sessionOf[e.VisitorID] = e.SessionID
	}
	for visitor, n := range newSessions {
		if n != 1 {
			t.Errorf("visiteur %s : %d événements ouvrent une session, want 1", visitor, n)
		}
	}
	if len(newSessions) != 5 {
		t.Errorf("%d visiteurs avec une session, want 5", len(newSessions))
	}
}

func TestRunnerRetriesWhenSessionStoreIsFlaky(t *testing.T) {
	cfg := testConfig()
	store := &flakyStore{inner: sessions.NewMemory(sessions.DefaultTimeout), failFirst: 2}
	cfg.Sessions = store
	r, src, dst := newTestRunner([]kafka.Message{kmsg(0, "site-42/v-1", rawEvent("evt-0", chromeUA))}, cfg)
	stop := startRunner(t, r)

	waitFor(t, "le commit après les nouveaux essais", func() bool { return len(src.committed()) == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	got := decodeEnriched(t, dst.messages()[0])
	if got.SessionID == "" || !got.NewSession {
		t.Errorf("session = %q new=%v, want une session neuve malgré les échecs", got.SessionID, got.NewSession)
	}
}

func TestRunnerWritesAndCommitsNothingWhenSessionStoreIsDown(t *testing.T) {
	cfg := testConfig()
	cfg.Sessions = &flakyStore{inner: sessions.NewMemory(sessions.DefaultTimeout), failFirst: -1}
	r, src, dst := newTestRunner([]kafka.Message{kmsg(0, "site-42/v-1", rawEvent("evt-0", chromeUA))}, cfg)

	if err := r.Run(context.Background()); err == nil {
		t.Fatal("Run() a réussi alors que le store de sessions est indisponible")
	}
	if n := len(dst.messages()); n != 0 {
		t.Errorf("%d messages écrits sans session : un événement sans session serait publié à tort", n)
	}
	if n := len(src.committed()); n != 0 {
		t.Errorf("%d offsets validés : les messages seraient perdus", n)
	}
}

func TestRunnerCountsWhatItWritesByOutcomeAndReason(t *testing.T) {
	msgs := []kafka.Message{
		kmsg(0, "site-42/v-1", rawEvent("evt-0", chromeUA)),
		kmsg(1, "site-42/v-2", rawEvent("evt-1", "Googlebot/2.1")),
		kmsg(2, "site-42/v-3", []byte(`{oops`)),
		kmsg(3, "site-42/v-4", []byte(`{"id":"incomplet"}`)),
		kmsg(4, "site-42/v-5", []byte(`{oops`)),
	}
	m := NewMetrics(prometheus.NewRegistry())
	cfg := testConfig()
	cfg.Metrics = m
	r, src, _ := newTestRunner(msgs, cfg)
	stop := startRunner(t, r)

	waitFor(t, "les 5 offsets validés", func() bool { return len(src.committed()) == 5 })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"enrichis (humain + robot)", testutil.ToFloat64(m.events.WithLabelValues("enriched")), 2},
		{"dead-letter", testutil.ToFloat64(m.events.WithLabelValues("dead_letter")), 3},
		{"raison invalid_json", testutil.ToFloat64(m.deadLetters.WithLabelValues(ReasonInvalidJSON)), 2},
		{"raison invalid_event", testutil.ToFloat64(m.deadLetters.WithLabelValues(ReasonInvalidEvent)), 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// Si l'écriture échoue, rien n'est publié : les compteurs ne doivent pas bouger.
func TestRunnerDoesNotCountWhenTheWriteFails(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	cfg := testConfig()
	cfg.Metrics = m
	r, _, dst := newTestRunner([]kafka.Message{kmsg(0, "k", rawEvent("evt-0", chromeUA))}, cfg)
	dst.failAlways = true

	if err := r.Run(context.Background()); err == nil {
		t.Fatal("Run() a réussi alors que l'écriture échoue")
	}
	if got := testutil.ToFloat64(m.events.WithLabelValues("enriched")); got != 0 {
		t.Errorf("enrichis = %v, want 0 : rien n'a été publié", got)
	}
}
