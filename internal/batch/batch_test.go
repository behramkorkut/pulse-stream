package batch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeSource délivre des messages puis attend l'annulation, comme un topic sans nouveau message.
type fakeSource struct {
	mu      sync.Mutex
	pending []kafka.Message
	commits []kafka.Message
	log     *[]string
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
	if s.log != nil {
		*s.log = append(*s.log, "commit")
	}
	return nil
}

func (s *fakeSource) committed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commits)
}

func messages(n int) []kafka.Message {
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		msgs[i] = kafka.Message{Offset: int64(i)}
	}
	return msgs
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

func TestRunCommitsOnlyAfterTheHandlerSucceeds(t *testing.T) {
	var order []string
	src := &fakeSource{pending: messages(5), log: &order}

	var mu sync.Mutex
	handled := 0
	handler := func(_ context.Context, lot []kafka.Message) error {
		mu.Lock()
		defer mu.Unlock()
		handled += len(lot)
		order = append(order, "handle")
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, src, Config{Size: 10, Wait: 20 * time.Millisecond}, handler) }()

	waitFor(t, "5 offsets validés", func() bool { return src.committed() == 5 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v, want nil après un arrêt demandé", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if handled != 5 {
		t.Errorf("%d messages traités, want 5", handled)
	}
	if len(order) < 2 || order[0] != "handle" || order[1] != "commit" {
		t.Errorf("ordre = %v, want handle puis commit", order)
	}
}

func TestRunDoesNotCommitWhenTheHandlerFails(t *testing.T) {
	src := &fakeSource{pending: messages(3)}
	boom := errors.New("traitement impossible")

	err := Run(context.Background(), src, Config{Wait: 10 * time.Millisecond},
		func(context.Context, []kafka.Message) error { return boom })

	if !errors.Is(err, boom) {
		t.Fatalf("Run() error = %v, want %v", err, boom)
	}
	if n := src.committed(); n != 0 {
		t.Errorf("%d offsets validés malgré l'échec : des messages seraient perdus", n)
	}
}

func TestCollect(t *testing.T) {
	fill := func(n int, closed bool) chan kafka.Message {
		ch := make(chan kafka.Message, n+1)
		for i := 0; i < n; i++ {
			ch <- kafka.Message{Offset: int64(i)}
		}
		if closed {
			close(ch)
		}
		return ch
	}

	t.Run("s'arrête à la taille maximale", func(t *testing.T) {
		lot, more := collect(fill(10, false), 4, time.Second)
		if len(lot) != 4 || !more {
			t.Errorf("len=%d more=%v, want 4 true", len(lot), more)
		}
	})

	t.Run("s'arrête à l'échéance", func(t *testing.T) {
		start := time.Now()
		lot, more := collect(fill(2, false), 10, 20*time.Millisecond)
		if len(lot) != 2 || !more {
			t.Errorf("len=%d more=%v, want 2 true", len(lot), more)
		}
		if time.Since(start) > 2*time.Second {
			t.Error("l'échéance n'a pas été respectée")
		}
	})

	t.Run("canal fermé après quelques messages", func(t *testing.T) {
		lot, more := collect(fill(1, true), 10, time.Second)
		if len(lot) != 1 || more {
			t.Errorf("len=%d more=%v, want 1 false", len(lot), more)
		}
	})

	t.Run("canal fermé et vide", func(t *testing.T) {
		lot, more := collect(fill(0, true), 10, time.Second)
		if len(lot) != 0 || more {
			t.Errorf("len=%d more=%v, want 0 false", len(lot), more)
		}
	})
}

func TestRetryPolicy(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}

	t.Run("réussit après des échecs passagers", func(t *testing.T) {
		calls := 0
		err := policy.Do(context.Background(), quiet, "test", func() error {
			calls++
			if calls < 3 {
				return errors.New("passager")
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Errorf("err=%v calls=%d, want nil 3", err, calls)
		}
	})

	t.Run("abandonne après MaxAttempts", func(t *testing.T) {
		boom := errors.New("permanent")
		calls := 0
		err := policy.Do(context.Background(), quiet, "test", func() error { calls++; return boom })
		if !errors.Is(err, boom) || calls != 3 {
			t.Errorf("err=%v calls=%d, want l'erreur d'origine après 3 essais", err, calls)
		}
	})

	t.Run("s'interrompt si le contexte est annulé", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		slow := RetryPolicy{MaxAttempts: 5, Backoff: time.Hour}
		calls := 0
		err := slow.Do(ctx, quiet, "test", func() error {
			calls++
			cancel() // annulation pendant l'attente avant le 2e essai
			return errors.New("échec")
		})
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Errorf("err=%v calls=%d, want context.Canceled après 1 essai", err, calls)
		}
	})
}

func TestRunRecordsMetricsForSuccessfulBatches(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry(), "test")
	src := &fakeSource{pending: messages(5)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, src, Config{Size: 10, Wait: 20 * time.Millisecond, Metrics: m},
			func(context.Context, []kafka.Message) error { return nil })
	}()

	waitFor(t, "5 offsets validés", func() bool { return src.committed() == 5 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := testutil.ToFloat64(m.messages); got != 5 {
		t.Errorf("pulse_batch_messages_total = %v, want 5", got)
	}
	if got := testutil.ToFloat64(m.batches.WithLabelValues("ok")); got < 1 {
		t.Errorf("lots ok = %v, want au moins 1", got)
	}
	if got := testutil.ToFloat64(m.batches.WithLabelValues("error")); got != 0 {
		t.Errorf("lots en erreur = %v, want 0", got)
	}
}

// Un lot qui échoue ne doit PAS être compté comme des messages traités : sinon le débit affiché
// dépasserait ce qui est réellement validé.
func TestRunRecordsMetricsForFailedBatches(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry(), "test")
	src := &fakeSource{pending: messages(3)}

	err := Run(context.Background(), src, Config{Wait: 10 * time.Millisecond, Metrics: m},
		func(context.Context, []kafka.Message) error { return errors.New("boom") })
	if err == nil {
		t.Fatal("Run() a réussi alors que le traitement échoue")
	}

	if got := testutil.ToFloat64(m.batches.WithLabelValues("error")); got != 1 {
		t.Errorf("lots en erreur = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.messages); got != 0 {
		t.Errorf("pulse_batch_messages_total = %v, want 0 (le lot n'a pas été traité)", got)
	}
}

func TestNilMetricsAreHarmless(t *testing.T) {
	var m *Metrics
	m.observe(10, time.Second, nil) // ne doit pas paniquer
}
