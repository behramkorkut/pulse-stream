package kafkautil

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestPartitionLag(t *testing.T) {
	tests := []struct {
		name string
		p    PartitionOffsets
		want int64
	}{
		{"en retard", PartitionOffsets{First: 0, End: 100, Committed: 40}, 60},
		{"à jour", PartitionOffsets{First: 0, End: 100, Committed: 100}, 0},
		{"rien de validé : tout compte", PartitionOffsets{First: 0, End: 100, Committed: -1}, 100},
		{"rien de validé, début purgé", PartitionOffsets{First: 30, End: 100, Committed: -1}, 70},
		{"validé avant le début purgé", PartitionOffsets{First: 30, End: 100, Committed: 10}, 70},
		{"jamais négatif", PartitionOffsets{First: 0, End: 100, Committed: 150}, 0},
		{"partition vide", PartitionOffsets{First: 0, End: 0, Committed: -1}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Lag(); got != tt.want {
				t.Errorf("Lag() = %d, want %d", got, tt.want)
			}
		})
	}
}

// Le retard d'un groupe est la SOMME des partitions, pas celui d'une seule : c'est l'erreur qu'on corrige ici.
func TestTotalLagSumsAllPartitions(t *testing.T) {
	parts := []PartitionOffsets{
		{Partition: 0, End: 100, Committed: 50},
		{Partition: 1, End: 100, Committed: 0},
		{Partition: 2, End: 10, Committed: 10},
	}
	if got := TotalLag(parts); got != 150 {
		t.Errorf("TotalLag = %d, want 150", got)
	}
	if got := TotalLag(nil); got != 0 {
		t.Errorf("TotalLag(nil) = %d, want 0", got)
	}
}

// fakeSource renvoie des résultats programmés, un par appel (le dernier se répète).
type fakeSource struct {
	mu    sync.Mutex
	steps []fakeStep
	calls int
}

type fakeStep struct {
	parts []PartitionOffsets
	err   error
}

func (f *fakeSource) Offsets(context.Context) ([]PartitionOffsets, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.calls
	if i >= len(f.steps) {
		i = len(f.steps) - 1
	}
	f.calls++
	return f.steps[i].parts, f.steps[i].err
}

func (f *fakeSource) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func quietLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func TestTrackerRefreshStoresTheTotal(t *testing.T) {
	var tr LagTracker
	if tr.Value() != 0 {
		t.Fatalf("Value() avant toute mesure = %d, want 0", tr.Value())
	}
	src := &fakeSource{steps: []fakeStep{{parts: []PartitionOffsets{{End: 30, Committed: 0}, {End: 20, Committed: 10}}}}}
	if err := tr.Refresh(context.Background(), src); err != nil {
		t.Fatal(err)
	}
	if got := tr.Value(); got != 40 {
		t.Errorf("Value() = %d, want 40", got)
	}
}

func TestTrackerRefreshKeepsLastValueOnError(t *testing.T) {
	var tr LagTracker
	good := &fakeSource{steps: []fakeStep{{parts: []PartitionOffsets{{End: 7}}}}}
	if err := tr.Refresh(context.Background(), good); err != nil {
		t.Fatal(err)
	}
	bad := &fakeSource{steps: []fakeStep{{err: errors.New("broker en panne")}}}
	if err := tr.Refresh(context.Background(), bad); err == nil {
		t.Fatal("Refresh sans erreur, want erreur")
	}
	if got := tr.Value(); got != 7 {
		t.Errorf("Value() = %d, want 7 (dernière mesure réussie conservée)", got)
	}
}

func TestTrackerRunMeasuresRepeatedlyAndStopsOnCancel(t *testing.T) {
	var tr LagTracker
	src := &fakeSource{steps: []fakeStep{
		{parts: []PartitionOffsets{{End: 10}}},
		{err: errors.New("hoquet")},
		{parts: []PartitionOffsets{{End: 25}}},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		tr.Run(ctx, src, 10*time.Millisecond, quietLogger())
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for tr.Value() != 25 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := tr.Value(); got != 25 {
		t.Fatalf("Value() = %d, want 25 après une panne passagère", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run ne s'arrête pas quand le contexte est annulé")
	}
}
