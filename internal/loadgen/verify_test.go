package loadgen

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// scriptedSource rejoue une suite de lectures, puis répète la dernière.
type scriptedSource struct {
	mu     sync.Mutex
	script []Totals
	calls  int
	err    error
}

func (s *scriptedSource) Totals(context.Context, string) (Totals, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return Totals{}, s.err
	}
	i := s.calls
	if i >= len(s.script) {
		i = len(s.script) - 1
	}
	s.calls++
	return s.script[i], nil
}

var fast = VerifyOptions{Poll: 5 * time.Millisecond, Stable: 60 * time.Millisecond, Timeout: 2 * time.Second}

func TestVerifySucceedsOnceTheDatabaseCatchesUp(t *testing.T) {
	want := Totals{Pageviews: 10, Clicks: 4, Bots: 2}
	src := &scriptedSource{script: []Totals{{}, {Pageviews: 3}, {Pageviews: 8, Clicks: 2}, want}}

	res, err := Verify(context.Background(), src, "load-1-", want, fast)
	if err != nil || !res.OK {
		t.Fatalf("Verify() = %+v, %v, want OK après le rattrapage", res, err)
	}
}

// Un compteur au-dessus de l'attendu est définitif (les compteurs ne font que croître) : échec immédiat.
func TestVerifyFailsImmediatelyOnDoubleCounting(t *testing.T) {
	want := Totals{Pageviews: 10}
	src := &scriptedSource{script: []Totals{{Pageviews: 5}, {Pageviews: 11}}}

	start := time.Now()
	res, err := Verify(context.Background(), src, "p", want, VerifyOptions{Poll: 5 * time.Millisecond, Stable: time.Hour, Timeout: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if res.OK || res.Got.Pageviews != 11 {
		t.Errorf("Verify() = %+v, want un échec avec 11 pageviews", res)
	}
	if time.Since(start) > time.Second {
		t.Errorf("l'échec a pris %v : un dépassement est définitif, inutile d'attendre", time.Since(start))
	}
}

func TestVerifyFailsWhenTheDatabaseStopsBelowTheExpectedTotals(t *testing.T) {
	want := Totals{Pageviews: 10, Clicks: 4}
	src := &scriptedSource{script: []Totals{{Pageviews: 9, Clicks: 4}}} // un pageview manque, et plus rien ne bouge

	res, err := Verify(context.Background(), src, "p", want, fast)
	if err != nil {
		t.Fatal(err)
	}
	if res.OK {
		t.Fatalf("Verify() a réussi alors qu'il manque un pageview : %+v", res)
	}
	if res.Reason == "" {
		t.Error("la raison de l'échec est vide")
	}
}

func TestVerifyKeepsWaitingWhileTheDatabaseKeepsProgressing(t *testing.T) {
	want := Totals{Pageviews: 30}
	script := make([]Totals, 0, 31)
	for i := 0; i <= 30; i++ {
		script = append(script, Totals{Pageviews: int64(i)})
	}
	// Chaque lecture progresse : la durée "stable" (60 ms) n'est jamais atteinte, malgré 30 lectures de 5 ms.
	res, err := Verify(context.Background(), &scriptedSource{script: script}, "p", want, fast)
	if err != nil || !res.OK {
		t.Fatalf("Verify() = %+v, %v, want OK : la base progressait", res, err)
	}
}

func TestVerifyTimesOut(t *testing.T) {
	src := &scriptedSource{script: func() []Totals {
		s := make([]Totals, 10_000)
		for i := range s {
			s[i] = Totals{Pageviews: int64(i)} // progresse toujours, sans jamais atteindre l'attendu
		}
		return s
	}()}
	res, err := Verify(context.Background(), src, "p", Totals{Pageviews: 1_000_000},
		VerifyOptions{Poll: 5 * time.Millisecond, Stable: time.Hour, Timeout: 100 * time.Millisecond})
	if err != nil || res.OK || res.Reason == "" {
		t.Fatalf("Verify() = %+v, %v, want un échec par délai dépassé", res, err)
	}
}

func TestVerifyPropagatesSourceErrors(t *testing.T) {
	_, err := Verify(context.Background(), &scriptedSource{err: errors.New("mongo injoignable")}, "p", Totals{}, fast)
	if err == nil {
		t.Error("Verify() a réussi alors que la source est en erreur")
	}
}

func TestVerifySucceedsImmediatelyWhenNothingWasExpected(t *testing.T) {
	res, err := Verify(context.Background(), &scriptedSource{script: []Totals{{}}}, "p", Totals{}, fast)
	if err != nil || !res.OK {
		t.Fatalf("Verify() = %+v, %v, want OK", res, err)
	}
}
