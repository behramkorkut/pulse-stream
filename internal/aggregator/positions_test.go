package aggregator

import (
	"reflect"
	"testing"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

func TestNotYetAppliedKeepsOnlyOffsetsBeyondThePosition(t *testing.T) {
	p0 := Partition{Topic: "enriched-events", ID: 0}
	p1 := Partition{Topic: "enriched-events", ID: 1}
	ev := func(id string) event.Enriched {
		return human(id, "site-42", event.TypePageview, "desktop", "chrome", time.Second, false)
	}

	events := []Counted{
		{Event: ev("a"), From: p0, Offset: 9},  // couvert : la position de p0 est 10
		{Event: ev("b"), From: p0, Offset: 10}, // couvert (égal)
		{Event: ev("c"), From: p0, Offset: 11}, // nouveau
		{Event: ev("d"), From: p1, Offset: 0},  // p1 n'a encore rien écrit
	}
	got := NotYetApplied(events, map[Partition]int64{p0: 10})

	var ids []string
	for _, e := range got {
		ids = append(ids, e.ID)
	}
	if !reflect.DeepEqual(ids, []string{"c", "d"}) {
		t.Errorf("NotYetApplied = %v, want [c d]", ids)
	}
}

func TestAdvancedNeverMovesAPositionBack(t *testing.T) {
	p0 := Partition{Topic: "t", ID: 0}
	p1 := Partition{Topic: "t", ID: 1}
	p2 := Partition{Topic: "t", ID: 2}

	got := Advanced(map[Partition]int64{p0: 20, p1: 5, p2: 3}, map[Partition]int64{p0: 10, p1: 8})
	want := map[Partition]int64{p0: 20, p2: 3} // p1 est déjà plus loin (8 > 5) : sa position ne recule pas
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Advanced = %v, want %v", got, want)
	}
}
