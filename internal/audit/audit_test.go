package audit

import (
	"fmt"
	"testing"

	"github.com/behramkorkut/pulse-stream/internal/loadgen"
)

func line(partition int, offset int64, id, site, typ string, bot bool) string {
	return fmt.Sprintf(`%d %d {"id":%q,"type":%q,"site_id":%q,"visitor_id":"v","url":"https://e.com/",`+
		`"timestamp":"2026-10-05T11:52:30Z","is_bot":%v}`, partition, offset, id, typ, site, bot)
}

func TestIndexCountsEachEventOnceAndKeepsItsCopies(t *testing.T) {
	ix := NewIndex("load-1-")
	for _, l := range []string{
		line(2, 10, "e1", "load-1-s0", "pageview", false),
		line(2, 11, "e2", "load-1-s0", "click", false),
		line(2, 15, "e1", "load-1-s0", "pageview", false), // copie de e1, plus loin dans la partition
		line(4, 3, "e3", "load-1-s1", "pageview", true),
		line(0, 1, "x", "load-2-s0", "pageview", false), // autre charge : ignoré
	} {
		if err := ix.AddLine(l); err != nil {
			t.Fatal(err)
		}
	}

	total, byBucket := ix.Totals()
	if want := (loadgen.Totals{Pageviews: 1, Clicks: 1, Bots: 1}); total != want {
		t.Errorf("Totals = %+v, want %+v", total, want)
	}
	if got := byBucket["load-1-s0|2026-10-05T11:52Z"]; got != (loadgen.Totals{Pageviews: 1, Clicks: 1}) {
		t.Errorf("document load-1-s0 = %+v, want pv 1 clics 1", got)
	}
	if ix.Messages != 4 {
		t.Errorf("Messages = %d, want 4 (copies comprises, autre charge exclue)", ix.Messages)
	}

	events := ix.Events()
	if len(events) != 3 || len(events[0].Copies) != 2 || events[0].Copies[1] != (Copy{Partition: 2, Offset: 15}) {
		t.Errorf("événements = %d, copies de e1 = %v", len(events), events[0].Copies)
	}
	if got := BucketOf(events[0].Enriched); got != "load-1-s0|2026-10-05T11:52Z" {
		t.Errorf("BucketOf = %q", got)
	}
}

func TestIndexRejectsUnreadableLines(t *testing.T) {
	ix := NewIndex("load-1-")
	for _, l := range []string{"", "2 x {}", "2 3 {oops"} {
		if err := ix.AddLine(l); err == nil {
			t.Errorf("AddLine(%q) : want une erreur", l)
		}
	}
}
