// Package audit compare, pour une charge du générateur, ce qui est passé dans enriched-events à ce que MongoDB a
// compté. Il dit de quel côté vient un écart (en amont de l'aggregator, ou dans l'aggregator) et, pour un double
// comptage, dans quels documents (site, minute) chercher.
package audit

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/aggregator"
	"github.com/behramkorkut/pulse-stream/internal/event"
	"github.com/behramkorkut/pulse-stream/internal/loadgen"
)

// Copy est la position d'un message dans enriched-events.
type Copy struct {
	Partition int
	Offset    int64
}

func (c Copy) String() string { return fmt.Sprintf("%d:%d", c.Partition, c.Offset) }

// Event est un événement (site, identifiant) du topic, avec toutes ses copies.
type Event struct {
	Enriched event.Enriched
	Copies   []Copy
}

// Partitions compte les partitions distinctes où l'événement apparaît (1 attendu : même clé, même partition).
func (e *Event) Partitions() int {
	seen := map[int]bool{}
	for _, c := range e.Copies {
		seen[c.Partition] = true
	}
	return len(seen)
}

// Index regroupe par événement les messages d'une charge.
type Index struct {
	prefix   string
	Messages int // messages de la charge lus dans le topic, copies comprises
	events   map[string]*Event
	order    []string
}

// NewIndex crée un index pour les sites qui commencent par prefix (ceux d'une charge du générateur).
func NewIndex(prefix string) *Index {
	return &Index{prefix: prefix, events: map[string]*Event{}}
}

// AddLine lit une ligne "<partition> <offset> <json>", le format -f '%p %o %v\n' de rpk. Les messages d'autres
// charges sont ignorés.
func (ix *Index) AddLine(line string) error {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 {
		return fmt.Errorf("ligne illisible : %.80q", line)
	}
	partition, err := strconv.Atoi(parts[0])
	if err != nil {
		return fmt.Errorf("partition illisible : %.80q", line)
	}
	offset, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return fmt.Errorf("offset illisible : %.80q", line)
	}
	var e event.Enriched
	if err := json.Unmarshal([]byte(parts[2]), &e); err != nil {
		return fmt.Errorf("événement illisible à %d:%d : %w", partition, offset, err)
	}
	if !strings.HasPrefix(e.SiteID, ix.prefix) {
		return nil
	}

	ix.Messages++
	key := e.SiteID + "\x00" + e.ID
	ev, known := ix.events[key]
	if !known {
		ev = &Event{Enriched: e}
		ix.events[key] = ev
		ix.order = append(ix.order, key)
	}
	ev.Copies = append(ev.Copies, Copy{Partition: partition, Offset: offset})
	return nil
}

// Events retourne chaque événement une fois, dans l'ordre de première apparition.
func (ix *Index) Events() []*Event {
	out := make([]*Event, len(ix.order))
	for i, k := range ix.order {
		out[i] = ix.events[k]
	}
	return out
}

// Totals retourne ce qu'un aggregator exact écrirait (chaque événement compté une fois, mêmes règles que
// l'aggregator : robots à part, minute de l'événement), au total et par document (site, minute).
func (ix *Index) Totals() (loadgen.Totals, map[string]loadgen.Totals) {
	unique := make([]event.Enriched, 0, len(ix.order))
	for _, e := range ix.Events() {
		unique = append(unique, e.Enriched)
	}

	var total loadgen.Totals
	byBucket := map[string]loadgen.Totals{}
	for _, b := range aggregator.Aggregate(unique) {
		t := loadgen.Totals{Pageviews: b.Pageviews, Clicks: b.Clicks, Bots: b.BotEvents}
		byBucket[b.ID()] = t
		total.Pageviews += t.Pageviews
		total.Clicks += t.Clicks
		total.Bots += t.Bots
	}
	return total, byBucket
}

// BucketOf retourne l'identifiant du document (site, minute) où l'aggregator compte cet événement.
func BucketOf(e event.Enriched) string {
	return aggregator.Bucket{SiteID: e.SiteID, Minute: e.Timestamp.UTC().Truncate(time.Minute)}.ID()
}
