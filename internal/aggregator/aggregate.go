// Package aggregator consomme les événements enrichis, écarte les doublons et cumule des compteurs
// par site et par minute dans MongoDB.
package aggregator

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// Bucket regroupe les compteurs d'un site pour une minute (instant de l'ÉVÉNEMENT, pas de son arrivée :
// un événement arrivé en retard retombe dans la bonne minute, le cumul par $inc s'en accommode).
type Bucket struct {
	SiteID string
	Minute time.Time // début de la minute, en UTC

	Pageviews int64
	Clicks    int64
	BotEvents int64 // événements de robots : comptés à part, jamais dans les autres compteurs
	Sessions  int64 // nombre de sessions ouvertes pendant cette minute

	Devices  map[string]int64 // répartition des humains par type d'appareil
	Browsers map[string]int64 // répartition des humains par navigateur
}

// ID est l'identifiant du document MongoDB : un document par site et par minute.
func (b Bucket) ID() string {
	return b.SiteID + "|" + b.Minute.Format("2006-01-02T15:04Z")
}

// Les clés des répartitions deviennent des noms de champs MongoDB (par exemple "devices.mobile").
// On n'accepte que des valeurs connues, sinon "other" : le topic est une frontière, et un nom contenant
// un point ou un "$" ne doit jamais atteindre le moteur de requêtes.
var (
	knownDevices  = map[string]bool{"desktop": true, "mobile": true, "tablet": true}
	knownBrowsers = map[string]bool{"chrome": true, "firefox": true, "safari": true, "edge": true, "opera": true}
)

func oneOf(value string, known map[string]bool) string {
	if known[value] {
		return value
	}
	return "other"
}

// Aggregate cumule des événements en compteurs par site et par minute. C'est une fonction pure.
// Le résultat est trié par identifiant : même entrée, même sortie, ce qui simplifie les tests.
func Aggregate(events []event.Enriched) []Bucket {
	byID := map[string]*Bucket{}

	for _, e := range events {
		minute := e.Timestamp.UTC().Truncate(time.Minute)
		probe := Bucket{SiteID: e.SiteID, Minute: minute}

		b, ok := byID[probe.ID()]
		if !ok {
			probe.Devices = map[string]int64{}
			probe.Browsers = map[string]int64{}
			b = &probe
			byID[probe.ID()] = b
		}

		if e.IsBot {
			b.BotEvents++
			continue
		}

		switch e.Type {
		case event.TypePageview:
			b.Pageviews++
		case event.TypeClick:
			b.Clicks++
		}
		if e.NewSession {
			b.Sessions++
		}
		b.Devices[oneOf(e.Device, knownDevices)]++
		b.Browsers[oneOf(e.Browser, knownBrowsers)]++
	}

	buckets := make([]Bucket, 0, len(byID))
	for _, b := range byID {
		buckets = append(buckets, *b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].ID() < buckets[j].ID() })
	return buckets
}

// Decode lit un message du topic enriched-events. Un message sans identifiant, sans site ou sans
// horodatage est refusé : sans eux, il ne pourrait ni être dédoublonné ni être compté au bon endroit.
func Decode(raw []byte) (event.Enriched, error) {
	var e event.Enriched
	if err := json.Unmarshal(raw, &e); err != nil {
		return event.Enriched{}, err
	}
	if e.ID == "" || e.SiteID == "" || e.Timestamp.IsZero() {
		return event.Enriched{}, errors.New("missing id, site_id or timestamp")
	}
	return e, nil
}
