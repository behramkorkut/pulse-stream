// Package event définit le modèle d'un événement d'analytics et ses règles de validation.
package event

import (
	"fmt"
	"net/url"
	"time"
)

// Types d'événements acceptés.
const (
	TypePageview = "pageview"
	TypeClick    = "click"
)

const (
	maxFieldLen = 128
	maxURLLen   = 2048
)

// MaxFutureSkew est l'avance tolérée sur l'horloge du serveur. Au-delà, l'horloge du client est fausse (ou il
// triche) : l'événement est refusé.
const MaxFutureSkew = 5 * time.Minute

// MaxLateness est le retard maximal toléré entre l'instant d'un événement et sa réception par le collector
// (« allowed lateness »). Le collector accepte les événements plus anciens, mais le processor les envoie en
// dead-letter (raison too_late) : au-delà, la mémoire des doublons et l'état des sessions ne les couvrent plus.
// Les durées de vie de ces deux mémoires sont calculées à partir de cette valeur.
const MaxLateness = time.Hour

// Event est un événement émis par un navigateur (ou notre générateur de charge).
//
// L'identifiant ID est fourni par l'émetteur : c'est ce qui permettra plus tard
// de dédoublonner (idempotence) si un événement est livré deux fois.
type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	SiteID    string    `json:"site_id"`
	VisitorID string    `json:"visitor_id"`
	URL       string    `json:"url"`
	Referrer  string    `json:"referrer,omitempty"`
	UserAgent string    `json:"user_agent,omitempty"`
	Timestamp time.Time `json:"timestamp"` // instant de l'événement côté émetteur

	// Champs renseignés par le serveur : toute valeur envoyée par le client est écrasée.
	IP         string    `json:"ip,omitempty"` // tronquée par le collector (IPv4 /24, IPv6 /48)
	ReceivedAt time.Time `json:"received_at"`
}

// Validate retourne la liste des problèmes détectés (vide si l'événement est valide).
// L'heure courante est passée en paramètre pour rendre la fonction testable.
func (e Event) Validate(now time.Time) []string {
	var problems []string

	problems = append(problems, required("id", e.ID, maxFieldLen)...)
	problems = append(problems, required("site_id", e.SiteID, maxFieldLen)...)
	problems = append(problems, required("visitor_id", e.VisitorID, maxFieldLen)...)

	switch e.Type {
	case TypePageview, TypeClick:
	default:
		problems = append(problems, fmt.Sprintf("type must be %q or %q", TypePageview, TypeClick))
	}

	problems = append(problems, required("url", e.URL, maxURLLen)...)
	if e.URL != "" {
		if u, err := url.Parse(e.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			problems = append(problems, "url must be an absolute http(s) URL")
		}
	}

	// Les événements anciens sont acceptés ici (un mobile peut envoyer en différé) : c'est le processor qui
	// applique MaxLateness, mesuré depuis la réception. Un événement "du futur" indique en revanche une horloge
	// cassée ou une tentative de triche.
	switch {
	case e.Timestamp.IsZero():
		problems = append(problems, "timestamp is required")
	case e.Timestamp.After(now.Add(MaxFutureSkew)):
		problems = append(problems, "timestamp is in the future")
	}

	return problems
}

// Lateness est le retard de l'événement à sa réception par le collector (ReceivedAt - Timestamp). Le mesurer
// depuis l'heure du TRAITEMENT serait faux : un arriéré dans Kafka rendrait « trop vieux » des événements arrivés
// à l'heure. Sans heure de réception (événement publié par un autre producteur), ref la remplace.
func (e Event) Lateness(ref time.Time) time.Duration {
	if !e.ReceivedAt.IsZero() {
		ref = e.ReceivedAt
	}
	return ref.Sub(e.Timestamp)
}

func required(field, value string, maxLen int) []string {
	switch {
	case value == "":
		return []string{field + " is required"}
	case len(value) > maxLen:
		return []string{fmt.Sprintf("%s must be at most %d characters", field, maxLen)}
	}
	return nil
}
