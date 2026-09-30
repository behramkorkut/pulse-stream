package event

import "time"

// Enriched est un événement validé puis enrichi par le processor.
//
// L'événement d'origine est intégré (embedded) : en JSON, ses champs sont mis "à plat"
// au même niveau que les champs d'enrichissement, sans sous-objet.
type Enriched struct {
	Event

	Browser     string    `json:"browser"`      // chrome, firefox, safari, edge, opera, other
	Device      string    `json:"device"`       // desktop, mobile, tablet, bot
	IsBot       bool      `json:"is_bot"`       // trafic automatisé détecté par heuristique sur le user-agent
	ProcessedAt time.Time `json:"processed_at"` // instant du traitement par le processor
}
