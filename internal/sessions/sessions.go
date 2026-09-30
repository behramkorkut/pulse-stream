// Package sessions rattache chaque événement à la session de son visiteur.
//
// Règle : une session est une suite d'événements d'un même visiteur sur un même site dont
// deux événements consécutifs sont espacés d'au plus DefaultTimeout. Au-delà, une nouvelle
// session commence.
//
// Deux choix structurants :
//
//   - Le temps est celui de l'ÉVÉNEMENT (son Timestamp), pas celui de l'horloge du serveur.
//     Ainsi, retraiter un arriéré d'événements donne les mêmes sessions qu'en temps réel.
//   - L'identifiant de session est DÉTERMINISTE (dérivé du premier événement) : traiter deux
//     fois le même événement, après un crash par exemple, donne le même résultat.
package sessions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// DefaultTimeout est l'inactivité au-delà de laquelle une nouvelle session commence.
const DefaultTimeout = 30 * time.Minute

// Session est le résultat du rattachement d'un événement à une session.
type Session struct {
	ID  string
	New bool // vrai uniquement pour l'événement qui a ouvert la session
}

// Store rattache un événement à une session, en créant la session si nécessaire.
//
// Touch doit être idempotent : rappeler Touch avec le même événement retourne le même résultat.
// C'est ce qui rend sûr le retraitement d'un lot après une panne.
type Store interface {
	Touch(ctx context.Context, e event.Event) (Session, error)
}

// NewSessionID calcule l'identifiant d'une session à partir de son premier événement.
func NewSessionID(siteID, visitorID, firstEventID string) string {
	sum := sha256.Sum256([]byte(siteID + "\x00" + visitorID + "\x00" + firstEventID))
	return "s_" + hex.EncodeToString(sum[:8])
}

// sessionKey identifie l'état d'un visiteur : une session en cours par site et par visiteur.
func sessionKey(e event.Event) string {
	return e.SiteID + "/" + e.VisitorID
}
