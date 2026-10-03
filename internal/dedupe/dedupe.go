// Package dedupe mémorise les identifiants d'événements déjà comptés, pour ne jamais compter deux fois
// un événement livré deux fois (nouvel essai d'un client, relecture d'un lot après une panne).
//
// L'interface sépare volontairement la LECTURE (Seen) de l'ÉCRITURE (Mark) : l'appelant marque
// les identifiants APRÈS avoir écrit ses résultats. Si le processus s'arrête entre les deux, les événements
// seront recomptés (doublon) plutôt que perdus. Marquer avant risquerait l'inverse.
package dedupe

import (
	"context"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// MinTTL est la durée minimale de mémorisation d'un événement compté : le retard maximal toléré plus l'avance
// d'horloge tolérée. Un client peut renvoyer un événement tant qu'il n'est pas trop en retard (au-delà, le
// processor le met en dead-letter) : en dessous de cette durée, un tel renvoi ne serait plus reconnu et serait
// compté une seconde fois.
const MinTTL = event.MaxLateness + event.MaxFutureSkew

// DefaultTTL est la durée pendant laquelle un événement compté reste mémorisé : MinTTL (65 min) plus 25 min de
// marge pour un retard de traitement.
//
// Limite assumée : la mémoire nécessaire croît avec le débit x la durée. À des centaines de milliers
// d'événements par seconde, une telle fenêtre ne tiendrait pas dans un Redis : on utiliserait alors un retard
// toléré plus court, un filtre de Bloom, ou un état local par partition. Ici, l'objectif est de montrer le
// mécanisme.
const DefaultTTL = 90 * time.Minute

// Key identifie un événement pour le dédoublonnage : son site ET son identifiant.
//
// L'identifiant est fourni par le client : il n'est unique qu'au sein d'un site. Deux sites peuvent envoyer le
// même (compteur local, SDK mal configuré, ou volontairement pour effacer le trafic d'un autre site). Avec
// l'identifiant seul, l'événement du second site serait écarté comme doublon, sans aucun signal.
type Key struct {
	SiteID  string
	EventID string
}

// Store mémorise des événements déjà comptés.
type Store interface {
	// Seen indique, pour chaque clé (dans le même ordre), si elle a déjà été marquée.
	Seen(ctx context.Context, keys []Key) ([]bool, error)

	// Mark mémorise des clés. Marquer deux fois la même clé est sans effet.
	Mark(ctx context.Context, keys []Key) error
}
