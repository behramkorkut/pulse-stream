// Package dedupe désigne, pour chaque événement, le SEUL message qui le comptera.
//
// Un même événement peut arriver dans plusieurs messages Kafka : renvoi du client, relecture d'un lot par le
// processor. Le premier message qui réserve l'événement en devient le « propriétaire » ; les autres sont des
// doublons, écartés. Le propriétaire est un identifiant de message (topic, partition, offset), et c'est ce qui
// rend sûr de réserver AVANT d'écrire :
//
//   - si le traitement échoue après la réservation, le message est relu, retrouve sa propre réservation et compte
//     l'événement : rien n'est perdu ;
//   - si deux instances traitent le même message (rééquilibrage), elles s'y reconnaissent toutes deux propriétaires :
//     c'est l'aggregator qui garantit qu'une seule écrit (position par partition, dans la même transaction MongoDB
//     que les compteurs).
//
// La version précédente marquait les identifiants APRÈS l'écriture (Seen, puis Mark) : un crash entre les deux,
// ou deux instances qui consultaient Redis avant que l'une ait marqué, comptaient deux fois.
package dedupe

import (
	"context"
	"fmt"
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

// Store réserve chaque événement pour un message propriétaire.
type Store interface {
	// Claim réserve chaque clé pour owners[i] si elle est libre, et retourne pour chaque clé, dans le même ordre, le
	// propriétaire retenu : owners[i] si la clé était libre ou déjà réservée par ce même message, sinon le
	// propriétaire précédent. Dans un même appel, la première occurrence d'une clé l'emporte.
	Claim(ctx context.Context, keys []Key, owners []string) ([]string, error)
}

// checkLengths vérifie qu'il y a un propriétaire par clé.
func checkLengths(keys []Key, owners []string) error {
	if len(keys) != len(owners) {
		return fmt.Errorf("dedupe: %d clés pour %d propriétaires", len(keys), len(owners))
	}
	return nil
}
