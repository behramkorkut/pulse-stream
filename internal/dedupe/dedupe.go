// Package dedupe mémorise les identifiants d'événements déjà comptés, pour ne jamais compter deux fois
// un événement livré deux fois (nouvel essai d'un client, relecture d'un lot après une panne).
//
// L'interface sépare volontairement la LECTURE (Seen) de l'ÉCRITURE (Mark) : l'appelant marque
// les identifiants APRÈS avoir écrit ses résultats. Si le processus s'arrête entre les deux, les événements
// seront recomptés (doublon) plutôt que perdus. Marquer avant risquerait l'inverse.
package dedupe

import "context"

// Store mémorise des identifiants d'événements.
type Store interface {
	// Seen indique, pour chaque identifiant (dans le même ordre), s'il a déjà été marqué.
	Seen(ctx context.Context, ids []string) ([]bool, error)

	// Mark mémorise des identifiants. Marquer deux fois le même identifiant est sans effet.
	Mark(ctx context.Context, ids []string) error
}
