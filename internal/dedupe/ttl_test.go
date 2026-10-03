package dedupe

import "testing"

// La durée de vie par défaut doit couvrir la fenêtre de retard toléré : sinon le renvoi tardif d'un événement
// déjà compté (mais encore accepté par le processor) serait compté une seconde fois.
func TestDefaultTTLCoversTheAllowedLateness(t *testing.T) {
	if DefaultTTL < MinTTL {
		t.Errorf("DefaultTTL = %v < MinTTL = %v", DefaultTTL, MinTTL)
	}
}
