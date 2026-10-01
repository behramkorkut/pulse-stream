package loadgen

import (
	"context"
	"fmt"
	"time"
)

// TotalsSource lit, dans la base, les compteurs cumulés des sites dont l'identifiant commence par prefix.
type TotalsSource interface {
	Totals(ctx context.Context, sitePrefix string) (Totals, error)
}

// VerifyOptions règle l'attente de la fin du traitement.
type VerifyOptions struct {
	Poll    time.Duration // intervalle de lecture (défaut : 1 s)
	Stable  time.Duration // durée sans changement après laquelle on conclut à un manque (défaut : 10 s)
	Timeout time.Duration // durée maximale d'attente (défaut : 2 min)
}

// VerifyResult est le verdict de la comparaison.
type VerifyResult struct {
	Want, Got Totals
	OK        bool
	Waited    time.Duration
	Reason    string // vide si OK
}

// Verify attend que la base rattrape ce que le collector a accepté, puis compare.
//
// Le pipeline est asynchrone : au moment où la charge s'arrête, des événements sont encore en route.
// On lit donc la base jusqu'à égalité. Deux verdicts d'échec, de nature différente :
//   - un compteur DÉPASSE l'attendu : définitif, car les compteurs ne font que croître. C'est un double comptage ;
//   - la base cesse de progresser en restant SOUS l'attendu : des événements acceptés ont été perdus.
func Verify(ctx context.Context, src TotalsSource, prefix string, want Totals, o VerifyOptions) (VerifyResult, error) {
	if o.Poll <= 0 {
		o.Poll = time.Second
	}
	if o.Stable <= 0 {
		o.Stable = 10 * time.Second
	}
	if o.Timeout <= 0 {
		o.Timeout = 2 * time.Minute
	}

	start := time.Now()
	lastChange := start
	var last Totals
	first := true

	for {
		got, err := src.Totals(ctx, prefix)
		if err != nil {
			return VerifyResult{}, fmt.Errorf("read totals: %w", err)
		}
		res := VerifyResult{Want: want, Got: got, Waited: time.Since(start)}

		switch {
		case got == want:
			res.OK = true
			return res, nil
		case got.Pageviews > want.Pageviews || got.Clicks > want.Clicks || got.Bots > want.Bots:
			res.Reason = "double comptage : un compteur dépasse ce que le collector a accepté"
			return res, nil
		}

		if first || got != last {
			lastChange, last, first = time.Now(), got, false
		}
		if time.Since(lastChange) >= o.Stable {
			res.Reason = "perte : la base ne progresse plus et reste sous ce que le collector a accepté"
			return res, nil
		}
		if time.Since(start) >= o.Timeout {
			res.Reason = "délai dépassé : le traitement n'a pas fini à temps"
			return res, nil
		}

		select {
		case <-time.After(o.Poll):
		case <-ctx.Done():
			return VerifyResult{}, ctx.Err()
		}
	}
}
