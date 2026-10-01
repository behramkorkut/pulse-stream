package loadgen

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"
	"time"
)

// WriteTable affiche les paliers sous forme de tableau lisible.
func WriteTable(w io.Writer, steps []StepResult) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "visé/s\tacceptés/s\tperdus\terreurs\tcodes\tp50\tp95\tp99\tmax\t")
	for _, s := range steps {
		fmt.Fprintf(tw, "%d\t%.0f\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t\n",
			s.Rate, s.Accepted, s.Dropped, s.Errors, formatCodes(s.Codes),
			ms(s.P50), ms(s.P95), ms(s.P99), ms(s.Max))
	}
	_ = tw.Flush()
}

func formatCodes(codes map[int]int) string {
	keys := make([]int, 0, len(codes))
	for c := range codes {
		keys = append(keys, c)
	}
	sort.Ints(keys)
	out := ""
	for i, c := range keys {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%d:%d", c, codes[c])
	}
	return out
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000) }

// Report est la trace JSON d'une exécution complète : de quoi refaire une comparaison plus tard.
type Report struct {
	RunID     string       `json:"run_id"`
	StartedAt time.Time    `json:"started_at"`
	URL       string       `json:"url"`
	Workers   int          `json:"workers"`
	Mix       Mix          `json:"mix"`
	Steps     []StepReport `json:"steps"`
	Expected  Totals       `json:"expected"`
	Verified  *Verified    `json:"verified,omitempty"`
}

// StepReport est un palier, avec des durées en millisecondes.
type StepReport struct {
	TargetRate int         `json:"target_rate"`
	DurationS  float64     `json:"duration_s"`
	Scheduled  int         `json:"scheduled"`
	Dropped    int         `json:"dropped"`
	Errors     int         `json:"errors"`
	Codes      map[int]int `json:"codes"`
	AcceptedPS float64     `json:"accepted_per_s"`
	P50ms      float64     `json:"p50_ms"`
	P95ms      float64     `json:"p95_ms"`
	P99ms      float64     `json:"p99_ms"`
	MaxMs      float64     `json:"max_ms"`
}

// Verified est le verdict de la comparaison avec MongoDB.
type Verified struct {
	OK     bool   `json:"ok"`
	Got    Totals `json:"got"`
	Reason string `json:"reason,omitempty"`
}

// StepReportOf convertit un palier pour la trace JSON.
func StepReportOf(s StepResult) StepReport {
	f := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return StepReport{
		TargetRate: s.Rate, DurationS: s.Duration.Seconds(), Scheduled: s.Scheduled, Dropped: s.Dropped,
		Errors: s.Errors, Codes: s.Codes, AcceptedPS: s.Accepted,
		P50ms: f(s.P50), P95ms: f(s.P95), P99ms: f(s.P99), MaxMs: f(s.Max),
	}
}

// WriteFile écrit la trace dans un fichier JSON (en créant le dossier si besoin).
func (r Report) WriteFile(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
