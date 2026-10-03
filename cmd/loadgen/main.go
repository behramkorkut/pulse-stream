// Commande loadgen : envoie une charge synthétique au collector, palier par palier, et vérifie ensuite
// que MongoDB a compté exactement ce que le collector a accepté.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/behramkorkut/pulse-stream/internal/loadgen"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		url       = flag.String("url", "http://localhost:8080/collect", "adresse du collector")
		rates     = flag.String("rates", "200,500,1000", "paliers de débit, en requêtes par seconde (séparés par des virgules)")
		duration  = flag.Duration("duration", 20*time.Second, "durée de chaque palier")
		cooldown  = flag.Duration("cooldown", 5*time.Second, "pause entre deux paliers (le pipeline finit de s'écouler)")
		workers   = flag.Int("workers", 128, "requêtes simultanées au maximum")
		sites     = flag.Int("sites", 3, "nombre de sites distincts")
		visitors  = flag.Int("visitors", 10_000, "nombre de visiteurs distincts par site")
		dup       = flag.Float64("dup", 0.02, "part des requêtes qui renvoient un événement déjà envoyé")
		invalid   = flag.Float64("invalid", 0.01, "part des requêtes invalides (refusées par le collector)")
		bots      = flag.Float64("bots", 0.05, "part des événements valides émis par des robots")
		clicks    = flag.Float64("clicks", 0.20, "part des événements valides qui sont des clics")
		late      = flag.Float64("late", 0.02, "part des événements valides envoyés en retard, dans la limite tolérée (comptés)")
		tooLate   = flag.Float64("too-late", 0.005, "part des événements valides envoyés trop en retard (dead-letter, jamais comptés)")
		seed      = flag.Uint64("seed", 1, "graine du générateur (même graine, mêmes événements)")
		out       = flag.String("out", "bench/last-run.json", "fichier JSON où écrire la trace de l'exécution")
		verify    = flag.Bool("verify", true, "comparer, à la fin, ce que MongoDB a compté à ce que le collector a accepté")
		mongoURI  = flag.String("mongo-uri", "mongodb://localhost:27017", "adresse de MongoDB (pour -verify)")
		mongoDB   = flag.String("mongo-db", "pulse", "base MongoDB (pour -verify)")
		mongoColl = flag.String("mongo-collection", "minute_stats", "collection MongoDB (pour -verify)")
		verifyFor = flag.Duration("verify-timeout", 2*time.Minute, "attente maximale de la fin du traitement (pour -verify)")
	)
	flag.Parse()

	steps, err := parseRates(*rates)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	runID := strconv.FormatInt(time.Now().Unix(), 10)
	mix := loadgen.Mix{Duplicate: *dup, Invalid: *invalid, Bot: *bots, Click: *clicks, Late: *late, TooLate: *tooLate}
	gen := loadgen.NewGenerator(loadgen.GenConfig{RunID: runID, Sites: *sites, Visitors: *visitors, Mix: mix, Seed: *seed})

	fmt.Printf("Charge %s : paliers %v pendant %s chacun, %d requêtes simultanées au plus\n", runID, steps, *duration, *workers)
	fmt.Printf("Composition : %.0f%% de renvois, %.0f%% d'invalides, %.0f%% de robots, %.0f%% de clics, "+
		"%.1f%% en retard, %.1f%% trop en retard\n\n",
		*dup*100, *invalid*100, *bots*100, *clicks*100, *late*100, *tooLate*100)

	report := loadgen.Report{RunID: runID, StartedAt: time.Now().UTC(), URL: *url, Workers: *workers, Mix: mix}
	acked := loadgen.Tally{}
	var results []loadgen.StepResult

	for i, rate := range steps {
		if ctx.Err() != nil {
			break
		}
		fmt.Printf("Palier %d/%d : %d requêtes/s pendant %s...\n", i+1, len(steps), rate, *duration)
		res, err := loadgen.RunStep(ctx, loadgen.Options{URL: *url, Rate: rate, Duration: *duration, Workers: *workers, Gen: gen})
		if err != nil {
			return err
		}
		results = append(results, res)
		report.Steps = append(report.Steps, loadgen.StepReportOf(res))
		acked.Merge(res.Acked)

		if res.Errors > 0 && res.Codes[202] == 0 {
			return fmt.Errorf("le collector ne répond pas sur %s (%d erreurs, aucun 202) : est-il lancé ?", *url, res.Errors)
		}
		if i < len(steps)-1 {
			select {
			case <-time.After(*cooldown):
			case <-ctx.Done():
			}
		}
	}

	fmt.Println()
	loadgen.WriteTable(os.Stdout, results)
	report.Expected = acked.Totals()
	report.TooLate = acked.TooLate()
	fmt.Printf("\nAttendu dans MongoDB (événements acceptés, chacun une fois) : %d pageviews, %d clics, %d événements de robots\n",
		report.Expected.Pageviews, report.Expected.Clicks, report.Expected.Bots)
	if report.TooLate > 0 {
		fmt.Printf("Exclus : %d événements acceptés mais trop en retard (attendus dans dead-letter, raison too_late)\n", report.TooLate)
	}

	if *verify && ctx.Err() == nil {
		v, err := verifyMongo(ctx, log, *mongoURI, *mongoDB, *mongoColl, gen.SitePrefix(), report.Expected, *verifyFor)
		if err != nil {
			return err
		}
		report.Verified = &loadgen.Verified{OK: v.OK, Got: v.Got, Reason: v.Reason}
		printVerdict(v)
	}

	if err := report.WriteFile(*out); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	fmt.Printf("\nTrace enregistrée dans %s\n", *out)

	if report.Verified != nil && !report.Verified.OK {
		return fmt.Errorf("vérification échouée : %s", report.Verified.Reason)
	}
	return nil
}

func verifyMongo(ctx context.Context, log *slog.Logger, uri, db, coll, prefix string, want loadgen.Totals, timeout time.Duration) (loadgen.VerifyResult, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(3 * time.Second))
	if err != nil {
		return loadgen.VerifyResult{}, fmt.Errorf("mongo connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		return loadgen.VerifyResult{}, fmt.Errorf("mongo not ready at %s (relancer avec -verify=false pour ignorer) : %w", uri, err)
	}

	fmt.Println("\nVérification : attente de la fin du traitement par le processor et l'aggregator...")
	log.Debug("verifying", slog.String("prefix", prefix))
	return loadgen.Verify(ctx, loadgen.NewMongoTotals(client.Database(db).Collection(coll)), prefix, want,
		loadgen.VerifyOptions{Timeout: timeout})
}

func printVerdict(v loadgen.VerifyResult) {
	fmt.Printf("MongoDB après %s : %d pageviews, %d clics, %d événements de robots\n",
		v.Waited.Round(100*time.Millisecond), v.Got.Pageviews, v.Got.Clicks, v.Got.Bots)
	if v.OK {
		fmt.Println("RÉSULTAT : OK, aucun événement perdu, aucun double comptage.")
		return
	}
	fmt.Printf("RÉSULTAT : ÉCHEC (%s)\n", v.Reason)
	fmt.Printf("  écart : %+d pageviews, %+d clics, %+d robots\n",
		v.Got.Pageviews-v.Want.Pageviews, v.Got.Clicks-v.Want.Clicks, v.Got.Bots-v.Want.Bots)
}

func parseRates(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("débit invalide %q : un entier positif est attendu", part)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("aucun palier de débit (-rates)")
	}
	return out, nil
}
