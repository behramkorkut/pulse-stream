// Commande audit : localise un écart entre ce qu'une charge du générateur a fait accepter et ce que MongoDB a compté.
//
// Elle lit la trace de la charge (bench/last-run.json), le topic enriched-events sur l'entrée standard (format de
// rpk -f '%p %o %v\n') et les compteurs de MongoDB, puis répond à deux questions :
//   - l'écart existe-t-il déjà dans enriched-events (problème en amont), ou seulement dans MongoDB (aggregator) ?
//   - si c'est l'aggregator, dans quels documents (site, minute), et quels événements en plusieurs exemplaires
//     s'y trouvent ?
//
// Usage : make k8s-audit (après un make k8s-chaos ou k8s-load-verify).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/behramkorkut/pulse-stream/internal/audit"
	"github.com/behramkorkut/pulse-stream/internal/loadgen"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "audit:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		reportPath = flag.String("report", "bench/last-run.json", "trace de la charge à auditer (écrite par loadgen)")
		mongoURI   = flag.String("mongo-uri", "mongodb://localhost:27018/?directConnection=true", "adresse de MongoDB")
		mongoDB    = flag.String("mongo-db", "pulse", "base MongoDB")
		mongoColl  = flag.String("mongo-collection", "minute_stats", "collection des compteurs")
		show       = flag.Int("show", 15, "nombre maximal de documents en écart à détailler")
	)
	flag.Parse()

	report, err := readReport(*reportPath)
	if err != nil {
		return err
	}
	prefix := "load-" + report.RunID + "-"

	ix := audit.NewIndex(prefix)
	unreadable := 0
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if err := ix.AddLine(sc.Text()); err != nil {
			unreadable++
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("lecture de l'entrée : %w", err)
	}
	if ix.Messages == 0 {
		return fmt.Errorf("aucun message de la charge %s sur l'entrée (%d lignes illisibles) : vérifier la commande rpk", report.RunID, unreadable)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stored, err := readMongo(ctx, *mongoURI, *mongoDB, *mongoColl, prefix)
	if err != nil {
		return err
	}

	unique, uniqueByBucket := ix.Totals()
	var mongoTotal loadgen.Totals
	for _, t := range stored {
		mongoTotal = add(mongoTotal, t)
	}

	multi, multiPartition := 0, 0
	for _, e := range ix.Events() {
		if len(e.Copies) > 1 {
			multi++
		}
		if e.Partitions() > 1 {
			multiPartition++
		}
	}

	fmt.Printf("Charge %s (sites %s*)\n\n", report.RunID, prefix)
	fmt.Printf("  %s %10s %8s %8s\n", pad("", 46), "pageviews", "clics", "robots")
	row("attendu par le générateur", report.Expected)
	row("enriched-events, chaque événement une fois", unique)
	row("MongoDB", mongoTotal)
	fmt.Printf("\n  %d messages lus pour cette charge, %d événements distincts : %d en plusieurs exemplaires, dont %d sur\n"+
		"  plusieurs partitions. %d lignes illisibles.\n\n", ix.Messages, len(ix.Events()), multi, multiPartition, unreadable)

	switch {
	case unique == report.Expected && mongoTotal == unique:
		fmt.Println("VERDICT : tout concorde.")
		return nil
	case unique != report.Expected:
		fmt.Println("VERDICT : l'écart existe déjà dans enriched-events. Il vient de l'amont (générateur, collector ou")
		fmt.Println("processor), pas de l'aggregator : l'aggregator compte bien ce qu'il reçoit, une fois.")
		if mongoTotal != unique {
			fmt.Println("En plus, MongoDB diffère d'enriched-events : l'aggregator a aussi un écart (détail ci-dessous).")
		} else {
			return nil
		}
	default:
		fmt.Println("VERDICT : enriched-events concorde avec le générateur, MongoDB non : l'écart vient de l'aggregator.")
	}

	printBuckets(ix, stored, uniqueByBucket, *show)
	return nil
}

// printBuckets détaille les documents où MongoDB s'écarte d'enriched-events, avec les événements en plusieurs
// exemplaires qu'ils contiennent : si le double comptage touche des copies, c'est la réservation qui a failli ; s'il
// touche des événements en un seul exemplaire, c'est la position (un même message compté deux fois).
func printBuckets(ix *audit.Index, stored, unique map[string]loadgen.Totals, show int) {
	ids := map[string]bool{}
	for id := range stored {
		ids[id] = true
	}
	for id := range unique {
		ids[id] = true
	}
	var off []string
	for id := range ids {
		if stored[id] != unique[id] {
			off = append(off, id)
		}
	}
	sort.Strings(off)

	byBucket := map[string][]*audit.Event{}
	for _, e := range ix.Events() {
		if len(e.Copies) > 1 {
			b := audit.BucketOf(e.Enriched)
			byBucket[b] = append(byBucket[b], e)
		}
	}

	fmt.Printf("\n%d documents (site, minute) en écart :\n", len(off))
	for i, id := range off {
		if i == show {
			fmt.Printf("  ... et %d autres (option -show)\n", len(off)-show)
			break
		}
		d := sub(stored[id], unique[id])
		fmt.Printf("  %s : %+d pageviews, %+d clics, %+d robots\n", id, d.Pageviews, d.Clicks, d.Bots)
		for _, e := range byBucket[id] {
			copies := make([]string, len(e.Copies))
			for j, c := range e.Copies {
				copies[j] = c.String()
			}
			fmt.Printf("      en plusieurs exemplaires : %s (%s, robot=%v) aux positions %s\n",
				e.Enriched.ID, e.Enriched.Type, e.Enriched.IsBot, strings.Join(copies, ", "))
		}
		if len(byBucket[id]) == 0 {
			fmt.Println("      aucun événement en plusieurs exemplaires dans ce document")
		}
	}
}

func row(label string, t loadgen.Totals) {
	fmt.Printf("  %s %10d %8d %8d\n", pad(label, 46), t.Pageviews, t.Clicks, t.Bots)
}

// pad complète à width caractères (et non octets : les accents comptent pour un).
func pad(s string, width int) string {
	if n := utf8.RuneCountInString(s); n < width {
		return s + strings.Repeat(" ", width-n)
	}
	return s
}

func add(a, b loadgen.Totals) loadgen.Totals {
	return loadgen.Totals{Pageviews: a.Pageviews + b.Pageviews, Clicks: a.Clicks + b.Clicks, Bots: a.Bots + b.Bots}
}

func sub(a, b loadgen.Totals) loadgen.Totals {
	return loadgen.Totals{Pageviews: a.Pageviews - b.Pageviews, Clicks: a.Clicks - b.Clicks, Bots: a.Bots - b.Bots}
}

func readReport(path string) (loadgen.Report, error) {
	var r loadgen.Report
	data, err := os.ReadFile(path)
	if err != nil {
		return r, fmt.Errorf("lecture de la trace : %w", err)
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return r, fmt.Errorf("trace %s illisible : %w", path, err)
	}
	if r.RunID == "" {
		return r, fmt.Errorf("trace %s sans run_id", path)
	}
	return r, nil
}

// readMongo lit les compteurs de chaque document (site, minute) de la charge.
func readMongo(ctx context.Context, uri, db, coll, prefix string) (map[string]loadgen.Totals, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(5 * time.Second))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	cur, err := client.Database(db).Collection(coll).Find(ctx,
		bson.M{"site_id": bson.M{"$regex": "^" + regexp.QuoteMeta(prefix)}})
	if err != nil {
		return nil, fmt.Errorf("mongo find (tunnel vers le cluster ouvert ?) : %w", err)
	}
	var docs []struct {
		ID        string `bson:"_id"`
		Pageviews int64  `bson:"pageviews"`
		Clicks    int64  `bson:"clicks"`
		BotEvents int64  `bson:"bot_events"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("mongo read: %w", err)
	}
	out := make(map[string]loadgen.Totals, len(docs))
	for _, d := range docs {
		out[d.ID] = loadgen.Totals{Pageviews: d.Pageviews, Clicks: d.Clicks, Bots: d.BotEvents}
	}
	return out, nil
}
