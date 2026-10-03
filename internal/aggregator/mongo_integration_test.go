//go:build integration

// Test d'intégration : nécessite MongoDB (make up). Chaque test travaille dans une base à nom unique,
// supprimée à la fin : aucune donnée existante n'est touchée.

package aggregator

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// minuteDoc est la forme d'un document lu dans la collection.
type minuteDoc struct {
	ID        string           `bson:"_id"`
	SiteID    string           `bson:"site_id"`
	Minute    time.Time        `bson:"minute"`
	Pageviews int64            `bson:"pageviews"`
	Clicks    int64            `bson:"clicks"`
	BotEvents int64            `bson:"bot_events"`
	Sessions  int64            `bson:"sessions"`
	Devices   map[string]int64 `bson:"devices"`
	Browsers  map[string]int64 `bson:"browsers"`
}

// testCollection retourne une collection vide dans une base jetable.
func testCollection(t *testing.T) *mongo.Collection {
	t.Helper()

	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017/?directConnection=true" // jeu de répliques d'un nœud (make up)
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(3 * time.Second))
	if err != nil {
		t.Fatalf("mongo.Connect(%s) : %v", uri, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		t.Fatalf("MongoDB injoignable sur %s (make up ?) : %v", uri, err)
	}

	db := client.Database(fmt.Sprintf("pulse_it_%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		_ = db.Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
	coll := db.Collection("minute_stats")
	if err := NewMongo(coll).EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes : %v", err)
	}
	return coll
}

func readDoc(t *testing.T, coll *mongo.Collection, id string) minuteDoc {
	t.Helper()

	var doc minuteDoc
	if err := coll.FindOne(context.Background(), map[string]any{"_id": id}).Decode(&doc); err != nil {
		t.Fatalf("lecture du document %q : %v", id, err)
	}
	return doc
}

var testPartition = Partition{Topic: "enriched-events", ID: 0}

// batchAt fabrique un lot d'événements aux offsets first, first+1, ... de la partition de test, avec la position
// de fin de lot correspondante.
func batchAt(first int64, events ...event.Enriched) ([]Counted, map[Partition]int64) {
	out := make([]Counted, len(events))
	for i, e := range events {
		out[i] = Counted{Event: e, From: testPartition, Offset: first + int64(i)}
	}
	return out, map[Partition]int64{testPartition: first + int64(len(events)) - 1}
}

// sampleEvents : 2 affichages mobiles (dont une ouverture de session), 1 clic desktop, 1 robot, à 11:00 UTC.
func sampleEvents(prefix string) []event.Enriched {
	return []event.Enriched{
		human(prefix+"-1", "site-42", event.TypePageview, "mobile", "safari", time.Second, true),
		human(prefix+"-2", "site-42", event.TypePageview, "mobile", "safari", 2*time.Second, false),
		human(prefix+"-3", "site-42", event.TypeClick, "desktop", "chrome", 3*time.Second, false),
		bot(prefix+"-4", "site-42", 4*time.Second),
	}
}

func apply(t *testing.T, store *Mongo, events []Counted, upTo map[Partition]int64) Applied {
	t.Helper()
	got, err := store.Apply(context.Background(), events, upTo)
	if err != nil {
		t.Fatalf("Apply() : %v", err)
	}
	return got
}

// applyBatch écrit un lot d'événements placés aux offsets first, first+1, ...
func applyBatch(t *testing.T, store *Mongo, first int64, events ...event.Enriched) Applied {
	t.Helper()
	batch, upTo := batchAt(first, events...)
	return apply(t, store, batch, upTo)
}

const minuteID = "site-42|2026-09-30T11:00Z"

// Le cœur du magasin : deux lots DIFFÉRENTS s'additionnent (c'est un cumul, pas une copie).
func TestMongoApplyCreatesThenAccumulates(t *testing.T) {
	coll := testCollection(t)
	store := NewMongo(coll)

	applyBatch(t, store, 0, sampleEvents("a")...)
	doc := readDoc(t, coll, minuteID)
	if doc.SiteID != "site-42" || !doc.Minute.Equal(time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC)) {
		t.Errorf("document créé = %+v, want site-42 à 11:00 UTC", doc)
	}
	if doc.Pageviews != 2 || doc.Clicks != 1 || doc.BotEvents != 1 || doc.Sessions != 1 {
		t.Errorf("compteurs = %+v, want 2 1 1 1", doc)
	}

	applyBatch(t, store, 4, sampleEvents("b")...)
	doc = readDoc(t, coll, minuteID)
	if doc.Pageviews != 4 || doc.Clicks != 2 || doc.BotEvents != 2 || doc.Sessions != 2 {
		t.Errorf("après cumul, compteurs = %+v, want 4 2 2 2", doc)
	}
	if doc.Devices["mobile"] != 4 || doc.Devices["desktop"] != 2 || doc.Browsers["safari"] != 4 {
		t.Errorf("répartitions = %v / %v, want mobile 4, desktop 2, safari 4", doc.Devices, doc.Browsers)
	}

	n, err := coll.CountDocuments(context.Background(), map[string]any{})
	if err != nil || n != 1 {
		t.Errorf("CountDocuments = %d, %v, want 1 seul document (l'upsert ne duplique pas)", n, err)
	}
}

// L'écriture idempotente : rejouer un lot, ou un lot qui chevauche le précédent, n'ajoute que ce qui est nouveau.
func TestMongoApplyIgnoresWhatThePositionAlreadyCovers(t *testing.T) {
	coll := testCollection(t)
	store := NewMongo(coll)
	events := sampleEvents("a") // offsets 0 à 3

	first := applyBatch(t, store, 0, events...)
	if len(first.Events) != 4 {
		t.Fatalf("premier lot : %d événements comptés, want 4", len(first.Events))
	}

	again := applyBatch(t, store, 0, events...)
	if len(again.Events) != 0 || again.Buckets != 0 {
		t.Errorf("lot rejoué : %d événements, %d documents, want 0 et 0", len(again.Events), again.Buckets)
	}

	// Lot qui chevauche : offsets 2 et 3 (déjà écrits), puis 4 et 5 (nouveaux).
	more := sampleEvents("c")
	overlap := applyBatch(t, store, 2, events[2], events[3], more[0], more[1])
	if len(overlap.Events) != 2 {
		t.Errorf("lot chevauchant : %d événements comptés, want 2 (offsets 4 et 5)", len(overlap.Events))
	}

	doc := readDoc(t, coll, minuteID)
	if doc.Pageviews != 4 || doc.Clicks != 1 || doc.BotEvents != 1 {
		t.Errorf("compteurs = %+v, want pv 4 (2 + 2 nouveaux), clics 1, robots 1", doc)
	}
}

// Le test qui valide l'hypothèse centrale sur un vrai MongoDB : deux transactions concurrentes sur le même lot
// (deux instances pendant un rééquilibrage) se heurtent sur le document de position, et une seule compte.
func TestMongoConcurrentApplyOfTheSameBatchCountsOnce(t *testing.T) {
	coll := testCollection(t)
	const rounds, perBatch = 20, 10

	for r := 0; r < rounds; r++ {
		events := make([]event.Enriched, perBatch)
		for i := range events {
			events[i] = human(fmt.Sprintf("r%d-%d", r, i), "site-42", event.TypePageview, "desktop", "chrome", time.Second, false)
		}
		batch, upTo := batchAt(int64(r*perBatch), events...)

		var wg sync.WaitGroup
		counted := make([]int, 2)
		errs := make([]error, 2)
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				got, err := NewMongo(coll).Apply(context.Background(), batch, upTo)
				counted[w], errs[w] = len(got.Events), err
			}(w)
		}
		wg.Wait()

		for _, err := range errs {
			if err != nil {
				t.Fatalf("tour %d : Apply() : %v", r, err)
			}
		}
		if counted[0]+counted[1] != perBatch {
			t.Fatalf("tour %d : %d + %d événements comptés, want %d au total", r, counted[0], counted[1], perBatch)
		}
	}

	if doc := readDoc(t, coll, minuteID); doc.Pageviews != rounds*perBatch {
		t.Errorf("pageviews = %d, want %d : des lots ont été comptés par les deux transactions", doc.Pageviews, rounds*perBatch)
	}
}

func TestMongoApplyWritesOneDocumentPerBucket(t *testing.T) {
	coll := testCollection(t)
	store := NewMongo(coll)

	applyBatch(t, store, 0,
		human("e1", "site-42", event.TypePageview, "desktop", "chrome", time.Second, false),
		human("e2", "site-42", event.TypePageview, "desktop", "chrome", time.Minute+time.Second, false),
		human("e3", "site-43", event.TypePageview, "desktop", "chrome", time.Second, false),
	)

	n, err := coll.CountDocuments(context.Background(), map[string]any{})
	if err != nil || n != 3 {
		t.Errorf("CountDocuments = %d, %v, want 3", n, err)
	}
	readDoc(t, coll, "site-42|2026-09-30T11:01Z")
	readDoc(t, coll, "site-43|2026-09-30T11:00Z")
}

// Un lot sans événement à compter (doublons, messages inexploitables) fait quand même avancer la position.
func TestMongoApplyAdvancesThePositionWithoutEvents(t *testing.T) {
	coll := testCollection(t)
	store := NewMongo(coll)

	if got := apply(t, store, nil, map[Partition]int64{testPartition: 5}); len(got.Events) != 0 || got.Buckets != 0 {
		t.Errorf("lot vide : %+v, want rien d'écrit", got)
	}
	late := apply(t, store, []Counted{{Event: sampleEvents("a")[0], From: testPartition, Offset: 3}},
		map[Partition]int64{testPartition: 3})
	if len(late.Events) != 0 {
		t.Errorf("offset 3 compté alors que la position est déjà à 5")
	}
	n, err := coll.CountDocuments(context.Background(), map[string]any{})
	if err != nil || n != 0 {
		t.Errorf("CountDocuments = %d, %v, want 0", n, err)
	}
}

func TestMongoEnsureIndexesCanRunTwice(t *testing.T) {
	store := NewMongo(testCollection(t))
	for i := 1; i <= 2; i++ {
		if err := store.EnsureIndexes(context.Background()); err != nil {
			t.Fatalf("EnsureIndexes() appel %d : %v", i, err)
		}
	}
}

// waitForDoc attend (jusqu'à 30 s) qu'un document existe et vérifie la condition : les lots sont
// traités en arrière-plan, le test doit donc patienter sans deviner un délai fixe.
func waitForDoc(t *testing.T, coll *mongo.Collection, id string, cond func(minuteDoc) bool) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var last minuteDoc
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = coll.FindOne(context.Background(), map[string]any{"_id": id}).Decode(&last)
		if lastErr == nil && cond(last) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout : le document %q n'a pas atteint l'état attendu (dernier état %+v, erreur %v)", id, last, lastErr)
}
