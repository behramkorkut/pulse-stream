//go:build integration

// Test d'intégration : nécessite MongoDB (make up). Chaque test travaille dans une base à nom unique,
// supprimée à la fin : aucune donnée existante n'est touchée.

package aggregator

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
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
		uri = "mongodb://localhost:27017"
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
	return db.Collection("minute_stats")
}

func readDoc(t *testing.T, coll *mongo.Collection, id string) minuteDoc {
	t.Helper()

	var doc minuteDoc
	if err := coll.FindOne(context.Background(), map[string]any{"_id": id}).Decode(&doc); err != nil {
		t.Fatalf("lecture du document %q : %v", id, err)
	}
	return doc
}

func sampleBucket() Bucket {
	return Bucket{
		SiteID:    "site-42",
		Minute:    time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC),
		Pageviews: 2, Clicks: 1, BotEvents: 3, Sessions: 1,
		Devices:  map[string]int64{"mobile": 2, "desktop": 1},
		Browsers: map[string]int64{"safari": 2, "chrome": 1},
	}
}

// Le cœur du magasin : appliquer deux fois le même bucket ADDITIONNE (c'est un cumul, pas une copie).
func TestMongoApplyCreatesThenAccumulates(t *testing.T) {
	coll := testCollection(t)
	store := NewMongo(coll)
	ctx := context.Background()

	if err := store.Apply(ctx, []Bucket{sampleBucket()}); err != nil {
		t.Fatalf("premier Apply() : %v", err)
	}
	doc := readDoc(t, coll, "site-42|2026-09-30T11:00Z")
	if doc.SiteID != "site-42" || !doc.Minute.Equal(time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC)) {
		t.Errorf("document créé = %+v, want site-42 à 11:00 UTC", doc)
	}
	if doc.Pageviews != 2 || doc.Clicks != 1 || doc.BotEvents != 3 || doc.Sessions != 1 {
		t.Errorf("compteurs = %+v, want 2 1 3 1", doc)
	}

	if err := store.Apply(ctx, []Bucket{sampleBucket()}); err != nil {
		t.Fatalf("second Apply() : %v", err)
	}
	doc = readDoc(t, coll, "site-42|2026-09-30T11:00Z")
	if doc.Pageviews != 4 || doc.Clicks != 2 || doc.BotEvents != 6 || doc.Sessions != 2 {
		t.Errorf("après cumul, compteurs = %+v, want 4 2 6 2", doc)
	}
	if doc.Devices["mobile"] != 4 || doc.Devices["desktop"] != 2 || doc.Browsers["safari"] != 4 {
		t.Errorf("répartitions = %v / %v, want mobile 4, desktop 2, safari 4", doc.Devices, doc.Browsers)
	}

	n, err := coll.CountDocuments(ctx, map[string]any{})
	if err != nil || n != 1 {
		t.Errorf("CountDocuments = %d, %v, want 1 seul document (l'upsert ne duplique pas)", n, err)
	}
}

func TestMongoApplyWritesOneDocumentPerBucket(t *testing.T) {
	coll := testCollection(t)
	store := NewMongo(coll)

	next := sampleBucket()
	next.Minute = next.Minute.Add(time.Minute)
	other := sampleBucket()
	other.SiteID = "site-43"

	if err := store.Apply(context.Background(), []Bucket{sampleBucket(), next, other}); err != nil {
		t.Fatalf("Apply() : %v", err)
	}

	n, err := coll.CountDocuments(context.Background(), map[string]any{})
	if err != nil || n != 3 {
		t.Errorf("CountDocuments = %d, %v, want 3", n, err)
	}
	readDoc(t, coll, "site-42|2026-09-30T11:01Z")
	readDoc(t, coll, "site-43|2026-09-30T11:00Z")
}

func TestMongoApplyWithNoBucketIsANoOp(t *testing.T) {
	if err := NewMongo(testCollection(t)).Apply(context.Background(), nil); err != nil {
		t.Errorf("Apply(nil) = %v, want nil", err)
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
