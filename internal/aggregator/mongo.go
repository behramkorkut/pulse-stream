package aggregator

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Store écrit des compteurs. Apply ADDITIONNE les compteurs reçus à ceux déjà stockés.
type Store interface {
	Apply(ctx context.Context, buckets []Bucket) error
}

// Mongo est un Store adossé à une collection MongoDB : un document par site et par minute.
type Mongo struct {
	coll *mongo.Collection
}

// NewMongo crée un Store MongoDB.
func NewMongo(coll *mongo.Collection) *Mongo {
	return &Mongo{coll: coll}
}

// EnsureIndexes crée l'index qui sert aux lectures "les dernières minutes d'un site".
// L'identifiant (_id, un par site et par minute) est déjà indexé par MongoDB.
func (m *Mongo) EnsureIndexes(ctx context.Context) error {
	_, err := m.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "site_id", Value: 1}, {Key: "minute", Value: -1}},
	})
	if err != nil {
		return fmt.Errorf("mongo create index: %w", err)
	}
	return nil
}

// Apply cumule les compteurs de chaque minute avec un upsert : le document est créé au premier
// événement de la minute, puis incrémenté ($inc). Tous les buckets d'un lot partent en une seule
// requête réseau (BulkWrite).
//
// Limite assumée : $inc n'est pas idempotent. Si un appel échoue après avoir été appliqué en partie,
// puis est retenté, des compteurs peuvent être comptés deux fois. Éviter cela exigerait des transactions
// (Mongo en jeu de répliques) ou un état idempotent par construction. Voir docs/architecture.md.
func (m *Mongo) Apply(ctx context.Context, buckets []Bucket) error {
	if len(buckets) == 0 {
		return nil
	}

	models := make([]mongo.WriteModel, 0, len(buckets))
	for _, b := range buckets {
		// Les quatre compteurs sont toujours présents (même à zéro) : tous les documents ont le même schéma.
		inc := bson.M{
			"pageviews":  b.Pageviews,
			"clicks":     b.Clicks,
			"bot_events": b.BotEvents,
			"sessions":   b.Sessions,
		}
		for name, n := range b.Devices {
			inc["devices."+name] = n
		}
		for name, n := range b.Browsers {
			inc["browsers."+name] = n
		}

		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": b.ID()}).
			SetUpdate(bson.M{
				"$setOnInsert": bson.M{"site_id": b.SiteID, "minute": b.Minute},
				"$inc":         inc,
			}).
			SetUpsert(true))
	}

	if _, err := m.coll.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("mongo bulk write: %w", err)
	}
	return nil
}
