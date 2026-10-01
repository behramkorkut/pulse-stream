package loadgen

import (
	"context"
	"fmt"
	"regexp"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// MongoTotals lit les compteurs de la collection que remplit l'aggregator.
type MongoTotals struct {
	coll *mongo.Collection
}

// NewMongoTotals crée une source de totaux adossée à MongoDB.
func NewMongoTotals(coll *mongo.Collection) *MongoTotals { return &MongoTotals{coll: coll} }

// Totals additionne les compteurs de tous les documents (site, minute) dont le site commence par prefix.
func (m *MongoTotals) Totals(ctx context.Context, prefix string) (Totals, error) {
	sum := func(field string) bson.D { return bson.D{{Key: "$sum", Value: "$" + field}} }

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "site_id", Value: bson.D{{Key: "$regex", Value: "^" + regexp.QuoteMeta(prefix)}}}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "pageviews", Value: sum("pageviews")},
			{Key: "clicks", Value: sum("clicks")},
			{Key: "bots", Value: sum("bot_events")},
		}}},
	}

	cur, err := m.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return Totals{}, fmt.Errorf("mongo aggregate: %w", err)
	}
	var rows []struct {
		Pageviews int64 `bson:"pageviews"`
		Clicks    int64 `bson:"clicks"`
		Bots      int64 `bson:"bots"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return Totals{}, fmt.Errorf("mongo read: %w", err)
	}
	if len(rows) == 0 {
		return Totals{}, nil // rien encore : le traitement n'a pas commencé
	}
	return Totals{Pageviews: rows[0].Pageviews, Clicks: rows[0].Clicks, Bots: rows[0].Bots}, nil
}
