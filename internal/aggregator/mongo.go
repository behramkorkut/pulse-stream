package aggregator

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// PositionsCollection garde, pour chaque partition Kafka, l'offset du dernier message dont les compteurs sont écrits.
const PositionsCollection = "kafka_positions"

// codeNamespaceExists : MongoDB < 7 refuse de recréer une collection existante avec ce code.
const codeNamespaceExists = 48

// Mongo est un Store adossé à MongoDB : un document par site et par minute, et un document de position par
// partition Kafka. Il exige un jeu de répliques (même d'un seul nœud) : les transactions n'existent pas sur une
// instance isolée.
type Mongo struct {
	coll      *mongo.Collection
	positions *mongo.Collection
}

// NewMongo crée un Store MongoDB. Les positions sont rangées dans la même base que les compteurs.
func NewMongo(coll *mongo.Collection) *Mongo {
	return &Mongo{coll: coll, positions: coll.Database().Collection(PositionsCollection)}
}

// EnsureIndexes crée l'index qui sert aux lectures "les dernières minutes d'un site", ainsi que la collection des
// positions (la créer à l'avance évite de dépendre de sa création implicite à l'intérieur d'une transaction).
// L'identifiant (_id, un par site et par minute) est déjà indexé par MongoDB.
func (m *Mongo) EnsureIndexes(ctx context.Context) error {
	_, err := m.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "site_id", Value: 1}, {Key: "minute", Value: -1}},
	})
	if err != nil {
		return fmt.Errorf("mongo create index: %w", err)
	}

	err = m.positions.Database().CreateCollection(ctx, PositionsCollection)
	var serverErr mongo.ServerError
	if err != nil && !(errors.As(err, &serverErr) && serverErr.HasErrorCode(codeNamespaceExists)) {
		return fmt.Errorf("mongo create collection %s: %w", PositionsCollection, err)
	}
	return nil
}

type positionDoc struct {
	ID     string `bson:"_id"`
	Offset int64  `bson:"offset"`
}

func positionID(p Partition) string { return p.Topic + "|" + strconv.Itoa(p.ID) }

// Apply écrit, dans UNE transaction : la lecture des positions, les compteurs des seuls événements non encore
// couverts, puis les nouvelles positions.
//
// Deux instances qui traitent le même lot (rééquilibrage) lisent la même position et veulent toutes deux l'avancer :
// MongoDB détecte le conflit d'écriture sur le document de position, annule l'une des deux transactions, et
// WithTransaction la rejoue. Au second passage, elle lit la position avancée et n'ajoute rien.
func (m *Mongo) Apply(ctx context.Context, events []Counted, upTo map[Partition]int64) (Applied, error) {
	if len(upTo) == 0 {
		return Applied{}, nil
	}

	sess, err := m.coll.Database().Client().StartSession()
	if err != nil {
		return Applied{}, fmt.Errorf("mongo start session: %w", err)
	}
	defer sess.EndSession(ctx)

	txn := options.Transaction().SetWriteConcern(writeconcern.Majority())
	res, err := sess.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		// La fonction peut être rejouée (conflit, erreur transitoire) : elle ne modifie rien hors de la transaction.
		stored, err := m.readPositions(ctx, upTo)
		if err != nil {
			return nil, err
		}
		fresh := NotYetApplied(events, stored)
		buckets := Aggregate(fresh)
		if err := m.writeCounters(ctx, buckets); err != nil {
			return nil, err
		}
		if err := m.writePositions(ctx, Advanced(upTo, stored)); err != nil {
			return nil, err
		}
		return Applied{Events: fresh, Buckets: len(buckets)}, nil
	}, txn)
	if err != nil {
		return Applied{}, fmt.Errorf("mongo transaction: %w", err)
	}
	return res.(Applied), nil
}

// readPositions lit la position enregistrée des partitions du lot (absente : la partition n'a encore rien écrit).
func (m *Mongo) readPositions(ctx context.Context, upTo map[Partition]int64) (map[Partition]int64, error) {
	ids := make([]string, 0, len(upTo))
	byID := make(map[string]Partition, len(upTo))
	for p := range upTo {
		ids = append(ids, positionID(p))
		byID[positionID(p)] = p
	}

	cur, err := m.positions.Find(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return nil, fmt.Errorf("mongo read positions: %w", err)
	}
	var docs []positionDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("mongo read positions: %w", err)
	}

	stored := make(map[Partition]int64, len(docs))
	for _, d := range docs {
		stored[byID[d.ID]] = d.Offset
	}
	return stored, nil
}

// writeCounters cumule les compteurs de chaque minute avec un upsert : le document est créé au premier événement
// de la minute, puis incrémenté ($inc). Tous les buckets partent en une seule requête (BulkWrite).
func (m *Mongo) writeCounters(ctx context.Context, buckets []Bucket) error {
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

// writePositions enregistre les positions qui avancent. Une écriture réelle sur le document de position est ce qui
// fait détecter le conflit entre deux transactions concurrentes sur la même partition.
func (m *Mongo) writePositions(ctx context.Context, advanced map[Partition]int64) error {
	if len(advanced) == 0 {
		return nil
	}

	models := make([]mongo.WriteModel, 0, len(advanced))
	for p, offset := range advanced {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": positionID(p)}).
			SetUpdate(bson.M{"$set": bson.M{"offset": offset, "topic": p.Topic, "partition": p.ID}}).
			SetUpsert(true))
	}
	if _, err := m.positions.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("mongo write positions: %w", err)
	}
	return nil
}
