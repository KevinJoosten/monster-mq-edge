package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"monstermq.io/edge/internal/stores"
)

// UnsMappingStore keeps UNS Modeler mapping documents as raw JSON text.
type UnsMappingStore struct{ db *DB }

var _ stores.UnsMappingStore = (*UnsMappingStore)(nil)

func (u *UnsMappingStore) Close() error            { return nil }
func (u *UnsMappingStore) coll() *mongo.Collection { return u.db.db.Collection("unsmappings") }

func (u *UnsMappingStore) EnsureTable(ctx context.Context) error {
	_, err := u.coll().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "device", Value: 1}, {Key: "slot", Value: 1}}, Options: options.Index().SetUnique(true),
	})
	return err
}

func (u *UnsMappingStore) Get(ctx context.Context, device, slot string) (json.RawMessage, error) {
	var row struct {
		Document string `bson:"document"`
	}
	err := u.coll().FindOne(ctx, bson.M{"device": device, "slot": slot}).Decode(&row)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(row.Document), nil
}

func (u *UnsMappingStore) Save(ctx context.Context, device, slot string, doc json.RawMessage) error {
	_, err := u.coll().UpdateOne(ctx, bson.M{"device": device, "slot": slot},
		bson.M{"$set": bson.M{"document": string(doc), "updated_at": time.Now().UTC()}},
		options.UpdateOne().SetUpsert(true))
	return err
}

func (u *UnsMappingStore) Delete(ctx context.Context, device string) error {
	_, err := u.coll().DeleteMany(ctx, bson.M{"device": device})
	return err
}
