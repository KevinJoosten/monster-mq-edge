package oastore

import (
	"context"
	"encoding/json"

	"monstermq.io/edge/internal/stores"
)

const kindUnsMapping = "unsmapping"

// UnsMappingStore keeps UNS Modeler mapping documents in MMQConfigs records
// of their own kind; the envelope data is the document itself.
type UnsMappingStore struct{ r *recordSet }

var _ stores.UnsMappingStore = (*UnsMappingStore)(nil)

func (u *UnsMappingStore) Close() error { return nil }

func unsKey(device, slot string) string { return slot + ":" + device }

func (u *UnsMappingStore) Get(ctx context.Context, device, slot string) (json.RawMessage, error) {
	var doc json.RawMessage
	ok, err := u.r.get(ctx, kindUnsMapping, unsKey(device, slot), &doc)
	if err != nil || !ok {
		return nil, err
	}
	return doc, nil
}

func (u *UnsMappingStore) Save(ctx context.Context, device, slot string, doc json.RawMessage) error {
	return u.r.put(ctx, kindUnsMapping, unsKey(device, slot), doc, typeValue(kindUnsMapping))
}

func (u *UnsMappingStore) Delete(ctx context.Context, device string) error {
	for _, slot := range []string{stores.UnsSlotDraft, stores.UnsSlotApplied} {
		if err := u.r.del(ctx, kindUnsMapping, unsKey(device, slot)); err != nil {
			return err
		}
	}
	return nil
}
