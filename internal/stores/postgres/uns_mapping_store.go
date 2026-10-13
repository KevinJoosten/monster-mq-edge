package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"monstermq.io/edge/internal/stores"
)

// UnsMappingStore keeps UNS Modeler mapping documents as raw JSON text.
type UnsMappingStore struct{ db *DB }

var _ stores.UnsMappingStore = (*UnsMappingStore)(nil)

func (u *UnsMappingStore) Close() error { return nil }

func (u *UnsMappingStore) EnsureTable(ctx context.Context) error {
	_, err := u.db.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS unsmappings (
            device TEXT NOT NULL,
            slot TEXT NOT NULL,
            document TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (device, slot)
        )`)
	return err
}

func (u *UnsMappingStore) Get(ctx context.Context, device, slot string) (json.RawMessage, error) {
	var doc string
	err := u.db.pool.QueryRow(ctx, `SELECT document FROM unsmappings WHERE device=$1 AND slot=$2`, device, slot).Scan(&doc)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(doc), nil
}

func (u *UnsMappingStore) Save(ctx context.Context, device, slot string, doc json.RawMessage) error {
	_, err := u.db.pool.Exec(ctx, `INSERT INTO unsmappings (device, slot, document, updated_at) VALUES ($1, $2, $3, now())
        ON CONFLICT (device, slot) DO UPDATE SET document = EXCLUDED.document, updated_at = now()`, device, slot, string(doc))
	return err
}

func (u *UnsMappingStore) Delete(ctx context.Context, device string) error {
	_, err := u.db.pool.Exec(ctx, `DELETE FROM unsmappings WHERE device=$1`, device)
	return err
}
