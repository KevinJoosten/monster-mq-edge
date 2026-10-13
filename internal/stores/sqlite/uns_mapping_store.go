package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"monstermq.io/edge/internal/stores"
)

const unsMappingTable = "unsmappings"

// UnsMappingStore keeps UNS Modeler mapping documents as raw JSON text.
type UnsMappingStore struct{ db *DB }

var _ stores.UnsMappingStore = (*UnsMappingStore)(nil)

func NewUnsMappingStore(db *DB) *UnsMappingStore { return &UnsMappingStore{db: db} }

func (u *UnsMappingStore) Close() error { return nil }

func (u *UnsMappingStore) EnsureTable(ctx context.Context) error {
	_, err := u.db.Exec(`CREATE TABLE IF NOT EXISTS ` + unsMappingTable + ` (
            device TEXT NOT NULL,
            slot TEXT NOT NULL,
            document TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (device, slot)
        )`)
	return err
}

func (u *UnsMappingStore) Get(ctx context.Context, device, slot string) (json.RawMessage, error) {
	var doc string
	err := u.db.Conn().QueryRowContext(ctx, `SELECT document FROM `+unsMappingTable+` WHERE device = ? AND slot = ?`, device, slot).Scan(&doc)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(doc), nil
}

func (u *UnsMappingStore) Save(ctx context.Context, device, slot string, doc json.RawMessage) error {
	_, err := u.db.Exec(`INSERT INTO `+unsMappingTable+` (device, slot, document, updated_at) VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT (device, slot) DO UPDATE SET document = excluded.document, updated_at = datetime('now')`, device, slot, string(doc))
	return err
}

func (u *UnsMappingStore) Delete(ctx context.Context, device string) error {
	_, err := u.db.Exec(`DELETE FROM `+unsMappingTable+` WHERE device = ?`, device)
	return err
}
