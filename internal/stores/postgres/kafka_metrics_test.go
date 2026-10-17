package postgres

import (
	"context"
	"os"
	"testing"

	"monstermq.io/edge/internal/stores/storetest"
)

// Runs against a real database when EDGE_TEST_POSTGRES_DSN is set.
func TestMetricsStoreKafkaClientKind(t *testing.T) {
	dsn := os.Getenv("EDGE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("EDGE_TEST_POSTGRES_DSN not set")
	}
	db, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ms := &MetricsStore{db: db}
	if err := ms.EnsureTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	storetest.CheckKafkaMetrics(t, ms)
}
