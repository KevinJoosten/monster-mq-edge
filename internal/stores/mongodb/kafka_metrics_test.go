package mongodb

import (
	"context"
	"os"
	"testing"

	"monstermq.io/edge/internal/stores/storetest"
)

// Runs against a real database when EDGE_TEST_MONGODB_URI is set
// (EDGE_TEST_MONGODB_DB names the database, default monstermq_test).
func TestMetricsStoreKafkaClientKind(t *testing.T) {
	uri := os.Getenv("EDGE_TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("EDGE_TEST_MONGODB_URI not set")
	}
	name := os.Getenv("EDGE_TEST_MONGODB_DB")
	if name == "" {
		name = "monstermq_test"
	}
	db, err := Open(context.Background(), uri, name)
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
