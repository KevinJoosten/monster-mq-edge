package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/stores"
	storecratedb "monstermq.io/edge/internal/stores/cratedb"
	storepg "monstermq.io/edge/internal/stores/postgres"
)

// Aggregation with field "" counts a payload only if it is a plain JSON number
// (monster-mq#207). A JSON object such as a WinCC OA tag {"time":..,"value":..},
// a string, a boolean or plain text is NULL: it moves neither AVG/MIN/MAX nor
// COUNT, and no backend fails on it.
var rawAggPayloads = []struct{ topic, payload string }{
	{"agg/mixed", "1"},
	{"agg/mixed", "3"},
	{"agg/mixed", `{"time":"2026-10-17T10:00:00.000Z","value":42.5}`},
	{"agg/other", " 5 "},
	{"agg/other", "-2.5e1"},
	{"agg/other", `"7"`},
	{"agg/other", "true"},
	{"agg/other", "[1]"},
	{"agg/other", "abc"},
	{"agg/other", "007"},
	{"agg/other", "+5"},
	{"agg/other", "NaN"},
}

// rawAggWant maps column -> value for field "" over rawAggPayloads.
var rawAggWant = map[string]float64{
	"agg/mixed_avg": 2, "agg/mixed_min": 1, "agg/mixed_max": 3, "agg/mixed_count": 2,
	"agg/other_avg": -10, "agg/other_min": -25, "agg/other_max": 5, "agg/other_count": 2,
}

func TestAggregatedRawField_SQLite(t *testing.T) {
	checkRawFieldAggregation(t, 22080, 24080, stores.ArchiveSQLite, nil, nil)
}

func TestAggregatedRawField_Postgres(t *testing.T) {
	dsn := os.Getenv("EDGE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("EDGE_TEST_POSTGRES_DSN not set")
	}
	conn := &stores.DatabaseConnectionConfig{Name: "AggPg", Type: stores.DatabaseConnectionPostgres, URL: dsn}
	checkRawFieldAggregation(t, 22081, 24081, stores.ArchivePostgres, conn, func(tables []string) {
		ctx := context.Background()
		db, err := storepg.Open(ctx, dsn)
		if err != nil {
			t.Logf("cleanup: %v", err)
			return
		}
		defer db.Close()
		for _, table := range tables {
			_, _ = db.Pool().Exec(ctx, "DROP TABLE IF EXISTS "+table)
		}
	})
}

func TestAggregatedRawField_MongoDB(t *testing.T) {
	uri := os.Getenv("EDGE_TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("EDGE_TEST_MONGODB_URI not set")
	}
	dbName := os.Getenv("EDGE_TEST_MONGODB_DB")
	if dbName == "" {
		dbName = "monstermq_test"
	}
	conn := &stores.DatabaseConnectionConfig{Name: "AggMongo", Type: stores.DatabaseConnectionMongoDB, URL: uri, Database: dbName}
	checkRawFieldAggregation(t, 22082, 24082, stores.ArchiveMongoDB, conn, func(collections []string) {
		ctx := context.Background()
		cli, err := mongo.Connect(options.Client().ApplyURI(uri))
		if err != nil {
			t.Logf("cleanup: %v", err)
			return
		}
		defer cli.Disconnect(ctx)
		for _, coll := range collections {
			_ = cli.Database(dbName).Collection(coll).Drop(ctx)
		}
	})
}

func TestAggregatedRawField_CrateDB(t *testing.T) {
	dsn := os.Getenv("MONSTERMQ_TEST_CRATEDB_URL")
	if dsn == "" {
		t.Skip("MONSTERMQ_TEST_CRATEDB_URL not set")
	}
	conn := &stores.DatabaseConnectionConfig{Name: "AggCrate", Type: stores.DatabaseConnectionCrateDB, URL: dsn}
	checkRawFieldAggregation(t, 22083, 24083, stores.ArchiveCrateDB, conn, func(tables []string) {
		ctx := context.Background()
		db, err := storecratedb.Open(ctx, dsn, "", "")
		if err != nil {
			t.Logf("cleanup: %v", err)
			return
		}
		defer db.Close()
		for _, table := range tables {
			_, _ = db.Pool().Exec(ctx, "DROP TABLE IF EXISTS "+table)
		}
	})
}

// checkRawFieldAggregation archives rawAggPayloads into one JSON and one
// DEFAULT format group (payload_json vs. payload_blob paths) and checks field
// "" on both, plus field "value" on the JSON group.
func checkRawFieldAggregation(t *testing.T, mqttPort, gqlPort int, archiveType stores.MessageArchiveType,
	conn *stores.DatabaseConnectionConfig, drop func(tables []string)) {
	srv, gqlURL := startWithGraphQL(t, mqttPort, gqlPort)
	defer srv.Close()
	ctx := context.Background()

	connName := ""
	if conn != nil {
		if err := srv.Storage().ArchiveConfig.SaveDatabaseConnection(ctx, *conn); err != nil {
			t.Fatalf("save database connection: %v", err)
		}
		connName = conn.Name
	}
	suffix := time.Now().UnixNano()
	groups := map[stores.PayloadFormat]string{
		stores.PayloadJSON:    fmt.Sprintf("AggJson_%d", suffix),
		stores.PayloadDefault: fmt.Sprintf("AggDefault_%d", suffix),
	}
	if drop != nil {
		tables := []string{}
		for _, g := range groups {
			tables = append(tables, strings.ToLower(archive.ArchiveName(g)))
		}
		defer drop(tables)
	}
	for format, name := range groups {
		if err := srv.Storage().ArchiveConfig.Save(ctx, stores.ArchiveGroupConfig{
			Name:                   name,
			Enabled:                true,
			TopicFilters:           []string{"agg/#"},
			LastValType:            stores.MessageStoreMemory,
			ArchiveType:            archiveType,
			DatabaseConnectionName: connName,
			PayloadFormat:          format,
		}); err != nil {
			t.Fatalf("save group %s: %v", name, err)
		}
	}
	if err := srv.Archives().Reload(ctx); err != nil {
		t.Fatalf("reload archives: %v", err)
	}

	client := mqtt.NewClient(mqttOpts(mqttPort, fmt.Sprintf("agg-raw-%d", mqttPort)))
	if tok := client.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	for _, p := range rawAggPayloads {
		if tok := client.Publish(p.topic, 1, false, p.payload); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
			t.Fatal(tok.Error())
		}
		time.Sleep(2 * time.Millisecond) // distinct (topic, time) keys
	}
	client.Disconnect(100)

	for format, group := range groups {
		t.Run(string(format), func(t *testing.T) {
			checkAggregated(t, gqlURL, group, []string{"agg/mixed", "agg/other"}, nil, rawAggWant)
			if format == stores.PayloadJSON {
				checkAggregated(t, gqlURL, group, []string{"agg/mixed"}, []string{"value"},
					map[string]float64{"agg/mixed.value_avg": 42.5, "agg/mixed.value_count": 1})
			}
		})
	}
}

// checkAggregated queries one ONE_DAY bucket until it holds the wanted
// values, so a slow archive flush or CrateDB refresh does not fail the test.
func checkAggregated(t *testing.T, gqlURL, group string, topics, fields []string, want map[string]float64) {
	t.Helper()
	now := time.Now().UTC()
	vars := map[string]any{
		"topics": topics,
		"start":  now.Add(-time.Hour).Format(time.RFC3339),
		"end":    now.Add(time.Hour).Format(time.RFC3339),
		"fields": fields,
		"group":  group,
	}
	const q = `query($topics: [String!]!, $start: String!, $end: String!, $fields: [String!], $group: String) {
		aggregatedMessages(topics: $topics, interval: ONE_DAY, startTime: $start, endTime: $end,
			functions: [AVG, MIN, MAX, COUNT], fields: $fields, archiveGroup: $group) { columns rows }
	}`
	deadline := time.Now().Add(10 * time.Second)
	for {
		res := gqlQuery(t, gqlURL, q, vars)["aggregatedMessages"].(map[string]any)
		columns, _ := res["columns"].([]any)
		rows, _ := res["rows"].([]any)
		if len(rows) > 1 {
			t.Fatalf("expected one ONE_DAY bucket, got %v", rows)
		}
		row := map[string]any{}
		if len(rows) == 1 {
			for i, v := range rows[0].([]any) {
				row[columns[i].(string)] = v
			}
		}
		var diffs []string
		for col, w := range want {
			if v, ok := row[col].(float64); !ok || v != w {
				diffs = append(diffs, fmt.Sprintf("%s = %v, want %v", col, row[col], w))
			}
		}
		if len(diffs) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %s (row %v)", group, strings.Join(diffs, "; "), row)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
