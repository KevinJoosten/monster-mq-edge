// Package storetest holds checks shared by the store backends' tests.
package storetest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"monstermq.io/edge/internal/stores"
)

// CheckKafkaMetrics stores and reads back Kafka client metrics under Core's
// metric kind "kafka", next to another kind for the same identifier.
func CheckKafkaMetrics(t *testing.T, ms stores.MetricsStore) {
	t.Helper()
	ctx := context.Background()
	if stores.MetricKafkaClient != "kafka" {
		t.Fatalf("MetricKafkaClient = %q, want Core's \"kafka\"", stores.MetricKafkaClient)
	}
	id := fmt.Sprintf("kc-%d", time.Now().UnixNano())
	ts := time.Now().UTC().Truncate(time.Millisecond)
	if err := ms.StoreMetrics(ctx, stores.MetricKafkaClient, ts, id, `{"messagesIn":2.5,"messagesOut":1,"messagesDropped":3}`); err != nil {
		t.Fatal(err)
	}
	if err := ms.StoreMetrics(ctx, stores.MetricMqttClient, ts, id, `{"messagesIn":9}`); err != nil {
		t.Fatal(err)
	}
	h, err := ms.GetHistory(ctx, stores.MetricKafkaClient, id, ts.Add(-time.Minute), ts.Add(time.Minute), 10)
	if err != nil || len(h) != 1 {
		t.Fatalf("kafka history = %+v err=%v", h, err)
	}
	var got struct {
		MessagesIn      float64 `json:"messagesIn"`
		MessagesDropped float64 `json:"messagesDropped"`
	}
	if err := json.Unmarshal([]byte(h[0].Payload), &got); err != nil || got.MessagesIn != 2.5 || got.MessagesDropped != 3 {
		t.Fatalf("kafka payload = %s (%v)", h[0].Payload, err)
	}
	if _, payload, err := ms.GetLatest(ctx, stores.MetricKafkaClient, id); err != nil || payload == "" {
		t.Fatalf("latest kafka = %q err=%v", payload, err)
	}
}
