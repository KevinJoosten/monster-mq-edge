package resolvers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"monstermq.io/edge/internal/bridge/kafkaclient"
	"monstermq.io/edge/internal/graphql/generated"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/stores/memory"
)

func kafkaInput(cfg *generated.KafkaClientConfigInput) generated.KafkaClientInput {
	return generated.KafkaClientInput{Name: "k1", Namespace: "orders", NodeID: "edge", Config: cfg}
}

// The prefix is stored as entered, like Core's mutation, and normalised when read.
func TestKafkaCreateStoresCoreShapedConfig(t *testing.T) {
	format := generated.KafkaPayloadFormatJSON
	raw, errs := validateKafkaInput(kafkaInput(&generated.KafkaClientConfigInput{
		PayloadFormat:          &format,
		DestinationTopicPrefix: sp("plant"),
		OutboundKafkaTopic:     sp("  "),
		ExtraConsumerConfig:    map[string]any{"auto.offset.reset": "earliest", "max.poll.records": 100},
	}))
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	var stored map[string]any
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatal(err)
	}
	if stored["destinationTopicPrefix"] != "plant" || stored["outboundKafkaTopic"] != nil || stored["payloadFormat"] != "JSON" ||
		stored["bootstrapServers"] != "localhost:9092" || stored["reconnectDelayMs"] != float64(5000) {
		t.Fatalf("stored config = %s", raw)
	}

	r := &Resolver{Logger: slog.New(slog.DiscardHandler), NodeID: "edge"}
	c := r.deviceToKafkaClient(stores.DeviceConfig{Name: "k1", Namespace: "orders", NodeID: "edge", Type: kafkaclient.DeviceTypeKafkaClient, Config: raw})
	if *c.Config.DestinationTopicPrefix != "plant/" || c.Config.ExtraConsumerConfig["max.poll.records"] != "100" || !c.IsOnCurrentNode {
		t.Fatalf("query view = %+v", c.Config)
	}
}

func TestKafkaCreateValidation(t *testing.T) {
	low := int64(100)
	outbound := true
	_, errs := validateKafkaInput(kafkaInput(&generated.KafkaClientConfigInput{
		ReconnectDelayMs:    &low,
		OutboundEnabled:     &outbound,
		TopicKeyRegex:       sp("("),
		ExtraProducerConfig: map[string]any{"sasl.kerberos.service.name": "kafka", "transactional.id": "t"},
	}))
	joined := strings.Join(errs, "|")
	for _, want := range []string{"reconnectDelayMs", "outboundTopicFilters", "topicKeyRegex"} {
		if !strings.Contains(joined, want) {
			t.Errorf("errors %v miss %s", errs, want)
		}
	}

	_, errs = validateKafkaInput(kafkaInput(&generated.KafkaClientConfigInput{DestinationTopicPrefix: sp("a/#")}))
	if len(errs) != 1 || !strings.HasPrefix(errs[0], "Invalid Kafka config: ") {
		t.Fatalf("wildcard prefix errors = %v", errs)
	}
}

func TestKafkaFeatureDisabledError(t *testing.T) {
	if res := kafkaDisabledResult(); res.Success || res.Errors[0] != "Kafka feature is not enabled on this node" {
		t.Fatalf("disabled result = %+v", res)
	}
}

// Kafka client properties are never refused, as in Core.
func TestKafkaCreateAcceptsAnyKafkaProperty(t *testing.T) {
	_, errs := validateKafkaInput(kafkaInput(&generated.KafkaClientConfigInput{
		ExtraConsumerConfig: map[string]any{"sasl.mechanism": "GSSAPI", "check.crcs": true},
		ExtraProducerConfig: map[string]any{"transactional.id": "t", "partitioner.class": "x.Y"},
	}))
	if len(errs) > 0 {
		t.Fatalf("errors = %v, want none", errs)
	}
	for _, name := range []string{"bad name", "ok_name-1"} {
		in := kafkaInput(nil)
		in.Name = name
		_, errs := validateKafkaInput(in)
		if bad := strings.Contains(strings.Join(errs, "|"), "letters, numbers"); bad != (name == "bad name") {
			t.Errorf("name %q: errors %v", name, errs)
		}
	}
}

// Metrics and history follow Core: newest stored sample of the last minute,
// rates rounded to two decimals, history only with a range, newest first.
func TestKafkaMetricsMatchCore(t *testing.T) {
	ms := memory.NewMetricsStore(100)
	r := &Resolver{Storage: &stores.Storage{Metrics: ms}, Logger: slog.New(slog.DiscardHandler)}
	kr := &kafkaClientResolver{r}
	ctx := context.Background()
	now := time.Now().UTC()
	for i, rate := range []float64{1.234, 2.345, 3.456} {
		ts := now.Add(time.Duration(i-2) * 20 * time.Second)
		payload := fmt.Sprintf(`{"messagesIn":%v,"messagesOut":0.005,"timestamp":%q}`, rate, ts.Format(time.RFC3339Nano))
		if err := ms.StoreMetrics(ctx, stores.MetricKafkaClient, ts, "k1", payload); err != nil {
			t.Fatal(err)
		}
	}
	obj := &generated.KafkaClient{Name: "k1"}
	m, err := kr.Metrics(ctx, obj)
	if err != nil || len(m) != 1 || m[0].MessagesIn != 3.46 || m[0].MessagesOut != 0 {
		t.Fatalf("metrics = %+v err=%v", m, err)
	}
	h, err := kr.MetricsHistory(ctx, obj, nil, nil, nil)
	if err != nil || len(h) != 0 {
		t.Fatalf("history without range = %+v err=%v", h, err)
	}
	five := 5
	h, err = kr.MetricsHistory(ctx, obj, nil, nil, &five)
	if err != nil || len(h) != 3 || h[0].MessagesIn != 3.46 || h[2].MessagesIn != 1.23 {
		t.Fatalf("history = %+v err=%v", h, err)
	}
	bad := "yesterday"
	if _, err := kr.MetricsHistory(ctx, obj, &bad, nil, nil); err == nil {
		t.Fatal("invalid from accepted")
	}
	if m, _ := kr.Metrics(ctx, &generated.KafkaClient{Name: "none"}); len(m) != 1 || m[0].MessagesIn != 0 {
		t.Fatalf("unknown client metrics = %+v", m)
	}
}
