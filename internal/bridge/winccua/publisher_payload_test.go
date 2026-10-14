package winccua

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGraphQLPublishTagValueOmitsQualityWhenIncludeQualityDisabled(t *testing.T) {
	pub := newPublisher("ns", TransformConfig{}, FormatJSONISO)
	var captured []byte

	conn := &graphqlConnector{
		cfg:     &ConnectionConfig{},
		pub:     pub,
		publish: func(_ string, payload []byte, _ bool, _ byte) error { captured = append([]byte(nil), payload...); return nil },
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	conn.publishTagValue(Address{
		Topic:          "tags",
		Retained:       false,
		IncludeQuality: false,
	}, "Pump.Speed", 12, "2026-01-01T00:00:00Z", map[string]any{"quality": "Unknown"}, map[string]any{"Name": "Pump.Speed", "Value": 12})

	out := map[string]any{}
	if err := json.Unmarshal(captured, &out); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if _, ok := out["quality"]; ok {
		t.Fatalf("expected quality to be omitted, got payload %s", string(captured))
	}
}

func TestPipePublishTagValueOmitsQualityWhenIncludeQualityDisabled(t *testing.T) {
	pub := newPublisher("ns", TransformConfig{}, FormatJSONISO)
	var captured []byte

	conn := &pipeConnector{
		cfg:     &ConnectionConfig{},
		pub:     pub,
		publish: func(_ string, payload []byte, _ bool, _ byte) error { captured = append([]byte(nil), payload...); return nil },
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	conn.publishTagValue(Address{
		Topic:          "tags",
		Retained:       false,
		IncludeQuality: false,
	}, "Pump.Speed", 12, "2026-01-01T00:00:00Z", map[string]any{"quality": "Unknown"}, map[string]any{"Name": "Pump.Speed", "Value": 12})

	out := map[string]any{}
	if err := json.Unmarshal(captured, &out); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if _, ok := out["quality"]; ok {
		t.Fatalf("expected quality to be omitted, got payload %s", string(captured))
	}
}

func TestPipePublishTagValueIncludesFlatQualityWhenIncludeQualityEnabled(t *testing.T) {
	pub := newPublisher("ns", TransformConfig{}, FormatJSONISO)
	var captured []byte

	conn := &pipeConnector{
		cfg:     &ConnectionConfig{},
		pub:     pub,
		publish: func(_ string, payload []byte, _ bool, _ byte) error { captured = append([]byte(nil), payload...); return nil },
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	rawTag := map[string]any{
		"Name":             "Pump.Speed",
		"Quality":          "Good",
		"QualityCode":      "192",
		"TimeStamp":        "2026-08-20 11:54:49.3160000",
		"Value":            "2179.176758",
		"ErrorCode":        0,
		"ErrorDescription": "",
	}

	conn.publishTagValue(Address{
		Topic:          "tags",
		Retained:       false,
		IncludeQuality: true,
	}, "Pump.Speed", "2179.176758", "2026-08-20 11:54:49.3160000", map[string]any{"quality": "Good", "qualityCode": "192"}, rawTag)

	out := map[string]any{}
	if err := json.Unmarshal(captured, &out); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if out["quality"] != "Good" {
		t.Fatalf("expected quality='Good', got %v (payload: %s)", out["quality"], string(captured))
	}
	if out["qualityCode"] != "192" {
		t.Fatalf("expected qualityCode='192', got %v (payload: %s)", out["qualityCode"], string(captured))
	}
	if out["value"] != "2179.176758" {
		t.Fatalf("expected value='2179.176758', got %v", out["value"])
	}
	if out["time"] != "2026-08-20 11:54:49.3160000" {
		t.Fatalf("expected time='2026-08-20 11:54:49.3160000', got %v", out["time"])
	}
}

func TestPipePublishTagValueFormatRawJSON(t *testing.T) {
	pub := newPublisher("ns", TransformConfig{}, FormatRawJSON)
	var captured []byte

	conn := &pipeConnector{
		cfg:     &ConnectionConfig{},
		pub:     pub,
		publish: func(_ string, payload []byte, _ bool, _ byte) error { captured = append([]byte(nil), payload...); return nil },
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	rawTag := map[string]any{
		"Name":             "Tag_0",
		"Quality":          "Good",
		"QualityCode":      "192",
		"TimeStamp":        "2019-01-30T11:25:35Z",
		"Value":            "16",
		"ErrorCode":        float64(0),
		"ErrorDescription": "",
	}

	conn.publishTagValue(Address{
		Topic:          "tags",
		Retained:       false,
		IncludeQuality: true,
	}, "Tag_0", "16", "2019-01-30T11:25:35Z", map[string]any{"quality": "Good", "qualityCode": "192"}, rawTag)

	out := map[string]any{}
	if err := json.Unmarshal(captured, &out); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if out["Name"] != "Tag_0" || out["Value"] != "16" || out["Quality"] != "Good" || out["QualityCode"] != "192" {
		t.Fatalf("expected raw tag 1:1, got: %s", string(captured))
	}
}

func TestExtractWriteData_RawAndJSON(t *testing.T) {
	// 1. Raw 1:1 for GraphQL (coerced scalar) and OpenPipe (raw string)
	rawAddr := Address{Type: AddressTypeTagWrite, Topic: "cmd/t1", TagName: "Tag_1"}
	valGQL, tsGQL, err := extractWriteData([]byte("42.5"), rawAddr, true)
	if err != nil || valGQL != float64(42.5) || tsGQL != "" {
		t.Fatalf("raw GraphQL: got val=%v (%T) ts=%q err=%v", valGQL, valGQL, tsGQL, err)
	}
	valPipe, tsPipe, err := extractWriteData([]byte("42.50"), rawAddr, false)
	if err != nil || valPipe != "42.50" || tsPipe != "" {
		t.Fatalf("raw OpenPipe: got val=%v (%T) ts=%q err=%v", valPipe, valPipe, tsPipe, err)
	}

	// 2. JSON path with ISO timestamp
	isoAddr := Address{
		Type:              AddressTypeTagWrite,
		Topic:             "cmd/t2",
		TagName:           "Tag_2",
		JsonPath:          "data.temperature.value",
		TimestampJsonPath: "meta.ts",
		TimestampFormat:   TimestampFormatISO,
	}
	payloadISO := []byte(`{"data":{"temperature":{"value":23.75}},"meta":{"ts":"2026-06-06T14:00:00+02:00"}}`)
	val, ts, err := extractWriteData(payloadISO, isoAddr, true)
	if err != nil {
		t.Fatalf("json ISO: %v", err)
	}
	if formatPipeValue(val) != "23.75" {
		t.Errorf("json ISO value: got %v", val)
	}
	if ts != "2026-06-06T12:00:00Z" {
		t.Errorf("json ISO timestamp: got %q want 2026-06-06T12:00:00Z", ts)
	}

	// 3. JSON path with MS timestamp
	msAddr := Address{
		Type:              AddressTypeTagWrite,
		Topic:             "cmd/t3",
		TagName:           "Tag_3",
		JsonPath:          "value",
		TimestampJsonPath: "time",
		TimestampFormat:   TimestampFormatMS,
	}
	payloadMS := []byte(`{"value":true,"time":1749211200500}`)
	val, ts, err = extractWriteData(payloadMS, msAddr, true)
	if err != nil {
		t.Fatalf("json MS: %v", err)
	}
	if val != true {
		t.Errorf("json MS value: got %v", val)
	}
	if ts != "2025-06-06T12:00:00.500Z" {
		t.Errorf("json MS timestamp: got %q want 2025-06-06T12:00:00.500Z", ts)
	}
}

func TestGraphQLWriteTagValueSendsMutation(t *testing.T) {
	var receivedQuery string
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		body := map[string]any{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		receivedQuery, _ = body["query"].(string)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"writeTagValues":[{"name":"HMI_Tag_1","error":{"code":"0","description":""}}]}}`))
	}))
	defer srv.Close()

	conn := &graphqlConnector{
		name:       "dev1",
		cfg:        &ConnectionConfig{GraphqlEndpoint: srv.URL},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		authToken:  "tok-123",
		httpClient: srv.Client(),
	}

	addr := Address{
		Type:              AddressTypeTagWrite,
		Topic:             "plant/setpoint",
		TagName:           "HMI_Tag_1",
		JsonPath:          "payload.val",
		TimestampJsonPath: "payload.ts",
		TimestampFormat:   TimestampFormatMS,
	}
	conn.writeTagValue(context.Background(), addr, []byte(`{"payload":{"val":99.5,"ts":1749211200000}}`))

	if receivedAuth != "Bearer tok-123" {
		t.Errorf("expected Bearer tok-123, got %q", receivedAuth)
	}
	wantSubstr := `writeTagValues(input: [{ name: "HMI_Tag_1", value: 99.5, timestamp: "2025-06-06T12:00:00.000Z" }])`
	if !strings.Contains(receivedQuery, wantSubstr) {
		t.Errorf("query %q did not contain %q", receivedQuery, wantSubstr)
	}
}

type nopCloserBuffer struct {
	bytes.Buffer
}

func (nopCloserBuffer) Close() error { return nil }

func TestPipeWriteTagValueSendsWriteTagCommand(t *testing.T) {
	buf := &nopCloserBuffer{}
	conn := &pipeConnector{
		name:   "dev1",
		cfg:    &ConnectionConfig{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		conn:   buf,
	}

	addr := Address{
		Type:     AddressTypeTagWrite,
		Topic:    "plant/setpoint",
		TagName:  "HMI_Tag_1",
		JsonPath: "data.sp",
	}
	conn.writeTagValue(context.Background(), addr, []byte(`{"data":{"sp":123.45}}`))

	line := strings.TrimSpace(buf.String())
	msg := map[string]any{}
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		t.Fatalf("unmarshal pipe command %q: %v", line, err)
	}
	if msg["Message"] != "WriteTag" {
		t.Errorf("expected Message=WriteTag, got %v", msg["Message"])
	}
	params, _ := msg["Params"].(map[string]any)
	tags, _ := params["Tags"].([]any)
	if len(tags) != 1 {
		t.Fatalf("expected 1 tag in Params.Tags, got %v", tags)
	}
	tag0, _ := tags[0].(map[string]any)
	if tag0["Name"] != "HMI_Tag_1" || tag0["Value"] != "123.45" {
		t.Errorf("unexpected tag entry: %v", tag0)
	}
}


