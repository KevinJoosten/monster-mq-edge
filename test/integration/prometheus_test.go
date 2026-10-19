package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/crypto/bcrypt"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
	storepg "monstermq.io/edge/internal/stores/postgres"
	storequestdb "monstermq.io/edge/internal/stores/questdb"
)

func startWithPrometheus(t *testing.T, mqttPort, promPort int, cfgFns ...func(*config.Config)) (*broker.Server, string) {
	t.Helper()
	cfg := config.Default()
	cfg.NodeID = fmt.Sprintf("p-%d", promPort)
	cfg.TCP.Enabled = true
	cfg.TCP.Port = mqttPort
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Prometheus.Enabled = true
	cfg.Prometheus.Port = promPort
	cfg.SQLite.Path = filepath.Join(t.TempDir(), "p.db")
	for _, fn := range cfgFns {
		fn(cfg)
	}
	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	go func() { _ = srv.Serve() }()
	baseURL := fmt.Sprintf("http://localhost:%d", promPort)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusUnauthorized {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	return srv, baseURL
}

func addHistGroup(t *testing.T, srv *broker.Server) {
	t.Helper()
	ctx := context.Background()
	if err := srv.Storage().ArchiveConfig.Save(ctx, stores.ArchiveGroupConfig{
		Name:         "Hist",
		Enabled:      true,
		TopicFilters: []string{"#"},
		LastValType:  stores.MessageStoreSQLite,
		ArchiveType:  stores.ArchiveSQLite,
	}); err != nil {
		t.Fatalf("save Hist group: %v", err)
	}
	if err := srv.Archives().Reload(ctx); err != nil {
		t.Fatalf("reload archives: %v", err)
	}
}

func promGet(t *testing.T, rawURL string, hdrs map[string]string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, string(b)
}

func promPostForm(t *testing.T, rawURL string, vals url.Values, hdrs map[string]string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, rawURL, strings.NewReader(vals.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, string(b)
}

func TestPrometheusRootScrapeLabelsSeriesAndInstant(t *testing.T) {
	mqttPort := 22301
	promPort := 25301
	srv, promURL := startWithPrometheus(t, mqttPort, promPort)
	defer srv.Close()

	// Root
	code, hdr, body := promGet(t, promURL+"/", nil)
	if code != 200 {
		t.Fatalf("GET / status = %d, want 200", code)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("GET / Content-Type = %q, want application/json", ct)
	}
	if body != `{"status":"ok","message":"MonsterMQ Prometheus API"}` {
		t.Fatalf("GET / body = %q", body)
	}

	// Publish test topics
	pub := mqtt.NewClient(mqttOpts(mqttPort, "prom-pub-1"))
	if tok := pub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	msgs := []struct {
		topic   string
		payload string
	}{
		{"sensors/t1", "21.5"},
		{"sensors/n", "42"},
		{"sensors/j1", `{"b":"2.5","a":1,"c":"x","d":true}`},
		{"sensors/nested", `{"m":{"rpm":1e7}}`},
		{`q"x\y`, "1"},
	}
	for _, m := range msgs {
		if tok := pub.Publish(m.topic, 1, false, m.payload); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
			t.Fatal(tok.Error())
		}
	}
	pub.Disconnect(100)
	time.Sleep(150 * time.Millisecond)

	// Scrape
	code, hdr, scrape := promGet(t, promURL+"/metrics", nil)
	if code != 200 {
		t.Fatalf("GET /metrics status = %d", code)
	}
	if ct := hdr.Get("Content-Type"); ct != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("GET /metrics Content-Type = %q", ct)
	}
	wantSubstrings := []string{
		"# HELP metrics MonsterMQ broker metric\n# TYPE metrics gauge\n",
		"# HELP topics Current value of MQTT topic\n# TYPE topics gauge\n",
		`topics{group="Default",topic="sensors/t1",field=""} 21.5` + "\n",
		`topics{group="Default",topic="sensors/n",field=""} 42.0` + "\n",
		`topics{group="Default",topic="q\"x\\y",field=""} 1.0` + "\n",
	}
	for _, ws := range wantSubstrings {
		if !strings.Contains(scrape, ws) {
			t.Fatalf("scrape missing %q in:\n%s", ws, scrape)
		}
	}

	// Check 9 broker metric lines in exact order
	wantBrokerLabels := []string{
		"messages_in", "messages_out", "sessions", "subscriptions", "queue_depth", "bus_in", "bus_out", "winccoa_in", "winccoa_out",
	}
	lastIdx := -1
	for _, bl := range wantBrokerLabels {
		prefix := fmt.Sprintf(`metrics{node="p-%d",metric="%s"} `, promPort, bl)
		idx := strings.Index(scrape, prefix)
		if idx < 0 {
			t.Fatalf("scrape missing broker metric %q in:\n%s", prefix, scrape)
		}
		if idx <= lastIdx {
			t.Fatalf("broker metric %q out of order (idx=%d <= %d)", bl, idx, lastIdx)
		}
		lastIdx = idx
	}

	// Check sensors/j1 payload order: field="b" before field="a", and no c or d
	idxB := strings.Index(scrape, `topics{group="Default",topic="sensors/j1",field="b"} 2.5`)
	idxA := strings.Index(scrape, `topics{group="Default",topic="sensors/j1",field="a"} 1.0`)
	if idxB < 0 || idxA < 0 || idxB >= idxA {
		t.Fatalf("sensors/j1 field order wrong: idxB=%d, idxA=%d in:\n%s", idxB, idxA, scrape)
	}
	if strings.Contains(scrape, `topic="sensors/j1",field="c"`) || strings.Contains(scrape, `topic="sensors/j1",field="d"`) {
		t.Fatalf("sensors/j1 should not include c or d in:\n%s", scrape)
	}

	// Labels (GET and POST)
	wantLabelsJSON := `{"status":"success","data":["__name__","group","topic","field","node","metric"]}`
	_, _, lblGet := promGet(t, promURL+"/api/v1/labels", nil)
	if lblGet != wantLabelsJSON {
		t.Fatalf("GET /api/v1/labels = %s, want %s", lblGet, wantLabelsJSON)
	}
	_, _, lblPost := promPostForm(t, promURL+"/api/v1/labels", nil, nil)
	if lblPost != wantLabelsJSON {
		t.Fatalf("POST /api/v1/labels = %s, want %s", lblPost, wantLabelsJSON)
	}

	// Label values
	_, _, vName := promGet(t, promURL+"/api/v1/label/__name__/values", nil)
	if vName != `{"status":"success","data":["topics","metrics"]}` {
		t.Fatalf("label/__name__/values = %s", vName)
	}
	_, _, vGroup := promGet(t, promURL+"/api/v1/label/group/values?match[]="+url.QueryEscape(`topics{group="NonExistent"}`), nil)
	if vGroup != `{"status":"success","data":["Default"]}` {
		t.Fatalf("label/group/values = %s", vGroup)
	}
	_, _, vTopic := promGet(t, promURL+"/api/v1/label/topic/values?match[]="+url.QueryEscape(`topics{group="Default",topic="sensors/#"}`), nil)
	if vTopic != `{"status":"success","data":["sensors/j1","sensors/n","sensors/nested","sensors/t1"]}` {
		t.Fatalf("label/topic/values = %s", vTopic)
	}
	_, _, vFieldJ1 := promGet(t, promURL+"/api/v1/label/field/values?match[]="+url.QueryEscape(`topics{topic="sensors/j1"}`), nil)
	if vFieldJ1 != `{"status":"success","data":["a","b"]}` {
		t.Fatalf("label/field/values (j1) = %s", vFieldJ1)
	}
	_, _, vFieldT1 := promGet(t, promURL+"/api/v1/label/field/values?match[]="+url.QueryEscape(`topics{topic="sensors/t1"}`), nil)
	if vFieldT1 != `{"status":"success","data":[""]}` {
		t.Fatalf("label/field/values (t1) = %s", vFieldT1)
	}
	want19Metrics := `{"status":"success","data":["messages_in","messages_out","sessions","subscriptions","queue_depth","bus_in","bus_out","winccoa_in","winccoa_out","mqtt_client_in","mqtt_client_out","kafka_client_in","kafka_client_out","winccoa_client_in","winccua_client_in","nats_client_in","nats_client_out","redis_client_in","redis_client_out"]}`
	_, _, vMetric := promGet(t, promURL+"/api/v1/label/metric/values", nil)
	if vMetric != want19Metrics {
		t.Fatalf("label/metric/values = %s, want %s", vMetric, want19Metrics)
	}
	_, _, vNode := promGet(t, promURL+"/api/v1/label/node/values", nil)
	if vNode != fmt.Sprintf(`{"status":"success","data":["p-%d"]}`, promPort) {
		t.Fatalf("label/node/values = %s", vNode)
	}
	_, _, vFoo := promGet(t, promURL+"/api/v1/label/foo/values", nil)
	if vFoo != `{"status":"success","data":[]}` {
		t.Fatalf("label/foo/values = %s", vFoo)
	}

	// Series (GET and POST, F3 and F4)
	wantJ1Series := `{"status":"success","data":[{"__name__":"topics","group":"Default","topic":"sensors/j1","field":"b"},{"__name__":"topics","group":"Default","topic":"sensors/j1","field":"a"}]}`
	_, _, sJ1Get := promGet(t, promURL+"/api/v1/series?match[]="+url.QueryEscape(`{__name__="topics",topic="sensors/j1"}`), nil)
	if sJ1Get != wantJ1Series {
		t.Fatalf("GET /series (j1) = %s, want %s", sJ1Get, wantJ1Series)
	}
	_, _, sJ1Post := promPostForm(t, promURL+"/api/v1/series", url.Values{"match[]": {`{__name__="topics",topic="sensors/j1"}`}}, nil)
	if sJ1Post != wantJ1Series {
		t.Fatalf("POST /series (j1) = %s, want %s", sJ1Post, wantJ1Series)
	}

	// F4: match[]=topics{topic="sensors/t1"} returns only topic series (no metrics series)
	wantT1Series := `{"status":"success","data":[{"__name__":"topics","group":"Default","topic":"sensors/t1","field":""}]}`
	_, _, sT1 := promGet(t, promURL+"/api/v1/series?match[]="+url.QueryEscape(`topics{topic="sensors/t1"}`), nil)
	if sT1 != wantT1Series {
		t.Fatalf("GET /series (t1, F4) = %s, want %s", sT1, wantT1Series)
	}

	// match[]=metrics gives 19 metric series
	_, _, sMetrics := promGet(t, promURL+"/api/v1/series?match[]=metrics", nil)
	var sMetricsObj struct {
		Status string              `json:"status"`
		Data   []map[string]string `json:"data"`
	}
	if err := json.Unmarshal([]byte(sMetrics), &sMetricsObj); err != nil {
		t.Fatal(err)
	}
	if len(sMetricsObj.Data) != 19 {
		t.Fatalf("series(metrics) len = %d, want 19", len(sMetricsObj.Data))
	}

	// With no match[], both kinds are returned
	_, _, sAll := promGet(t, promURL+"/api/v1/series", nil)
	if !strings.Contains(sAll, `"__name__":"topics"`) || !strings.Contains(sAll, `"__name__":"metrics"`) {
		t.Fatalf("series(all) missing topics or metrics: %s", sAll)
	}

	// Instant query
	checkInstantVal := func(q, wantVal string) {
		t.Helper()
		_, _, getBody := promGet(t, promURL+"/api/v1/query?query="+url.QueryEscape(q), nil)
		_, _, postBody := promPostForm(t, promURL+"/api/v1/query", url.Values{"query": {q}}, nil)
		for _, b := range []string{getBody, postBody} {
			var res struct {
				Status string `json:"status"`
				Data   struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []any             `json:"value"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(b), &res); err != nil {
				t.Fatalf("unmarshal %s: %v", b, err)
			}
			if res.Status != "success" || res.Data.ResultType != "vector" {
				t.Fatalf("unexpected query envelope: %s", b)
			}
			if wantVal == "" {
				if len(res.Data.Result) != 0 {
					t.Fatalf("query %s got %d results, want 0: %s", q, len(res.Data.Result), b)
				}
				continue
			}
			if len(res.Data.Result) != 1 {
				t.Fatalf("query %s got %d results, want 1: %s", q, len(res.Data.Result), b)
			}
			gotVal, _ := res.Data.Result[0].Value[1].(string)
			if gotVal != wantVal {
				t.Fatalf("query %s value = %q, want %q", q, gotVal, wantVal)
			}
		}
	}

	checkInstantVal(`topics{group="Default",topic="sensors/t1"}`, "21.5")
	checkInstantVal(`topics{topic="sensors/n"}`, "42.0")
	checkInstantVal(`topics{topic="sensors/j1",field="b"}`, "2.5")
	checkInstantVal(`topics{topic="sensors/j1"}`, "")
	checkInstantVal(`topics{topic="sensors/nested",field="m.rpm"}`, "1.0E7")
	checkInstantVal(`metrics{metric="sessions"}`, "0.0")
	checkInstantVal(`metrics{metric="nats_client_in"}`, "")
	checkInstantVal(`metrics{node="other",metric="sessions"}`, "")
}

func TestPrometheusRangeRawAndAggregated(t *testing.T) {
	mqttPort := 22302
	promPort := 25302
	srv, promURL := startWithPrometheus(t, mqttPort, promPort)
	defer srv.Close()

	addHistGroup(t, srv)
	hist := srv.Archives().Get("Hist").Archive()

	ctx := context.Background()
	base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	// Insert 6 values 1s apart: t+0=10, t+1=20, t+2=30, t+3=40, t+4=50, t+5=60
	var msgs []stores.BrokerMessage
	for i := 0; i < 6; i++ {
		msgs = append(msgs, stores.BrokerMessage{
			MessageUUID: fmt.Sprintf("u-%d", i),
			TopicName:   "h/1",
			Payload:     []byte(fmt.Sprintf("%d", (i+1)*10)),
			Time:        base.Add(time.Duration(i) * time.Second),
		})
	}
	if err := hist.AddHistory(ctx, msgs); err != nil {
		t.Fatal(err)
	}

	// Raw query with step 2 over [base, base+4s] (points at base+5s has bucket 2 -> base+4s <= end; let's set end = base+3s so bucket 2 (base+4s) is after end and dropped)
	startSec := base.Unix()
	endSec := base.Add(3 * time.Second).Unix()
	qURL := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=2",
		promURL, url.QueryEscape(`topics{group="Hist",topic="h/1"}`), startSec, endSec)
	_, _, body := promGet(t, qURL, nil)
	wantRaw := fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"topics","group":"Hist","topic":"h/1","field":""},"values":[[%d,"15.0"],[%d,"35.0"]]}]}}`,
		startSec, startSec+2)
	if body != wantRaw {
		t.Fatalf("raw range = %s\nwant      = %s", body, wantRaw)
	}

	// Aggregated query: step 60 gives AVG over minute bucket (all 6 points average to 35.0)
	aggEnd := base.Add(120 * time.Second).Unix()
	qAgg := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=60",
		promURL, url.QueryEscape(`topics{group="Hist",topic="h/1"}`), startSec, aggEnd)
	_, _, aggBody := promGet(t, qAgg, nil)
	wantAgg := fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"topics","group":"Hist","topic":"h/1","field":""},"values":[[%d,"35.0"]]}]}}`,
		startSec)
	if aggBody != wantAgg {
		t.Fatalf("agg AVG range = %s\nwant          = %s", aggBody, wantAgg)
	}

	// max_over_time with step 15 (F1)
	qMax := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=15",
		promURL, url.QueryEscape(`max_over_time(topics{group="Hist",topic="h/1"}[5m])`), startSec, aggEnd)
	code, _, maxBody := promGet(t, qMax, nil)
	if code != 200 {
		t.Fatalf("max_over_time status = %d, body = %s", code, maxBody)
	}
	wantMax := fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"topics","group":"Hist","topic":"h/1","field":""},"values":[[%d,"60.0"]]}]}}`,
		startSec)
	if maxBody != wantMax {
		t.Fatalf("max_over_time = %s\nwant          = %s", maxBody, wantMax)
	}

	// count_over_time
	qCount := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=15",
		promURL, url.QueryEscape(`count_over_time(topics{group="Hist",topic="h/1"}[5m])`), startSec, aggEnd)
	_, _, countBody := promGet(t, qCount, nil)
	wantCount := fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"topics","group":"Hist","topic":"h/1","field":""},"values":[[%d,"6.0"]]}]}}`,
		startSec)
	if countBody != wantCount {
		t.Fatalf("count_over_time = %s\nwant            = %s", countBody, wantCount)
	}

	// Topic not in archive -> series with values: []
	qMissing := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=60",
		promURL, url.QueryEscape(`topics{group="Hist",topic="no/such/topic"}`), startSec, aggEnd)
	_, _, missBody := promGet(t, qMissing, nil)
	if missBody != `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"topics","group":"Hist","topic":"no/such/topic","field":""},"values":[]}]}}` {
		t.Fatalf("missing topic range = %s", missBody)
	}
}

func TestPrometheusRawQueryLimit(t *testing.T) {
	mqttPort := 22303
	promPort := 25303
	srv, promURL := startWithPrometheus(t, mqttPort, promPort, func(c *config.Config) {
		c.Prometheus.RawQueryLimit = 3
	})
	defer srv.Close()

	addHistGroup(t, srv)
	hist := srv.Archives().Get("Hist").Archive()

	ctx := context.Background()
	base := time.Date(2026, 10, 17, 13, 0, 0, 0, time.UTC)
	var msgs []stores.BrokerMessage
	for i := 0; i < 6; i++ {
		msgs = append(msgs, stores.BrokerMessage{
			MessageUUID: fmt.Sprintf("lim-%d", i),
			TopicName:   "h/lim",
			Payload:     []byte(fmt.Sprintf("%d", (i+1)*10)),
			Time:        base.Add(time.Duration(i) * time.Second),
		})
	}
	if err := hist.AddHistory(ctx, msgs); err != nil {
		t.Fatal(err)
	}

	// With RawQueryLimit: 3, only the newest 3 values (t+3=40, t+4=50, t+5=60) remain.
	startSec := base.Unix()
	endSec := base.Add(6 * time.Second).Unix()
	qURL := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=1",
		promURL, url.QueryEscape(`topics{group="Hist",topic="h/lim"}`), startSec, endSec)
	_, _, body := promGet(t, qURL, nil)
	want := fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"topics","group":"Hist","topic":"h/lim","field":""},"values":[[%d,"40.0"],[%d,"50.0"],[%d,"60.0"]]}]}}`,
		startSec+3, startSec+4, startSec+5)
	if body != want {
		t.Fatalf("RawQueryLimit=3 body = %s\nwant                 = %s", body, want)
	}
}

func TestPrometheusRangeMetricsF2(t *testing.T) {
	mqttPort := 22304
	promPort := 25304
	srv, promURL := startWithPrometheus(t, mqttPort, promPort, func(c *config.Config) {
		c.Metrics.Enabled = false // avoid background collector ticks interfering with exact counts
		c.Metrics.CollectionIntervalSeconds = 1
	})
	defer srv.Close()

	ctx := context.Background()
	nodeID := fmt.Sprintf("p-%d", promPort)
	now := time.Now().UTC().Truncate(time.Second)
	start := now.Add(-5000 * time.Second)

	// Insert 5000 broker rows over [now-5000s, now-1s]
	for i := 0; i < 5000; i++ {
		ts := start.Add(time.Duration(i) * time.Second)
		if err := srv.Storage().Metrics.StoreMetrics(ctx, stores.MetricBroker, ts, nodeID, `{"nodeSessionCount":5}`); err != nil {
			t.Fatal(err)
		}
	}

	qURL := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=10",
		promURL, url.QueryEscape(`metrics{metric="sessions"}`), start.Unix(), now.Unix())
	_, _, body := promGet(t, qURL, nil)
	var res struct {
		Data struct {
			Result []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Data.Result) != 1 || len(res.Data.Result[0].Values) != 500 {
		t.Fatalf("got %d buckets, want 500 (including newest)", len(res.Data.Result[0].Values))
	}
	lastTs := int64(res.Data.Result[0].Values[499][0].(float64))
	if lastTs != now.Unix()-10 {
		t.Fatalf("newest bucket ts = %d, want %d", lastTs, now.Unix()-10)
	}

	// Insert one row at now-200000s; query [now-200000s, now] with step 3600 -> clamped window excludes old row
	oldTs := now.Add(-200000 * time.Second)
	if err := srv.Storage().Metrics.StoreMetrics(ctx, stores.MetricBroker, oldTs, nodeID, `{"nodeSessionCount":99}`); err != nil {
		t.Fatal(err)
	}
	qClamp := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=3600",
		promURL, url.QueryEscape(`metrics{metric="sessions"}`), oldTs.Unix(), now.Unix())
	_, _, clampBody := promGet(t, qClamp, nil)
	if strings.Contains(clampBody, `"99.0"`) || strings.Contains(clampBody, fmt.Sprintf("[%d,", oldTs.Unix())) {
		t.Fatalf("old row outside 50000s clamp should be absent, got: %s", clampBody)
	}
}

func TestPrometheusNilStores(t *testing.T) {
	mqttPort := 22305
	promPort := 25305
	srv, promURL := startWithPrometheus(t, mqttPort, promPort, func(c *config.Config) {
		c.Metrics.Enabled = false
		c.Metrics.StoreType = config.StoreNone
	})
	defer srv.Close()

	ctx := context.Background()
	if err := srv.Storage().ArchiveConfig.Save(ctx, stores.ArchiveGroupConfig{
		Name:         "NoStores",
		Enabled:      true,
		TopicFilters: []string{"#"},
		LastValType:  stores.MessageStoreNone,
		ArchiveType:  stores.ArchiveNone,
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Archives().Reload(ctx); err != nil {
		t.Fatal(err)
	}

	// Metrics.Enabled: false -> scrape still has 9 zero lines, instant metrics empty
	_, _, scrape := promGet(t, promURL+"/metrics", nil)
	if strings.Count(scrape, fmt.Sprintf(`metrics{node="p-%d",metric=`, promPort)) != 9 {
		t.Fatalf("scrape should have 9 zero metric lines:\n%s", scrape)
	}
	_, _, instMetric := promGet(t, promURL+"/api/v1/query?query="+url.QueryEscape(`metrics{metric="sessions"}`), nil)
	if instMetric != `{"status":"success","data":{"resultType":"vector","result":[]}}` {
		t.Fatalf("instant metrics with collector nil = %s", instMetric)
	}

	// Metrics.StoreType: NONE -> metrics range gives empty matrix
	_, _, rngMetric := promGet(t, promURL+"/api/v1/query_range?query="+url.QueryEscape(`metrics{metric="sessions"}`), nil)
	if rngMetric != `{"status":"success","data":{"resultType":"matrix","result":[]}}` {
		t.Fatalf("range metrics with store nil = %s", rngMetric)
	}

	// LastValType: NONE -> empty vector
	_, _, instTopic := promGet(t, promURL+"/api/v1/query?query="+url.QueryEscape(`topics{group="NoStores",topic="a/b"}`), nil)
	if instTopic != `{"status":"success","data":{"resultType":"vector","result":[]}}` {
		t.Fatalf("instant topic with LastVal NONE = %s", instTopic)
	}

	// ArchiveType: NONE -> empty matrix
	_, _, rngTopic := promGet(t, promURL+"/api/v1/query_range?query="+url.QueryEscape(`topics{group="NoStores",topic="a/b"}`), nil)
	if rngTopic != `{"status":"success","data":{"resultType":"matrix","result":[]}}` {
		t.Fatalf("range topic with Archive NONE = %s", rngTopic)
	}
}

func TestPrometheusAuthAndACL(t *testing.T) {
	mqttPort := 22306
	promPort := 25306
	gqlPort := 24306
	srv, promURL := startWithPrometheus(t, mqttPort, promPort, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = false
		c.UserManagement.AllowAnonymousLocalhost = false
		c.GraphQL.Enabled = true
		c.GraphQL.Port = gqlPort
		c.GraphQL.TLSPort = 0
		c.RestApi.Enabled = true
	})
	defer srv.Close()

	ctx := context.Background()
	addHistGroup(t, srv)

	// Create reader user and rules
	readerHashBytes, err := bcrypt.GenerateFromPassword([]byte("readerpass"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Storage().Users.CreateUser(ctx, stores.User{
		Username:     "reader",
		PasswordHash: string(readerHashBytes),
		Enabled:      true,
		CanSubscribe: true,
		CanPublish:   false,
		IsAdmin:      false,
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Storage().Users.CreateAclRule(ctx, stores.AclRule{
		ID:           "r-allow",
		Username:     "reader",
		TopicPattern: "allowed/#",
		CanSubscribe: true,
		CanPublish:   false,
		Priority:     10,
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Storage().Users.CreateAclRule(ctx, stores.AclRule{
		ID:           "r-deny",
		Username:     "reader",
		TopicPattern: "allowed/secret/#",
		CanSubscribe: false,
		CanPublish:   false,
		Priority:     20,
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.AuthCache().Refresh(ctx); err != nil {
		t.Fatal(err)
	}

	// 1. Auth checks
	// No header -> 401
	code, hdr, body := promGet(t, promURL+"/api/v1/labels", nil)
	if code != 401 || hdr.Get("WWW-Authenticate") != `Basic realm="MonsterMQ Prometheus"` || body != `{"error":"Authentication required"}` {
		t.Fatalf("no header: code=%d www=%q body=%s", code, hdr.Get("WWW-Authenticate"), body)
	}
	// Bad Bearer -> 401
	code, hdr, body = promGet(t, promURL+"/api/v1/labels", map[string]string{"Authorization": "Bearer bad-token"})
	if code != 401 || hdr.Get("WWW-Authenticate") != `Bearer error="invalid_token"` || body != `{"error":"Invalid or expired token"}` {
		t.Fatalf("bad bearer: code=%d www=%q body=%s", code, hdr.Get("WWW-Authenticate"), body)
	}
	// Wrong Basic credentials -> 401
	code, hdr, body = promGet(t, promURL+"/api/v1/labels", map[string]string{"Authorization": "Basic " + basicB64("reader:wrong")})
	if code != 401 || hdr.Get("WWW-Authenticate") != `Basic realm="MonsterMQ Prometheus"` || body != `{"error":"Invalid credentials"}` {
		t.Fatalf("wrong basic: code=%d www=%q body=%s", code, hdr.Get("WWW-Authenticate"), body)
	}
	// Basic without ':' -> 401
	code, hdr, body = promGet(t, promURL+"/api/v1/labels", map[string]string{"Authorization": "Basic " + basicB64("nocolon")})
	if code != 401 || hdr.Get("WWW-Authenticate") != `Basic realm="MonsterMQ Prometheus"` || body != `{"error":"Authentication required"}` {
		t.Fatalf("basic no colon: code=%d www=%q body=%s", code, hdr.Get("WWW-Authenticate"), body)
	}
	// Valid Basic -> 200
	readerBasic := map[string]string{"Authorization": "Basic " + basicB64("reader:readerpass")}
	adminBasic := map[string]string{"Authorization": "Basic " + basicB64("Admin:Admin")}
	code, _, _ = promGet(t, promURL+"/api/v1/labels", readerBasic)
	if code != 200 {
		t.Fatalf("valid basic status = %d", code)
	}

	// Bearer token from REST POST /api/v1/login -> 200
	loginBody, _ := json.Marshal(map[string]string{"username": "reader", "password": "readerpass"})
	loginResp, err := http.Post(fmt.Sprintf("http://localhost:%d/api/v1/login", gqlPort), "application/json", bytes.NewReader(loginBody))
	if err != nil {
		t.Fatal(err)
	}
	var loginData struct {
		Token string `json:"token"`
	}
	_ = json.NewDecoder(loginResp.Body).Decode(&loginData)
	_ = loginResp.Body.Close()
	if loginData.Token == "" {
		t.Fatal("REST login returned empty token")
	}
	readerBearer := map[string]string{"Authorization": "Bearer " + loginData.Token}
	code, _, _ = promGet(t, promURL+"/api/v1/labels", readerBearer)
	if code != 200 {
		t.Fatalf("valid bearer status = %d", code)
	}

	// OPTIONS preflight succeeds without credentials and carries CORS headers
	optReq, _ := http.NewRequest(http.MethodOptions, promURL+"/api/v1/query", nil)
	optResp, err := http.DefaultClient.Do(optReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = optResp.Body.Close()
	if optResp.StatusCode >= 300 || optResp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("OPTIONS status=%d allow-origin=%q", optResp.StatusCode, optResp.Header.Get("Access-Control-Allow-Origin"))
	}

	// 2. Read ACL (F7)
	base := time.Now().UTC().Truncate(time.Minute).Add(-2 * time.Minute)
	lv := srv.Archives().Get("Hist").LastValue()
	ar := srv.Archives().Get("Hist").Archive()
	aclMsgs := []stores.BrokerMessage{
		{MessageUUID: "a1", TopicName: "allowed/a", Payload: []byte(`{"fa":10}`), Time: base},
		{MessageUUID: "a2", TopicName: "allowed/secret/b", Payload: []byte(`{"fb":20}`), Time: base},
		{MessageUUID: "a3", TopicName: "other/c", Payload: []byte(`{"fc":30}`), Time: base},
	}
	if err := lv.AddAll(ctx, aclMsgs); err != nil {
		t.Fatal(err)
	}
	if err := ar.AddHistory(ctx, aclMsgs); err != nil {
		t.Fatal(err)
	}

	for _, hdrs := range []map[string]string{readerBasic, readerBearer} {
		// Scrape
		_, _, sc := promGet(t, promURL+"/metrics", hdrs)
		if !strings.Contains(sc, `topic="allowed/a"`) || strings.Contains(sc, `topic="allowed/secret/b"`) || strings.Contains(sc, `topic="other/c"`) {
			t.Fatalf("reader scrape ACL violation:\n%s", sc)
		}
		if strings.Count(sc, fmt.Sprintf(`metrics{node="p-%d",metric=`, promPort)) != 9 {
			t.Fatalf("reader scrape missing 9 broker metrics:\n%s", sc)
		}

		// label/topic/values
		_, _, tv := promGet(t, promURL+"/api/v1/label/topic/values?match[]="+url.QueryEscape(`topics{group="Hist",topic="allowed/#"}`), hdrs)
		if tv != `{"status":"success","data":["allowed/a"]}` {
			t.Fatalf("reader label/topic/values = %s", tv)
		}

		// label/field/values
		_, _, fv := promGet(t, promURL+"/api/v1/label/field/values?match[]="+url.QueryEscape(`topics{group="Hist"}`), hdrs)
		if fv != `{"status":"success","data":["fa"]}` {
			t.Fatalf("reader label/field/values = %s", fv)
		}

		// /series
		_, _, sr := promGet(t, promURL+"/api/v1/series?match[]="+url.QueryEscape(`{group="Hist"}`), hdrs)
		if !strings.Contains(sr, `"allowed/a"`) || strings.Contains(sr, `"allowed/secret/b"`) || strings.Contains(sr, `"other/c"`) {
			t.Fatalf("reader /series = %s", sr)
		}

		// /query
		_, _, qr := promGet(t, promURL+"/api/v1/query?query="+url.QueryEscape(`topics{group="Hist",topic="allowed/#",field="fa"}`), hdrs)
		if !strings.Contains(qr, `"allowed/a"`) || strings.Contains(qr, `"allowed/secret/b"`) {
			t.Fatalf("reader /query = %s", qr)
		}

		// /query_range on denied topic (raw and aggregated)
		startS := base.Unix()
		endS := base.Add(120 * time.Second).Unix()
		_, _, rngDeniedRaw := promGet(t, fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=10",
			promURL, url.QueryEscape(`topics{group="Hist",topic="allowed/secret/b",field="fb"}`), startS, endS), hdrs)
		if rngDeniedRaw != `{"status":"success","data":{"resultType":"matrix","result":[]}}` {
			t.Fatalf("reader denied raw range = %s", rngDeniedRaw)
		}
		_, _, rngDeniedAgg := promGet(t, fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=60",
			promURL, url.QueryEscape(`topics{group="Hist",topic="allowed/secret/b",field="fb"}`), startS, endS), hdrs)
		if rngDeniedAgg != `{"status":"success","data":{"resultType":"matrix","result":[]}}` {
			t.Fatalf("reader denied agg range = %s", rngDeniedAgg)
		}

		// /query_range raw with wildcard uses only allowed/a rows
		// Add a "fa" field to allowed/secret/b to verify it is not averaged in
		if err := ar.AddHistory(ctx, []stores.BrokerMessage{
			{MessageUUID: "a2-fa", TopicName: "allowed/secret/b", Payload: []byte(`{"fa":990}`), Time: base},
		}); err != nil {
			t.Fatal(err)
		}
		_, _, rngWild := promGet(t, fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=10",
			promURL, url.QueryEscape(`topics{group="Hist",topic="allowed/#",field="fa"}`), startS, endS), hdrs)
		if !strings.Contains(rngWild, `"10.0"`) || strings.Contains(rngWild, `"500.0"`) {
			t.Fatalf("reader wildcard raw range leaked denied row: %s", rngWild)
		}
	}

	// Admin sees all three topics
	_, _, adminTopics := promGet(t, promURL+"/api/v1/label/topic/values?match[]="+url.QueryEscape(`topics{group="Hist"}`), adminBasic)
	if adminTopics != `{"status":"success","data":["allowed/a","allowed/secret/b","other/c"]}` {
		t.Fatalf("admin topics = %s", adminTopics)
	}

	// Cap counting: 5000 denied topics do not use up the 5000 cap
	var bulkDenied []stores.BrokerMessage
	for i := 0; i < 5005; i++ {
		bulkDenied = append(bulkDenied, stores.BrokerMessage{
			MessageUUID: fmt.Sprintf("d-%d", i),
			TopicName:   fmt.Sprintf("allowed/secret/n%04d", i),
			Payload:     []byte("1"),
			Time:        base,
		})
	}
	bulkDenied = append(bulkDenied, stores.BrokerMessage{
		MessageUUID: "z-allow",
		TopicName:   "allowed/z",
		Payload:     []byte("1"),
		Time:        base,
	})
	if err := lv.AddAll(ctx, bulkDenied); err != nil {
		t.Fatal(err)
	}
	_, _, capTopics := promGet(t, promURL+"/api/v1/label/topic/values?match[]="+url.QueryEscape(`topics{group="Hist"}`), readerBasic)
	if !strings.Contains(capTopics, `"allowed/z"`) {
		t.Fatalf("allowed/z missing after 5005 denied topics: %s", capTopics)
	}
}

func TestPrometheusAnonymousAndLocalhostACL(t *testing.T) {
	mqttPort := 22307
	promPort := 25307
	srv, promURL := startWithPrometheus(t, mqttPort, promPort, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = true
		c.UserManagement.AllowAnonymousLocalhost = false
	})
	defer srv.Close()

	ctx := context.Background()
	addHistGroup(t, srv)

	if err := srv.Storage().Users.CreateUser(ctx, stores.User{
		Username:     "Anonymous",
		PasswordHash: "",
		Enabled:      true,
		CanSubscribe: true,
		CanPublish:   false,
		IsAdmin:      false,
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Storage().Users.CreateAclRule(ctx, stores.AclRule{
		ID:           "anon-other",
		Username:     "Anonymous",
		TopicPattern: "other/#",
		CanSubscribe: true,
		CanPublish:   false,
		Priority:     10,
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.AuthCache().Refresh(ctx); err != nil {
		t.Fatal(err)
	}

	lv := srv.Archives().Get("Hist").LastValue()
	now := time.Now().UTC()
	if err := lv.AddAll(ctx, []stores.BrokerMessage{
		{MessageUUID: "1", TopicName: "allowed/a", Payload: []byte("1"), Time: now},
		{MessageUUID: "2", TopicName: "allowed/secret/b", Payload: []byte("2"), Time: now},
		{MessageUUID: "3", TopicName: "other/c", Payload: []byte("3"), Time: now},
	}); err != nil {
		t.Fatal(err)
	}

	// Anonymous without AllowAnonymousLocalhost sees only other/c
	_, _, anonTopics := promGet(t, promURL+"/api/v1/label/topic/values?match[]="+url.QueryEscape(`topics{group="Hist"}`), nil)
	if anonTopics != `{"status":"success","data":["other/c"]}` {
		t.Fatalf("anonymous topics = %s, want only other/c", anonTopics)
	}

	// Now test with AllowAnonymousLocalhost = true (E1 runs as LocalhostUser -> sees all three)
	mqttPort2 := 22308
	promPort2 := 25308
	srv2, promURL2 := startWithPrometheus(t, mqttPort2, promPort2, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = false
		c.UserManagement.AllowAnonymousLocalhost = true
	})
	defer srv2.Close()

	addHistGroup(t, srv2)
	lv2 := srv2.Archives().Get("Hist").LastValue()
	if err := lv2.AddAll(ctx, []stores.BrokerMessage{
		{MessageUUID: "1", TopicName: "allowed/a", Payload: []byte("1"), Time: now},
		{MessageUUID: "2", TopicName: "allowed/secret/b", Payload: []byte("2"), Time: now},
		{MessageUUID: "3", TopicName: "other/c", Payload: []byte("3"), Time: now},
	}); err != nil {
		t.Fatal(err)
	}

	_, _, localTopics := promGet(t, promURL2+"/api/v1/label/topic/values?match[]="+url.QueryEscape(`topics{group="Hist"}`), nil)
	if localTopics != `{"status":"success","data":["allowed/a","allowed/secret/b","other/c"]}` {
		t.Fatalf("localhost E1 topics = %s, want all 3", localTopics)
	}
}

func TestPrometheusConfigAndColumnNames(t *testing.T) {
	// Column names
	if got := stores.AggregatedColumnName("a/b", "x.y", "AVG"); got != "a/b.x_y_avg" {
		t.Fatalf("AggregatedColumnName = %q, want a/b.x_y_avg", got)
	}
	if got := stores.AggregatedColumnName("a/b", "", "max"); got != "a/b_max" {
		t.Fatalf("AggregatedColumnName = %q, want a/b_max", got)
	}

	// Config validation: RawQueryLimit: 0 -> 10000
	cfg := config.Default()
	cfg.Prometheus.Enabled = true
	cfg.Prometheus.Port = 3001
	cfg.Prometheus.RawQueryLimit = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with RawQueryLimit 0 failed: %v", err)
	}
	if cfg.Prometheus.RawQueryLimit != 10000 {
		t.Fatalf("RawQueryLimit = %d, want 10000", cfg.Prometheus.RawQueryLimit)
	}

	// Port: 70000 -> Validate error
	cfgBad := config.Default()
	cfgBad.Prometheus.Enabled = true
	cfgBad.Prometheus.Port = 70000
	if err := cfgBad.Validate(); err == nil {
		t.Fatal("expected Validate error for Port 70000")
	}

	// Enabled: false -> nothing listens on the port
	mqttPort := 22309
	promPort := 25309
	cfgOff := config.Default()
	cfgOff.NodeID = "p-off"
	cfgOff.TCP.Enabled = true
	cfgOff.TCP.Port = mqttPort
	cfgOff.WS.Enabled = false
	cfgOff.GraphQL.Enabled = false
	cfgOff.Prometheus.Enabled = false
	cfgOff.Prometheus.Port = promPort
	cfgOff.SQLite.Path = filepath.Join(t.TempDir(), "off.db")
	srv, err := broker.New(cfgOff, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	defer srv.Close()
	time.Sleep(150 * time.Millisecond)
	if resp, err := http.Get(fmt.Sprintf("http://localhost:%d/", promPort)); err == nil {
		_ = resp.Body.Close()
		t.Fatalf("expected connection refused when Prometheus.Enabled is false, got status %d", resp.StatusCode)
	}
}

func TestPrometheusQuestDBLive(t *testing.T) {
	rawURL := os.Getenv("EDGE_TEST_QUESTDB_URL")
	if rawURL == "" {
		t.Skip("EDGE_TEST_QUESTDB_URL not set")
	}
	mqttPort := 22310
	gqlPort := 24310
	promPort := 25310
	srv, gqlURL := startWithGraphQL(t, mqttPort, gqlPort, func(c *config.Config) {
		c.Prometheus.Enabled = true
		c.Prometheus.Port = promPort
	})
	defer srv.Close()

	ctx := context.Background()
	conn := stores.DatabaseConnectionConfig{Name: "PromQuest", Type: stores.DatabaseConnectionQuestDB, URL: rawURL}
	if err := srv.Storage().ArchiveConfig.SaveDatabaseConnection(ctx, conn); err != nil {
		t.Fatal(err)
	}
	groupName := fmt.Sprintf("PromQuest_%d", time.Now().UnixNano())
	tableName := strings.ToLower(archive.ArchiveName(groupName))
	defer func() {
		dsn := storequestdb.NormalizePGDSN(rawURL, "", "")
		if db, err := storepg.Open(ctx, dsn); err == nil {
			_, _ = db.Pool().Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
			_ = db.Close()
		}
	}()
	if err := srv.Storage().ArchiveConfig.Save(ctx, stores.ArchiveGroupConfig{
		Name:                   groupName,
		Enabled:                true,
		TopicFilters:           []string{"qdb/#"},
		LastValType:            stores.MessageStoreMemory,
		ArchiveType:            stores.ArchiveQuestDB,
		DatabaseConnectionName: conn.Name,
		PayloadFormat:          stores.PayloadDefault,
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Archives().Reload(ctx); err != nil {
		t.Fatal(err)
	}

	pub := mqtt.NewClient(mqttOpts(mqttPort, "prom-qdb-pub"))
	if tok := pub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	for _, v := range []string{"10", "30"} {
		if tok := pub.Publish("qdb/val", 1, false, v); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
			t.Fatal(tok.Error())
		}
		time.Sleep(5 * time.Millisecond)
	}
	pub.Disconnect(100)

	checkAggregated(t, gqlURL, groupName, []string{"qdb/val"}, nil, map[string]float64{"qdb/val_avg": 20})

	now := time.Now().UTC()
	qURL := fmt.Sprintf("http://localhost:%d/api/v1/query_range?query=%s&start=%d&end=%d&step=60",
		promPort, url.QueryEscape(fmt.Sprintf(`topics{group="%s",topic="qdb/val"}`, groupName)),
		now.Add(-time.Hour).Unix(), now.Add(time.Hour).Unix())
	_, _, body := promGet(t, qURL, nil)
	if !strings.Contains(body, `"20.0"`) {
		t.Fatalf("QuestDB Prometheus range query missing 20.0: %s", body)
	}
}

func basicB64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}
