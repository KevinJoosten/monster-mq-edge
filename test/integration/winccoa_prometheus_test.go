package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/oahost"
)

func TestWinCCOaPrometheusQueries(t *testing.T) {
	sim, client := newSim(128)
	defer sim.Close()

	mqttPort := getFreePort(t)
	promPort := getFreePort(t)

	env := startNative(t, mqttPort, filepath.Join(t.TempDir(), "wprom.db"), sim, client, func(c *config.Config) {
		c.Prometheus.Enabled = true
		c.Prometheus.Port = promPort
		c.Metrics.Enabled = true
		c.Metrics.CollectionIntervalSeconds = 1
	}, broker.Options{})
	defer env.srv.Close()

	promURL := fmt.Sprintf("http://localhost:%d", promPort)

	// Set online value and timed archive value
	now := time.Now().UTC()
	if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 42.5}, now); err != nil {
		t.Fatal(err)
	}
	base := now.Add(-10 * time.Minute)
	for i := 0; i < 5; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: float64(10 + i*10)}, ts); err != nil {
			t.Fatal(err)
		}
	}

	// 1. Instant query with field="value"
	q1 := `topics{topic="winccoa/tags/Pump1/speed",field="value"}`
	_, _, body1 := promGet(t, promURL+"/api/v1/query?query="+url.QueryEscape(q1), nil)
	var res1 struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body1), &res1); err != nil {
		t.Fatalf("unmarshal instant with field: %v", err)
	}
	if len(res1.Data.Result) != 1 || res1.Data.Result[0].Value[1] != "50.0" {
		t.Fatalf("instant query with field failed: %s", body1)
	}

	// 2. Instant query WITHOUT field (should automatically extract value for WinCC OA)
	q2 := `topics{topic="winccoa/tags/Pump1/speed"}`
	_, _, body2 := promGet(t, promURL+"/api/v1/query?query="+url.QueryEscape(q2), nil)
	var res2 struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body2), &res2); err != nil {
		t.Fatalf("unmarshal instant without field: %v", err)
	}
	if len(res2.Data.Result) != 1 || res2.Data.Result[0].Value[1] != "50.0" {
		t.Fatalf("instant query without field failed: %s", body2)
	}

	// 3. Raw range query (step < 60) with field="value"
	startSec := base.Add(-time.Minute).Unix()
	endSec := now.Add(time.Minute).Unix()
	rangeQ1 := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=15",
		promURL, url.QueryEscape(q1), startSec, endSec)
	respR1, err := http.Get(rangeQ1)
	if err != nil {
		t.Fatal(err)
	}
	defer respR1.Body.Close()
	var rangeRes1 struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(respR1.Body).Decode(&rangeRes1); err != nil {
		t.Fatal(err)
	}
	if len(rangeRes1.Data.Result) != 1 || len(rangeRes1.Data.Result[0].Values) == 0 {
		t.Fatalf("raw range with field failed: %+v", rangeRes1)
	}

	// 4. Raw range query (step < 60) WITHOUT field (should automatically extract value for WinCC OA)
	rangeQ2 := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=15",
		promURL, url.QueryEscape(q2), startSec, endSec)
	respR2, err := http.Get(rangeQ2)
	if err != nil {
		t.Fatal(err)
	}
	defer respR2.Body.Close()
	var rangeRes2 struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(respR2.Body).Decode(&rangeRes2); err != nil {
		t.Fatal(err)
	}
	if len(rangeRes2.Data.Result) != 1 || len(rangeRes2.Data.Result[0].Values) == 0 {
		t.Fatalf("raw range without field failed: %+v", rangeRes2)
	}
	if len(rangeRes2.Data.Result[0].Values) != len(rangeRes1.Data.Result[0].Values) {
		t.Fatalf("expected raw range without field to match with field: got %d vs %d",
			len(rangeRes2.Data.Result[0].Values), len(rangeRes1.Data.Result[0].Values))
	}

	// 5. Aggregated range query (step >= 60) WITHOUT field
	aggQ2 := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=60",
		promURL, url.QueryEscape(q2), startSec, endSec)
	respAgg, err := http.Get(aggQ2)
	if err != nil {
		t.Fatal(err)
	}
	defer respAgg.Body.Close()
	var aggRes struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(respAgg.Body).Decode(&aggRes); err != nil {
		t.Fatal(err)
	}
	if len(aggRes.Data.Result) != 1 || len(aggRes.Data.Result[0].Values) == 0 {
		t.Fatalf("aggregated range without field failed: %+v", aggRes)
	}

	// 6. Check broker metrics and winccoa metrics via Prometheus
	qWinIn := `metrics{metric="winccoa_in"}`
	_, _, bodyWinIn := promGet(t, promURL+"/api/v1/query?query="+url.QueryEscape(qWinIn), nil)
	if !strings.Contains(bodyWinIn, `"metric":"winccoa_in"`) {
		t.Fatalf("expected winccoa_in metric, got: %s", bodyWinIn)
	}

	qWinOut := `metrics{metric="winccoa_out"}`
	_, _, bodyWinOut := promGet(t, promURL+"/api/v1/query?query="+url.QueryEscape(qWinOut), nil)
	if !strings.Contains(bodyWinOut, `"metric":"winccoa_out"`) {
		t.Fatalf("expected winccoa_out metric, got: %s", bodyWinOut)
	}

	qMsgIn := `metrics{metric="messages_in"}`
	_, _, bodyMsgIn := promGet(t, promURL+"/api/v1/query?query="+url.QueryEscape(qMsgIn), nil)
	if !strings.Contains(bodyMsgIn, `"metric":"messages_in"`) {
		t.Fatalf("expected messages_in metric, got: %s", bodyMsgIn)
	}
}
