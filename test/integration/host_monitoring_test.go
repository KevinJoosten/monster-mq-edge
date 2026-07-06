package integration

import (
	"encoding/json"
	"log/slog"
	"runtime"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/hostinfo"
)

func TestHostMonitoring(t *testing.T) {
	port := 21890
	cfg := config.Default()
	cfg.NodeID = "test-host-monitor"
	cfg.TCP.Enabled = true
	cfg.TCP.Port = port
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = t.TempDir() + "/test-host-monitor.db"

	// Enable Host Monitoring
	cfg.HostMonitoring.Enabled = true
	cfg.HostMonitoring.IntervalSeconds = 1
	cfg.HostMonitoring.BaseTopic = "nodes/{NodeId}/host"
	cfg.HostMonitoring.QoS = 1

	logger := slogNewDiscard()
	srv, err := broker.New(cfg, logger, nil)
	if err != nil {
		t.Fatalf("broker init: %v", err)
	}
	go func() { _ = srv.Serve() }()
	defer srv.Close()

	time.Sleep(100 * time.Millisecond)

	subClient := mqtt.NewClient(mqttOpts(port, "sub-host-monitor"))
	if tok := subClient.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("sub connect: %v", tok.Error())
	}
	defer subClient.Disconnect(100)

	msgChan := make(chan []byte, 5)

	targetTopic := "nodes/test-host-monitor/host"
	if tok := subClient.Subscribe(targetTopic, 1, func(_ mqtt.Client, m mqtt.Message) {
		msgChan <- m.Payload()
	}); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("subscribe to %s: %v", targetTopic, tok.Error())
	}

	select {
	case payload := <-msgChan:
		var stats hostinfo.HostStats
		if err := json.Unmarshal(payload, &stats); err != nil {
			t.Fatalf("failed to unmarshal host stats JSON: %v. Payload: %s", err, string(payload))
		}

		if stats.Timestamp == "" {
			t.Error("expected non-empty timestamp")
		}

		if stats.CPUPercent < 0 || stats.CPUPercent > 100 {
			t.Errorf("invalid CPUPercent: %f", stats.CPUPercent)
		}

		maxProcCPU := 100.0 * float64(runtime.NumCPU())
		if stats.ProcessCPUPercent < 0 || stats.ProcessCPUPercent > maxProcCPU {
			t.Errorf("invalid ProcessCPUPercent: %f (max: %f)", stats.ProcessCPUPercent, maxProcCPU)
		}

		if stats.Memory.Total == 0 {
			t.Error("expected non-zero Memory.Total")
		}
		if stats.Memory.UsedPercent < 0 || stats.Memory.UsedPercent > 100 {
			t.Errorf("invalid Memory.UsedPercent: %f", stats.Memory.UsedPercent)
		}

		if stats.Disk.Total == 0 {
			t.Error("expected non-zero Disk.Total")
		}
		if stats.Disk.UsedPercent < 0 || stats.Disk.UsedPercent > 100 {
			t.Errorf("invalid Disk.UsedPercent: %f", stats.Disk.UsedPercent)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for host monitoring MQTT payload")
	}
}

// helper to create a discard logger because slog.New(slog.DiscardHandler) is used across files,
// or we can just use a helper function or slog.Default().
func slogNewDiscard() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
