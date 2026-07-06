package config

import (
	"strings"
	"testing"
)

func TestValidateRejectsUnsupportedPersistentQueueOverride(t *testing.T) {
	cfg := Default()
	cfg.DefaultStoreType = StoreSQLite
	cfg.QueueStoreType = StorePostgres

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "QueueStoreType") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateAllowsMemoryQueueOverride(t *testing.T) {
	cfg := Default()
	cfg.DefaultStoreType = StoreSQLite
	cfg.QueueStoreType = StoreMemory

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateHostMonitoring(t *testing.T) {
	t.Run("valid settings", func(t *testing.T) {
		cfg := Default()
		cfg.HostMonitoring.Enabled = true
		cfg.HostMonitoring.IntervalSeconds = 10
		cfg.HostMonitoring.QoS = 1
		cfg.HostMonitoring.BaseTopic = "nodes/test/host"
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected valid config, got error: %v", err)
		}
	})

	t.Run("invalid interval", func(t *testing.T) {
		cfg := Default()
		cfg.HostMonitoring.Enabled = true
		cfg.HostMonitoring.IntervalSeconds = 0
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for interval <= 0")
		}
	})

	t.Run("invalid QoS", func(t *testing.T) {
		cfg := Default()
		cfg.HostMonitoring.Enabled = true
		cfg.HostMonitoring.QoS = 3
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for QoS > 2")
		}
	})

	t.Run("empty BaseTopic", func(t *testing.T) {
		cfg := Default()
		cfg.HostMonitoring.Enabled = true
		cfg.HostMonitoring.BaseTopic = ""
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for empty BaseTopic")
		}
	})
}
