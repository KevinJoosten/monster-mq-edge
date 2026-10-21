package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBindAddressDefaults(t *testing.T) {
	c := Default()
	if got := c.Prometheus.ListenAddress(); got != "0.0.0.0" {
		t.Errorf("Prometheus.ListenAddress() = %q, want 0.0.0.0", got)
	}
	if got := c.Redfish.ListenAddress(); got != "0.0.0.0" {
		t.Errorf("Redfish.ListenAddress() = %q, want 0.0.0.0", got)
	}
	if got := c.NATSListenAddress(); got != "0.0.0.0" {
		t.Errorf("NATSListenAddress() = %q, want 0.0.0.0", got)
	}
}

func TestBindAddressLoadYAML(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "config.yaml")
	yaml := []byte("NATS: 4222\nNATSAddress: 127.0.0.1\n" +
		"Prometheus:\n  Enabled: true\n  Port: 3001\n  Address: 127.0.0.1\n" +
		"Redfish:\n  Enabled: true\n  Port: 8000\n  Address: 127.0.0.1\n")
	if err := os.WriteFile(tmp, yaml, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(tmp)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Prometheus.ListenAddress(); got != "127.0.0.1" {
		t.Errorf("Prometheus.ListenAddress() = %q, want 127.0.0.1", got)
	}
	if got := cfg.Redfish.ListenAddress(); got != "127.0.0.1" {
		t.Errorf("Redfish.ListenAddress() = %q, want 127.0.0.1", got)
	}
	if got := cfg.NATSListenAddress(); got != "127.0.0.1" {
		t.Errorf("NATSListenAddress() = %q, want 127.0.0.1", got)
	}
}
