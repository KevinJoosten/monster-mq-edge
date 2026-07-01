package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
)

// TestProvisionQRBootstrap verifies the provisioning endpoint:
// 1. GET /provision/qr returns a challenge payload
// 2. POST /provision/enroll with the correct challenge succeeds
// 3. POST again returns 410 Gone (one-time use)
func TestProvisionQRBootstrap(t *testing.T) {
	dir := t.TempDir()

	cfg := config.Default()
	cfg.NodeID = "prov-test"
	cfg.TCP.Enabled = true
	cfg.TCP.Port = 27883
	cfg.TCPS.Enabled = false
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = filepath.Join(dir, "test.db")
	cfg.Provision = config.ProvisionConfig{
		Enabled:        true,
		BootstrapPort:  27443,
		ChallengeBytes: 16,
	}

	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	defer srv.Close()
	time.Sleep(300 * time.Millisecond)

	baseURL := fmt.Sprintf("http://localhost:%d", cfg.Provision.BootstrapPort)

	// Step 1: Get the QR payload.
	resp, err := http.Get(baseURL + "/provision/qr")
	if err != nil {
		t.Fatalf("GET /provision/qr failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var qr broker.ProvisionQR
	if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
		t.Fatal(err)
	}
	if qr.NodeID != "prov-test" {
		t.Fatalf("unexpected nodeId: %s", qr.NodeID)
	}
	if qr.Challenge == "" {
		t.Fatal("empty challenge")
	}
	if qr.Endpoint == "" {
		t.Fatal("empty endpoint")
	}

	// Step 2: Present wrong challenge — should fail.
	badReq := broker.ProvisionRequest{
		Challenge: "wrong",
		CertPEM:   "fake-cert",
		KeyPEM:    "fake-key",
	}
	body, _ := json.Marshal(badReq)
	resp2, err := http.Post(baseURL+"/provision/enroll", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for bad challenge, got %d", resp2.StatusCode)
	}

	// Step 3: Present correct challenge — should succeed.
	goodReq := broker.ProvisionRequest{
		Challenge: qr.Challenge,
		CertPEM:   "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n",
		KeyPEM:    "-----BEGIN EC PRIVATE KEY-----\nMHQ...\n-----END EC PRIVATE KEY-----\n",
	}
	body, _ = json.Marshal(goodReq)
	resp3, err := http.Post(baseURL+"/provision/enroll", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for correct challenge, got %d", resp3.StatusCode)
	}

	// Step 4: Try again — should get 410 Gone (already provisioned).
	time.Sleep(100 * time.Millisecond)
	resp4, err := http.Get(baseURL + "/provision/qr")
	if err != nil {
		t.Fatal(err)
	}
	resp4.Body.Close()
	if resp4.StatusCode != http.StatusGone {
		t.Fatalf("expected 410 after provisioning, got %d", resp4.StatusCode)
	}
}

// TestProvisionRejectsInvalidMethod verifies only POST is accepted for enroll.
func TestProvisionRejectsInvalidMethod(t *testing.T) {
	dir := t.TempDir()

	cfg := config.Default()
	cfg.NodeID = "prov-method-test"
	cfg.TCP.Enabled = true
	cfg.TCP.Port = 27884
	cfg.TCPS.Enabled = false
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = filepath.Join(dir, "test.db")
	cfg.Provision = config.ProvisionConfig{
		Enabled:        true,
		BootstrapPort:  27444,
		ChallengeBytes: 16,
	}

	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	defer srv.Close()
	time.Sleep(300 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/provision/enroll", cfg.Provision.BootstrapPort))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}
