package broker

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"monstermq.io/edge/internal/config"
)

// startTestAgent starts a provision agent on a random port with temp cert
// paths and returns the agent, its https base URL, and a cancel func.
func startTestAgent(t *testing.T) (*provisionAgent, string, context.CancelFunc) {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.Config{
		NodeID: "edge-test",
		Provision: config.ProvisionConfig{
			Enabled:        true,
			BootstrapPort:  0, // random free port
			ChallengeBytes: 8,
			CertPath:       filepath.Join(dir, "server.crt"),
			KeyPath:        filepath.Join(dir, "server.key"),
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pa, err := startProvisionAgent(ctx, cfg, nil, nil, logger)
	if err != nil {
		cancel()
		t.Fatalf("start agent: %v", err)
	}
	_, port, err := net.SplitHostPort(pa.listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("listener addr: %v", err)
	}
	return pa, "https://127.0.0.1:" + port, cancel
}

// insecureClient trusts any TLS cert — the bootstrap cert is ephemeral and
// self-signed by design; trust is established via the bfp pin from the QR.
func insecureClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
}

func TestCSREndpointServesDeviceCSR(t *testing.T) {
	_, base, cancel := startTestAgent(t)
	defer cancel()

	resp, err := insecureClient().Get(base + "/provision/csr")
	if err != nil {
		t.Fatalf("GET /provision/csr: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		t.Fatalf("response is not a PEM CSR: %q", string(raw[:min(len(raw), 60)]))
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatalf("parse CSR: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		t.Fatalf("CSR signature invalid: %v", err)
	}
	if csr.Subject.CommonName != "edge-test" {
		t.Fatalf("CSR CN = %q, want edge-test", csr.Subject.CommonName)
	}
}

func TestQRIncludesBootstrapFingerprint(t *testing.T) {
	_, base, cancel := startTestAgent(t)
	defer cancel()

	resp, err := insecureClient().Get(base + "/provision/qr")
	if err != nil {
		t.Fatalf("GET /provision/qr: %v", err)
	}
	defer resp.Body.Close()
	var qr struct {
		NodeID   string `json:"nodeId"`
		Endpoint string `json:"endpoint"`
		Bfp      string `json:"bfp"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
		t.Fatalf("decode QR payload: %v", err)
	}
	if qr.Bfp == "" {
		t.Fatal("QR payload missing bfp fingerprint")
	}
	if len(qr.Endpoint) < 8 || qr.Endpoint[:8] != "https://" {
		t.Fatalf("QR endpoint is not https: %q", qr.Endpoint)
	}

	// The fingerprint must match the certificate the listener presents.
	conn, err := tls.Dial("tcp", base[len("https://"):], &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer conn.Close()
	sum := sha256.Sum256(conn.ConnectionState().PeerCertificates[0].Raw)
	if got := hex.EncodeToString(sum[:]); got != qr.Bfp {
		t.Fatalf("bfp %q does not match presented cert fingerprint %q", qr.Bfp, got)
	}
}

// signCSR issues a cert for the CSR from a throwaway test CA.
func signCSR(t *testing.T, csr *x509.CertificateRequest) (certPEM, caPEM string) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)

	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      csr.Subject,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, csr.PublicKey, caKey)
	if err != nil {
		t.Fatalf("sign csr: %v", err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}))
	caPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))
	return certPEM, caPEM
}

func enroll(t *testing.T, base string, body map[string]string) *http.Response {
	t.Helper()
	payload, _ := json.Marshal(body)
	resp, err := insecureClient().Post(base+"/provision/enroll", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST /provision/enroll: %v", err)
	}
	return resp
}

func TestEnrollWithEmptyKeyUsesDeviceKey(t *testing.T) {
	pa, base, cancel := startTestAgent(t)
	defer cancel()

	// Fetch the device CSR and sign it.
	resp, err := insecureClient().Get(base + "/provision/csr")
	if err != nil {
		t.Fatalf("GET csr: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	block, _ := pem.Decode(raw)
	if block == nil {
		t.Fatal("no CSR served")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatalf("parse csr: %v", err)
	}
	certPEM, caPEM := signCSR(t, csr)

	// Enroll WITHOUT a key — the device must pair the cert with its own key.
	resp = enroll(t, base, map[string]string{
		"challenge": pa.challenge,
		"certPem":   certPEM,
		"caPem":     caPEM,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("enroll failed: %d %s", resp.StatusCode, string(b))
	}

	// The key written to disk must match the CSR public key.
	keyRaw, err := os.ReadFile(pa.cfg.KeyPath)
	if err != nil {
		t.Fatalf("device key not written: %v", err)
	}
	keyBlock, _ := pem.Decode(keyRaw)
	if keyBlock == nil {
		t.Fatal("key file is not PEM")
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Fatalf("parse written key: %v", err)
	}
	wantPub, _ := x509.MarshalPKIXPublicKey(csr.PublicKey)
	gotPub, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if !bytes.Equal(wantPub, gotPub) {
		t.Fatal("written key does not match the CSR public key")
	}
}

func TestEnrollLegacyWithProvidedKey(t *testing.T) {
	pa, base, cancel := startTestAgent(t)
	defer cancel()

	// Legacy CA-generated key path: CA sends both cert and key.
	legacyKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	keyDER, _ := x509.MarshalECPrivateKey(legacyKey)
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))

	csrDER, _ := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "edge-test"},
	}, legacyKey)
	csr, _ := x509.ParseCertificateRequest(csrDER)
	certPEM, caPEM := signCSR(t, csr)

	resp := enroll(t, base, map[string]string{
		"challenge": pa.challenge,
		"certPem":   certPEM,
		"keyPem":    keyPEM,
		"caPem":     caPEM,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("legacy enroll failed: %d %s", resp.StatusCode, string(b))
	}

	got, err := os.ReadFile(pa.cfg.KeyPath)
	if err != nil {
		t.Fatalf("key not written: %v", err)
	}
	if string(got) != keyPEM {
		t.Fatal("written key differs from the CA-provided key")
	}
}
