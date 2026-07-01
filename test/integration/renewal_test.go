package integration

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.mozilla.org/pkcs7"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
)

// TestCertRenewalViaEST starts a broker with a short-lived cert, a mock EST
// server, and verifies the renewal agent re-enrolls and the new cert is
// picked up by TLS hot-reload.
func TestCertRenewalViaEST(t *testing.T) {
	dir := t.TempDir()

	// Generate CA
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)

	// Initial server cert — very short-lived (5 seconds total, threshold at ~1.7s)
	srvKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	srvTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-4 * time.Second),
		NotAfter:     time.Now().Add(1 * time.Second),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}
	srvDER, _ := x509.CreateCertificate(rand.Reader, srvTemplate, caCert, &srvKey.PublicKey, caKey)

	certPath := filepath.Join(dir, "server-cert.pem")
	keyPath := filepath.Join(dir, "server-key.pem")
	caPath := filepath.Join(dir, "ca.pem")

	writePEMFile(t, certPath, "CERTIFICATE", srvDER)
	writeKeyFile(t, keyPath, srvKey)
	writePEMFile(t, caPath, "CERTIFICATE", caDER)

	// Serial tracker for the mock EST server.
	nextSerial := int64(100)

	// Mock EST server that issues a new cert from the same CA.
	// Mock EST server — uses httptest's own self-signed cert. We'll extract it
	// below so the renewal agent can verify it via CAFilePath.
	estServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/est/simplereenroll" {
			http.Error(w, "not found", 404)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}

		// Parse CSR from base64 DER body
		body := make([]byte, r.ContentLength)
		if _, err := r.Body.Read(body); err != nil && err.Error() != "EOF" {
			http.Error(w, "bad body", 400)
			return
		}
		csrDER, err := base64.StdEncoding.DecodeString(string(body))
		if err != nil {
			http.Error(w, "bad base64", 400)
			return
		}
		csr, err := x509.ParseCertificateRequest(csrDER)
		if err != nil {
			http.Error(w, "bad CSR", 400)
			return
		}

		// Issue a new cert with longer lifetime (1 hour)
		nextSerial++
		newTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(nextSerial),
			Subject:      csr.Subject,
			NotBefore:    time.Now().Add(-time.Minute),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
			DNSNames:     csr.DNSNames,
		}
		newDER, err := x509.CreateCertificate(rand.Reader, newTemplate, caCert, csr.PublicKey, caKey)
		if err != nil {
			http.Error(w, fmt.Sprintf("sign failed: %v", err), 500)
			return
		}

		// Return as PKCS#7 (certs-only, base64-encoded)
		p7DER, err := pkcs7.DegenerateCertificate(newDER)
		if err != nil {
			http.Error(w, fmt.Sprintf("pkcs7 failed: %v", err), 500)
			return
		}
		w.Header().Set("Content-Type", "application/pkcs7-mime")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(base64.StdEncoding.EncodeToString(p7DER)))
	}))
	defer estServer.Close()

	// Extract the httptest server's self-signed cert and write it as the CA so
	// the renewal agent can verify the EST server's TLS certificate.
	estLeaf, err := x509.ParseCertificate(estServer.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatalf("parse EST server cert: %v", err)
	}
	estCAPath := filepath.Join(dir, "est-ca.pem")
	writePEMFile(t, estCAPath, "CERTIFICATE", estLeaf.Raw)

	// Configure the broker with renewal enabled.
	cfg := config.Default()
	cfg.NodeID = "renewal-test"
	cfg.TCP.Enabled = true
	cfg.TCP.Port = 26883
	cfg.TCPS.Enabled = true
	cfg.TCPS.Port = 26884
	cfg.TCPS.KeyStorePath = certPath + ":" + keyPath
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = filepath.Join(dir, "test.db")
	cfg.CertRenewal = config.CertRenewalConfig{
		Enabled:       true,
		ESTURL:        estServer.URL,
		CAFilePath:    estCAPath,
		CheckInterval: "500ms",
		CertPath:      certPath,
		KeyPath:       keyPath,
	}

	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	defer srv.Close()

	// Wait for renewal to fire (cert is already past 2/3 lifetime).
	time.Sleep(2 * time.Second)

	// Verify the cert on disk has been replaced (serial > 2).
	newCertPEM, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(newCertPEM)
	if block == nil {
		t.Fatal("no PEM block in renewed cert")
	}
	newCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if newCert.SerialNumber.Int64() <= 2 {
		t.Fatalf("expected renewed cert serial > 2, got %d", newCert.SerialNumber.Int64())
	}
	if time.Until(newCert.NotAfter) < 30*time.Minute {
		t.Fatalf("expected renewed cert to be valid for at least 30min, expires in %v", time.Until(newCert.NotAfter))
	}

	// Verify TLS connection works with the new cert (hot-reload picked it up).
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	opts := mqtt.NewClientOptions()
	opts.AddBroker("ssl://localhost:26884")
	opts.SetClientID("renewal-verify")
	opts.SetConnectTimeout(3 * time.Second)
	opts.SetTLSConfig(&tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	})
	c := mqtt.NewClient(opts)
	tok := c.Connect()
	if !tok.WaitTimeout(3 * time.Second) {
		t.Fatal("TLS connect timed out after renewal")
	}
	if tok.Error() != nil {
		t.Fatalf("TLS connect failed after renewal: %v", tok.Error())
	}
	c.Disconnect(100)
}

func writeKeyFile(t *testing.T, path string, key *ecdsa.PrivateKey) {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writePEMFile(t, path, "EC PRIVATE KEY", der)
}
