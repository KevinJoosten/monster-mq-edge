package broker

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestMTLSListenerHotReloadsServerCert reproduces the field defect where a
// renewed server cert was never served on mTLS listeners: GetConfigForClient
// returned the cached certificate and Go never consults GetCertificate when
// the per-client config already carries Certificates.
func TestMTLSListenerHotReloadsServerCert(t *testing.T) {
	dir := t.TempDir()

	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "reload-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)

	issue := func(serial int64) ([]byte, *ecdsa.PrivateKey) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: "reload-test"},
			NotBefore:    time.Now().Add(-time.Minute),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		return der, key
	}

	writePair := func(certPath, keyPath string, der []byte, key *ecdsa.PrivateKey) {
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyDER, _ := x509.MarshalECPrivateKey(key)
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
		if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
			t.Fatal(err)
		}
	}

	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")
	caPath := filepath.Join(dir, "ca.crt")
	der1, key1 := issue(100)
	writePair(certPath, keyPath, der1, key1)
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0644); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg, err := loadTLS(certPath+":"+keyPath, "", caPath, "", true, logger)
	if err != nil {
		t.Fatalf("loadTLS: %v", err)
	}

	servedSerial := func() int64 {
		t.Helper()
		perClient, err := cfg.GetConfigForClient(nil)
		if err != nil {
			t.Fatalf("GetConfigForClient: %v", err)
		}
		leaf, err := x509.ParseCertificate(perClient.Certificates[0].Certificate[0])
		if err != nil {
			t.Fatalf("parse served cert: %v", err)
		}
		return leaf.SerialNumber.Int64()
	}

	if got := servedSerial(); got != 100 {
		t.Fatalf("initial serial = %d, want 100", got)
	}

	// Rotate the cert on disk (renewal), forcing a distinct mtime.
	der2, key2 := issue(200)
	writePair(certPath, keyPath, der2, key2)
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(certPath, future, future); err != nil {
		t.Fatal(err)
	}

	if got := servedSerial(); got != 200 {
		t.Fatalf("after rotation, mTLS handshake serves serial %d, want 200 (hot-reload missed)", got)
	}
}
