package broker

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// loadTLS builds a tls.Config that hot-reloads certificates on new connections.
// The cert/key and CA files are re-read from disk whenever their modification
// time changes, so the broker can accept rotated certificates without restart.
//
// path uses "cert.pem:key.pem" format or a single combined PEM.
// Password parameter is reserved for future PKCS12 support.
func loadTLS(path, _ string, caFile string, requireClient bool, logger *slog.Logger) (*tls.Config, error) {
	if path == "" {
		return nil, fmt.Errorf("KeyStorePath is empty")
	}
	certPath, keyPath := splitCertKeyPath(path)

	r := &certReloader{
		certPath: certPath,
		keyPath:  keyPath,
		caPath:   caFile,
		logger:   logger,
	}
	if err := r.loadCert(); err != nil {
		return nil, err
	}
	if caFile != "" {
		if err := r.loadCA(); err != nil {
			return nil, err
		}
	}

	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: r.GetCertificate,
	}
	if caFile != "" {
		cfg.GetConfigForClient = r.GetConfigForClient
	}
	if requireClient {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return cfg, nil
}

// certReloader caches TLS certificates and reloads them from disk when
// the file modification time changes. Thread-safe for concurrent handshakes.
type certReloader struct {
	certPath string
	keyPath  string
	caPath   string
	logger   *slog.Logger

	mu       sync.RWMutex
	cert     *tls.Certificate
	certTime time.Time
	ca       *x509.CertPool
	caTime   time.Time
}

func (r *certReloader) loadCert() error {
	info, err := os.Stat(r.certPath)
	if err != nil {
		return fmt.Errorf("cert %s: %w", r.certPath, err)
	}
	cert, err := tls.LoadX509KeyPair(r.certPath, r.keyPath)
	if err != nil {
		return fmt.Errorf("load keypair: %w", err)
	}
	r.mu.Lock()
	r.cert = &cert
	r.certTime = info.ModTime()
	r.mu.Unlock()
	return nil
}

func (r *certReloader) loadCA() error {
	info, err := os.Stat(r.caPath)
	if err != nil {
		return fmt.Errorf("CA file %s: %w", r.caPath, err)
	}
	pem, err := os.ReadFile(r.caPath)
	if err != nil {
		return fmt.Errorf("read CA file %s: %w", r.caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return fmt.Errorf("no valid certificates in CA file %s", r.caPath)
	}
	r.mu.Lock()
	r.ca = pool
	r.caTime = info.ModTime()
	r.mu.Unlock()
	return nil
}

// GetCertificate is called on every TLS handshake. It checks whether the
// cert file has been modified and reloads if needed.
func (r *certReloader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	if r.needsReloadCert() {
		if err := r.loadCert(); err != nil {
			r.logger.Warn("cert hot-reload failed, using cached", "err", err)
		} else {
			r.logger.Info("cert hot-reloaded", "cert", r.certPath)
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert, nil
}

// GetConfigForClient is called on every TLS handshake when client auth is
// configured. It reloads the CA pool if the file changed.
func (r *certReloader) GetConfigForClient(_ *tls.ClientHelloInfo) (*tls.Config, error) {
	if r.caPath != "" && r.needsReloadCA() {
		if err := r.loadCA(); err != nil {
			r.logger.Warn("CA hot-reload failed, using cached", "err", err)
		} else {
			r.logger.Info("CA hot-reloaded", "ca", r.caPath)
		}
	}
	r.mu.RLock()
	cert := r.cert
	ca := r.ca
	r.mu.RUnlock()

	return &tls.Config{
		Certificates: []tls.Certificate{*cert},
		ClientCAs:    ca,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func (r *certReloader) needsReloadCert() bool {
	info, err := os.Stat(r.certPath)
	if err != nil {
		return false
	}
	r.mu.RLock()
	stale := !info.ModTime().Equal(r.certTime)
	r.mu.RUnlock()
	return stale
}

func (r *certReloader) needsReloadCA() bool {
	info, err := os.Stat(r.caPath)
	if err != nil {
		return false
	}
	r.mu.RLock()
	stale := !info.ModTime().Equal(r.caTime)
	r.mu.RUnlock()
	return stale
}

// splitCertKeyPath splits "cert.pem:key.pem" into two paths, handling
// Windows drive letters (e.g. "C:\a\cert.pem:C:\b\key.pem") correctly.
func splitCertKeyPath(path string) (string, string) {
	for i := 1; i < len(path); i++ {
		if path[i] != ':' {
			continue
		}
		// A drive-letter colon is always at position 1 (e.g. "C:\...").
		if i == 1 && path[0] >= 'A' && path[0] <= 'z' && i+1 < len(path) && (path[i+1] == '\\' || path[i+1] == '/') {
			continue
		}
		return path[:i], path[i+1:]
	}
	return path, path
}
