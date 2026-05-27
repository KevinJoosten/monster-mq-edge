package broker

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// loadTLS reads a PEM bundle from path. The file may contain either:
//   - one PEM block with both cert and key,
//   - or two files separated by ":" in the path (cert.pem:key.pem).
//
// When caFile is non-empty, it is loaded as the ClientCAs pool.
// When requireClient is true, the server demands a valid client certificate (mTLS).
// Password parameter is reserved for future PKCS12 support.
func loadTLS(path, _ string, caFile string, requireClient bool) (*tls.Config, error) {
	if path == "" {
		return nil, fmt.Errorf("KeyStorePath is empty")
	}
	certPath, keyPath := splitCertKeyPath(path)
	if _, err := os.Stat(certPath); err != nil {
		return nil, fmt.Errorf("cert %s: %w", certPath, err)
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load keypair: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}

	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no valid certificates in CA file %s", caFile)
		}
		cfg.ClientCAs = pool
	}
	if requireClient {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return cfg, nil
}

// splitCertKeyPath splits "cert.pem:key.pem" into two paths, handling
// Windows drive letters (e.g. "C:\a\cert.pem:C:\b\key.pem") correctly.
func splitCertKeyPath(path string) (string, string) {
	for i := 1; i < len(path); i++ {
		if path[i] != ':' {
			continue
		}
		// A drive-letter colon is [alpha]:[\\/] — skip it.
		if path[i-1] >= 'A' && path[i-1] <= 'z' && i+1 < len(path) && (path[i+1] == '\\' || path[i+1] == '/') {
			continue
		}
		return path[:i], path[i+1:]
	}
	return path, path
}
