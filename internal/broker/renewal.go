package broker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"monstermq.io/edge/internal/config"

	"go.mozilla.org/pkcs7"
)

// CertStatus is the JSON payload published to $SYS/cert/status/<NodeId>.
type CertStatus struct {
	State       string `json:"state"`
	Serial      string `json:"serial,omitempty"`
	NotAfter    string `json:"notAfter,omitempty"`
	LastRenewal string `json:"lastRenewal,omitempty"`
	NextCheck   string `json:"nextCheck,omitempty"`
	Error       string `json:"error,omitempty"`
}

// publishFunc is an abstraction for publishing MQTT messages (avoids
// importing the full mochi server into this file's API).
type publishFunc func(topic string, payload []byte, retain bool, qos byte) error

// renewalAgent manages the automatic TLS certificate renewal lifecycle.
type renewalAgent struct {
	cfg     config.CertRenewalConfig
	nodeID  string
	logger  *slog.Logger
	publish publishFunc
	cancel  context.CancelFunc
}

func startRenewalAgent(ctx context.Context, cfg *config.Config, publish publishFunc, logger *slog.Logger) *renewalAgent {
	childCtx, cancel := context.WithCancel(ctx)
	ra := &renewalAgent{
		cfg:     cfg.CertRenewal,
		nodeID:  cfg.NodeID,
		logger:  logger.With("component", "cert-renewal"),
		publish: publish,
		cancel:  cancel,
	}
	go ra.run(childCtx)
	return ra
}

func (ra *renewalAgent) stop() {
	ra.cancel()
}

func (ra *renewalAgent) run(ctx context.Context) {
	interval, err := time.ParseDuration(ra.cfg.CheckInterval)
	if err != nil || interval <= 0 {
		interval = time.Hour
	}

	if ra.cfg.CAFilePath != "" {
		ra.logger.Info("started", "interval", interval, "estUrl", ra.cfg.ESTURL, "caFile", ra.cfg.CAFilePath)
	} else {
		ra.logger.Warn("started without CaFilePath — renewal server must be trusted by the system root pool",
			"interval", interval, "estUrl", ra.cfg.ESTURL)
	}

	// Initial check immediately, then on ticker.
	ra.check(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			ra.logger.Info("stopped")
			return
		case <-ticker.C:
			ra.check(ctx)
		}
	}
}

func (ra *renewalAgent) check(ctx context.Context) {
	nextCheck := time.Now().Add(ra.checkInterval())

	cert, err := ra.loadCurrentCert()
	if err != nil {
		ra.logger.Warn("cannot load current cert", "err", err)
		ra.publishStatus(CertStatus{State: "error", Error: err.Error(), NextCheck: nextCheck.UTC().Format(time.RFC3339)})
		return
	}

	remaining := time.Until(cert.NotAfter)
	lifetime := cert.NotAfter.Sub(cert.NotBefore)
	threshold := lifetime / 3

	ra.logger.Debug("cert check",
		"serial", formatSerial(cert.SerialNumber.Bytes()),
		"notAfter", cert.NotAfter,
		"remaining", remaining.Round(time.Second),
		"threshold", threshold.Round(time.Second),
	)

	if remaining > threshold {
		ra.publishStatus(CertStatus{
			State:    "ok",
			Serial:   formatSerial(cert.SerialNumber.Bytes()),
			NotAfter: cert.NotAfter.UTC().Format(time.RFC3339),
			NextCheck: nextCheck.UTC().Format(time.RFC3339),
		})
		return
	}

	ra.logger.Info("cert approaching expiry, renewing",
		"remaining", remaining.Round(time.Second),
		"notAfter", cert.NotAfter,
	)
	ra.publishStatus(CertStatus{State: "renewing", NextCheck: nextCheck.UTC().Format(time.RFC3339)})

	if err := ra.renew(ctx); err != nil {
		ra.logger.Error("renewal failed", "err", err)
		ra.publishStatus(CertStatus{State: "error", Error: err.Error(), NextCheck: nextCheck.UTC().Format(time.RFC3339)})
		return
	}

	// Verify the new cert
	newCert, err := ra.loadCurrentCert()
	if err != nil {
		ra.logger.Error("cannot load renewed cert", "err", err)
		return
	}
	ra.logger.Info("renewal successful",
		"newSerial", formatSerial(newCert.SerialNumber.Bytes()),
		"newNotAfter", newCert.NotAfter,
	)
	ra.publishStatus(CertStatus{
		State:       "ok",
		Serial:      formatSerial(newCert.SerialNumber.Bytes()),
		NotAfter:    newCert.NotAfter.UTC().Format(time.RFC3339),
		LastRenewal: time.Now().UTC().Format(time.RFC3339),
		NextCheck:   nextCheck.UTC().Format(time.RFC3339),
	})
}

func (ra *renewalAgent) renew(ctx context.Context) error {
	switch ra.cfg.Protocol {
	case "stepca":
		return ra.renewStepCA(ctx)
	default:
		return ra.renewEST(ctx)
	}
}

// renewStepCA uses Smallstep's native /renew endpoint (mTLS auth, no CSR needed).
func (ra *renewalAgent) renewStepCA(ctx context.Context) error {
	client, err := ra.mtlsClient()
	if err != nil {
		return err
	}

	url := ra.cfg.ESTURL + "/renew"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, http.NoBody)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("step-ca renew request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("step-ca returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Crt       string `json:"crt"`
		Ca        string `json:"ca"`
		BridgeCrt string `json:"bridgeCrt"`
		BridgeKey string `json:"bridgeKey"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode step-ca response: %w", err)
	}
	if result.Crt == "" {
		return fmt.Errorf("step-ca returned empty certificate")
	}

	certPEM := []byte(result.Crt)
	if result.Ca != "" {
		certPEM = append(certPEM, []byte(result.Ca)...)
	}

	// step-ca /renew reuses the same key, so only write the cert.
	if err := atomicWrite(ra.cfg.CertPath, certPEM); err != nil {
		return fmt.Errorf("write cert: %w", err)
	}

	// If the response includes a renewed bridge cert, write it too.
	if result.BridgeCrt != "" && ra.cfg.BridgeCertPath != "" {
		if err := atomicWrite(ra.cfg.BridgeCertPath, []byte(result.BridgeCrt)); err != nil {
			return fmt.Errorf("write bridge cert: %w", err)
		}
		ra.logger.Info("bridge cert renewed", "path", ra.cfg.BridgeCertPath)
	}
	if result.BridgeKey != "" && ra.cfg.BridgeKeyPath != "" {
		if err := atomicWrite(ra.cfg.BridgeKeyPath, []byte(result.BridgeKey)); err != nil {
			return fmt.Errorf("write bridge key: %w", err)
		}
	}
	return nil
}

// renewEST uses the RFC 7030 EST /simplereenroll endpoint.
func (ra *renewalAgent) renewEST(ctx context.Context) error {
	// Generate new key pair
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}

	// Load the current cert to copy subject/SANs
	currentCert, err := ra.loadCurrentCert()
	if err != nil {
		return fmt.Errorf("load current cert for CSR: %w", err)
	}

	// Build CSR with same subject and SANs
	csrTemplate := &x509.CertificateRequest{
		Subject:  currentCert.Subject,
		DNSNames: currentCert.DNSNames,
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, csrTemplate, key)
	if err != nil {
		return fmt.Errorf("create CSR: %w", err)
	}

	// EST reenrollment: POST base64(DER) CSR with mTLS auth
	url := ra.cfg.ESTURL + "/.well-known/est/simplereenroll"
	body := base64.StdEncoding.EncodeToString(csrDER)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/pkcs10")
	req.Body = io.NopCloser(stringReader(body))
	req.ContentLength = int64(len(body))

	client, err := ra.mtlsClient()
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("EST request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("EST returned %d: %s", resp.StatusCode, string(respBody))
	}

	// Parse response — could be PKCS#7 or raw PEM depending on CA
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read EST response: %w", err)
	}

	certPEM, err := ra.extractCertFromResponse(respBytes, resp.Header.Get("Content-Type"))
	if err != nil {
		return fmt.Errorf("parse EST response: %w", err)
	}

	// Write key
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	// Atomic write: key first, then cert (certReloader checks cert mtime)
	if err := atomicWrite(ra.cfg.KeyPath, keyPEM); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	if err := atomicWrite(ra.cfg.CertPath, certPEM); err != nil {
		return fmt.Errorf("write cert: %w", err)
	}

	return nil
}

func (ra *renewalAgent) mtlsClient() (*http.Client, error) {
	clientCert, err := tls.LoadX509KeyPair(ra.cfg.CertPath, ra.cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("load client cert for mTLS: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		MinVersion:   tls.VersionTLS12,
	}

	if ra.cfg.CAFilePath != "" {
		caPEM, err := os.ReadFile(ra.cfg.CAFilePath)
		if err != nil {
			return nil, fmt.Errorf("load CA file for renewal mTLS: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("no valid certificates found in %s", ra.cfg.CAFilePath)
		}
		tlsCfg.RootCAs = pool
	}
	// When CAFilePath is empty, the system root pool is used (tlsCfg.RootCAs == nil).

	transport := &http.Transport{TLSClientConfig: tlsCfg}
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}, nil
}

func (ra *renewalAgent) extractCertFromResponse(data []byte, contentType string) ([]byte, error) {
	// Try PEM first (some CAs return PEM directly)
	if block, _ := pem.Decode(data); block != nil && block.Type == "CERTIFICATE" {
		return data, nil
	}

	// Try base64-encoded PKCS#7 (standard EST response)
	der, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		// Maybe it's raw DER PKCS#7
		der = data
	}

	p7, err := pkcs7.Parse(der)
	if err != nil {
		return nil, fmt.Errorf("cannot parse as PKCS#7: %w", err)
	}
	if len(p7.Certificates) == 0 {
		return nil, fmt.Errorf("PKCS#7 contains no certificates")
	}

	// Encode all certs as PEM (leaf first, then intermediates)
	var pemOut []byte
	for _, cert := range p7.Certificates {
		pemOut = append(pemOut, pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: cert.Raw,
		})...)
	}
	return pemOut, nil
}

func (ra *renewalAgent) loadCurrentCert() (*x509.Certificate, error) {
	certPEM, err := os.ReadFile(ra.cfg.CertPath)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, fmt.Errorf("no PEM block in %s", ra.cfg.CertPath)
	}
	return x509.ParseCertificate(block.Bytes)
}

func (ra *renewalAgent) checkInterval() time.Duration {
	d, err := time.ParseDuration(ra.cfg.CheckInterval)
	if err != nil || d <= 0 {
		return time.Hour
	}
	return d
}

func (ra *renewalAgent) publishStatus(status CertStatus) {
	if ra.publish == nil {
		return
	}
	payload, err := json.Marshal(status)
	if err != nil {
		return
	}
	topic := "$SYS/cert/status/" + ra.nodeID
	if err := ra.publish(topic, payload, true, 1); err != nil {
		ra.logger.Debug("failed to publish cert status", "err", err)
	}
}

func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func formatSerial(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	s := ""
	for i, v := range b {
		if i > 0 {
			s += ":"
		}
		s += fmt.Sprintf("%02X", v)
	}
	return s
}

type stringReaderImpl struct {
	s string
	i int
}

func stringReader(s string) io.Reader {
	return &stringReaderImpl{s: s}
}

func (r *stringReaderImpl) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}
