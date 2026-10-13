package mqttclient

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/youmark/pkcs8"
	"software.sslmate.com/src/go-pkcs12"
)

// testPKI is a CA with a server certificate for "broker.test" and a client
// certificate, written as PEM files.
type testPKI struct {
	dir                 string
	ca                  *x509.Certificate
	caKey               crypto.Signer
	caPath              string
	serverCert          tls.Certificate
	clientCert          *x509.Certificate
	clientKey           crypto.Signer
	clientPath, keyPath string
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	p := &testPKI{dir: t.TempDir()}
	p.caKey = ecKey(t)
	p.ca = issue(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: "test ca"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign,
	}, p.caKey.Public(), nil, p.caKey)
	p.caPath = writePEM(t, p.dir, "ca.pem", "CERTIFICATE", p.ca.Raw)

	sk := ecKey(t)
	sc := issue(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: "broker.test"}, DNSNames: []string{"broker.test"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, sk.Public(), p.ca, p.caKey)
	p.serverCert = tls.Certificate{Certificate: [][]byte{sc.Raw}, PrivateKey: sk}

	p.clientKey = ecKey(t)
	p.clientCert = p.issueClient(t, p.clientKey)
	p.clientPath = writePEM(t, p.dir, "client.pem", "CERTIFICATE", p.clientCert.Raw)
	der, err := x509.MarshalPKCS8PrivateKey(p.clientKey)
	if err != nil {
		t.Fatal(err)
	}
	p.keyPath = writePEM(t, p.dir, "client.key", "PRIVATE KEY", der)
	return p
}

func (p *testPKI) issueClient(t *testing.T, key crypto.Signer) *x509.Certificate {
	return issue(t, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "bridge"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, key.Public(), p.ca, p.caKey)
}

var serial int64

func issue(t *testing.T, tmpl *x509.Certificate, pub crypto.PublicKey, parent *x509.Certificate, key crypto.Signer) *x509.Certificate {
	t.Helper()
	serial++
	tmpl.SerialNumber = big.NewInt(serial)
	tmpl.NotBefore, tmpl.NotAfter = time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	if parent == nil {
		parent = tmpl
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func ecKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func writePEM(t *testing.T, dir, name, typ string, der []byte) string {
	t.Helper()
	return writeFile(t, dir, name, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}))
}

func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// handshake runs a TLS handshake between a client using cfg and a server that
// presents the test server certificate and requires a client certificate
// signed by the test CA. It returns the client's error and what the server saw.
func (p *testPKI) handshake(t *testing.T, cfg *tls.Config) (error, tls.ConnectionState) {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(p.ca)
	var hello *tls.ClientHelloInfo
	srvCfg := &tls.Config{
		Certificates: []tls.Certificate{p.serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		NextProtos:   []string{"x-amzn-mqtt-ca", "mqtt"},
		GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
			hello = h
			return nil, nil
		},
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan tls.ConnectionState, 1)
	go func() {
		s, err := ln.Accept()
		if err != nil {
			done <- tls.ConnectionState{}
			return
		}
		_ = s.SetDeadline(time.Now().Add(5 * time.Second))
		srv := tls.Server(s, srvCfg)
		_ = srv.Handshake()
		done <- srv.ConnectionState()
		_ = srv.Close()
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	cl := tls.Client(c, cfg)
	err = cl.Handshake()
	if err == nil {
		// TLS 1.3 checks the client certificate after the client finished:
		// a read surfaces the server's verdict.
		_ = cl.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		if _, rerr := cl.Read(make([]byte, 1)); rerr != nil && !errors.Is(rerr, os.ErrDeadlineExceeded) && !errors.Is(rerr, io.EOF) {
			err = rerr
		}
	}
	_ = cl.Close()
	st := <-done
	if hello != nil {
		st.ServerName = hello.ServerName
	}
	return err, st
}

func pemConfig(p *testPKI) Config {
	return Config{
		BrokerURL: "ssl://broker.test:8883", TlsCaCertPath: p.caPath,
		TlsClientCertPath: p.clientPath, TlsClientKeyPath: p.keyPath,
	}
}

func TestTLSConfigPEMClientCertificate(t *testing.T) {
	p := newTestPKI(t)
	cfg, err := buildTLSConfig(pemConfig(p))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2", cfg.MinVersion)
	}
	err, st := p.handshake(t, cfg)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if len(st.PeerCertificates) == 0 || st.PeerCertificates[0].Subject.CommonName != "bridge" {
		t.Errorf("server saw client certificates %v, want the bridge certificate", st.PeerCertificates)
	}
}

func TestTLSConfigServerNameAndALPN(t *testing.T) {
	p := newTestPKI(t)
	c := pemConfig(p)
	c.BrokerURL = "ssl://10.0.0.1:443"
	c.TlsServerName = "broker.test"
	c.TlsAlpnProtocols = []string{"x-amzn-mqtt-ca"}
	cfg, err := buildTLSConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	err, st := p.handshake(t, cfg)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if st.ServerName != "broker.test" || st.NegotiatedProtocol != "x-amzn-mqtt-ca" {
		t.Errorf("SNI %q ALPN %q, want broker.test and x-amzn-mqtt-ca", st.ServerName, st.NegotiatedProtocol)
	}
}

func TestTLSConfigVerifiesServerNameAgainstURLHost(t *testing.T) {
	p := newTestPKI(t)
	c := pemConfig(p)
	c.BrokerURL = "ssl://other.test:8883"
	cfg, err := buildTLSConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if err, _ := p.handshake(t, cfg); err == nil {
		t.Fatal("a server certificate for broker.test was accepted for other.test")
	}
}

func TestTLSConfigWithoutCAFileRejectsPrivateCA(t *testing.T) {
	p := newTestPKI(t)
	c := pemConfig(p)
	c.TlsCaCertPath = ""
	cfg, err := buildTLSConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if err, _ := p.handshake(t, cfg); err == nil {
		t.Fatal("a private CA was trusted without tlsCaCertPath")
	}
}

func TestTLSConfigVerifyOffAcceptsAnyServer(t *testing.T) {
	p := newTestPKI(t)
	c := pemConfig(p)
	c.TlsCaCertPath = ""
	off := false
	c.SslVerifyCertificate = &off
	cfg, err := buildTLSConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if err, _ := p.handshake(t, cfg); err != nil {
		t.Fatalf("handshake with verification off: %v", err)
	}
}

func TestTLSConfigKeyFormats(t *testing.T) {
	p := newTestPKI(t)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	rsaCert := p.issueClient(t, rsaKey)
	encPKCS8, err := pkcs8.MarshalPrivateKey(p.clientKey, []byte("secret"), nil)
	if err != nil {
		t.Fatal(err)
	}
	//lint:ignore SA1019 legacy encrypted PEM is a supported input format
	legacy, err := x509.EncryptPEMBlock(rand.Reader, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(rsaKey), []byte("secret"), x509.PEMCipherAES256)
	if err != nil {
		t.Fatal(err)
	}
	ecDER, err := x509.MarshalECPrivateKey(p.clientKey.(*ecdsa.PrivateKey))
	if err != nil {
		t.Fatal(err)
	}
	rsaCertPath := writePEM(t, p.dir, "rsa.pem", "CERTIFICATE", rsaCert.Raw)
	for name, c := range map[string]struct {
		cert, key, password string
	}{
		"PKCS#1 RSA":        {rsaCertPath, writePEM(t, p.dir, "rsa1.key", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(rsaKey)), ""},
		"SEC1 EC":           {p.clientPath, writePEM(t, p.dir, "ec.key", "EC PRIVATE KEY", ecDER), ""},
		"encrypted PKCS#8":  {p.clientPath, writePEM(t, p.dir, "enc8.key", "ENCRYPTED PRIVATE KEY", encPKCS8), "secret"},
		"legacy encrypted":  {rsaCertPath, writeFile(t, p.dir, "legacy.key", pem.EncodeToMemory(legacy)), "secret"},
		"PKCS#8 with chain": {writeFile(t, p.dir, "chain.pem", append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.clientCert.Raw}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.ca.Raw})...)), p.keyPath, ""},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := buildTLSConfig(Config{
				BrokerURL: "ssl://broker.test:8883", TlsCaCertPath: p.caPath,
				TlsClientCertPath: c.cert, TlsClientKeyPath: c.key, TlsClientKeyPassword: c.password,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err, _ := p.handshake(t, cfg); err != nil {
				t.Fatalf("handshake: %v", err)
			}
		})
	}
}

func TestTLSConfigPKCS12(t *testing.T) {
	p := newTestPKI(t)
	bundle, err := pkcs12.Modern.Encode(p.clientKey, p.clientCert, []*x509.Certificate{p.ca}, "secret")
	if err != nil {
		t.Fatal(err)
	}
	path := writeFile(t, p.dir, "client.p12", bundle)
	cfg, err := buildTLSConfig(Config{
		BrokerURL: "ssl://broker.test:8883", TlsCaCertPath: p.caPath,
		TlsClientCertPath: path, TlsClientKeyFormat: "pkcs12", TlsClientKeyPassword: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err, _ := p.handshake(t, cfg); err != nil {
		t.Fatalf("handshake: %v", err)
	}
}

func TestTLSConfigFileErrors(t *testing.T) {
	p := newTestPKI(t)
	missing := filepath.Join(p.dir, "missing.pem")
	other := p.issueClient(t, ecKey(t))
	otherPath := writePEM(t, p.dir, "other.pem", "CERTIFICATE", other.Raw)
	enc, err := pkcs8.MarshalPrivateKey(p.clientKey, []byte("secret"), nil)
	if err != nil {
		t.Fatal(err)
	}
	encPath := writePEM(t, p.dir, "enc.key", "ENCRYPTED PRIVATE KEY", enc)
	bundle, err := pkcs12.Modern.Encode(p.clientKey, p.clientCert, nil, "secret")
	if err != nil {
		t.Fatal(err)
	}
	p12 := writeFile(t, p.dir, "c.p12", bundle)
	empty := writeFile(t, p.dir, "empty.pem", []byte("nothing here\n"))

	for name, c := range map[string]struct {
		mod  func(*Config)
		want string
	}{
		"CA missing":       {func(c *Config) { c.TlsCaCertPath = missing }, "tlsCaCertPath '" + missing + "': file not found"},
		"CA empty":         {func(c *Config) { c.TlsCaCertPath = empty }, "tlsCaCertPath '" + empty + "': no certificates found"},
		"cert missing":     {func(c *Config) { c.TlsClientCertPath = missing }, "tlsClientCertPath '" + missing + "': file not found"},
		"cert empty":       {func(c *Config) { c.TlsClientCertPath = empty }, "tlsClientCertPath '" + empty + "': no certificates found"},
		"key missing":      {func(c *Config) { c.TlsClientKeyPath = missing }, "tlsClientKeyPath '" + missing + "': file not found"},
		"key empty":        {func(c *Config) { c.TlsClientKeyPath = empty }, "tlsClientKeyPath '" + empty + "': no PEM private key found"},
		"no password":      {func(c *Config) { c.TlsClientKeyPath = encPath }, "tlsClientKeyPath '" + encPath + "': private key is encrypted but no tlsClientKeyPassword is set"},
		"wrong password":   {func(c *Config) { c.TlsClientKeyPath, c.TlsClientKeyPassword = encPath, "nope" }, "tlsClientKeyPath '" + encPath + "': cannot decrypt or read private key (wrong tlsClientKeyPassword?)"},
		"key mismatch":     {func(c *Config) { c.TlsClientCertPath = otherPath }, "tlsClientKeyPath '" + p.keyPath + "' does not match the certificate in tlsClientCertPath '" + otherPath + "'"},
		"PKCS12 password":  {func(c *Config) { c.TlsClientCertPath, c.TlsClientKeyPath, c.TlsClientKeyFormat, c.TlsClientKeyPassword = p12, "", "PKCS12", "nope" }, "tlsClientCertPath '" + p12 + "': cannot open PKCS12 bundle (wrong tlsClientKeyPassword?)"},
	} {
		t.Run(name, func(t *testing.T) {
			cc := pemConfig(p)
			c.mod(&cc)
			cfg, err := buildTLSConfig(cc)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("build err = %v, want it to contain %q", err, c.want)
			}
			// The config still works as soon as the files are right: the
			// handshake reads them again and fails with the same message now.
			if cfg == nil {
				t.Fatal("no TLS config returned for a file error")
			}
			if err, _ := p.handshake(t, cfg); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("handshake err = %v, want it to contain %q", err, c.want)
			}
		})
	}
}

// A certificate file that appears, or is replaced, after start is used on the
// next connection without rebuilding the TLS config.
func TestTLSConfigReadsFilesPerHandshake(t *testing.T) {
	p := newTestPKI(t)
	c := pemConfig(p)
	c.TlsClientCertPath = filepath.Join(p.dir, "later.pem")
	cfg, err := buildTLSConfig(c)
	if err == nil {
		t.Fatal("missing certificate not reported at build")
	}
	if err, _ := p.handshake(t, cfg); err == nil {
		t.Fatal("handshake succeeded without the certificate file")
	}
	writePEM(t, p.dir, "later.pem", "CERTIFICATE", p.clientCert.Raw)
	if err, _ := p.handshake(t, cfg); err != nil {
		t.Fatalf("handshake after the file appeared: %v", err)
	}
}

// A config error stops the bridge at start, as in Core; nothing is left running.
func TestStartRefusesInvalidTLSConfig(t *testing.T) {
	c := NewConnector("bad", Config{BrokerURL: "tcp://127.0.0.1:1", TlsCaCertPath: "/ca.pem"}, nil, nil, slog.New(slog.DiscardHandler))
	err := c.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Invalid MQTT client configuration: TLS options (tls*) require an ssl:// or wss:// brokerUrl") {
		t.Fatalf("Start err = %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil || c.queue != nil {
		t.Error("a refused bridge left its client or queue behind")
	}
}

// A certificate file that is missing at start is a warning, not a refusal:
// the bridge keeps trying and picks the file up when it appears.
func TestStartWarnsAboutMissingCertificateFile(t *testing.T) {
	var logs syncBuffer
	missing := filepath.Join(t.TempDir(), "client.pem")
	c := NewConnector("later", Config{
		BrokerURL: "ssl://127.0.0.1:1", TlsClientCertPath: missing, TlsClientKeyPath: missing,
		ReconnectDelay: 1,
	}, nil, nil, slog.New(slog.NewTextHandler(&logs, nil)))
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()
	if !strings.Contains(logs.String(), "Invalid MQTT client TLS configuration") || !strings.Contains(logs.String(), "file not found") {
		t.Errorf("no TLS warning logged:\n%s", logs.String())
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
