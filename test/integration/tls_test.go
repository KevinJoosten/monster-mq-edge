package integration

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/stores/sqlite"
)

// testPKI holds ephemeral certificates generated for a single test.
type testPKI struct {
	CACertPath     string
	ServerCertPath string
	ServerKeyPath  string
	ClientCertPath string
	ClientKeyPath  string
	CACertPool     *x509.CertPool
}

// newTestPKI generates a self-signed CA, server cert, and client cert into dir.
func newTestPKI(t *testing.T, dir string) *testPKI {
	t.Helper()

	// CA key + cert
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)

	// Server cert
	srvKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	srvTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTemplate, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	// Client cert
	cliKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cliTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTemplate, caCert, &cliKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	// Write PEM files
	writePEM := func(path string, typ string, der []byte) {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
			t.Fatal(err)
		}
	}
	writeKey := func(path string, key *ecdsa.PrivateKey) {
		der, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		writePEM(path, "EC PRIVATE KEY", der)
	}

	pki := &testPKI{
		CACertPath:     filepath.Join(dir, "ca.pem"),
		ServerCertPath: filepath.Join(dir, "server-cert.pem"),
		ServerKeyPath:  filepath.Join(dir, "server-key.pem"),
		ClientCertPath: filepath.Join(dir, "client-cert.pem"),
		ClientKeyPath:  filepath.Join(dir, "client-key.pem"),
	}

	writePEM(pki.CACertPath, "CERTIFICATE", caDER)
	writePEM(pki.ServerCertPath, "CERTIFICATE", srvDER)
	writeKey(pki.ServerKeyPath, srvKey)
	writePEM(pki.ClientCertPath, "CERTIFICATE", cliDER)
	writeKey(pki.ClientKeyPath, cliKey)

	pki.CACertPool = x509.NewCertPool()
	pki.CACertPool.AddCert(caCert)

	return pki
}

// TestTLSListenerAcceptsConnection verifies that a TCPS listener with a
// server cert accepts TLS connections from a client that trusts the CA.
func TestTLSListenerAcceptsConnection(t *testing.T) {
	dir := t.TempDir()
	pki := newTestPKI(t, dir)

	cfg := config.Default()
	cfg.NodeID = "tls-test"
	cfg.TCP.Enabled = false
	cfg.TCPS.Enabled = true
	cfg.TCPS.Port = 25883
	cfg.TCPS.KeyStorePath = pki.ServerCertPath + ":" + pki.ServerKeyPath
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = filepath.Join(dir, "test.db")

	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	defer srv.Close()
	time.Sleep(150 * time.Millisecond)

	// Connect with TLS, trusting our test CA.
	opts := mqtt.NewClientOptions()
	opts.AddBroker("ssl://localhost:25883")
	opts.SetClientID("tls-client")
	opts.SetConnectTimeout(3 * time.Second)
	opts.SetTLSConfig(&tls.Config{
		RootCAs:    pki.CACertPool,
		MinVersion: tls.VersionTLS12,
	})

	c := mqtt.NewClient(opts)
	tok := c.Connect()
	if !tok.WaitTimeout(3 * time.Second) {
		t.Fatal("TLS connect timed out")
	}
	if tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	c.Disconnect(100)
}

// TestTLSListenerRejectsUntrusted verifies that a client without the CA
// cannot complete a TLS handshake (cert verification fails).
func TestTLSListenerRejectsUntrusted(t *testing.T) {
	dir := t.TempDir()
	pki := newTestPKI(t, dir)

	cfg := config.Default()
	cfg.NodeID = "tls-reject"
	cfg.TCP.Enabled = false
	cfg.TCPS.Enabled = true
	cfg.TCPS.Port = 25884
	cfg.TCPS.KeyStorePath = pki.ServerCertPath + ":" + pki.ServerKeyPath
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = filepath.Join(dir, "test.db")

	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	defer srv.Close()
	time.Sleep(150 * time.Millisecond)

	// Connect without trusting the CA — should fail.
	opts := mqtt.NewClientOptions()
	opts.AddBroker("ssl://localhost:25884")
	opts.SetClientID("untrusted-client")
	opts.SetConnectTimeout(2 * time.Second)
	opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})

	c := mqtt.NewClient(opts)
	tok := c.Connect()
	tok.WaitTimeout(3 * time.Second)
	if tok.Error() == nil {
		c.Disconnect(50)
		t.Fatal("expected TLS handshake to fail without trusted CA")
	}
}

// TestMTLSListenerRequiresClientCert verifies RequireClientCert=true
// rejects clients without a valid client certificate.
func TestMTLSListenerRequiresClientCert(t *testing.T) {
	dir := t.TempDir()
	pki := newTestPKI(t, dir)

	cfg := config.Default()
	cfg.NodeID = "mtls-test"
	cfg.TCP.Enabled = false
	cfg.TCPS.Enabled = true
	cfg.TCPS.Port = 25885
	cfg.TCPS.KeyStorePath = pki.ServerCertPath + ":" + pki.ServerKeyPath
	cfg.TCPS.CaFilePath = pki.CACertPath
	cfg.TCPS.RequireClientCert = true
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = filepath.Join(dir, "test.db")

	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	defer srv.Close()
	time.Sleep(150 * time.Millisecond)

	// Without client cert — should fail.
	opts := mqtt.NewClientOptions()
	opts.AddBroker("ssl://localhost:25885")
	opts.SetClientID("no-cert")
	opts.SetConnectTimeout(2 * time.Second)
	opts.SetTLSConfig(&tls.Config{
		RootCAs:    pki.CACertPool,
		MinVersion: tls.VersionTLS12,
	})
	c := mqtt.NewClient(opts)
	tok := c.Connect()
	tok.WaitTimeout(3 * time.Second)
	if tok.Error() == nil {
		c.Disconnect(50)
		t.Fatal("expected mTLS to reject client without certificate")
	}

	// With valid client cert — should succeed.
	clientCert, err := tls.LoadX509KeyPair(pki.ClientCertPath, pki.ClientKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	opts2 := mqtt.NewClientOptions()
	opts2.AddBroker("ssl://localhost:25885")
	opts2.SetClientID("with-cert")
	opts2.SetConnectTimeout(3 * time.Second)
	opts2.SetTLSConfig(&tls.Config{
		RootCAs:      pki.CACertPool,
		Certificates: []tls.Certificate{clientCert},
		MinVersion:   tls.VersionTLS12,
	})
	c2 := mqtt.NewClient(opts2)
	tok2 := c2.Connect()
	if !tok2.WaitTimeout(3 * time.Second) {
		t.Fatal("mTLS connect timed out")
	}
	if tok2.Error() != nil {
		t.Fatal(tok2.Error())
	}
	c2.Disconnect(100)
}

// TestBridgeTLSWithCustomCA verifies the bridge can connect to a TLS broker
// using a custom CA file (sslVerifyCertificate=true + caFile).
func TestBridgeTLSWithCustomCA(t *testing.T) {
	dir := t.TempDir()
	pki := newTestPKI(t, dir)

	// Broker A — TLS "remote".
	cfgA := config.Default()
	cfgA.NodeID = "bridge-tls-a"
	cfgA.TCP.Enabled = false
	cfgA.TCPS.Enabled = true
	cfgA.TCPS.Port = 25886
	cfgA.TCPS.KeyStorePath = pki.ServerCertPath + ":" + pki.ServerKeyPath
	cfgA.WS.Enabled = false
	cfgA.GraphQL.Enabled = false
	cfgA.Metrics.Enabled = false
	cfgA.SQLite.Path = filepath.Join(dir, "a.db")
	srvA, err := broker.New(cfgA, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srvA.Serve() }()
	defer srvA.Close()
	time.Sleep(150 * time.Millisecond)

	// Broker B — with bridge to A over TLS using custom CA.
	dbB := filepath.Join(dir, "b.db")
	bootDB, _ := sqlite.Open(dbB)
	dcs := sqlite.NewDeviceConfigStore(bootDB)
	if err := dcs.EnsureTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	sslVerify := true
	bridgeCfg := map[string]any{
		"brokerUrl":            "ssl://localhost:25886",
		"clientId":             "bridge-ca-test",
		"cleanSession":        true,
		"keepAlive":            10,
		"sslVerifyCertificate": sslVerify,
		"caFile":               pki.CACertPath,
		"addresses": []map[string]any{
			{"mode": "PUBLISH", "localTopic": "out/+", "remoteTopic": "fwd"},
		},
	}
	cfgJSON, _ := json.Marshal(bridgeCfg)
	if err := dcs.Save(context.Background(), stores.DeviceConfig{
		Name: "tls-bridge", Namespace: "bridge", NodeID: "bridge-tls-b", Type: "MQTT_CLIENT",
		Enabled: true, Config: string(cfgJSON),
	}); err != nil {
		t.Fatal(err)
	}
	bootDB.Close()

	cfgB := config.Default()
	cfgB.NodeID = "bridge-tls-b"
	cfgB.TCP.Enabled = true
	cfgB.TCP.Port = 25887
	cfgB.WS.Enabled = false
	cfgB.GraphQL.Enabled = false
	cfgB.Metrics.Enabled = false
	cfgB.SQLite.Path = dbB
	cfgB.Features.MqttClient = true
	srvB, err := broker.New(cfgB, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srvB.Serve() }()
	defer srvB.Close()
	time.Sleep(1 * time.Second) // bridge connect + subscribe

	// Subscribe on A (via TLS) to see bridged messages.
	optsA := mqtt.NewClientOptions()
	optsA.AddBroker("ssl://localhost:25886")
	optsA.SetClientID("sub-on-a")
	optsA.SetConnectTimeout(3 * time.Second)
	optsA.SetTLSConfig(&tls.Config{
		RootCAs:    pki.CACertPool,
		MinVersion: tls.VersionTLS12,
	})
	subA := mqtt.NewClient(optsA)
	if tok := subA.Connect(); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatal("subA connect failed:", tok.Error())
	}
	defer subA.Disconnect(100)
	gotCh := make(chan string, 1)
	subA.Subscribe("fwd/#", 0, func(_ mqtt.Client, m mqtt.Message) {
		gotCh <- string(m.Payload())
	})
	time.Sleep(200 * time.Millisecond)

	// Publish on B (plain TCP).
	pubB := mqtt.NewClient(mqttOpts(25887, "pubB-tls"))
	if tok := pubB.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatal("pubB connect:", tok.Error())
	}
	defer pubB.Disconnect(100)
	pubB.Publish("out/temp", 0, false, "hello-tls")

	select {
	case v := <-gotCh:
		if v != "hello-tls" {
			t.Fatalf("unexpected payload: %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not forward message over TLS")
	}
}

// TestBridgeTLSInsecureSkipVerify verifies sslVerifyCertificate=false
// allows connecting to a TLS broker without trusting its CA.
func TestBridgeTLSInsecureSkipVerify(t *testing.T) {
	dir := t.TempDir()
	pki := newTestPKI(t, dir)

	// Broker A — TLS.
	cfgA := config.Default()
	cfgA.NodeID = "bridge-insecure-a"
	cfgA.TCP.Enabled = false
	cfgA.TCPS.Enabled = true
	cfgA.TCPS.Port = 25888
	cfgA.TCPS.KeyStorePath = pki.ServerCertPath + ":" + pki.ServerKeyPath
	cfgA.WS.Enabled = false
	cfgA.GraphQL.Enabled = false
	cfgA.Metrics.Enabled = false
	cfgA.SQLite.Path = filepath.Join(dir, "a.db")
	srvA, err := broker.New(cfgA, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srvA.Serve() }()
	defer srvA.Close()
	time.Sleep(150 * time.Millisecond)

	// Broker B — bridge with sslVerifyCertificate=false (no CA file).
	dbB := filepath.Join(dir, "b.db")
	bootDB, _ := sqlite.Open(dbB)
	dcs := sqlite.NewDeviceConfigStore(bootDB)
	if err := dcs.EnsureTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	sslVerify := false
	bridgeCfg := map[string]any{
		"brokerUrl":            "ssl://localhost:25888",
		"clientId":             "bridge-insecure",
		"cleanSession":        true,
		"keepAlive":            10,
		"sslVerifyCertificate": sslVerify,
		"addresses": []map[string]any{
			{"mode": "PUBLISH", "localTopic": "out/+", "remoteTopic": "fwd"},
		},
	}
	cfgJSON, _ := json.Marshal(bridgeCfg)
	if err := dcs.Save(context.Background(), stores.DeviceConfig{
		Name: "insecure-bridge", Namespace: "bridge", NodeID: "bridge-insecure-b", Type: "MQTT_CLIENT",
		Enabled: true, Config: string(cfgJSON),
	}); err != nil {
		t.Fatal(err)
	}
	bootDB.Close()

	cfgB := config.Default()
	cfgB.NodeID = "bridge-insecure-b"
	cfgB.TCP.Enabled = true
	cfgB.TCP.Port = 25889
	cfgB.WS.Enabled = false
	cfgB.GraphQL.Enabled = false
	cfgB.Metrics.Enabled = false
	cfgB.SQLite.Path = dbB
	cfgB.Features.MqttClient = true
	srvB, err := broker.New(cfgB, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srvB.Serve() }()
	defer srvB.Close()
	time.Sleep(1 * time.Second)

	// Subscribe on A.
	optsA := mqtt.NewClientOptions()
	optsA.AddBroker("ssl://localhost:25888")
	optsA.SetClientID("sub-insecure-a")
	optsA.SetConnectTimeout(3 * time.Second)
	optsA.SetTLSConfig(&tls.Config{
		RootCAs:    pki.CACertPool,
		MinVersion: tls.VersionTLS12,
	})
	subA := mqtt.NewClient(optsA)
	if tok := subA.Connect(); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatal("subA:", tok.Error())
	}
	defer subA.Disconnect(100)
	gotCh := make(chan string, 1)
	subA.Subscribe("fwd/#", 0, func(_ mqtt.Client, m mqtt.Message) {
		gotCh <- string(m.Payload())
	})
	time.Sleep(200 * time.Millisecond)

	// Publish on B.
	pubB := mqtt.NewClient(mqttOpts(25889, "pubB-insecure"))
	if tok := pubB.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatal("pubB:", tok.Error())
	}
	defer pubB.Disconnect(100)
	pubB.Publish("out/temp", 0, false, "insecure-ok")

	select {
	case v := <-gotCh:
		if v != "insecure-ok" {
			t.Fatalf("unexpected: %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not forward with InsecureSkipVerify")
	}
}

// TestBridgeMTLSClientCert verifies the bridge can present a client
// certificate when connecting to a broker that requires mTLS.
func TestBridgeMTLSClientCert(t *testing.T) {
	dir := t.TempDir()
	pki := newTestPKI(t, dir)

	// Broker A — mTLS required.
	cfgA := config.Default()
	cfgA.NodeID = "bridge-mtls-a"
	cfgA.TCP.Enabled = false
	cfgA.TCPS.Enabled = true
	cfgA.TCPS.Port = 25890
	cfgA.TCPS.KeyStorePath = pki.ServerCertPath + ":" + pki.ServerKeyPath
	cfgA.TCPS.CaFilePath = pki.CACertPath
	cfgA.TCPS.RequireClientCert = true
	cfgA.WS.Enabled = false
	cfgA.GraphQL.Enabled = false
	cfgA.Metrics.Enabled = false
	cfgA.SQLite.Path = filepath.Join(dir, "a.db")
	srvA, err := broker.New(cfgA, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srvA.Serve() }()
	defer srvA.Close()
	time.Sleep(150 * time.Millisecond)

	// Broker B — bridge with client cert.
	dbB := filepath.Join(dir, "b.db")
	bootDB, _ := sqlite.Open(dbB)
	dcs := sqlite.NewDeviceConfigStore(bootDB)
	if err := dcs.EnsureTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	sslVerify := true
	bridgeCfg := map[string]any{
		"brokerUrl":            "ssl://localhost:25890",
		"clientId":             "bridge-mtls-client",
		"cleanSession":        true,
		"keepAlive":            10,
		"sslVerifyCertificate": sslVerify,
		"caFile":               pki.CACertPath,
		"clientCertFile":       pki.ClientCertPath,
		"clientKeyFile":        pki.ClientKeyPath,
		"addresses": []map[string]any{
			{"mode": "PUBLISH", "localTopic": "out/+", "remoteTopic": "fwd"},
		},
	}
	cfgJSON, _ := json.Marshal(bridgeCfg)
	if err := dcs.Save(context.Background(), stores.DeviceConfig{
		Name: "mtls-bridge", Namespace: "bridge", NodeID: "bridge-mtls-b", Type: "MQTT_CLIENT",
		Enabled: true, Config: string(cfgJSON),
	}); err != nil {
		t.Fatal(err)
	}
	bootDB.Close()

	cfgB := config.Default()
	cfgB.NodeID = "bridge-mtls-b"
	cfgB.TCP.Enabled = true
	cfgB.TCP.Port = 25891
	cfgB.WS.Enabled = false
	cfgB.GraphQL.Enabled = false
	cfgB.Metrics.Enabled = false
	cfgB.SQLite.Path = dbB
	cfgB.Features.MqttClient = true
	srvB, err := broker.New(cfgB, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srvB.Serve() }()
	defer srvB.Close()
	time.Sleep(1 * time.Second)

	// Subscribe on A with client cert.
	clientCert, _ := tls.LoadX509KeyPair(pki.ClientCertPath, pki.ClientKeyPath)
	optsA := mqtt.NewClientOptions()
	optsA.AddBroker("ssl://localhost:25890")
	optsA.SetClientID("sub-mtls-a")
	optsA.SetConnectTimeout(3 * time.Second)
	optsA.SetTLSConfig(&tls.Config{
		RootCAs:      pki.CACertPool,
		Certificates: []tls.Certificate{clientCert},
		MinVersion:   tls.VersionTLS12,
	})
	subA := mqtt.NewClient(optsA)
	if tok := subA.Connect(); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatal("subA mtls:", tok.Error())
	}
	defer subA.Disconnect(100)
	gotCh := make(chan string, 1)
	subA.Subscribe("fwd/#", 0, func(_ mqtt.Client, m mqtt.Message) {
		gotCh <- string(m.Payload())
	})
	time.Sleep(200 * time.Millisecond)

	// Publish on B.
	pubB := mqtt.NewClient(mqttOpts(25891, "pubB-mtls"))
	if tok := pubB.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatal("pubB:", tok.Error())
	}
	defer pubB.Disconnect(100)
	pubB.Publish("out/data", 0, false, "mtls-payload")

	select {
	case v := <-gotCh:
		if v != "mtls-payload" {
			t.Fatalf("unexpected: %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bridge with client cert did not deliver")
	}
}
