package integration

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	engine "monstermq.io/edge/internal/mqtt"
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/stores/sqlite"
)

// startBridgeBroker starts a broker on tcpPort with one bridge "up" seeded
// from cfg.
func startBridgeBroker(t *testing.T, tcpPort int, cfg map[string]any) *broker.Server {
	t.Helper()
	db := filepath.Join(t.TempDir(), "b.db")
	boot, err := sqlite.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	dcs := sqlite.NewDeviceConfigStore(boot)
	if err := dcs.EnsureTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(cfg)
	node := fmt.Sprintf("bridge-%d", tcpPort)
	if err := dcs.Save(context.Background(), stores.DeviceConfig{
		Name: "up", Namespace: "bridge", NodeID: node, Type: "MQTT_CLIENT", Enabled: true, Config: string(raw),
	}); err != nil {
		t.Fatal(err)
	}
	boot.Close()

	c := config.Default()
	c.NodeID = node
	c.TCP.Port = tcpPort
	c.WS.Enabled = false
	c.GraphQL.Enabled = false
	c.Metrics.Enabled = false
	c.SQLite.Path = db
	c.Features.MqttClient = true
	srv, err := broker.New(c, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// mtlsRemote starts an mTLS broker that knows the test client certificate's
// user.
func mtlsRemote(t *testing.T, port int) (*broker.Server, testCerts) {
	t.Helper()
	certs := createTestCertificates(t)
	srv, storage := startMTLSBroker(t, port, certs)
	t.Cleanup(func() { _ = srv.Close() })
	hash, _ := sqlite.HashPassword("secret")
	if err := storage.Users.CreateUser(context.Background(), stores.User{
		Username: certs.clientCertCN, PasswordHash: hash, Enabled: true, CanSubscribe: true, CanPublish: true,
	}); err != nil {
		t.Fatal(err)
	}
	_ = srv.AuthCache().Refresh(context.Background())
	return srv, certs
}

func mtlsBridge(port int, certs testCerts) map[string]any {
	return map[string]any{
		"brokerUrl": fmt.Sprintf("ssl://localhost:%d", port), "clientId": "bridge-mtls",
		"cleanSession": true, "keepAlive": 10, "reconnectDelay": 1000,
		"tlsCaCertPath": certs.caPath, "tlsClientCertPath": certs.clientCert, "tlsClientKeyPath": certs.clientKey,
		"addresses": []map[string]any{{"mode": "PUBLISH", "localTopic": "out/#", "remoteTopic": "fwd/#", "removePath": true}},
	}
}

func connectedClient(srv *broker.Server, id string) bool {
	cl, ok := srv.MQTT().Clients.Get(id)
	return ok && !cl.Closed()
}

func waitConnected(t *testing.T, srv *broker.Server, id string, within time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if connectedClient(srv, id) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func TestBridgeMTLSConnectsWithClientCertificate(t *testing.T) {
	remote, certs := mtlsRemote(t, 24951)
	got := make(chan string, 4)
	if err := remote.MQTT().Subscribe("fwd/#", 1, func(_ *engine.Client, _ packets.Subscription, pk packets.Packet) {
		got <- pk.TopicName + "=" + string(pk.Payload)
	}); err != nil {
		t.Fatal(err)
	}
	local := startBridgeBroker(t, 24952, mtlsBridge(24951, certs))
	if !waitConnected(t, remote, "bridge-mtls", 10*time.Second) {
		t.Fatal("the bridge never connected over mTLS")
	}
	deadline := time.After(5 * time.Second)
	for {
		_ = local.MQTT().Publish("out/temp", []byte("21"), false, 1)
		select {
		case m := <-got:
			if m != "fwd/temp=21" {
				t.Fatalf("remote got %q, want fwd/temp=21", m)
			}
			return
		case <-time.After(300 * time.Millisecond):
		case <-deadline:
			t.Fatal("no message forwarded over the mTLS bridge")
		}
	}
}

func TestBridgeMTLSWithoutClientCertificateIsRefused(t *testing.T) {
	remote, certs := mtlsRemote(t, 24953)
	cfg := mtlsBridge(24953, certs)
	delete(cfg, "tlsClientCertPath")
	delete(cfg, "tlsClientKeyPath")
	startBridgeBroker(t, 24954, cfg)
	if waitConnected(t, remote, "bridge-mtls", 3*time.Second) {
		t.Fatal("the bridge got in without a client certificate")
	}
}

func TestBridgeInvalidTLSConfigDoesNotStart(t *testing.T) {
	local := startBridgeBroker(t, 24955, map[string]any{
		"brokerUrl": "tcp://localhost:24999", "clientId": "bad", "tlsServerName": "remote",
	})
	time.Sleep(500 * time.Millisecond)
	if slices.Contains(local.Bridges().Active(), "up") {
		t.Fatal("a bridge with tls* options on tcp:// was started")
	}
}

// The client certificate is not there yet when the bridge starts (not yet
// provisioned): the bridge keeps trying and connects once the files appear.
func TestBridgeMTLSPicksUpCertificateFilesLater(t *testing.T) {
	remote, certs := mtlsRemote(t, 24956)
	dir := t.TempDir()
	cfg := mtlsBridge(24956, certs)
	cfg["tlsClientCertPath"] = filepath.Join(dir, "client.pem")
	cfg["tlsClientKeyPath"] = filepath.Join(dir, "client.key")
	local := startBridgeBroker(t, 24957, cfg)
	time.Sleep(1500 * time.Millisecond)
	if !slices.Contains(local.Bridges().Active(), "up") {
		t.Fatal("a missing certificate file refused the bridge instead of being retried")
	}
	for src, dst := range map[string]string{certs.clientCert: "client.pem", certs.clientKey: "client.key"} {
		b, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, dst), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if !waitConnected(t, remote, "bridge-mtls", 40*time.Second) {
		t.Fatal("the bridge did not connect after the certificate files appeared")
	}
}

// SNI and ALPN from the bridge config reach the server.
func TestBridgeTLSSendsServerNameAndALPN(t *testing.T) {
	certs := createTestCertificates(t)
	pair, err := tls.LoadX509KeyPair(certs.serverCert, certs.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	hellos := make(chan *tls.ClientHelloInfo, 4)
	ln, err := tls.Listen("tcp", "127.0.0.1:24958", &tls.Config{
		Certificates: []tls.Certificate{pair},
		GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
			hellos <- h
			return nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = c.(*tls.Conn).Handshake(); _ = c.Close() }()
		}
	}()
	startBridgeBroker(t, 24959, map[string]any{
		"brokerUrl": "ssl://127.0.0.1:24958", "clientId": "sni", "reconnectDelay": 1000,
		"tlsServerName": "iot.example.com", "tlsAlpnProtocols": []string{"x-amzn-mqtt-ca"},
	})
	select {
	case h := <-hellos:
		if h.ServerName != "iot.example.com" || !slices.Equal(h.SupportedProtos, []string{"x-amzn-mqtt-ca"}) {
			t.Errorf("SNI %q ALPN %v, want iot.example.com and [x-amzn-mqtt-ca]", h.ServerName, h.SupportedProtos)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the bridge never sent a ClientHello")
	}
}
