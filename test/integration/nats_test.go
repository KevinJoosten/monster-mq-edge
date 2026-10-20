package integration

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats.go"
	"golang.org/x/crypto/bcrypt"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/stores"
)

func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForPort(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for port %d", port)
}

func startWithNats(t *testing.T, cfgFns ...func(*config.Config)) (*broker.Server, int, int) {
	t.Helper()
	mqttPort := getFreePort(t)
	natsPort := getFreePort(t)
	cfg := config.Default()
	cfg.NodeID = fmt.Sprintf("n-%d", natsPort)
	cfg.TCP.Enabled = true
	cfg.TCP.Port = mqttPort
	cfg.WS.Enabled = false
	cfg.NATS = natsPort
	cfg.SQLite.Path = filepath.Join(t.TempDir(), "nats.db")
	for _, fn := range cfgFns {
		fn(cfg)
	}
	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	go func() { _ = srv.Serve() }()
	waitForPort(t, natsPort)
	return srv, mqttPort, natsPort
}

// 1. INFO bytes
func TestNatsInfoLine(t *testing.T) {
	// With user management off
	srv, _, natsPort := startWithNats(t, func(c *config.Config) {
		c.UserManagement.Enabled = false
	})
	defer srv.Close()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	expected := "INFO {\"server_id\":\"monstermq\",\"server_name\":\"MonsterMQ\",\"version\":\"2.10.0\",\"proto\":1,\"headers\":true,\"max_payload\":1048576,\"jetstream\":true,\"auth_required\":false}\r\n"
	if line != expected {
		t.Fatalf("expected INFO line:\n%q\ngot:\n%q", expected, line)
	}

	// With user management on
	srvAuth, _, natsPortAuth := startWithNats(t, func(c *config.Config) {
		c.UserManagement.Enabled = true
	})
	defer srvAuth.Close()

	connAuth, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPortAuth))
	if err != nil {
		t.Fatal(err)
	}
	defer connAuth.Close()

	readerAuth := bufio.NewReader(connAuth)
	lineAuth, err := readerAuth.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	expectedAuth := "INFO {\"server_id\":\"monstermq\",\"server_name\":\"MonsterMQ\",\"version\":\"2.10.0\",\"proto\":1,\"headers\":true,\"max_payload\":1048576,\"jetstream\":true,\"auth_required\":true}\r\n"
	if lineAuth != expectedAuth {
		t.Fatalf("expected auth INFO line:\n%q\ngot:\n%q", expectedAuth, lineAuth)
	}
}

// 2. Raw protocol, one socket
func TestNatsRawProtocol(t *testing.T) {
	srv, _, natsPort := startWithNats(t)
	defer srv.Close()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n') // drop INFO

	readLine := func() string {
		l, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		return l
	}

	write := func(s string) {
		if _, err := conn.Write([]byte(s)); err != nil {
			t.Fatalf("write error: %v", err)
		}
	}

	// PING before CONNECT -> PONG
	write("PING\r\n")
	if l := readLine(); l != "PONG\r\n" {
		t.Fatalf("expected PONG, got %q", l)
	}

	// FOO -> -ERR 'Unknown Protocol Operation', next PING -> PONG
	write("FOO\r\n")
	if l := readLine(); l != "-ERR 'Unknown Protocol Operation'\r\n" {
		t.Fatalf("expected unknown operation error, got %q", l)
	}
	write("PING\r\n")
	if l := readLine(); l != "PONG\r\n" {
		t.Fatalf("expected PONG, got %q", l)
	}

	// CONNECT {x -> -ERR 'Invalid CONNECT JSON'
	write("CONNECT {x\r\n")
	if l := readLine(); l != "-ERR 'Invalid CONNECT JSON'\r\n" {
		t.Fatalf("expected invalid connect JSON error, got %q", l)
	}

	// CONNECT {"verbose":true} -> +OK
	write("CONNECT {\"verbose\":true}\r\n")
	if l := readLine(); l != "+OK\r\n" {
		t.Fatalf("expected +OK, got %q", l)
	}

	// sub a.b 1 (lowercase), then PUB a.b 5 + hello -> MSG a.b 1 5\r\nhello\r\n
	write("sub a.b 1\r\n")
	write("PUB a.b 5\r\nhello\r\n")
	if l := readLine(); l != "MSG a.b 1 5\r\n" {
		t.Fatalf("expected MSG header, got %q", l)
	}
	if l := readLine(); l != "hello\r\n" {
		t.Fatalf("expected MSG payload, got %q", l)
	}

	// SUB x -> -ERR 'Invalid SUB'
	write("SUB x\r\n")
	if l := readLine(); l != "-ERR 'Invalid SUB'\r\n" {
		t.Fatalf("expected invalid sub, got %q", l)
	}

	// PUB a -1 -> -ERR 'Invalid PUB byte count'
	write("PUB a -1\r\n")
	if l := readLine(); l != "-ERR 'Invalid PUB byte count'\r\n" {
		t.Fatalf("expected invalid byte count, got %q", l)
	}

	// HPUB a 10 5 -> -ERR 'Invalid HPUB byte count'
	write("HPUB a 10 5\r\n")
	if l := readLine(); l != "-ERR 'Invalid HPUB byte count'\r\n" {
		t.Fatalf("expected invalid hpub byte count, got %q", l)
	}

	// PUB line and payload in one TCP write
	write("PUB a.b 4\r\ntest\r\n")
	if l := readLine(); l != "MSG a.b 1 4\r\n" {
		t.Fatalf("expected MSG header, got %q", l)
	}
	if l := readLine(); l != "test\r\n" {
		t.Fatalf("expected MSG payload, got %q", l)
	}
}

// 3. MQTT interop
func TestNatsMqttInterop(t *testing.T) {
	srv, mqttPort, natsPort := startWithNats(t)
	defer srv.Close()

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	mqttOpts := paho.NewClientOptions().AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", mqttPort)).SetClientID("mqtt-test")
	mc := paho.NewClient(mqttOpts)
	if tok := mc.Connect(); tok.Wait() && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	defer mc.Disconnect(100)

	// MQTT publish sensors/temp/room1 reaches nats.go sensors.> and sensors.*.room1 as sensors.temp.room1
	msgWild := make(chan *nats.Msg, 2)
	msgStar := make(chan *nats.Msg, 2)
	_, _ = nc.Subscribe("sensors.>", func(m *nats.Msg) { msgWild <- m })
	_, _ = nc.Subscribe("sensors.*.room1", func(m *nats.Msg) { msgStar <- m })
	time.Sleep(100 * time.Millisecond)

	mc.Publish("sensors/temp/room1", 0, false, []byte("21.5"))

	select {
	case m := <-msgWild:
		if m.Subject != "sensors.temp.room1" || string(m.Data) != "21.5" {
			t.Fatalf("unexpected msg on sensors.>: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sensors.>")
	}

	select {
	case m := <-msgStar:
		if m.Subject != "sensors.temp.room1" || string(m.Data) != "21.5" {
			t.Fatalf("unexpected msg on sensors.*.room1: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sensors.*.room1")
	}

	// NATS publish test.hello reaches MQTT test/#
	mqttMsgs := make(chan paho.Message, 2)
	mc.Subscribe("test/#", 0, func(_ paho.Client, m paho.Message) {
		mqttMsgs <- m
	})
	time.Sleep(100 * time.Millisecond)

	_ = nc.Publish("test.hello", []byte("world"))

	select {
	case m := <-mqttMsgs:
		if m.Topic() != "test/hello" || string(m.Payload()) != "world" {
			t.Fatalf("unexpected mqtt msg: topic=%q payload=%q", m.Topic(), string(m.Payload()))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for mqtt message")
	}

	// MQTT a reaches NATS a.>
	msgA := make(chan *nats.Msg, 2)
	_, _ = nc.Subscribe("a.>", func(m *nats.Msg) { msgA <- m })
	time.Sleep(100 * time.Millisecond)
	mc.Publish("a", 0, false, []byte("match_a"))
	select {
	case m := <-msgA:
		if m.Subject != "a" || string(m.Data) != "match_a" {
			t.Fatalf("unexpected msg on a.>: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for a.>")
	}

	// MQTT room 1/t arrives as room_1.t
	msgRoom := make(chan *nats.Msg, 2)
	subRoom, _ := nc.Subscribe(">", func(m *nats.Msg) {
		if strings.HasPrefix(m.Subject, "room") {
			msgRoom <- m
		}
	})
	defer subRoom.Unsubscribe()
	time.Sleep(100 * time.Millisecond)
	mc.Publish("room 1/t", 0, false, []byte("temp"))
	select {
	case m := <-msgRoom:
		if m.Subject != "room_1.t" || string(m.Data) != "temp" {
			t.Fatalf("unexpected msg on room_1.t: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for room_1.t")
	}

	// A NATS client receives its own publish
	echoMsgs := make(chan *nats.Msg, 2)
	_, _ = nc.Subscribe("echo.test", func(m *nats.Msg) { echoMsgs <- m })
	time.Sleep(100 * time.Millisecond)
	_ = nc.Publish("echo.test", []byte("self"))
	select {
	case m := <-echoMsgs:
		if string(m.Data) != "self" {
			t.Fatalf("unexpected self msg: %s", string(m.Data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for self publish")
	}

	// Queue groups are ignored: both receive every message
	q1Chan := make(chan *nats.Msg, 2)
	q2Chan := make(chan *nats.Msg, 2)
	nc2, _ := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	defer nc2.Close()
	_, _ = nc.QueueSubscribe("queue.topic", "q", func(m *nats.Msg) { q1Chan <- m })
	_, _ = nc2.QueueSubscribe("queue.topic", "q", func(m *nats.Msg) { q2Chan <- m })
	time.Sleep(100 * time.Millisecond)
	_ = nc.Publish("queue.topic", []byte("broadcast"))

	select {
	case <-q1Chan:
	case <-time.After(2 * time.Second):
		t.Fatal("q1 did not receive message")
	}
	select {
	case <-q2Chan:
	case <-time.After(2 * time.Second):
		t.Fatal("q2 did not receive message")
	}
}

// 4. Headers and reply
func TestNatsHeadersAndReply(t *testing.T) {
	srv, mqttPort, natsPort := startWithNats(t)
	defer srv.Close()

	// A raw client without headers gets MSG for an MQTT v5 publish with user properties
	connRaw, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer connRaw.Close()
	rRaw := bufio.NewReader(connRaw)
	_, _ = rRaw.ReadString('\n') // INFO
	_, _ = connRaw.Write([]byte("SUB test.hdr 1\r\n"))
	time.Sleep(100 * time.Millisecond)

	// Dial raw MQTT v5 client
	c5, _ := dialRaw(t, mqttPort, rawConnect{ClientID: "v5-publisher", Version: 5, Clean: true})
	defer c5.Close()
	pk1 := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 0},
		TopicName:       "test/hdr",
		Payload:         []byte("data"),
		ProtocolVersion: 5,
	}
	pk1.Properties.User = []packets.UserProperty{{Key: "foo", Val: "bar"}}
	var b1 bytes.Buffer
	_ = pk1.PublishEncode(&b1)
	_, _ = c5.conn.Write(b1.Bytes())

	line1, _ := rRaw.ReadString('\n')
	if line1 != "MSG test.hdr 1 4\r\n" {
		t.Fatalf("expected MSG without headers, got %q", line1)
	}
	line2, _ := rRaw.ReadString('\n')
	if line2 != "data\r\n" {
		t.Fatalf("expected payload data, got %q", line2)
	}

	// With headers:true it gets HMSG with NATS/1.0\r\nk: v\r\n\r\n
	connHdr, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer connHdr.Close()
	rHdr := bufio.NewReader(connHdr)
	_, _ = rHdr.ReadString('\n')
	_, _ = connHdr.Write([]byte("CONNECT {\"headers\":true}\r\nSUB test.hdr 1\r\n"))
	time.Sleep(100 * time.Millisecond)

	var b2 bytes.Buffer
	_ = pk1.PublishEncode(&b2)
	_, _ = c5.conn.Write(b2.Bytes())

	hmsgLine, _ := rHdr.ReadString('\n')
	if !strings.HasPrefix(hmsgLine, "HMSG test.hdr 1 ") {
		t.Fatalf("expected HMSG prefix, got %q", hmsgLine)
	}
	// Header block
	hdrLine1, _ := rHdr.ReadString('\n') // NATS/1.0\r\n
	if hdrLine1 != "NATS/1.0\r\n" {
		t.Fatalf("expected NATS/1.0, got %q", hdrLine1)
	}
	hdrLine2, _ := rHdr.ReadString('\n') // foo: bar\r\n
	if hdrLine2 != "foo: bar\r\n" {
		t.Fatalf("expected foo: bar, got %q", hdrLine2)
	}
	hdrLine3, _ := rHdr.ReadString('\n') // \r\n
	if hdrLine3 != "\r\n" {
		t.Fatalf("expected empty line after headers, got %q", hdrLine3)
	}
	payloadLine, _ := rHdr.ReadString('\n')
	if payloadLine != "data\r\n" {
		t.Fatalf("expected data, got %q", payloadLine)
	}

	// HPUB k: v reaches an MQTT v5 subscriber as user property
	c5Sub, _ := dialRaw(t, mqttPort, rawConnect{ClientID: "v5-sub", Version: 5, Clean: true})
	defer c5Sub.Close()
	c5Sub.Subscribe(sub("from/nats", 0))
	time.Sleep(100 * time.Millisecond)

	hdrBlock := "NATS/1.0\r\nx-key: x-val\r\n\r\n"
	_, _ = connHdr.Write([]byte(fmt.Sprintf("HPUB from.nats %d %d\r\n%sbody\r\n", len(hdrBlock), len(hdrBlock)+4, hdrBlock)))

	pk, ok := c5Sub.NextOn("from/nats", 2*time.Second)
	if !ok {
		t.Fatal("mqtt subscriber did not receive HPUB message")
	}
	if string(pk.Payload) != "body" {
		t.Fatalf("expected body, got %q", string(pk.Payload))
	}
	if len(pk.Properties.User) == 0 || pk.Properties.User[0].Key != "x-key" || pk.Properties.User[0].Val != "x-val" {
		t.Fatalf("expected user property x-key: x-val, got %+v", pk.Properties.User)
	}

	// NATS to NATS request/reply
	nc, _ := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	defer nc.Close()
	_, _ = nc.Subscribe("service.echo", func(m *nats.Msg) {
		_ = m.Respond([]byte("ack:" + string(m.Data)))
	})
	time.Sleep(100 * time.Millisecond)

	rep, err := nc.Request("service.echo", []byte("ping"), 2*time.Second)
	if err != nil {
		t.Fatalf("nc.Request error: %v", err)
	}
	if string(rep.Data) != "ack:ping" {
		t.Fatalf("expected ack:ping, got %q", string(rep.Data))
	}
}

// 5. Auth
func TestNatsAuth(t *testing.T) {
	srv, _, natsPort := startWithNats(t, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = false
	})
	defer srv.Close()

	hash, _ := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	_ = srv.Storage().Users.CreateUser(context.Background(), stores.User{
		Username:     "alice",
		PasswordHash: string(hash),
		Enabled:      true,
		CanPublish:   true,
		CanSubscribe: true,
	})
	_ = srv.AuthCache().Refresh(context.Background())

	// Wrong password -> -ERR 'Authorization Violation', then EOF
	conn, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n') // INFO
	_, _ = conn.Write([]byte("CONNECT {\"user\":\"alice\",\"pass\":\"wrong\"}\r\n"))
	errLine, _ := r.ReadString('\n')
	if errLine != "-ERR 'Authorization Violation'\r\n" {
		t.Fatalf("expected auth violation, got %q", errLine)
	}
	// Should be closed
	_, err := r.ReadByte()
	if err == nil {
		t.Fatal("expected connection to close on auth failure")
	}
	conn.Close()

	// Empty user with AnonymousEnabled: false -> error and close
	connAnon, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	rAnon := bufio.NewReader(connAnon)
	_, _ = rAnon.ReadString('\n')
	_, _ = connAnon.Write([]byte("CONNECT {\"user\":\"\",\"pass\":\"\"}\r\n"))
	errLineAnon, _ := rAnon.ReadString('\n')
	if errLineAnon != "-ERR 'Authorization Violation'\r\n" {
		t.Fatalf("expected auth violation for anonymous, got %q", errLineAnon)
	}
	connAnon.Close()

	// With user management on, PUB a 5\r\nhello\r\nPING\r\n before CONNECT -> exactly one -ERR 'Authorization Violation', then PONG (F1)
	connF1, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer connF1.Close()
	rF1 := bufio.NewReader(connF1)
	_, _ = rF1.ReadString('\n')
	_, _ = connF1.Write([]byte("PUB a 5\r\nhello\r\nPING\r\n"))
	resp1, _ := rF1.ReadString('\n')
	if resp1 != "-ERR 'Authorization Violation'\r\n" {
		t.Fatalf("expected auth violation on unauthenticated pub, got %q", resp1)
	}
	resp2, _ := rF1.ReadString('\n')
	if resp2 != "PONG\r\n" {
		t.Fatalf("expected PONG after payload consumed, got %q", resp2)
	}
}

// 6. ACL (user allowed allowed/#, deny rule allowed/secret/#)
func TestNatsACL(t *testing.T) {
	srv, mqttPort, natsPort := startWithNats(t, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = false
	})
	defer srv.Close()

	hash, _ := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	_ = srv.Storage().Users.CreateUser(context.Background(), stores.User{
		Username:     "bob",
		PasswordHash: string(hash),
		Enabled:      true,
		CanPublish:   true,
		CanSubscribe: true,
	})
	// Allow allowed/#
	_ = srv.Storage().Users.CreateAclRule(context.Background(), stores.AclRule{
		Username:     "bob",
		TopicPattern: "allowed/#",
		CanPublish:   true,
		CanSubscribe: true,
		Priority:     1,
	})
	// Deny allowed/secret/#
	_ = srv.Storage().Users.CreateAclRule(context.Background(), stores.AclRule{
		Username:     "bob",
		TopicPattern: "allowed/secret/#",
		CanPublish:   false,
		CanSubscribe: false,
		Priority:     10,
	})
	_ = srv.AuthCache().Refresh(context.Background())

	nc, err := nats.Connect(fmt.Sprintf("nats://bob:secret@127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// Raw connection to inspect exact error strings
	conn, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer conn.Close()
	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n')
	_, _ = conn.Write([]byte("CONNECT {\"user\":\"bob\",\"pass\":\"secret\"}\r\n"))

	// SUB denied.x -> -ERR 'Permissions Violation for Subscription to "denied.x"'
	_, _ = conn.Write([]byte("SUB denied.x 1\r\n"))
	l1, _ := r.ReadString('\n')
	if l1 != "-ERR 'Permissions Violation for Subscription to \"denied.x\"'\r\n" {
		t.Fatalf("expected sub permission violation, got %q", l1)
	}

	// PUB denied.x 5 + payload -> -ERR 'Permissions Violation for Publish to "denied.x"', then PING -> PONG (F1)
	_, _ = conn.Write([]byte("PUB denied.x 5\r\nhello\r\nPING\r\n"))
	l2, _ := r.ReadString('\n')
	if l2 != "-ERR 'Permissions Violation for Publish to \"denied.x\"'\r\n" {
		t.Fatalf("expected pub permission violation, got %q", l2)
	}
	l3, _ := r.ReadString('\n')
	if l3 != "PONG\r\n" {
		t.Fatalf("expected PONG after rejected PUB, got %q", l3)
	}

	// SUB allowed.> receives allowed/a but not allowed/secret/b (F6)
	allowedChan := make(chan *nats.Msg, 5)
	_, _ = nc.Subscribe("allowed.>", func(m *nats.Msg) {
		allowedChan <- m
	})
	time.Sleep(100 * time.Millisecond)

	// Create admin user for MQTT publish
	_ = srv.Storage().Users.CreateUser(context.Background(), stores.User{
		Username:     "admin",
		PasswordHash: string(hash),
		Enabled:      true,
		IsAdmin:      true,
		CanPublish:   true,
		CanSubscribe: true,
	})
	_ = srv.AuthCache().Refresh(context.Background())

	// Publish through an admin MQTT client
	mqttOpts := paho.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", mqttPort)).
		SetClientID("admin-pub").
		SetUsername("admin").
		SetPassword("secret").
		SetConnectTimeout(2 * time.Second)
	mc := paho.NewClient(mqttOpts)
	if tok := mc.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("mqtt connect failed: %v", tok.Error())
	}
	defer mc.Disconnect(0)

	mc.Publish("allowed/a", 0, false, []byte("ok"))
	mc.Publish("allowed/secret/b", 0, false, []byte("denied"))
	mc.Publish("allowed/c", 0, false, []byte("ok2"))

	var received []string
	deadline := time.After(2 * time.Second)
	for len(received) < 2 {
		select {
		case m := <-allowedChan:
			received = append(received, m.Subject)
			if m.Subject == "allowed.secret.b" {
				t.Fatalf("F6 violated: received denied topic allowed.secret.b")
			}
		case <-deadline:
			break
		}
	}
	if len(received) != 2 || received[0] != "allowed.a" || received[1] != "allowed.c" {
		t.Fatalf("expected allowed.a and allowed.c, got %+v", received)
	}
}

// 7. Fixes: F2, F4, F3, F7, F5
func TestNatsFixes(t *testing.T) {
	srv, _, natsPort := startWithNats(t, func(c *config.Config) {
		c.Metrics.CollectionIntervalSeconds = 1
	})
	defer srv.Close()

	// F2: SUB a 1, SUB b 1; publishing a and b delivers only b; after UNSUB 1 nothing arrives
	connF2, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer connF2.Close()
	rF2 := bufio.NewReader(connF2)
	_, _ = rF2.ReadString('\n')
	_, _ = connF2.Write([]byte("SUB a 1\r\nSUB b 1\r\n"))
	time.Sleep(50 * time.Millisecond)

	nc, _ := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	defer nc.Close()
	_ = nc.Publish("a", []byte("msg_a"))
	_ = nc.Publish("b", []byte("msg_b"))

	line1, _ := rF2.ReadString('\n')
	if line1 != "MSG b 1 5\r\n" {
		t.Fatalf("F2: expected MSG b 1 5, got %q", line1)
	}
	_, _ = rF2.ReadString('\n') // payload
	_, _ = connF2.Write([]byte("UNSUB 1\r\n"))
	time.Sleep(50 * time.Millisecond)
	_ = nc.Publish("b", []byte("msg_b2"))

	// Verify nothing arrives after UNSUB
	_ = connF2.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err := rF2.ReadString('\n')
	if err == nil {
		t.Fatal("F2: expected no message after UNSUB 1")
	}
	_ = connF2.SetReadDeadline(time.Time{})

	// F4: UNSUB 1 2 delivers exactly 2 messages; UNSUB 1 1 after 3 delivered removes SID at once
	connF4, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer connF4.Close()
	rF4 := bufio.NewReader(connF4)
	_, _ = rF4.ReadString('\n')
	_, _ = connF4.Write([]byte("SUB f4.test 1\r\nUNSUB 1 2\r\n"))
	time.Sleep(50 * time.Millisecond)

	_ = nc.Publish("f4.test", []byte("1"))
	_ = nc.Publish("f4.test", []byte("2"))
	_ = nc.Publish("f4.test", []byte("3"))

	m1H, _ := rF4.ReadString('\n')
	m1P, _ := rF4.ReadString('\n')
	m2H, _ := rF4.ReadString('\n')
	m2P, _ := rF4.ReadString('\n')
	if m1H != "MSG f4.test 1 1\r\n" || m1P != "1\r\n" || m2H != "MSG f4.test 1 1\r\n" || m2P != "2\r\n" {
		t.Fatalf("F4: unexpected delivery: %q %q %q %q", m1H, m1P, m2H, m2P)
	}
	// 3rd message must not arrive
	_ = connF4.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err = rF4.ReadString('\n')
	if err == nil {
		t.Fatal("F4: expected no 3rd message")
	}
	_ = connF4.SetReadDeadline(time.Time{})

	// F3: CONNECT {"user":..,"pass":..}\r\nSUB a 1\r\n in one write receives later publish
	connF3, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer connF3.Close()
	rF3 := bufio.NewReader(connF3)
	_, _ = rF3.ReadString('\n')
	_, _ = connF3.Write([]byte("CONNECT {\"verbose\":false}\r\nSUB f3.topic 1\r\n"))
	time.Sleep(50 * time.Millisecond)
	_ = nc.Publish("f3.topic", []byte("f3_data"))
	f3H, _ := rF3.ReadString('\n')
	if f3H != "MSG f3.topic 1 7\r\n" {
		t.Fatalf("F3: expected message header, got %q", f3H)
	}

	// F7: PUB a 2000000 -> -ERR 'Maximum Payload Violation', then EOF
	connF7, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer connF7.Close()
	rF7 := bufio.NewReader(connF7)
	_, _ = rF7.ReadString('\n')
	_, _ = connF7.Write([]byte("PUB a 2000000\r\n"))
	f7Err, _ := rF7.ReadString('\n')
	if f7Err != "-ERR 'Maximum Payload Violation'\r\n" {
		t.Fatalf("F7: expected Maximum Payload Violation, got %q", f7Err)
	}
	_, err = rF7.ReadByte()
	if err == nil {
		t.Fatal("F7: expected EOF after Maximum Payload Violation")
	}

	// F7: 70 KiB line -> -ERR 'Maximum Control Line Exceeded', then EOF
	connF7Line, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer connF7Line.Close()
	rF7Line := bufio.NewReader(connF7Line)
	_, _ = rF7Line.ReadString('\n')
	longLine := strings.Repeat("A", 70000) + "\r\n"
	_, _ = connF7Line.Write([]byte(longLine))
	f7LineErr, _ := rF7Line.ReadString('\n')
	if f7LineErr != "-ERR 'Maximum Control Line Exceeded'\r\n" {
		t.Fatalf("F7: expected Maximum Control Line Exceeded, got %q", f7LineErr)
	}
	_, err = rF7Line.ReadByte()
	if err == nil {
		t.Fatal("F7: expected EOF after Maximum Control Line Exceeded")
	}
}

// 8. Session (S5) & Dashboard removal
func TestNatsSessionLifecycle(t *testing.T) {
	mqttPort := getFreePort(t)
	gqlPort := getFreePort(t)
	natsPort := getFreePort(t)

	srv, url := startWithGraphQL(t, mqttPort, gqlPort, func(c *config.Config) {
		c.NATS = natsPort
	})
	defer srv.Close()
	waitForPort(t, natsPort)

	conn, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n') // INFO
	_, _ = conn.Write([]byte("CONNECT {\"verbose\":false}\r\n"))
	time.Sleep(100 * time.Millisecond)

	// Query GraphQL sessions
	data := gqlQuery(t, url, `{ sessions { clientId connected protocolVersion information } }`, nil)
	sessions := data["sessions"].([]any)
	var natsSession map[string]any
	for _, s := range sessions {
		sm := s.(map[string]any)
		if strings.HasPrefix(sm["clientId"].(string), "nats-") {
			natsSession = sm
			break
		}
	}
	if natsSession == nil {
		t.Fatalf("NATS session not found in GraphQL sessions: %+v", sessions)
	}
	if natsSession["connected"] != true {
		t.Fatalf("expected connected=true, got %v", natsSession["connected"])
	}
	if natsSession["protocolVersion"] != nil {
		t.Fatalf("expected null protocolVersion, got %v", natsSession["protocolVersion"])
	}
	infoStr := natsSession["information"].(string)
	if !strings.Contains(infoStr, `"ProtocolVersion":"NATS"`) {
		t.Fatalf("expected ProtocolVersion: NATS in information, got %q", infoStr)
	}

	natsID := natsSession["clientId"].(string)

	// Remove session from dashboard
	removeMutation := fmt.Sprintf(`mutation { session { removeSessions(clientIds: ["%s"]) { success removedCount } } }`, natsID)
	res := gqlQuery(t, url, removeMutation, nil)
	rMap := res["session"].(map[string]any)["removeSessions"].(map[string]any)
	if rMap["success"] != true || rMap["removedCount"] != float64(1) {
		t.Fatalf("removeSessions failed: %+v", rMap)
	}

	// Verify socket was closed (EOF)
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, err := r.ReadByte()
	if err == nil {
		t.Fatal("expected EOF on NATS socket after dashboard removal")
	}

	// Verify session record is gone
	present, err := srv.Storage().Sessions.IsPresent(context.Background(), natsID)
	if err != nil || present {
		t.Fatalf("expected session record to be deleted, present=%v", present)
	}
}

// 9. Slow consumer does not block publisher or disconnect subscriber when no limit is configured
func TestNatsSlowConsumer(t *testing.T) {
	srv, _, natsPort := startWithNats(t)
	defer srv.Close()

	conn, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer conn.Close()
	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n')
	_, _ = conn.Write([]byte("CONNECT {}\r\nSUB slow.test 1\r\n"))
	time.Sleep(50 * time.Millisecond)

	nc, _ := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	defer nc.Close()

	largePayload := make([]byte, 16*1024)
	for i := range largePayload {
		largePayload[i] = 'X'
	}

	// Don't read on conn; push messages and verify publisher is not blocked
	for i := 0; i < 2000; i++ {
		_ = nc.Publish("slow.test", largePayload)
	}
	_ = nc.Flush()

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("expected subscriber connection to stay open and receive messages, got err: %v", err)
	}
	if !strings.HasPrefix(line, "MSG slow.test 1 ") {
		t.Fatalf("expected MSG frame on subscriber, got: %q", line)
	}

	// Broker keeps serving: publish and receive normally
	nc2, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatalf("broker failed to keep serving: %v", err)
	}
	defer nc2.Close()
	sub2, _ := nc2.SubscribeSync("test.still.working")
	_ = nc2.Publish("test.still.working", []byte("alive"))
	msg, err := sub2.NextMsg(2 * time.Second)
	if err != nil || string(msg.Data) != "alive" {
		t.Fatalf("failed to receive message on still working broker: %v", err)
	}
}

// 10. Config: NATS: 0 binds nothing; brokerConfig.natsPort reports configured port
func TestNatsConfigAndDisabled(t *testing.T) {
	mqttPort := getFreePort(t)
	gqlPort := getFreePort(t)

	srv, url := startWithGraphQL(t, mqttPort, gqlPort, func(c *config.Config) {
		c.NATS = 0
	})
	defer srv.Close()

	if srv.NatsPort() != 0 {
		t.Fatalf("expected NatsPort 0, got %d", srv.NatsPort())
	}

	data := gqlQuery(t, url, `{ brokerConfig { natsPort } }`, nil)
	bc := data["brokerConfig"].(map[string]any)
	if bc["natsPort"].(float64) != 0 {
		t.Fatalf("expected natsPort 0 in brokerConfig, got %v", bc["natsPort"])
	}
}
