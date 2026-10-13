package mqttclient

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"monstermq.io/edge/internal/stores"
)

func TestParseBrokerURLAcceptsExistingForms(t *testing.T) {
	for _, raw := range []string{
		"tcp://remote:1883", "mqtt://remote:1883", "ssl://remote:8883",
		"tls://remote:8883", "mqtts://remote:8883", "ws://remote:1884", "wss://remote:8884",
	} {
		if _, err := parseBrokerURL(raw); err != nil {
			t.Errorf("%s: %v", raw, err)
		}
	}
	if _, err := parseBrokerURL("http://remote:80"); err == nil {
		t.Error("http:// must be rejected")
	}
}

// The old client prefixed tcp:// when the scheme was missing and accepted
// mqtt+ssl and tcps for TLS.
func TestParseBrokerURLLegacyForms(t *testing.T) {
	for raw, wantTLS := range map[string]bool{
		"host:1883": false, "mqtt+ssl://h:8883": true, "tcps://h:8883": true,
	} {
		u, err := parseBrokerURL(raw)
		if err != nil {
			t.Errorf("%s: %v", raw, err)
			continue
		}
		if isTLSScheme(u) != wantTLS {
			t.Errorf("%s: TLS = %v, want %v", raw, isTLSScheme(u), wantTLS)
		}
	}
	if u, _ := parseBrokerURL("host:1883"); u == nil || u.Scheme != "tcp" || u.Host != "host:1883" {
		t.Errorf("host:1883 parsed as %v, want tcp://host:1883", u)
	}
}

func TestSessionSettings(t *testing.T) {
	if cs, exp := sessionSettings(true); !cs || exp != 0 {
		t.Errorf("cleanSession=true → CleanStart=%v expiry=%d, want true 0", cs, exp)
	}
	if cs, exp := sessionSettings(false); cs || exp != 0xFFFFFFFF {
		t.Errorf("cleanSession=false → CleanStart=%v expiry=%d, want false 0xFFFFFFFF", cs, exp)
	}
}

func TestSubscribeOptionsSetNoLocalAndRetainAsPublished(t *testing.T) {
	got := subscribeOptions([]transportSubscription{{Topic: "data/#", QoS: 1}, {Topic: "other/#", QoS: 2}})
	want := []paho.SubscribeOptions{
		{Topic: "data/#", QoS: 1, NoLocal: true, RetainAsPublished: true},
		{Topic: "other/#", QoS: 2, NoLocal: true, RetainAsPublished: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("subscribe options = %+v, want %+v", got, want)
	}
}

// No Local on a shared subscription is a protocol error [MQTT-3.8.3-4].
func TestSubscribeOptionsNoLocalOnlyForNonSharedFilters(t *testing.T) {
	got := subscribeOptions([]transportSubscription{{Topic: "$share/g/data/#", QoS: 1}, {Topic: "$queue/cmd/#", QoS: 1}})
	for _, o := range got {
		if o.NoLocal || !o.RetainAsPublished {
			t.Errorf("%s: NoLocal=%v RetainAsPublished=%v, want false and true", o.Topic, o.NoLocal, o.RetainAsPublished)
		}
	}
}

func TestUnixURLsNeedMQTT311(t *testing.T) {
	if _, err := parseBrokerURL("unix:///run/mqtt.sock"); err != nil {
		t.Fatalf("unix:// must be accepted: %v", err)
	}
	_, err := newTransportFor(context.Background(), transportOptions{URL: "unix:///run/mqtt.sock", ProtocolVersion: 5})
	if err == nil || !strings.Contains(err.Error(), "protocolVersion 4") {
		t.Errorf("MQTT 5 over unix:// = %v, want an error naming protocolVersion 4", err)
	}
	tr, err := newTransportFor(context.Background(), transportOptions{URL: "unix:///run/mqtt.sock", ProtocolVersion: 4, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("MQTT 3.1.1 over unix:// = %v", err)
	}
	tr.Close()
}

func TestPublishConversionKeepsProperties(t *testing.T) {
	expiry := uint32(60)
	format := byte(1)
	in := stores.BrokerMessage{
		Payload:                []byte("21.5"),
		ContentType:            "text/plain",
		ResponseTopic:          "data/r",
		CorrelationData:        []byte("c"),
		MessageExpiryInterval:  &expiry,
		PayloadFormatIndicator: &format,
		UserProperties: []stores.UserProperty{
			{Key: "unit", Value: "C"},
			{Key: "unit", Value: "celsius"},
		},
	}
	p := toPahoPublish("data/a", in, 1, true)
	if p.Topic != "data/a" || p.QoS != 1 || !p.Retain {
		t.Fatalf("publish = topic %q qos %d retain %v", p.Topic, p.QoS, p.Retain)
	}
	out := fromPahoPublish(p)
	if out.TopicName != "data/a" || !out.IsRetain || out.QoS != 1 || string(out.Payload) != "21.5" {
		t.Errorf("message = %+v", out)
	}
	if out.ContentType != in.ContentType || out.ResponseTopic != in.ResponseTopic || string(out.CorrelationData) != "c" {
		t.Errorf("properties = %+v", out)
	}
	if *out.MessageExpiryInterval != 60 || *out.PayloadFormatIndicator != 1 {
		t.Errorf("expiry=%d format=%d", *out.MessageExpiryInterval, *out.PayloadFormatIndicator)
	}
	if !reflect.DeepEqual(out.UserProperties, in.UserProperties) {
		t.Errorf("user properties = %+v, want %+v", out.UserProperties, in.UserProperties)
	}
}

func TestIsV3Connack(t *testing.T) {
	for _, tc := range []struct {
		head []byte
		want bool
	}{
		{[]byte{0x20, 0x02}, true},
		{[]byte{0x20, 0x03}, false}, // MQTT 5: always a properties length
		{[]byte{0x20}, false},
		{[]byte{0x30, 0x02}, false},
	} {
		if got := isV3Connack(tc.head); got != tc.want {
			t.Errorf("isV3Connack(%x) = %v, want %v", tc.head, got, tc.want)
		}
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	b := backoff(5 * time.Second)
	if b(0) != 0 {
		t.Errorf("first attempt must not wait, got %v", b(0))
	}
	if b(1) != time.Second || b(2) != 2*time.Second {
		t.Errorf("backoff(1)=%v backoff(2)=%v, want 1s 2s", b(1), b(2))
	}
	if b(10) != 5*time.Second {
		t.Errorf("backoff(10) = %v, want the 5s cap", b(10))
	}
}

func TestWebSocketURLKeepsLibraryDialer(t *testing.T) {
	tr := &mqtt5Transport{}
	for raw, wantOwnDialer := range map[string]bool{
		"tcp://remote:1883": true, "ssl://remote:8883": true,
		"ws://remote:1884": false, "wss://remote:8884": false,
	} {
		u, _ := parseBrokerURL(raw)
		cfg, err := tr.clientConfig(context.Background(), transportOptions{ClientID: "c"}, u)
		if err != nil {
			t.Fatal(err)
		}
		if (cfg.AttemptConnection != nil) != wantOwnDialer {
			t.Errorf("%s: own dialer = %v, want %v", raw, cfg.AttemptConnection != nil, wantOwnDialer)
		}
	}
}

func TestPublishRejectsMessagesLargerThanServerLimit(t *testing.T) {
	tr := &mqtt5Transport{}
	tr.maxPacket.Store(64)
	err := tr.Publish(context.Background(), "data/a", stores.BrokerMessage{Payload: make([]byte, 100)}, 0, false)
	if !errors.Is(err, errTooLarge) {
		t.Errorf("err = %v, want errTooLarge", err)
	}
}

// A broker that only speaks MQTT 3.1.1 answers an MQTT 5 CONNECT with a 3.1.1
// CONNACK ("unacceptable protocol version") and closes, as Mosquitto 1.x does.
func TestTransportReportsRemoteNotMQTT5(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
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
			rejectAsV3(c)
		}
	}()

	errs := make(chan error, 8)
	tr, err := newMQTT5Transport(context.Background(), transportOptions{
		URL:            (&url.URL{Scheme: "tcp", Host: ln.Addr().String()}).String(),
		ClientID:       "bridge-test",
		ConnectTimeout: 2 * time.Second,
		ReconnectDelay: time.Second,
		OnConnectError: func(err error) {
			select {
			case errs <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	select {
	case err := <-errs:
		if !errors.Is(err, errRemoteNotMQTT5) {
			t.Errorf("err = %v, want errRemoteNotMQTT5", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no connect error reported")
	}
}

// Between the library dropping its client and OnConnectionDown, connected is
// still true, and cm.Publish reports ConnectionDownError.
func TestPublishOnDownConnectionReturnsErrTransportDown(t *testing.T) {
	tr, err := newMQTT5Transport(context.Background(), transportOptions{
		URL:            "tcp://127.0.0.1:1",
		ClientID:       "bridge-down",
		ConnectTimeout: 200 * time.Millisecond,
		ReconnectDelay: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	tr.connected.Store(true)
	err = tr.Publish(context.Background(), "data/a", stores.BrokerMessage{Payload: []byte("x")}, 1, false)
	if !errors.Is(err, errTransportDown) {
		t.Errorf("err = %v, want errTransportDown", err)
	}
}

// A 3.1.1 CONNACK on one attempt must not label later, unrelated failures.
func TestRemoteNotMQTT5IsNotStickyAcrossAttempts(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for first := true; ; first = false {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if first {
				rejectAsV3(c)
			}
			_ = c.Close()
		}
	}()
	errs := make(chan error, 8)
	tr, err := newMQTT5Transport(context.Background(), transportOptions{
		URL:            (&url.URL{Scheme: "tcp", Host: ln.Addr().String()}).String(),
		ClientID:       "bridge-test",
		ConnectTimeout: 2 * time.Second,
		ReconnectDelay: time.Second,
		OnConnectError: func(err error) {
			select {
			case errs <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	first, second := <-errs, <-errs
	if !errors.Is(first, errRemoteNotMQTT5) {
		t.Errorf("first = %v, want errRemoteNotMQTT5", first)
	}
	if errors.Is(second, errRemoteNotMQTT5) {
		t.Errorf("second = %v, must not be errRemoteNotMQTT5", second)
	}
}

// A connection that drops after it was up is reported with its error.
func TestTransportReportsConnectionDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = c.Read(make([]byte, 256))
		_, _ = c.Write([]byte{0x20, 0x03, 0x00, 0x00, 0x00}) // MQTT 5 CONNACK, success
		time.Sleep(100 * time.Millisecond)
		_ = c.Close()
	}()
	down := make(chan error, 1)
	tr, err := newMQTT5Transport(context.Background(), transportOptions{
		URL:            (&url.URL{Scheme: "tcp", Host: ln.Addr().String()}).String(),
		ClientID:       "bridge-test",
		ConnectTimeout: 2 * time.Second,
		ReconnectDelay: time.Second,
		OnConnectionDown: func(err error) {
			select {
			case down <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	select {
	case err := <-down:
		if err == nil {
			t.Error("connection down reported without an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connection down not reported")
	}
}

// rejectAsV3 answers with a 3.1.1 CONNACK and closes gracefully; closing with
// unread data would send an RST on Windows and discard the CONNACK.
func rejectAsV3(c net.Conn) {
	_ = c.SetDeadline(time.Now().Add(time.Second))
	_, _ = c.Read(make([]byte, 256))
	_, _ = c.Write([]byte{0x20, 0x02, 0x00, 0x01})
	_ = c.(*net.TCPConn).CloseWrite()
	_, _ = io.Copy(io.Discard, c)
	_ = c.Close()
}

// Close or a reload cancels the context; an in-flight dial must give up, or a
// late CONNECT could take over the new connector's session.
func TestDialHonoursContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tr := &mqtt5Transport{}
	for _, scheme := range []string{"tcp", "ssl"} {
		u := &url.URL{Scheme: scheme, Host: ln.Addr().String()}
		conn, err := tr.dial(ctx, autopaho.ClientConfig{ConnectTimeout: 5 * time.Second, TlsCfg: &tls.Config{}}, u)
		if err == nil {
			_ = conn.Close()
			t.Errorf("%s: dial with a cancelled context succeeded", scheme)
		}
	}
}

func TestMQTT5FileQueueUsesOwnSubdirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "paho-x")
	tr, err := newMQTT5Transport(context.Background(), withTestDefaults(transportOptions{
		URL: "tcp://" + freeAddr(t), ClientID: "q5", LibraryQueue: dir,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	if fi, err := os.Stat(filepath.Join(dir, "v5")); err != nil || !fi.IsDir() {
		t.Fatalf("MQTT 5 queue directory %s/v5 missing: %v", dir, err)
	}
}
