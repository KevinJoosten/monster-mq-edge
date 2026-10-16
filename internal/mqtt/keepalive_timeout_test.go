package mqtt

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"monstermq.io/edge/internal/mqtt/listeners"
	"monstermq.io/edge/internal/mqtt/packets"
)

type allowAllHook struct{ HookBase }

func (h *allowAllHook) ID() string                                         { return "allow" }
func (h *allowAllHook) Provides(b byte) bool                               { return b == OnConnectAuthenticate }
func (h *allowAllHook) OnConnectAuthenticate(*Client, packets.Packet) bool { return true }

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

type silentConn struct {
	version       byte
	secondConnect bool // send CONNECT twice
	deny          bool // no auth hook, so the CONNECT is refused
}

// silentClient connects with keepalive 1s, never pings, and returns every byte
// the broker sent until it closed the connection, plus the broker log.
func silentClient(t *testing.T, c silentConn) (received []byte, log string) {
	t.Helper()
	logs := new(syncBuffer)
	s := New(&Options{Logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))})
	if !c.deny {
		if err := s.AddHook(new(allowAllHook), nil); err != nil {
			t.Fatal(err)
		}
	}
	l := listeners.NewTCP(listeners.Config{ID: "t1", Address: "127.0.0.1:0"})
	if err := s.AddListener(l); err != nil {
		t.Fatal(err)
	}
	if err := s.Serve(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	conn, err := net.Dial("tcp", l.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: c.version,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: "pds-1",
			Keepalive:        1,
			Clean:            true,
			UsernameFlag:     true,
			Username:         []byte("machine"),
		},
	}
	buf := new(bytes.Buffer)
	if err := pk.ConnectEncode(buf); err != nil {
		t.Fatal(err)
	}
	if c.secondConnect {
		buf.Write(bytes.Clone(buf.Bytes()))
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	received, _ = io.ReadAll(conn)
	time.Sleep(100 * time.Millisecond) // let the listener goroutine log
	return received, logs.String()
}

// packetTypes splits a raw MQTT byte stream into (type, body) pairs.
func packetTypes(b []byte) (types []byte, bodies [][]byte) {
	for len(b) > 1 {
		typ := b[0] >> 4
		n, mul, i := 0, 1, 1
		for ; i < len(b); i++ {
			n += int(b[i]&127) * mul
			mul *= 128
			if b[i]&128 == 0 {
				break
			}
		}
		i++
		if i+n > len(b) {
			break
		}
		types = append(types, typ)
		bodies = append(bodies, b[i:i+n])
		b = b[i+n:]
	}
	return types, bodies
}

// warnLines returns the WARN lines, minus the below-minimum-keepalive warning
// that keepalive 1 also trips.
func warnLines(log string) (warns []string) {
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, "level=WARN") && !strings.Contains(line, ErrMinimumKeepalive.Error()) {
			warns = append(warns, line)
		}
	}
	return warns
}

func TestKeepaliveTimeoutLogsOneWarnWithClientContext(t *testing.T) {
	for _, version := range []byte{4, 5} {
		_, log := silentClient(t, silentConn{version: version})
		warns := warnLines(log)
		if len(warns) != 1 {
			t.Fatalf("v%d: got %d WARN lines, want 1:\n%s", version, len(warns), log)
		}
		for _, want := range []string{`msg="client disconnected"`, `reason="keep alive timeout"`, "reason_code=0x8D",
			"client=pds-1", "username=machine", "remote=127.0.0.1:", "keepalive=1"} {
			if !strings.Contains(warns[0], want) {
				t.Errorf("v%d: WARN line missing %q: %s", version, want, warns[0])
			}
		}
	}
}

func TestKeepaliveTimeoutSendsDisconnect0x8DToV5Client(t *testing.T) {
	received, _ := silentClient(t, silentConn{version: 5})
	types, bodies := packetTypes(received)
	if len(types) != 2 || types[0] != packets.Connack || types[1] != packets.Disconnect {
		t.Fatalf("got packet types %v, want [CONNACK DISCONNECT]", types)
	}
	if len(bodies[1]) == 0 || bodies[1][0] != packets.ErrKeepAliveTimeout.Code {
		t.Fatalf("DISCONNECT body %x, want reason code 0x8D", bodies[1])
	}
}

func TestErrorReasonCodeDisconnectLogsOneWarn(t *testing.T) {
	for name, tc := range map[string]struct {
		conn silentConn
		want string
	}{
		"second connect":  {silentConn{version: 5, secondConnect: true}, `reason="protocol violation: second connect packet" reason_code=0x82`},
		"refused connect": {silentConn{version: 4, deny: true}, `reason="bad username or password" reason_code=0x86`},
	} {
		_, log := silentClient(t, tc.conn)
		warns := warnLines(log)
		if len(warns) != 1 {
			t.Fatalf("%s: got %d WARN lines, want 1:\n%s", name, len(warns), log)
		}
		for _, want := range []string{`msg="client disconnected"`, tc.want, "client=pds-1", "username=machine"} {
			if !strings.Contains(warns[0], want) {
				t.Errorf("%s: WARN line missing %q: %s", name, want, warns[0])
			}
		}
	}
}
