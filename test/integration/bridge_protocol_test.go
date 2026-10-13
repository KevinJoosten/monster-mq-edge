package integration

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"monstermq.io/edge/internal/stores"
)

// startV311OnlyProxy plays a broker that supports only MQTT 3.1.1 in front of
// target: an MQTT 5 CONNECT gets a 3.1.1 CONNACK refusing the protocol
// version; any other connection is piped through. It counts the refusals.
func startV311OnlyProxy(t *testing.T, listenPort int, target string) *atomic.Int32 {
	t.Helper()
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", listenPort))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	refused := &atomic.Int32{}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go proxyV311Only(c, target, refused)
		}
	}()
	return refused
}

func proxyV311Only(c net.Conn, target string, refused *atomic.Int32) {
	defer c.Close()
	connect, err := readPacket(c)
	if err != nil {
		return
	}
	if connectProtocolLevel(connect) == 5 {
		refused.Add(1)
		_, _ = c.Write([]byte{0x20, 0x02, 0x00, 0x01}) // CONNACK: unacceptable protocol version
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
		_, _ = io.Copy(io.Discard, c) // let the client close first, so no RST eats the CONNACK
		return
	}
	up, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	defer up.Close()
	if _, err := up.Write(connect); err != nil {
		return
	}
	go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
	_, _ = io.Copy(c, up)
}

// readPacket reads one MQTT packet: fixed header byte, remaining length, body.
func readPacket(r io.Reader) ([]byte, error) {
	pkt := make([]byte, 1, 64)
	if _, err := io.ReadFull(r, pkt); err != nil {
		return nil, err
	}
	length, mult := 0, 1
	for i := 0; i < 4; i++ {
		b := make([]byte, 1)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		pkt = append(pkt, b[0])
		length += int(b[0]&0x7f) * mult
		if b[0]&0x80 == 0 {
			break
		}
		mult *= 128
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return append(pkt, body...), nil
}

// connectProtocolLevel returns a CONNECT packet's protocol level: after the
// fixed header come the protocol name (2-byte length + "MQTT") and the level.
func connectProtocolLevel(pkt []byte) byte {
	i := 1
	for i < len(pkt) && pkt[i]&0x80 != 0 {
		i++
	}
	i++ // past the last remaining-length byte
	if i+2 > len(pkt) {
		return 0
	}
	if lvl := i + 2 + (int(pkt[i])<<8 | int(pkt[i+1])); lvl < len(pkt) {
		return pkt[lvl]
	}
	return 0
}

func TestBridgeFallsBackToMQTT311WhenRemoteLacksMQTT5(t *testing.T) {
	dir := t.TempDir()
	remote := startNode(t, "remote", 27841, filepath.Join(dir, "remote.db"))
	t.Cleanup(func() { _ = remote.Close() })
	refused := startV311OnlyProxy(t, 27842, "127.0.0.1:27841")

	localDB := filepath.Join(dir, "local.db")
	// No protocolVersion: the bridge tries MQTT 5 first.
	seedBridges(t, localDB, "local", map[string]map[string]any{"to-remote": chainBridge(27842, "local-bridge")})
	local := startNode(t, "local", 27843, localDB)
	t.Cleanup(func() { _ = local.Close() })

	obsRemote := observe(t, remote, "data/#")
	obsLocal := observe(t, local, "data/#")
	waitBridged(t, local, obsRemote)
	if refused.Load() == 0 {
		t.Fatal("the proxy never refused an MQTT 5 CONNECT; the fallback was not exercised")
	}

	publishAt(t, remote, "pub-a", stores.BrokerMessage{TopicName: "data/in/x", Payload: []byte("1"), QoS: 1})
	if got := obsLocal.waitFor(t, "data/in/x", 1, 500*time.Millisecond); len(got) != 1 {
		t.Errorf("local got %d copies of data/in/x, want 1", len(got))
	}
}

// Over 3.1.1 there is no No Local: the remote sends a bridge's own messages
// back. The echo skip must stop them there, so the copies stay bounded.
func TestBridgeChainOverMQTT311DoesNotLoop(t *testing.T) {
	dir := t.TempDir()
	v311 := func(remotePort int, clientID string) map[string]any {
		b := chainBridge(remotePort, clientID)
		b["protocolVersion"] = 4
		return b
	}
	a := startNode(t, "a", 27844, filepath.Join(dir, "a.db"))
	t.Cleanup(func() { _ = a.Close() })
	bDB := filepath.Join(dir, "b.db")
	seedBridges(t, bDB, "b", map[string]map[string]any{"to-a": v311(27844, "b-bridge")})
	b := startNode(t, "b", 27845, bDB)
	t.Cleanup(func() { _ = b.Close() })
	cDB := filepath.Join(dir, "c.db")
	seedBridges(t, cDB, "c", map[string]map[string]any{"to-b": v311(27845, "c-bridge")})
	c := startNode(t, "c", 27846, cDB)
	t.Cleanup(func() { _ = c.Close() })

	obsA, obsB, obsC := observe(t, a, "data/#"), observe(t, b, "data/#"), observe(t, c, "data/#")
	waitBridged(t, c, obsA)

	const topic = "data/c/state"
	publishAt(t, c, "pub-1", stores.BrokerMessage{TopicName: topic, Payload: []byte("run"), QoS: 1})
	if got := obsA.waitFor(t, topic, 1, time.Second); len(got) != 1 {
		t.Errorf("A got %d copies, want exactly 1", len(got))
	}
	counts := func() [3]int { return [3]int{len(obsA.on(topic)), len(obsB.on(topic)), len(obsC.on(topic))} }
	before := counts()
	time.Sleep(2 * time.Second)
	if after := counts(); after != before {
		t.Fatalf("copies (A, B, C) still growing: %v, then %v", before, after)
	}
	// B: from C + echo from A; C: original + echo from B + A's echo via B
	if before[1] > 2 || before[2] > 3 {
		t.Errorf("copies (A, B, C) = %v, want at most (1, 2, 3)", before)
	}
}
