package mqtt

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"monstermq.io/edge/internal/mqtt/listeners"
	"monstermq.io/edge/internal/mqtt/packets"
)

// Throughput and per-connection memory of the engine over real TCP
// connections. Run with:
//
//	go test ./internal/mqtt -run '^$' -bench . -benchtime 200000x
//
// BenchmarkThroughput reports delivered messages per second; b.N is the number
// of messages each publisher sends.

type benchAllowHook struct{ HookBase }

func (h *benchAllowHook) ID() string           { return "bench-allow" }
func (h *benchAllowHook) Provides(b byte) bool { return b == OnConnectAuthenticate || b == OnACLCheck }
func (h *benchAllowHook) OnConnectAuthenticate(*Client, packets.Packet) bool {
	return true
}
func (h *benchAllowHook) OnACLCheck(*Client, string, bool) bool { return true }

func startBenchServer(b *testing.B, opts *Options) (*Server, string) {
	b.Helper()
	if opts == nil {
		opts = new(Options)
	}
	opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	// BENCH_QUEUE and BENCH_BUF override the per-connection defaults, to compare sizes.
	if v, err := strconv.Atoi(os.Getenv("BENCH_QUEUE")); err == nil && v > 0 {
		opts.Capabilities = NewDefaultServerCapabilities()
		opts.Capabilities.MaximumClientWritesPending = int32(v)
	}
	if v, err := strconv.Atoi(os.Getenv("BENCH_BUF")); err == nil && v > 0 {
		opts.ClientNetReadBufferSize, opts.ClientNetWriteBufferSize = v, v
	}
	s := New(opts)
	if err := s.AddHook(new(benchAllowHook), nil); err != nil {
		b.Fatal(err)
	}
	l := listeners.NewTCP(listeners.Config{ID: "bench", Address: "127.0.0.1:0"})
	if err := s.AddListener(l); err != nil {
		b.Fatal(err)
	}
	if err := s.Serve(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s, l.Address()
}

type benchConn struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func benchEncode(b *testing.B, pk packets.Packet) []byte {
	b.Helper()
	pk.ProtocolVersion = 4
	var buf bytes.Buffer
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(&buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(&buf)
	case packets.Publish:
		err = pk.PublishEncode(&buf)
	case packets.Puback:
		err = pk.PubackEncode(&buf)
	}
	if err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

// benchDial connects an MQTT 3.1.1 client and subscribes it to filter, if set.
func benchDial(b *testing.B, addr, id, filter string, qos byte) *benchConn {
	return benchDialSize(b, addr, id, filter, qos, 64*1024)
}

func benchDialSize(b *testing.B, addr, id, filter string, qos byte, bufSize int) *benchConn {
	b.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	c := &benchConn{conn: conn, r: bufio.NewReaderSize(conn, bufSize), w: bufio.NewWriterSize(conn, bufSize)}
	b.Cleanup(func() { _ = conn.Close() })
	_, _ = c.w.Write(benchEncode(b, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Connect},
		Connect:     packets.ConnectParams{ProtocolName: []byte("MQTT"), Clean: true, Keepalive: 60, ClientIdentifier: id},
	}))
	if filter != "" {
		_, _ = c.w.Write(benchEncode(b, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
			PacketID:    1,
			Filters:     packets.Subscriptions{{Filter: filter, Qos: qos}},
		}))
	}
	_ = c.w.Flush()
	want := 1
	if filter != "" {
		want = 2
	}
	for i := 0; i < want; i++ {
		if _, _, err := c.readPacket(); err != nil {
			b.Fatal(err)
		}
	}
	return c
}

// readPacket returns the type and body of the next packet.
func (c *benchConn) readPacket() (byte, []byte, error) {
	h, err := c.r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, _, err := packets.DecodeLength(c.r)
	if err != nil {
		return 0, nil, err
	}
	body := make([]byte, n)
	_, err = io.ReadFull(c.r, body)
	return h, body, err
}

func BenchmarkThroughput(b *testing.B) {
	for _, tc := range []struct {
		pubs, subs int
		qos        byte
	}{
		{1, 1, 0}, {4, 4, 0}, {8, 1, 0}, {1, 16, 0},
		{1, 1, 1}, {4, 4, 1},
	} {
		b.Run(fmt.Sprintf("qos%d_pub%d_sub%d", tc.qos, tc.pubs, tc.subs), func(b *testing.B) {
			benchThroughput(b, tc.pubs, tc.subs, tc.qos)
		})
	}
}

func benchThroughput(b *testing.B, pubs, subs int, qos byte) {
	_, addr := startBenchServer(b, nil)
	payload := bytes.Repeat([]byte("v"), 64)

	var delivered atomic.Int64
	var subWg sync.WaitGroup
	expected := int64(b.N) * int64(pubs) * int64(subs)
	for i := 0; i < subs; i++ {
		c := benchDial(b, addr, fmt.Sprintf("sub-%d", i), "bench/#", qos)
		subWg.Add(1)
		go func() {
			defer subWg.Done()
			for {
				h, body, err := c.readPacket()
				if err != nil {
					return
				}
				if h>>4 != packets.Publish {
					continue
				}
				if (h>>1)&3 > 0 { // QoS 1: ack it
					tl := int(binary.BigEndian.Uint16(body))
					id := binary.BigEndian.Uint16(body[2+tl:])
					_, _ = c.w.Write([]byte{packets.Puback << 4, 2, byte(id >> 8), byte(id)})
					if c.r.Buffered() == 0 {
						_ = c.w.Flush()
					}
				}
				if delivered.Add(1) == expected {
					_ = c.w.Flush()
				}
			}
		}()
	}

	pubConns := make([]*benchConn, pubs)
	for i := range pubConns {
		pubConns[i] = benchDial(b, addr, fmt.Sprintf("pub-%d", i), "", 0)
	}

	// Each publisher sends one encoded frame over and over; for QoS 1 only its
	// packet ID, just before the payload, changes.
	frames := make([][]byte, pubs)
	for i := range frames {
		frames[i] = benchEncode(b, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
			PacketID:    uint16(qos), TopicName: fmt.Sprintf("bench/%d", i), Payload: payload,
		})
	}

	b.ResetTimer()
	start := time.Now()
	var pubWg sync.WaitGroup
	for i, c := range pubConns {
		pubWg.Add(1)
		go func(frame []byte, c *benchConn) {
			defer pubWg.Done()
			if qos == 0 {
				for n := 0; n < b.N; n++ {
					_, _ = c.w.Write(frame)
				}
				_ = c.w.Flush()
				return
			}
			// QoS 1: keep up to 512 messages unacknowledged.
			idAt := len(frame) - len(payload) - 2
			window := make(chan struct{}, 512)
			acked := make(chan struct{})
			go func() {
				defer close(acked)
				for n := 0; n < b.N; n++ {
					if _, _, err := c.readPacket(); err != nil {
						return
					}
					<-window
				}
			}()
			for n := 0; n < b.N; n++ {
				window <- struct{}{}
				binary.BigEndian.PutUint16(frame[idAt:], uint16(n%60000)+1)
				_, _ = c.w.Write(frame)
				if len(window) == cap(window) || n == b.N-1 {
					_ = c.w.Flush()
				}
			}
			<-acked
		}(frames[i], c)
	}
	pubWg.Wait()

	// Wait for delivery to finish; QoS 0 messages may be dropped under overload.
	last, idle := delivered.Load(), time.Now()
	for delivered.Load() < expected && time.Since(idle) < time.Second {
		time.Sleep(5 * time.Millisecond)
		if d := delivered.Load(); d != last {
			last, idle = d, time.Now()
		}
	}
	elapsed := time.Since(start)
	if delivered.Load() < expected {
		elapsed -= time.Second // the idle wait
	}
	b.StopTimer()
	got := delivered.Load()
	b.ReportMetric(float64(got)/elapsed.Seconds(), "msgs/s")
	b.ReportMetric(100*float64(expected-got)/float64(expected), "%dropped")
}

// BenchmarkConnectionMemory reports heap in use per connected client after
// each client has subscribed, received one message and gone idle. The test
// clients run in the same process; their share is a few hundred bytes.
func BenchmarkConnectionMemory(b *testing.B) {
	const clients = 500
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		b.StartTimer()

		s, addr := startBenchServer(b, &Options{InlineClient: true})
		conns := make([]*benchConn, clients)
		for j := range conns {
			conns[j] = benchDialSize(b, addr, fmt.Sprintf("idle-%d-%d", i, j), "idle/#", 0, 16) // small, so the heap is mostly the broker's
		}
		if err := s.Publish("idle/x", []byte("hello"), false, 0); err != nil {
			b.Fatal(err)
		}
		for _, c := range conns {
			if _, _, err := c.readPacket(); err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(int64(after.HeapInuse)-int64(before.HeapInuse))/clients, "B/conn")
		for _, c := range conns {
			_ = c.conn.Close()
		}
		_ = s.Close()
		b.StartTimer()
	}
}
