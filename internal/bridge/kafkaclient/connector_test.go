package kafkaclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"monstermq.io/edge/internal/stores"
)

type fakeConsumer struct {
	recs   chan Record
	errs   chan error
	closed chan struct{}
	once   sync.Once
}

func newFakeConsumer() *fakeConsumer {
	return &fakeConsumer{recs: make(chan Record, 16), errs: make(chan error, 1), closed: make(chan struct{})}
}

func (f *fakeConsumer) Poll(ctx context.Context) ([]Record, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.closed:
		return nil, errClosed
	case err := <-f.errs:
		return nil, err
	case r := <-f.recs:
		return []Record{r}, nil
	}
}

func (f *fakeConsumer) Close() { f.once.Do(func() { close(f.closed) }) }

type fakeProducer struct {
	mu    sync.Mutex
	recs  []Record
	block chan struct{} // when set, Produce waits on it
}

func (f *fakeProducer) Produce(ctx context.Context, rec Record, done func(error)) {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			done(ctx.Err())
			return
		}
	}
	f.mu.Lock()
	f.recs = append(f.recs, rec)
	f.mu.Unlock()
	done(nil)
}

func (f *fakeProducer) Close() {}

func (f *fakeProducer) records() []Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Record(nil), f.recs...)
}

type fakeBus struct {
	ch chan stores.BrokerMessage
}

func (b *fakeBus) Subscribe(filters []string, buffer int) (int, <-chan stores.BrokerMessage) {
	return 1, b.ch
}
func (b *fakeBus) Unsubscribe(int) {}

type published struct {
	clientID string
	msg      stores.BrokerMessage
}

type recorder struct {
	mu  sync.Mutex
	got []published
}

func (r *recorder) publish(clientID string, msg stores.BrokerMessage) error {
	r.mu.Lock()
	r.got = append(r.got, published{clientID, msg})
	r.mu.Unlock()
	return nil
}

func (r *recorder) all() []published {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]published(nil), r.got...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func startInbound(t *testing.T, cfg Config) (*Connector, *fakeConsumer, *recorder) {
	t.Helper()
	rec := &recorder{}
	cons := newFakeConsumer()
	c := NewConnector("dev", "kafka.topic", cfg, rec.publish, nil, nil)
	var gotOpts consumerOptions
	c.newConsumer = func(o consumerOptions) (consumer, error) { gotOpts = o; return cons, nil }
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	waitFor(t, "consumer", func() bool { return gotOpts.topic != "" })
	if gotOpts.topic != "kafka.topic" || gotOpts.clientID != "kafkaclient-dev" || gotOpts.groupID != DefaultGroupID {
		t.Fatalf("consumer options = %+v", gotOpts)
	}
	return c, cons, rec
}

func TestInboundTextUsesKeyRegexAndPrefix(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PayloadFormat = FormatText
	re, repl, prefix := `\.`, "/", "plant/"
	cfg.TopicKeyRegex, cfg.TopicKeyReplacement, cfg.DestinationTopicPrefix = &re, &repl, &prefix
	c, cons, rec := startInbound(t, cfg)

	cons.recs <- Record{Key: []byte("line1.temp"), Value: []byte("21.5")}
	cons.recs <- Record{Key: nil, Value: []byte("dropped")}
	waitFor(t, "publish", func() bool { _, _, d, _ := c.Totals(); return len(rec.all()) == 1 && d == 1 })

	p := rec.all()[0]
	if p.clientID != "kafkaclient-dev" || p.msg.TopicName != "plant/line1/temp" || string(p.msg.Payload) != "21.5" || p.msg.QoS != 0 || p.msg.IsRetain {
		t.Fatalf("published %+v", p)
	}
	if in, _, _, _ := c.Totals(); in != 1 {
		t.Errorf("messagesIn = %d, want 1 (dropped records not counted)", in)
	}
}

func TestInboundDefaultWithJSONFallback(t *testing.T) {
	cfg := DefaultConfig()
	prefix := "x/"
	cfg.DestinationTopicPrefix = &prefix
	c, cons, rec := startInbound(t, cfg)

	ct := "text/plain"
	w := WireMessage{MessageUUID: "u", TopicName: "/a/b", Payload: []byte("p"), QoS: 1, ClientID: "orig", Time: time.Now(), ContentType: &ct}
	cons.recs <- Record{Value: EncodeWire(w)}
	cons.recs <- Record{Value: []byte(`{"topicName":"j/1","payload":"hello"}`)}
	cons.recs <- Record{Value: []byte(`not json`)}
	waitFor(t, "two publishes", func() bool { return len(rec.all()) == 2 })
	waitFor(t, "drop counted", func() bool { _, _, d, e := c.Totals(); return d == 1 && e == 1 })

	got := rec.all()
	if got[0].msg.TopicName != "x/a/b" || got[0].msg.QoS != 1 || got[0].msg.ContentType != "text/plain" || got[0].msg.ClientID != "kafkaclient-dev" {
		t.Errorf("codec record published as %+v", got[0].msg)
	}
	if got[1].msg.TopicName != "x/j/1" || string(got[1].msg.Payload) != "hello" {
		t.Errorf("JSON fallback published as %+v", got[1].msg)
	}
	if in, _, _, _ := c.Totals(); in != 2 {
		t.Errorf("messagesIn = %d, want 2", in)
	}
}

func TestInboundRecreatesConsumerAfterError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReconnectDelayMs = 500
	rec := &recorder{}
	var mu sync.Mutex
	created := 0
	first, second := newFakeConsumer(), newFakeConsumer()
	c := NewConnector("dev", "t", cfg, rec.publish, nil, nil)
	c.newConsumer = func(consumerOptions) (consumer, error) {
		mu.Lock()
		defer mu.Unlock()
		created++
		if created == 1 {
			return first, nil
		}
		return second, nil
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	first.errs <- errors.New("SASL authentication failed")
	waitFor(t, "second consumer", func() bool { mu.Lock(); defer mu.Unlock(); return created == 2 })
	if _, _, _, e := c.Totals(); e != 1 {
		t.Errorf("errors = %d, want 1", e)
	}
}

func startOutbound(t *testing.T, cfg Config, prod *fakeProducer) (*Connector, *fakeBus) {
	t.Helper()
	cfg.InboundEnabled = false
	cfg.OutboundEnabled = true
	if len(cfg.OutboundTopicFilters) == 0 {
		cfg.OutboundTopicFilters = []string{"plant/#"}
	}
	bus := &fakeBus{ch: make(chan stores.BrokerMessage, 16)}
	c := NewConnector("dev", "ns.topic", cfg, func(string, stores.BrokerMessage) error { return nil }, bus, nil)
	var gotOpts producerOptions
	var mu sync.Mutex
	c.newProducer = func(o producerOptions) (producer, error) { mu.Lock(); gotOpts = o; mu.Unlock(); return prod, nil }
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	waitFor(t, "producer", func() bool { mu.Lock(); defer mu.Unlock(); return gotOpts.clientID != "" })
	if gotOpts.clientID != "kafkaclient-producer-dev" {
		t.Fatalf("producer client id = %q", gotOpts.clientID)
	}
	return c, bus
}

func TestOutboundFormatsAndLoopPrevention(t *testing.T) {
	msg := stores.BrokerMessage{MessageUUID: "id", TopicName: "plant/a", Payload: []byte("v"), ClientID: "sensor", Time: time.Unix(1, 0)}
	for _, format := range []string{FormatDefault, FormatJSON, FormatBinary, FormatText} {
		t.Run(format, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.PayloadFormat = format
			re, repl, dest := "/", ".", "out.topic"
			cfg.OutboundTopicKeyRegex, cfg.OutboundTopicKeyReplacement, cfg.OutboundKafkaTopic = &re, &repl, &dest
			prod := &fakeProducer{}
			c, bus := startOutbound(t, cfg, prod)

			own := msg
			own.ClientID = "kafkaclient-dev"
			bus.ch <- own
			bus.ch <- msg
			waitFor(t, "record", func() bool { _, out, _, _ := c.Totals(); return out == 1 })
			time.Sleep(20 * time.Millisecond)

			recs := prod.records()
			if len(recs) != 1 {
				t.Fatalf("records = %d, want 1 (own message skipped)", len(recs))
			}
			r := recs[0]
			if r.Topic != "out.topic" || string(r.Key) != "plant.a" {
				t.Fatalf("record topic/key = %s/%s", r.Topic, r.Key)
			}
			switch format {
			case FormatDefault:
				w, err := DecodeWire(r.Value)
				if err != nil || w.TopicName != "plant/a" || w.ClientID != "sensor" {
					t.Fatalf("DEFAULT value decodes to %+v, %v", w, err)
				}
			case FormatJSON:
				var obj map[string]any
				if err := json.Unmarshal(r.Value, &obj); err != nil || obj["topic"] != "plant/a" {
					t.Fatalf("JSON value = %s", r.Value)
				}
			default:
				if !bytes.Equal(r.Value, []byte("v")) {
					t.Fatalf("raw value = %q", r.Value)
				}
			}
		})
	}
}

func TestOutboundDefaultsToNamespaceTopic(t *testing.T) {
	prod := &fakeProducer{}
	c, bus := startOutbound(t, DefaultConfig(), prod)
	bus.ch <- stores.BrokerMessage{TopicName: "plant/x", Payload: []byte("1")}
	waitFor(t, "record", func() bool { _, out, _, _ := c.Totals(); return out == 1 })
	if r := prod.records()[0]; r.Topic != "ns.topic" || string(r.Key) != "plant/x" {
		t.Fatalf("record = %s key %s", r.Topic, r.Key)
	}
}

func TestOutboundDropsOldestWhenBufferFull(t *testing.T) {
	prod := &fakeProducer{block: make(chan struct{})}
	c, _ := startOutbound(t, DefaultConfig(), prod)
	queue := make(chan stores.BrokerMessage, 2)
	in := make(chan stores.BrokerMessage, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.wg.Add(1)
	go c.runOutboundIntake(ctx, in, queue)
	for i := 0; i < 5; i++ {
		in <- stores.BrokerMessage{TopicName: "plant/x", MessageUUID: string(rune('a' + i))}
	}
	waitFor(t, "drops", func() bool { _, _, d, _ := c.Totals(); return d == 3 })
	first := <-queue
	if first.MessageUUID != "d" {
		t.Errorf("oldest kept message = %q, want d (a..c dropped)", first.MessageUUID)
	}
	close(prod.block)
}

// Tombstones are dropped as in Core: counted for BINARY/TEXT and for
// DEFAULT/JSON, never as an error.
func TestInboundDropsNullValues(t *testing.T) {
	for _, format := range []string{FormatText, FormatDefault} {
		cfg := DefaultConfig()
		cfg.PayloadFormat = format
		c, cons, rec := startInbound(t, cfg)
		cons.recs <- Record{Key: []byte("k"), Value: nil}
		waitFor(t, "drop", func() bool { _, _, d, _ := c.Totals(); return d == 1 })
		if _, _, _, e := c.Totals(); e != 0 || len(rec.all()) != 0 {
			t.Errorf("%s: errors=%d published=%d", format, e, len(rec.all()))
		}
	}
}

// Core's bridge subscribes at QoS 0 without retain-as-published, so its
// envelopes carry QoS 0 and no retain flag; '$' topics need a filter that
// does not start with a wildcard.
func TestOutboundEnvelopeFlagsAndDollarTopics(t *testing.T) {
	cfg := DefaultConfig()
	cfg.OutboundTopicFilters = []string{"#"}
	prod := &fakeProducer{}
	c, bus := startOutbound(t, cfg, prod)
	bus.ch <- stores.BrokerMessage{TopicName: "$internal/x", Payload: []byte("1"), ClientID: "a"}
	bus.ch <- stores.BrokerMessage{TopicName: "plant/x", Payload: []byte("2"), QoS: 2, IsRetain: true, ClientID: "a"}
	waitFor(t, "record", func() bool { _, out, _, _ := c.Totals(); return out == 1 })
	time.Sleep(20 * time.Millisecond)
	recs := prod.records()
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1 ($ topic skipped)", len(recs))
	}
	w, err := DecodeWire(recs[0].Value)
	if err != nil || w.QoS != 0 || w.IsRetain || w.TopicName != "plant/x" {
		t.Fatalf("envelope = %+v, %v", w, err)
	}
	if !dollarTopicMatches("$SYS/x", []string{"#", "$SYS/#"}) {
		t.Error("explicit $ filter does not match")
	}
}
