package kafkaclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"monstermq.io/edge/internal/stores"
)

// ClientIDPrefix starts the client ID inbound records are published under
// locally ("kafkaclient-<name>"), as in Core. Outbound skips messages with
// that ID, which is the loop prevention.
const ClientIDPrefix = "kafkaclient-"

// outboundBuffer is the number of MQTT messages held while Kafka is slow or
// unreachable; beyond it the oldest is dropped (edge-only extension E4).
const outboundBuffer = 10000

// LocalPublisher publishes a message into the local broker as clientID.
type LocalPublisher func(clientID string, msg stores.BrokerMessage) error

// LocalSubscriber lets the bridge listen to the local broker's publishes.
// mqttclient.BusAdapter implements it.
type LocalSubscriber interface {
	Subscribe(filters []string, buffer int) (id int, ch <-chan stores.BrokerMessage)
	Unsubscribe(id int)
}

// Connector is one KAFKA-Client device.
type Connector struct {
	name      string
	namespace string
	cfg       Config
	publisher LocalPublisher
	subBus    LocalSubscriber
	logger    *slog.Logger

	newConsumer func(consumerOptions) (consumer, error)
	newProducer func(producerOptions) (producer, error)

	topicKey    *keyRewrite
	outboundKey *keyRewrite

	cancel context.CancelFunc
	wg     sync.WaitGroup
	subID  int
	subbed bool

	IncIn  func()
	IncOut func()

	messagesIn      atomic.Int64
	messagesOut     atomic.Int64
	messagesDropped atomic.Int64
	errors          atomic.Int64
	lastRecordTs    atomic.Int64

	inRate    atomic.Int64
	outRate   atomic.Int64
	metricsMu sync.RWMutex
	latest    MetricsSnapshot
}

// MetricsSnapshot is one persisted metrics sample. messagesIn/messagesOut are
// rates, as for the MQTT bridge; the other fields are running totals.
type MetricsSnapshot struct {
	MessagesIn      float64   `json:"messagesIn"`
	MessagesOut     float64   `json:"messagesOut"`
	MessagesDropped int64     `json:"messagesDropped"`
	Errors          int64     `json:"errors"`
	LastRecordTs    int64     `json:"lastRecordTs,omitempty"`
	Timestamp       time.Time `json:"timestamp"`
}

func NewConnector(name, namespace string, cfg Config, publisher LocalPublisher, subBus LocalSubscriber, logger *slog.Logger) *Connector {
	if logger == nil {
		logger = slog.Default()
	}
	return &Connector{
		name: name, namespace: namespace, cfg: cfg,
		publisher: publisher, subBus: subBus,
		logger:      logger.With("kafkaClient", name),
		newConsumer: newKgoConsumer,
		newProducer: newKgoProducer,
	}
}

func (c *Connector) Name() string { return c.name }

func (c *Connector) localClientID() string { return ClientIDPrefix + c.name }

// Start validates the config and starts the inbound and outbound loops. The
// Kafka clients connect lazily and are recreated after reconnectDelayMs when
// they fail, so an unreachable cluster does not fail Start.
func (c *Connector) Start(ctx context.Context) error {
	if errs := c.cfg.ValidateStrict(); len(errs) > 0 {
		return fmt.Errorf("KafkaClient config errors: %s", strings.Join(errs, ", "))
	}
	if ignored := c.cfg.IgnoredProperties(); len(ignored) > 0 {
		c.logger.Warn("kafka client: these Kafka properties have no effect on edge", "keys", ignored)
	}
	var err error
	if c.topicKey, err = rewrite(c.cfg.TopicKeyRegex, c.cfg.TopicKeyReplacement); err != nil {
		return err
	}
	if c.outboundKey, err = rewrite(c.cfg.OutboundTopicKeyRegex, c.cfg.OutboundTopicKeyReplacement); err != nil {
		return err
	}

	consumerOpts, producerOpts, err := c.clientOptions()
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.logger.Info("kafka client starting", "topic", c.namespace, "format", c.cfg.PayloadFormat,
		"inboundEnabled", c.cfg.InboundEnabled, "outboundEnabled", c.cfg.OutboundEnabled)

	if c.cfg.InboundEnabled {
		c.wg.Add(1)
		go c.runInbound(runCtx, consumerOpts)
	}
	if c.cfg.OutboundEnabled && len(c.cfg.OutboundTopicFilters) > 0 && c.subBus != nil {
		id, ch := c.subBus.Subscribe(c.cfg.OutboundTopicFilters, 1024)
		c.subID, c.subbed = id, true
		queue := make(chan stores.BrokerMessage, outboundBuffer)
		c.wg.Add(2)
		go c.runOutboundIntake(runCtx, ch, queue)
		go c.runOutbound(runCtx, producerOpts, queue)
	}
	return nil
}

func (c *Connector) Stop() {
	if c.subbed {
		c.subBus.Unsubscribe(c.subID)
		c.subbed = false
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

// clientOptions applies Core's defaults, then the translated extra properties.
func (c *Connector) clientOptions() (consumerOptions, producerOptions, error) {
	seeds := splitServers(c.cfg.BootstrapServers)

	cs, err := translateProps(c.cfg.ExtraConsumerConfig, roleConsumer)
	if err != nil {
		return consumerOptions{}, producerOptions{}, fmt.Errorf("extraConsumerConfig: %w", err)
	}
	co := consumerOptions{
		seeds:          seeds,
		topic:          c.namespace,
		groupID:        c.cfg.GroupID,
		clientID:       ClientIDPrefix + c.name,
		maxPollRecords: cs.maxPollRecords,
		extra:          cs.opts,
	}
	if cs.bootstrap != nil {
		co.seeds = cs.bootstrap
	}
	if cs.clientID != "" {
		co.clientID = cs.clientID
	}
	if cs.groupID != "" {
		co.groupID = cs.groupID
	}

	ps, err := translateProps(c.cfg.ExtraProducerConfig, roleProducer)
	if err != nil {
		return consumerOptions{}, producerOptions{}, fmt.Errorf("extraProducerConfig: %w", err)
	}
	po := producerOptions{seeds: seeds, clientID: ClientIDPrefix + "producer-" + c.name, extra: ps.opts}
	if ps.bootstrap != nil {
		po.seeds = ps.bootstrap
	}
	if ps.clientID != "" {
		po.clientID = ps.clientID
	}
	return co, po, nil
}

func (c *Connector) reconnectDelay() time.Duration {
	return time.Duration(c.cfg.ReconnectDelayMs) * time.Millisecond
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Inbound (Kafka -> MQTT) ---------------------------------------------------

func (c *Connector) runInbound(ctx context.Context, opts consumerOptions) {
	defer c.wg.Done()
	for ctx.Err() == nil {
		cons, err := c.newConsumer(opts)
		if err != nil {
			c.errors.Add(1)
			c.logger.Warn("kafka consumer create failed", "err", err)
			if !sleepCtx(ctx, c.reconnectDelay()) {
				return
			}
			continue
		}
		c.logger.Info("kafka consumer subscribed", "topic", opts.topic, "groupId", opts.groupID)
		failed := c.consume(ctx, cons)
		cons.Close()
		if !failed || !sleepCtx(ctx, c.reconnectDelay()) {
			return
		}
	}
}

// consume polls until ctx ends (false) or the client fails (true).
func (c *Connector) consume(ctx context.Context, cons consumer) bool {
	for {
		recs, err := cons.Poll(ctx)
		for _, r := range recs {
			c.handleRecord(r)
		}
		if ctx.Err() != nil {
			return false
		}
		if errors.Is(err, errClosed) {
			return true
		}
		if err != nil {
			c.errors.Add(1)
			c.logger.Warn("kafka consumer error; recreating client", "err", err, "delayMs", c.cfg.ReconnectDelayMs)
			return true
		}
	}
}

// handleRecord publishes one record locally. Unlike Core, messagesIn counts
// only records that were published (edge-only extension E3).
func (c *Connector) handleRecord(r Record) {
	var msg stores.BrokerMessage
	switch c.cfg.PayloadFormat {
	case FormatBinary, FormatText:
		// Core drops a record without key, and a tombstone (null value).
		if r.Key == nil || r.Value == nil {
			c.messagesDropped.Add(1)
			return
		}
		payload := r.Value
		if c.cfg.PayloadFormat == FormatText {
			payload = utf8String(payload)
		}
		msg = stores.BrokerMessage{
			MessageUUID: uuid.NewString(),
			TopicName:   c.applyPrefix(c.applyTopicKey(string(utf8String(r.Key)))),
			Payload:     payload,
			Time:        time.Now().UTC(),
		}
	case FormatDefault, FormatJSON:
		if r.Value == nil {
			c.messagesDropped.Add(1) // as Core: dropped, not an error
			return
		}
		if c.cfg.PayloadFormat == FormatDefault {
			if w, err := DecodeWire(r.Value); err == nil {
				msg = w.ToBroker()
				break
			} else {
				c.logger.Warn("failed to decode Kafka record as BrokerMessageCodec; trying JSON fallback (check payloadFormat)",
					"topic", c.namespace, "err", err)
			}
		}
		m, ok := c.decodeJSONRecord(r.Value)
		if !ok {
			return
		}
		msg = m
	default:
		c.messagesDropped.Add(1)
		return
	}
	if c.cfg.PayloadFormat == FormatDefault || c.cfg.PayloadFormat == FormatJSON {
		msg.TopicName = c.applyPrefix(msg.TopicName)
	}
	msg.ClientID = c.localClientID()

	if err := c.publisher(c.localClientID(), msg); err != nil {
		c.errors.Add(1)
		c.logger.Warn("kafka record publish failed", "mqttTopic", msg.TopicName, "err", err)
		return
	}
	c.messagesIn.Add(1)
	c.inRate.Add(1)
	c.lastRecordTs.Store(time.Now().UnixMilli())
	if c.IncIn != nil {
		c.IncIn()
	}
}

func (c *Connector) decodeJSONRecord(value []byte) (stores.BrokerMessage, bool) {
	msg, err := DecodeJSON(value, c.localClientID())
	if errors.Is(err, errNoTopic) {
		c.messagesDropped.Add(1)
		return msg, false
	}
	if err != nil {
		c.messagesDropped.Add(1)
		c.errors.Add(1)
		c.logger.Warn("failed to decode Kafka record as JSON envelope; skipping", "topic", c.namespace, "err", err)
		return msg, false
	}
	return msg, true
}

func (c *Connector) applyTopicKey(key string) string {
	if c.topicKey == nil {
		return key
	}
	return c.topicKey.apply(key)
}

func (c *Connector) applyPrefix(topic string) string {
	if c.cfg.DestinationTopicPrefix == nil {
		return topic
	}
	return *c.cfg.DestinationTopicPrefix + strings.TrimLeft(topic, "/")
}

// Outbound (MQTT -> Kafka) --------------------------------------------------

// runOutboundIntake moves bus messages into the bounded queue, dropping the
// oldest when it is full so a stalled cluster cannot block the bus.
func (c *Connector) runOutboundIntake(ctx context.Context, in <-chan stores.BrokerMessage, queue chan stores.BrokerMessage) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-in:
			if !ok {
				return
			}
			if msg.ClientID == c.localClientID() || !dollarTopicMatches(msg.TopicName, c.cfg.OutboundTopicFilters) {
				continue
			}
			for {
				select {
				case queue <- msg:
				default:
					select {
					case <-queue:
						c.messagesDropped.Add(1)
					default:
					}
					continue
				}
				break
			}
		}
	}
}

// dollarTopicMatches applies the MQTT rule Core's topic tree follows: a topic
// starting with '$' is not matched by a filter starting with a wildcard.
func dollarTopicMatches(topic string, filters []string) bool {
	if !strings.HasPrefix(topic, "$") {
		return true
	}
	for _, f := range filters {
		if !strings.HasPrefix(f, "#") && !strings.HasPrefix(f, "+") {
			return true
		}
	}
	return false
}
func (c *Connector) runOutbound(ctx context.Context, opts producerOptions, queue <-chan stores.BrokerMessage) {
	defer c.wg.Done()
	for ctx.Err() == nil {
		prod, err := c.newProducer(opts)
		if err != nil {
			c.errors.Add(1)
			c.logger.Warn("kafka producer create failed", "err", err)
			if !sleepCtx(ctx, c.reconnectDelay()) {
				return
			}
			continue
		}
		c.produce(ctx, prod, queue)
		prod.Close()
		return
	}
}

func (c *Connector) produce(ctx context.Context, prod producer, queue <-chan stores.BrokerMessage) {
	dest := c.namespace
	if c.cfg.OutboundKafkaTopic != nil {
		dest = *c.cfg.OutboundKafkaTopic
	}
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-queue:
			rec := Record{Topic: dest, Key: []byte(c.applyOutboundKey(msg.TopicName)), Value: c.encode(msg)}
			prod.Produce(ctx, rec, func(err error) {
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						c.errors.Add(1)
						c.logger.Warn("kafka produce failed", "topic", dest, "err", err)
					}
					return
				}
				c.messagesOut.Add(1)
				c.outRate.Add(1)
				c.lastRecordTs.Store(time.Now().UnixMilli())
				if c.IncOut != nil {
					c.IncOut()
				}
			})
		}
	}
}

// encode builds the record value. Core's bridge receives local messages
// through a QoS 0 subscription without retain-as-published, so its envelopes
// carry QoS 0 and no retain flag; edge writes the same.
func (c *Connector) encode(msg stores.BrokerMessage) []byte {
	switch c.cfg.PayloadFormat {
	case FormatJSON:
		msg.QoS, msg.IsRetain = 0, false
		return EncodeJSON(msg)
	case FormatBinary:
		return msg.Payload
	case FormatText:
		return utf8String(msg.Payload)
	default:
		msg.QoS, msg.IsRetain = 0, false
		return EncodeWire(WireFromBroker(msg))
	}
}

func (c *Connector) applyOutboundKey(topic string) string {
	if c.outboundKey == nil {
		return topic
	}
	return c.outboundKey.apply(topic)
}

// Metrics -------------------------------------------------------------------

func (c *Connector) SampleMetrics(now time.Time, interval time.Duration) MetricsSnapshot {
	seconds := interval.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	snap := MetricsSnapshot{
		MessagesIn:      float64(c.inRate.Swap(0)) / seconds,
		MessagesOut:     float64(c.outRate.Swap(0)) / seconds,
		MessagesDropped: c.messagesDropped.Load(),
		Errors:          c.errors.Load(),
		LastRecordTs:    c.lastRecordTs.Load(),
		Timestamp:       now.UTC(),
	}
	c.metricsMu.Lock()
	c.latest = snap
	c.metricsMu.Unlock()
	return snap
}

func (c *Connector) LatestMetrics() MetricsSnapshot {
	c.metricsMu.RLock()
	snap := c.latest
	c.metricsMu.RUnlock()
	if snap.Timestamp.IsZero() {
		snap.Timestamp = time.Now().UTC()
	}
	return snap
}

// Totals returns the running counters (in, out, dropped, errors).
func (c *Connector) Totals() (in, out, dropped, errs int64) {
	return c.messagesIn.Load(), c.messagesOut.Load(), c.messagesDropped.Load(), c.errors.Load()
}
