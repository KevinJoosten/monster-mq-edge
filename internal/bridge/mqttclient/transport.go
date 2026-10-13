package mqttclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"monstermq.io/edge/internal/stores"
)

// transport is the bridge's connection to the remote broker. The bridge logic
// (topic mapping, skipping its own messages, buffering) stays independent of
// the client library behind it, so an MQTT 3.1.1 fallback can be added later
// as a second implementation.
type transport interface {
	// Publish sends one message. It fails fast with errTransportDown when there
	// is no connection, and with errTooLarge when the message exceeds the remote
	// broker's Maximum Packet Size.
	Publish(ctx context.Context, topic string, msg stores.BrokerMessage, qos byte, retain bool) error
	// Connected reports whether the connection to the remote broker is up.
	Connected() bool
	// Close disconnects and stops reconnecting.
	Close()
}

var (
	errTransportDown = errors.New("remote broker not connected")
	errTooLarge      = errors.New("message exceeds the remote broker's maximum packet size")
	// ErrRemoteNotMQTT5 is reported when the remote broker answers with an
	// MQTT 3.1.1 CONNACK, i.e. it does not support MQTT 5.
	ErrRemoteNotMQTT5 = errors.New("remote broker does not support MQTT 5")
)

// memoryQueue selects the client library's in-memory publish queue.
const memoryQueue = ":memory:"

type transportSubscription struct {
	Topic string
	QoS   byte
}

// transportOptions is everything a transport needs; the connector builds it
// from the bridge Config.
type transportOptions struct {
	URL            string
	ClientID       string
	Username       string
	Password       string
	KeepAlive      time.Duration
	ConnectTimeout time.Duration
	ReconnectDelay time.Duration // longest wait between connection attempts
	CleanSession   bool
	TLS            *tls.Config
	Subscriptions  []transportSubscription
	// LibraryQueue enables the client library's own publish queue (buffer
	// implementation PAHO): "" none, memoryQueue in memory, otherwise a directory.
	LibraryQueue   string
	Logger         *slog.Logger
	OnMessage      func(remoteTopic string, msg stores.BrokerMessage)
	OnConnected    func()
	OnConnectError func(error)
}

// parseBrokerURL accepts the URL forms bridges have always used; like the old
// client, a URL without a scheme means tcp://.
func parseBrokerURL(raw string) (*url.URL, error) {
	if !strings.Contains(raw, "://") {
		raw = "tcp://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("broker url %q: %w", raw, err)
	}
	switch strings.ToLower(u.Scheme) {
	case "tcp", "mqtt", "ssl", "tls", "mqtts", "mqtt+ssl", "tcps", "ws", "wss":
		return u, nil
	}
	return nil, fmt.Errorf("broker url %q: unsupported scheme %q", raw, u.Scheme)
}

func isTLSScheme(u *url.URL) bool {
	switch strings.ToLower(u.Scheme) {
	case "ssl", "tls", "mqtts", "mqtt+ssl", "tcps", "wss":
		return true
	}
	return false
}

func isWebSocketScheme(u *url.URL) bool {
	s := strings.ToLower(u.Scheme)
	return s == "ws" || s == "wss"
}

// sessionSettings maps the 3.1.1-style cleanSession flag to MQTT 5. A 3.1.1
// persistent session never expires; an MQTT 5 session ends at disconnect
// unless an expiry is set, so a persistent session gets 0xFFFFFFFF, which
// MQTT 5 defines as "never expires".
func sessionSettings(cleanSession bool) (cleanStart bool, expiry uint32) {
	if cleanSession {
		return true, 0
	}
	return false, 0xFFFFFFFF
}

// subscribeOptions sets No Local, so the remote broker never sends the bridge
// its own publishes back, and Retain As Published, so retained state keeps its
// flag on inbound messages.
func subscribeOptions(subs []transportSubscription) []paho.SubscribeOptions {
	out := make([]paho.SubscribeOptions, len(subs))
	for i, s := range subs {
		out[i] = paho.SubscribeOptions{Topic: s.Topic, QoS: s.QoS, NoLocal: true, RetainAsPublished: true}
	}
	return out
}

// backoff waits nothing before the first attempt, then 1s, 2s, 4s, … up to max.
func backoff(max time.Duration) func(int) time.Duration {
	if max <= 0 {
		max = 30 * time.Second
	}
	return func(attempt int) time.Duration {
		if attempt == 0 {
			return 0
		}
		return min(time.Duration(1<<min(attempt-1, 10))*time.Second, max)
	}
}

func toPahoPublish(topic string, m stores.BrokerMessage, qos byte, retain bool) *paho.Publish {
	p := &paho.Publish{
		Topic:   topic,
		QoS:     qos,
		Retain:  retain,
		Payload: m.Payload,
		Properties: &paho.PublishProperties{
			ContentType:     m.ContentType,
			ResponseTopic:   m.ResponseTopic,
			CorrelationData: m.CorrelationData,
			PayloadFormat:   m.PayloadFormatIndicator,
			MessageExpiry:   m.MessageExpiryInterval,
		},
	}
	for _, u := range m.UserProperties {
		p.Properties.User = append(p.Properties.User, paho.UserProperty{Key: u.Key, Value: u.Value})
	}
	return p
}

func fromPahoPublish(p *paho.Publish) stores.BrokerMessage {
	m := stores.BrokerMessage{
		TopicName: p.Topic,
		Payload:   p.Payload,
		QoS:       p.QoS,
		IsRetain:  p.Retain,
		Time:      time.Now().UTC(),
	}
	if pp := p.Properties; pp != nil {
		m.ContentType = pp.ContentType
		m.ResponseTopic = pp.ResponseTopic
		m.CorrelationData = pp.CorrelationData
		m.PayloadFormatIndicator = pp.PayloadFormat
		m.MessageExpiryInterval = pp.MessageExpiry
		for _, u := range pp.User {
			m.UserProperties = append(m.UserProperties, stores.UserProperty{Key: u.Key, Value: u.Value})
		}
	}
	return m
}

// isV3Connack reports whether the first two bytes a server sent are an MQTT
// 3.1.1 CONNACK: fixed header 0x20, remaining length 2. An MQTT 5 CONNACK
// always carries a properties length, so its remaining length is at least 3.
func isV3Connack(head []byte) bool {
	return len(head) >= 2 && head[0] == 0x20 && head[1] == 0x02
}
