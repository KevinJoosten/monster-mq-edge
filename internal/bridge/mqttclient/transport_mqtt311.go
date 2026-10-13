package mqttclient

import (
	"context"
	"fmt"
	"time"

	pahov3 "github.com/eclipse/paho.mqtt.golang"

	"monstermq.io/edge/internal/stores"
)

// mqtt311Transport connects to the remote broker with MQTT 3.1.1, for bridges
// set to protocolVersion 3 or 4 and for remotes without MQTT 5. It carries no
// MQTT 5 properties and has no No Local or Retain As Published.
type mqtt311Transport struct {
	client pahov3.Client
	queued bool // publishes go through the library's store (PAHO buffer)
}

// mqtt311Timeout bounds how long a publish or subscribe waits for the remote.
const mqtt311Timeout = 5 * time.Second

func newMQTT311Transport(o transportOptions) (*mqtt311Transport, error) {
	u, err := parseBrokerURL(o.URL)
	if err != nil {
		return nil, err
	}
	t := &mqtt311Transport{queued: o.LibraryQueue != ""}
	opts := pahov3.NewClientOptions().
		AddBroker(u.String()).
		SetClientID(o.ClientID).
		SetCleanSession(o.CleanSession).
		SetKeepAlive(o.KeepAlive).
		SetConnectTimeout(o.ConnectTimeout).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(o.ReconnectDelay).
		SetMaxReconnectInterval(o.ReconnectDelay)
	if o.Username != "" {
		opts.SetUsername(o.Username)
	}
	if o.Password != "" {
		opts.SetPassword(o.Password)
	}
	if o.TLS != nil {
		opts.SetTLSConfig(o.TLS)
	}
	switch o.LibraryQueue {
	case "":
	case memoryQueue:
		opts.SetStore(pahov3.NewMemoryStore())
	default:
		opts.SetStore(pahov3.NewFileStore(o.LibraryQueue))
	}
	// One handler for every PUBLISH, however many subscriptions match it: a
	// per-subscription callback would run once per matching filter.
	opts.SetDefaultPublishHandler(func(_ pahov3.Client, m pahov3.Message) {
		if o.OnMessage != nil {
			o.OnMessage(m.Topic(), stores.BrokerMessage{
				TopicName: m.Topic(), Payload: m.Payload(), QoS: m.Qos(), IsRetain: m.Retained(), Time: time.Now().UTC(),
			})
		}
	})
	opts.SetOnConnectHandler(func(c pahov3.Client) {
		t.subscribe(c, o)
		if o.OnConnected != nil {
			o.OnConnected()
		}
	})
	opts.SetConnectionLostHandler(func(_ pahov3.Client, err error) {
		if o.OnConnectionDown != nil {
			o.OnConnectionDown(err)
		}
	})
	opts.SetConnectionNotificationHandler(func(_ pahov3.Client, n pahov3.ConnectionNotification) {
		if f, ok := n.(pahov3.ConnectionNotificationBrokerFailed); ok && o.OnConnectError != nil {
			o.OnConnectError(f.Reason)
		}
	})
	t.client = pahov3.NewClient(opts)
	t.client.Connect() // retries on its own; the outcome arrives through the handlers
	return t, nil
}

// subscribe subscribes each mapping on its own, so one topic the remote
// refuses does not stop the others.
func (t *mqtt311Transport) subscribe(c pahov3.Client, o transportOptions) {
	for _, s := range o.Subscriptions {
		tok := c.Subscribe(s.Topic, s.QoS, nil)
		if !tok.WaitTimeout(mqtt311Timeout) {
			o.Logger.Warn("bridge subscribe timed out", "topic", s.Topic)
			continue
		}
		if err := tok.Error(); err != nil {
			o.Logger.Warn("bridge subscribe failed", "topic", s.Topic, "err", err)
			continue
		}
		if st, ok := tok.(*pahov3.SubscribeToken); ok {
			if code := st.Result()[s.Topic]; code >= 0x80 {
				o.Logger.Warn("bridge subscription rejected by remote broker", "topic", s.Topic, "reason", code)
			}
		}
	}
}

func (t *mqtt311Transport) Publish(_ context.Context, topic string, msg stores.BrokerMessage, qos byte, retain bool) error {
	open := t.client.IsConnectionOpen()
	// paho keeps a QoS 0 message only while connected, so queued mode cannot hold it either.
	if !open && (!t.queued || qos == 0) {
		return errTransportDown
	}
	tok := t.client.Publish(topic, qos, retain, msg.Payload)
	if t.queued {
		// A token that already failed (closed client, no message IDs, write error) is
		// an error; a pending one is stored by the library until the remote is reachable.
		select {
		case <-tok.Done():
			if err := tok.Error(); err != nil {
				return fmt.Errorf("%w: %v", errTransportDown, err)
			}
		default:
		}
		return nil
	}
	if !tok.WaitTimeout(mqtt311Timeout) {
		return fmt.Errorf("%w: publish timed out", errTransportDown)
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("%w: %v", errTransportDown, err)
	}
	return nil
}

func (t *mqtt311Transport) Connected() bool { return t.client.IsConnectionOpen() }

func (t *mqtt311Transport) Close() { t.client.Disconnect(250) }
