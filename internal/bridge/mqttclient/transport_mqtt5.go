package mqttclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/queue/file"
	"github.com/eclipse/paho.golang/autopaho/queue/memory"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"

	"monstermq.io/edge/internal/stores"
)

// mqtt5Transport connects to the remote broker with MQTT 5 and reconnects on
// its own.
type mqtt5Transport struct {
	cm        *autopaho.ConnectionManager
	cancel    context.CancelFunc
	queued    bool // publishes go through the library's queue (PAHO buffer)
	connected atomic.Bool
	maxPacket atomic.Uint32 // the remote broker's Maximum Packet Size; 0 = none announced
	maxQoS    atomic.Uint32 // the remote broker's Maximum QoS
	noRetain  atomic.Bool   // the remote broker announced Retain Available 0
	sawV3     atomic.Bool   // the last attempt got an MQTT 3.1.1 CONNACK
}

func newMQTT5Transport(ctx context.Context, o transportOptions) (*mqtt5Transport, error) {
	u, err := parseBrokerURL(o.URL)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(u.Scheme, "unix") {
		return nil, fmt.Errorf("broker url %q: unix sockets need protocolVersion 4 (MQTT 3.1.1)", o.URL)
	}
	t := &mqtt5Transport{queued: o.LibraryQueue != ""}
	t.maxQoS.Store(2)
	// Close cancels cctx, which also ends a subscribe still in flight.
	cctx, cancel := context.WithCancel(ctx)
	cfg, err := t.clientConfig(cctx, o, u)
	if err != nil {
		cancel()
		return nil, err
	}
	cm, err := autopaho.NewConnection(cctx, cfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("bridge transport: %w", err)
	}
	t.cm, t.cancel = cm, cancel
	return t, nil
}

func (t *mqtt5Transport) clientConfig(ctx context.Context, o transportOptions, u *url.URL) (autopaho.ClientConfig, error) {
	cleanStart, expiry := sessionSettings(o.CleanSession)
	cfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		TlsCfg:                        o.TLS,
		KeepAlive:                     uint16(o.KeepAlive / time.Second),
		CleanStartOnInitialConnection: cleanStart,
		SessionExpiryInterval:         expiry,
		ConnectTimeout:                o.ConnectTimeout,
		ReconnectBackoff:              backoff(o.ReconnectDelay),
		ConnectUsername:               o.Username,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, ca *paho.Connack) {
			t.sawV3.Store(false)
			t.maxPacket.Store(0)
			if ca != nil && ca.Properties != nil && ca.Properties.MaximumPacketSize != nil {
				t.maxPacket.Store(*ca.Properties.MaximumPacketSize)
			}
			t.maxQoS.Store(2)
			t.noRetain.Store(false)
			if ca != nil && ca.Properties != nil {
				if ca.Properties.MaximumQoS != nil {
					t.maxQoS.Store(uint32(*ca.Properties.MaximumQoS))
				}
				t.noRetain.Store(!ca.Properties.RetainAvailable)
			}
			t.connected.Store(true)
			if len(o.Subscriptions) > 0 {
				go t.subscribe(ctx, cm, o) // OnConnectionUp must not block
			}
			if o.OnConnected != nil {
				o.OnConnected()
			}
		},
		OnConnectionDown: func() bool {
			t.connected.Store(false)
			return true // keep reconnecting
		},
		OnConnectError: func(err error) {
			if t.sawV3.Load() {
				err = fmt.Errorf("%w: %v", errRemoteNotMQTT5, err)
			}
			if o.OnConnectError != nil {
				o.OnConnectError(err)
			}
		},
		ClientConfig: paho.ClientConfig{ClientID: o.ClientID},
	}
	if o.OnConnectionDown != nil {
		// autopaho forwards only the error that ended the connection.
		cfg.OnClientError = o.OnConnectionDown
		cfg.OnServerDisconnect = func(d *paho.Disconnect) {
			o.OnConnectionDown(fmt.Errorf("server requested disconnect (reason 0x%02x)", d.ReasonCode))
		}
	}
	if o.Password != "" {
		cfg.ConnectPassword = []byte(o.Password)
	}
	if !isWebSocketScheme(u) {
		cfg.AttemptConnection = t.dial // websockets keep the library's own dialer
	}
	if o.OnMessage != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				o.OnMessage(pr.Packet.Topic, fromPahoPublish(pr.Packet))
				return true, nil
			},
		}
	}
	switch o.LibraryQueue {
	case "":
	case memoryQueue:
		cfg.Queue = memory.New()
	default:
		// The MQTT 3.1.1 transport keeps the parent directory, so the two
		// libraries never read each other's files.
		dir := filepath.Join(o.LibraryQueue, "v5")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return cfg, fmt.Errorf("bridge queue %s: %w", dir, err)
		}
		q, err := file.New(dir, "queue", ".msg")
		if err != nil {
			return cfg, fmt.Errorf("bridge queue %s: %w", dir, err)
		}
		cfg.Queue = q
	}
	return cfg, nil
}

func (t *mqtt5Transport) subscribe(ctx context.Context, cm *autopaho.ConnectionManager, o transportOptions) {
	opts := subscribeOptions(o.Subscriptions)
	ack, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: opts})
	if err != nil && o.Logger != nil {
		o.Logger.Warn("bridge subscribe failed", "err", err)
	}
	if ack == nil || o.Logger == nil {
		return
	}
	for i, code := range ack.Reasons {
		if code >= 0x80 && i < len(opts) {
			o.Logger.Warn("bridge subscription rejected by remote broker", "topic", opts[i].Topic, "reason", code)
		}
	}
}

// dial opens TCP or TLS itself, so the first bytes the server sends can be
// checked for an MQTT 3.1.1 CONNACK; paho.golang reports that only as EOF.
func (t *mqtt5Transport) dial(ctx context.Context, cfg autopaho.ClientConfig, u *url.URL) (net.Conn, error) {
	t.sawV3.Store(false) // a v3 CONNACK on an earlier attempt must not label this one
	d := &net.Dialer{Timeout: cfg.ConnectTimeout}
	var (
		conn net.Conn
		err  error
	)
	if isTLSScheme(u) {
		conn, err = (&tls.Dialer{NetDialer: d, Config: cfg.TlsCfg}).DialContext(ctx, "tcp", u.Host)
	} else {
		conn, err = d.DialContext(ctx, "tcp", u.Host)
	}
	if err != nil {
		return nil, err
	}
	return &sniffConn{Conn: packets.NewThreadSafeConn(conn), sawV3: &t.sawV3}, nil
}

// sniffConn records whether the server's first two bytes are an MQTT 3.1.1
// CONNACK. Only paho's single reader goroutine calls Read.
type sniffConn struct {
	net.Conn
	sawV3 *atomic.Bool
	head  []byte
}

func (c *sniffConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if len(c.head) < 2 {
		c.head = append(c.head, b[:min(n, 2-len(c.head))]...)
		if isV3Connack(c.head) {
			c.sawV3.Store(true)
		}
	}
	return n, err
}

func (t *mqtt5Transport) Publish(ctx context.Context, topic string, msg stores.BrokerMessage, qos byte, retain bool) error {
	p := toPahoPublish(topic, msg, qos, retain)
	fitToServer(p, byte(t.maxQoS.Load()), !t.noRetain.Load())
	if limit := t.maxPacket.Load(); limit > 0 {
		if n, _ := p.Packet().WriteTo(io.Discard); uint32(n) > limit {
			return errTooLarge
		}
	}
	if t.queued {
		if err := t.cm.PublishViaQueue(ctx, &autopaho.QueuePublish{Publish: p}); err != nil {
			return fmt.Errorf("%w: %v", errTransportDown, err)
		}
		return nil
	}
	if !t.connected.Load() {
		return errTransportDown
	}
	resp, err := t.cm.Publish(ctx, p)
	// paho returns an error together with a QoS 1 refusal: check the reason first.
	if resp != nil && resp.ReasonCode >= 0x80 {
		return fmt.Errorf("remote broker refused publish: reason 0x%02x", resp.ReasonCode)
	}
	if errors.Is(err, paho.ErrInvalidArguments) {
		return fmt.Errorf("remote broker cannot take publish: %v", err)
	}
	if err != nil {
		// ConnectionDownError, or a write error on a dead socket before the library notices
		return fmt.Errorf("%w: %v", errTransportDown, err)
	}
	return nil
}

// fitToServer lowers QoS and clears retain to what the remote broker
// announced, as a broker does when it forwards to a subscriber.
func fitToServer(p *paho.Publish, maxQoS byte, retainAvailable bool) {
	p.QoS = min(p.QoS, maxQoS)
	p.Retain = p.Retain && retainAvailable
}

func (t *mqtt5Transport) Connected() bool { return t.connected.Load() }

func (t *mqtt5Transport) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = t.cm.Disconnect(ctx)
	t.cancel()
	t.connected.Store(false)
}
