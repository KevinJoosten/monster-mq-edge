package broker

import (
	"bytes"
	"strings"
	"sync"

	"monstermq.io/edge/internal/bridge/mqttclient"
	mqtt "monstermq.io/edge/internal/mqtt"
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/stores"
)

// InlinePublisher publishes into the local engine on behalf of named inline
// clients, keeping MQTT 5 properties. The MQTT bridge uses it so its local
// publishes carry its own client ID ("bridge:<name>") instead of "inline",
// which lets it recognise — and not forward back — what it brought in.
type InlinePublisher struct {
	srv     *mqtt.Server
	mu      sync.Mutex
	clients map[string]*mqtt.Client
}

func NewInlinePublisher(srv *mqtt.Server) *InlinePublisher {
	return &InlinePublisher{srv: srv, clients: map[string]*mqtt.Client{}}
}

// Publish injects msg as if clientID had published it. Inline clients bypass
// the ACL, as the engine's default inline client does.
func (p *InlinePublisher) Publish(clientID string, msg stores.BrokerMessage) error {
	return p.srv.InjectPacket(p.client(clientID), packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: msg.QoS, Retain: msg.IsRetain},
		TopicName:   msg.TopicName,
		Payload:     msg.Payload,
		PacketID:    uint16(msg.QoS), // as Server.Publish: a packet id for the validity checks
		Properties:  propsToPacket(msg),
	})
}

func (p *InlinePublisher) client(id string) *mqtt.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	cl, ok := p.clients[id]
	if !ok {
		cl = p.srv.NewClient(nil, mqtt.LocalListener, id, true)
		cl.Properties.ProtocolVersion = 5 // the engine keeps properties for MQTT 5 publishers
		p.clients[id] = cl
	}
	return cl
}

// bridgeIDHook refuses network clients whose ID starts with the bridge prefix:
// No Local would hide from them what the bridge of that name brings in.
type bridgeIDHook struct {
	mqtt.HookBase
	srv *mqtt.Server
}

func (h *bridgeIDHook) ID() string { return "bridge-client-ids" }

func (h *bridgeIDHook) Provides(b byte) bool { return bytes.Contains([]byte{mqtt.OnConnect}, []byte{b}) }

func (h *bridgeIDHook) OnConnect(cl *mqtt.Client, _ packets.Packet) error {
	if !strings.HasPrefix(cl.ID, mqttclient.ClientIDPrefix) {
		return nil
	}
	if h.srv != nil {
		_ = h.srv.SendConnack(cl, packets.ErrClientIdentifierNotValid, false, nil)
	}
	return packets.ErrClientIdentifierNotValid
}
