package broker

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"

	"monstermq.io/edge/internal/bridge/kafkaclient"
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
	snap    atomic.Pointer[map[string]*mqtt.Client]
}

func NewInlinePublisher(srv *mqtt.Server) *InlinePublisher {
	return &InlinePublisher{srv: srv, clients: map[string]*mqtt.Client{}}
}

// Publish injects msg as if clientID had published it. Inline clients bypass
// the ACL, as the engine's default inline client does.
//
// The broker keeps references to the topic and payload of msg after the call returns
// (subscriber queues, inflight messages, the last-value store), so the caller
// must not modify them afterwards.
func (p *InlinePublisher) Publish(clientID string, msg stores.BrokerMessage) error {
	return p.srv.InjectPacket(p.client(clientID), packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: msg.QoS, Retain: msg.IsRetain},
		TopicName:   msg.TopicName,
		Payload:     msg.Payload,
		PacketID:    uint16(msg.QoS), // as Server.Publish: a packet id for the validity checks
		Properties:  propsToPacket(msg),
	})
}

// PublishMessage injects msg as clientID and keeps its identity (UUID, time,
// message id, DUP and queued flags) for archives and bus subscribers, as Core
// republishes a message from a Kafka envelope.
//
// The broker keeps references to the topic and payload of msg after the call returns
// (subscriber queues, inflight messages, the last-value store), so the caller
// must not modify them afterwards.
func (p *InlinePublisher) PublishMessage(clientID string, msg stores.BrokerMessage) error {
	src := &packets.Source{MessageUUID: msg.MessageUUID, Time: msg.Time, MessageID: msg.MessageID, Dup: msg.IsDup, Queued: msg.IsQueued}
	return p.srv.InjectPacket(p.client(clientID), packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: msg.QoS, Retain: msg.IsRetain},
		TopicName:   msg.TopicName,
		Payload:     msg.Payload,
		PacketID:    uint16(msg.QoS),
		Properties:  propsToPacket(msg),
		Source:      src,
	})
}

func (p *InlinePublisher) refreshSnapLocked() {
	cp := make(map[string]*mqtt.Client, len(p.clients))
	for k, v := range p.clients {
		cp[k] = v
	}
	p.snap.Store(&cp)
}

func (p *InlinePublisher) client(id string) *mqtt.Client {
	if snap := p.snap.Load(); snap != nil {
		if cl, ok := (*snap)[id]; ok {
			return cl
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	cl, ok := p.clients[id]
	if !ok {
		cl = p.srv.NewClient(nil, mqtt.LocalListener, id, true)
		cl.Properties.ProtocolVersion = 5 // the engine keeps properties for MQTT 5 publishers
		p.clients[id] = cl
		p.refreshSnapLocked()
	}
	return cl
}

// Release removes the cached inline client for clientID so per-connection
// protocol session IDs do not accumulate after disconnect.
func (p *InlinePublisher) Release(clientID string) {
	p.mu.Lock()
	delete(p.clients, clientID)
	p.refreshSnapLocked()
	p.mu.Unlock()
}

// bridgeIDHook refuses network clients whose ID takes an inline publisher's
// prefix. For "bridge:" No Local would hide from them what the MQTT bridge of
// that name brings in. For "kafkaclient-" (only while the Kafka feature is on)
// the Kafka bridge's outbound loop prevention would silently drop their
// publishes.
type bridgeIDHook struct {
	mqtt.HookBase
	srv   *mqtt.Server
	kafka bool
}

func (h *bridgeIDHook) ID() string { return "bridge-client-ids" }

func (h *bridgeIDHook) Provides(b byte) bool { return bytes.Contains([]byte{mqtt.OnConnect}, []byte{b}) }

func (h *bridgeIDHook) OnConnect(cl *mqtt.Client, _ packets.Packet) error {
	reserved := strings.HasPrefix(cl.ID, mqttclient.ClientIDPrefix) ||
		(h.kafka && strings.HasPrefix(cl.ID, kafkaclient.ClientIDPrefix))
	if !reserved {
		return nil
	}
	if h.srv != nil {
		_ = h.srv.SendConnack(cl, packets.ErrClientIdentifierNotValid, false, nil)
	}
	return packets.ErrClientIdentifierNotValid
}
