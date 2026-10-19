package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"

	mqtt "monstermq.io/edge/internal/mqtt"
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/winccoanative"
)

// WinCCOaNativeHook connects the MQTT engine to the native WinCC OA
// namespace: per-filter SUBSCRIBE validation, interest tracking, typed
// writes and protection of the reserved winccoa/ branches.
type WinCCOaNativeHook struct {
	mqtt.HookBase
	svc    *winccoanative.Service
	server *mqtt.Server
	logger *slog.Logger

	mu      sync.Mutex
	existed map[string]bool
}

// ACLs, including the canonical-topic rule for native aliases, are applied
// by the auth hook before this hook runs.
func NewWinCCOaNativeHook(svc *winccoanative.Service, server *mqtt.Server, logger *slog.Logger) *WinCCOaNativeHook {
	return &WinCCOaNativeHook{svc: svc, server: server, logger: logger, existed: map[string]bool{}}
}

func (h *WinCCOaNativeHook) ID() string { return "winccoa-native" }

func (h *WinCCOaNativeHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnSubscribeValidate,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribed,
		mqtt.OnPublish,
	}, []byte{b})
}

func existedKey(clientID, filter string) string { return clientID + "\x00" + filter }

func (h *WinCCOaNativeHook) OnSubscribeValidate(cl *mqtt.Client, sub packets.Subscription) packets.Code {
	if cl.Net.Inline {
		return packets.CodeSuccess
	}
	v, why := h.svc.Validate(sub.Filter)
	switch v {
	case winccoanative.Accept:
		_, existed := cl.State.Subscriptions.Get(sub.Filter)
		h.mu.Lock()
		h.existed[existedKey(cl.ID, sub.Filter)] = existed
		h.mu.Unlock()
		return packets.CodeSuccess
	case winccoanative.NotNative:
		return packets.CodeSuccess
	}
	h.logger.Debug("native subscription rejected", "client", cl.ID, "filter", sub.Filter, "verdict", v.String(), "reason", why)
	return verdictCode(v, why)
}

func verdictCode(v winccoanative.Verdict, why string) packets.Code {
	var c packets.Code
	switch v {
	case winccoanative.Denied:
		c = packets.ErrNotAuthorized
	case winccoanative.Invalid:
		c = packets.ErrTopicFilterInvalid
	case winccoanative.WildcardDeny:
		c = packets.ErrWildcardSubscriptionsNotSupported
	case winccoanative.SharedDeny:
		c = packets.ErrSharedSubscriptionsNotSupported
	case winccoanative.BadPayload:
		c = packets.ErrPayloadFormatInvalid
	default:
		c = packets.ErrImplementationSpecificError
	}
	if why != "" {
		c.Reason = why
	}
	return c
}

func (h *WinCCOaNativeHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	for i, f := range pk.Filters {
		if i < len(reasonCodes) && reasonCodes[i] > packets.CodeGrantedQos2.Code {
			continue
		}
		k := existedKey(cl.ID, f.Filter)
		h.mu.Lock()
		existed, ok := h.existed[k]
		delete(h.existed, k)
		h.mu.Unlock()
		if !ok {
			continue
		}
		h.svc.Subscribed(cl.ID, f.Filter, existed)
	}
}

func (h *WinCCOaNativeHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	filters := make([]string, 0, len(pk.Filters))
	for _, f := range pk.Filters {
		filters = append(filters, f.Filter)
	}
	h.svc.Unsubscribed(cl.ID, filters)
}

func (h *WinCCOaNativeHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline {
		return pk, nil
	}
	return h.handlePublish(cl.ID, pk)
}

// inlinePublisher is the client id of in-process publishers (GraphQL, REST,
// scripts, bridges) for native commands. U+0000 is not allowed in MQTT
// strings, so no MQTT client id can collide.
const inlinePublisher = "\x00inline"

// InlinePublish applies the native publish rules to an in-process publish,
// which the engine sends through the inline client that OnPublish skips (the
// service publishes its own values through it). handled reports a publish
// the native namespace consumed or rejected; the caller must not deliver it.
func (h *WinCCOaNativeHook) InlinePublish(topic string, payload []byte, retain bool, qos byte) (handled bool, err error) {
	return h.InlinePublishMessage(inlinePublisher, stores.BrokerMessage{
		TopicName: topic,
		Payload:   payload,
		IsRetain:  retain,
		QoS:       qos,
	})
}

// InlinePublishMessage is InlinePublish for a protocol session, preserving its
// client ID, response topic and correlation data.
func (h *WinCCOaNativeHook) InlinePublishMessage(clientID string, msg stores.BrokerMessage) (handled bool, err error) {
	if clientID == "" {
		clientID = inlinePublisher
	}
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: msg.QoS, Retain: msg.IsRetain},
		TopicName:   msg.TopicName,
		Payload:     msg.Payload,
		Properties:  propsToPacket(msg),
	}
	_, err = h.handlePublish(clientID, pk)
	if errors.Is(err, packets.CodeSuccessIgnore) {
		return true, nil
	}
	return err != nil, err
}

func (h *WinCCOaNativeHook) handlePublish(clientID string, pk packets.Packet) (packets.Packet, error) {
	switch kind := h.svc.Names().Classify(pk.TopicName); kind {
	case winccoanative.KindOther:
		return pk, nil
	case winccoanative.KindTopics:
		return h.publishTopic(clientID, pk)
	case winccoanative.KindNative:
	default:
		if kind == winccoanative.KindStatus && h.svc.StaleStatusClear(pk.TopicName, pk.FixedHeader.Retain, pk.Payload) {
			h.logger.Info("stale node status cleared", "client", clientID, "topic", pk.TopicName)
			return pk, nil
		}
		h.logger.Info("publish to reserved topic rejected", "client", clientID, "topic", pk.TopicName)
		return pk, packets.ErrNotAuthorized
	}
	reply := winccoanative.Reply{Topic: pk.Properties.ResponseTopic, Correlation: pk.Properties.CorrelationData}
	v, why := h.svc.Command(clientID, pk.TopicName, pk.Payload, pk.FixedHeader.Retain, reply, h.sendReply)
	if v == winccoanative.Accept {
		return pk, packets.CodeSuccessIgnore
	}
	h.logger.Info("native command rejected", "client", clientID, "topic", pk.TopicName, "verdict", v.String(), "reason", why)
	h.rejectReply(pk, reply, v, why)
	code := verdictCode(v, why)
	if v == winccoanative.Invalid {
		code = packets.ErrTopicNameInvalid
		code.Reason = why
	}
	return pk, code
}

// publishTopic writes a publish of the topics branch to its datapoint. The
// broker does not deliver or retain it itself: subscribers on every system
// get it through their datapoint connection.
func (h *WinCCOaNativeHook) publishTopic(clientID string, pk packets.Packet) (packets.Packet, error) {
	v, why := h.svc.PublishTopic(pk.TopicName, pk.Payload, pk.FixedHeader.Retain, pk.FixedHeader.Qos > 0)
	if v == winccoanative.Accept {
		return pk, packets.CodeSuccessIgnore
	}
	h.logger.Info("topic publish rejected", "client", clientID, "topic", pk.TopicName, "verdict", v.String(), "reason", why)
	code := verdictCode(v, why)
	if v == winccoanative.Invalid {
		code = packets.ErrTopicNameInvalid
		code.Reason = why
	}
	return pk, code
}

// rejectReply reports a synchronous rejection on the reply topic too, so
// MQTT 3.1.1 and QoS 0 publishers can observe it.
func (h *WinCCOaNativeHook) rejectReply(pk packets.Packet, reply winccoanative.Reply, v winccoanative.Verdict, why string) {
	if reply.Topic == "" {
		if c, err := winccoanative.ParseCommand(pk.Payload); err == nil {
			reply.Topic = c.ReplyTo
		}
	}
	if reply.Topic == "" {
		return
	}
	id := ""
	if c, err := winccoanative.ParseCommand(pk.Payload); err == nil {
		id = c.ID
	}
	res := winccoanative.CommandResult{ID: id, Topic: pk.TopicName, Status: "rejected", Error: v.String() + ": " + why}
	b, _ := json.Marshal(res)
	h.sendReply(reply, b)
}

func (h *WinCCOaNativeHook) sendReply(r winccoanative.Reply, payload []byte) {
	if h.svc.Names().Classify(r.Topic) != winccoanative.KindOther {
		return
	}
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   r.Topic,
		Payload:     payload,
		PacketID:    1,
	}
	if len(r.Correlation) > 0 {
		pk.Properties.CorrelationData = r.Correlation
	}
	if err := h.server.PublishPacket(pk); err != nil {
		h.logger.Warn("native command reply failed", "topic", r.Topic, "err", err)
	}
}
