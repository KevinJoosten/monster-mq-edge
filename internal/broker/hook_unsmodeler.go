package broker

import (
	"bytes"
	"errors"
	"log/slog"

	mqtt "monstermq.io/edge/internal/mqtt"
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/unsmodeler"
)

// UnsModelerHook routes publishes on <uns-topic>/..set to the UNS Modeler
// device that maps the topic (write-through to WinCC OA). ACLs are applied
// by the auth hook before this hook runs.
type UnsModelerHook struct {
	mqtt.HookBase
	mgr    *unsmodeler.Manager
	logger *slog.Logger
}

func NewUnsModelerHook(mgr *unsmodeler.Manager, logger *slog.Logger) *UnsModelerHook {
	return &UnsModelerHook{mgr: mgr, logger: logger}
}

func (h *UnsModelerHook) ID() string { return "uns-modeler" }

func (h *UnsModelerHook) Provides(b byte) bool {
	return bytes.Contains([]byte{mqtt.OnPublish}, []byte{b})
}

func (h *UnsModelerHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline {
		return pk, nil
	}
	return pk, h.handle(cl.ID, pk.TopicName, pk.Payload)
}

// InlinePublish applies the write-through rules to an in-process publish.
// handled reports a publish the device consumed or rejected.
func (h *UnsModelerHook) InlinePublish(topic string, payload []byte) (handled bool, err error) {
	if !unsmodeler.IsSetTopic(topic) {
		return false, nil
	}
	err = h.handle("\x00inline", topic, payload)
	if errors.Is(err, packets.CodeSuccessIgnore) {
		return true, nil
	}
	return err != nil, err
}

func (h *UnsModelerHook) handle(clientID, topic string, payload []byte) error {
	if !unsmodeler.IsSetTopic(topic) {
		return nil
	}
	handled, err := h.mgr.Write(topic, payload)
	if !handled {
		return nil
	}
	if err == nil {
		return packets.CodeSuccessIgnore
	}
	h.logger.Info("uns write-through rejected", "logger", "UnsModeler", "client", clientID, "topic", topic, "err", err)
	if errors.Is(err, unsmodeler.ErrWriteDisabled) || errors.Is(err, unsmodeler.ErrStandby) {
		code := packets.ErrNotAuthorized
		code.Reason = err.Error()
		return code
	}
	code := packets.ErrPayloadFormatInvalid
	code.Reason = err.Error()
	return code
}
