package broker

import (
	"context"
	"time"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/config"
	mqtt "monstermq.io/edge/internal/mqtt"
	"monstermq.io/edge/internal/pubsub"
	"monstermq.io/edge/internal/stores"
)

// unsTopicSource gathers topics for UNS Modeler Direction B runs: live from
// retained messages plus a short sample of current publishes, or from the
// last values of an archive group.
type unsTopicSource struct {
	cfg      *config.Config
	server   *mqtt.Server
	storage  *stores.Storage
	archives *archive.Manager
	bus      *pubsub.Bus
	sample   time.Duration
}

func (s *unsTopicSource) retained(ctx context.Context, filter string, each func(topic string, payload []byte) bool) {
	if s.cfg.RetainedStore() == config.StoreMemory {
		for _, pk := range s.server.Topics.Messages(filter) {
			if !each(pk.TopicName, pk.Payload) {
				return
			}
		}
		return
	}
	if s.storage.Retained != nil {
		_ = s.storage.Retained.FindMatchingMessages(ctx, filter, func(m stores.BrokerMessage) bool { return each(m.TopicName, m.Payload) })
	}
}

func (s *unsTopicSource) Collect(ctx context.Context, mode, group string, filters []string, max int) (map[string][]byte, error) {
	out := map[string][]byte{}
	full := func() bool { return max > 0 && len(out) >= max }
	if mode == "archive" {
		g := s.archives.Get(group)
		if g == nil || g.LastValue() == nil {
			return out, nil
		}
		for _, f := range filters {
			_ = g.LastValue().FindMatchingMessages(ctx, f, func(m stores.BrokerMessage) bool {
				out[m.TopicName] = m.Payload
				return !full()
			})
		}
		return out, nil
	}
	for _, f := range filters {
		s.retained(ctx, f, func(t string, p []byte) bool {
			out[t] = p
			return !full()
		})
	}
	if s.bus != nil && s.sample > 0 && !full() {
		id, ch := s.bus.SubscribeLocal(filters, 4096)
		timer := time.NewTimer(s.sample)
	loop:
		for {
			select {
			case m := <-ch:
				out[m.TopicName] = m.Payload
				if full() {
					break loop
				}
			case <-timer.C:
				break loop
			case <-ctx.Done():
				break loop
			}
		}
		timer.Stop()
		s.bus.Unsubscribe(id)
	}
	return out, nil
}

func (s *unsTopicSource) Last(ctx context.Context, topic string) ([]byte, bool) {
	var got []byte
	found := false
	s.retained(ctx, topic, func(t string, p []byte) bool {
		if t == topic {
			got, found = p, true
			return false
		}
		return true
	})
	if found {
		return got, true
	}
	if g := s.archives.Get("Default"); g != nil && g.LastValue() != nil {
		if m, err := g.LastValue().Get(ctx, topic); err == nil && m != nil {
			return m.Payload, true
		}
	}
	return nil, false
}

func (s *unsTopicSource) ArchiveGroupExists(name string) bool { return s.archives.Get(name) != nil }

// unsSubscriber delivers publishes from the bus to the Direction B flow.
type unsSubscriber struct{ bus *pubsub.Bus }

func (u unsSubscriber) Subscribe(filters []string, fn func(topic string, payload []byte, at time.Time)) (func(), error) {
	id, ch := u.bus.SubscribeLocal(filters, 16384)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				fn(m.TopicName, m.Payload, m.Time)
			}
		}
	}()
	return func() {
		u.bus.Unsubscribe(id)
		close(done)
	}, nil
}
