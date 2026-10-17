package redisserver

import (
	"log/slog"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/auth"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/metrics"
	"monstermq.io/edge/internal/pubsub"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/winccoanative"
)

type Deps struct {
	Cfg              *config.Config
	Archives         *archive.Manager
	Bus              *pubsub.Bus
	Sessions         stores.SessionStore
	Auth             *auth.Cache
	Publish          func(clientID string, msg stores.BrokerMessage) error // S1 ProtocolPublish
	Release          func(clientID string)                                  // S1 InlinePublisher.Release
	Registry         interface{ Register(string, func()); Unregister(string) } // S5 ProtocolSessions
	NativeNames      func() winccoanative.Names                             // nil when the native namespace is off
	NativeValueWrite func(topic string) (string, bool)                      // S2, bound to NativeNames
	Collector        *metrics.Collector                                     // may be nil
	Logger           *slog.Logger
}
