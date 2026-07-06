package hostinfo

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

type Collector struct {
	monitor   *SystemMonitor
	interval  time.Duration
	topic     string
	qos       byte
	publishFn func(topic string, payload []byte, retain bool, qos byte) error
	logger    *slog.Logger
	stopCh    chan struct{}
}

func NewCollector(
	nodeID string,
	intervalSeconds int,
	baseTopic string,
	qos int,
	publishFn func(topic string, payload []byte, retain bool, qos byte) error,
	logger *slog.Logger,
) *Collector {
	topic := baseTopic
	topic = strings.ReplaceAll(topic, "{NodeId}", nodeID)
	topic = strings.ReplaceAll(topic, "{node_id}", nodeID)

	interval := time.Duration(intervalSeconds) * time.Second
	if interval <= 0 {
		interval = 60 * time.Second
	}

	return &Collector{
		monitor:   NewSystemMonitor(),
		interval:  interval,
		topic:     topic,
		qos:       byte(qos),
		publishFn: publishFn,
		logger:    logger,
		stopCh:    make(chan struct{}),
	}
}

func (c *Collector) Start(ctx context.Context) {
	c.logger.Info("starting host monitoring service", "topic", c.topic, "interval", c.interval)
	go func() {
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()

		// Publish initial reading right away on start
		c.collectAndPublish()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.collectAndPublish()
			}
		}
	}()
}

func (c *Collector) Stop() {
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
}

func (c *Collector) collectAndPublish() {
	stats, err := c.monitor.GetStats()
	if err != nil {
		c.logger.Error("failed to get host stats", "err", err)
		return
	}

	payload, err := json.Marshal(stats)
	if err != nil {
		c.logger.Error("failed to marshal host stats JSON", "err", err)
		return
	}

	if err := c.publishFn(c.topic, payload, false, c.qos); err != nil {
		c.logger.Warn("failed to publish host stats to mqtt", "topic", c.topic, "err", err)
	}
}
