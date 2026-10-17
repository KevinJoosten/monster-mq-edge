package prometheus

import (
	"math"
	"net/http"
	"strings"

	"monstermq.io/edge/internal/metrics"
	"monstermq.io/edge/internal/stores"
)

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	var sb strings.Builder
	s.appendBrokerMetrics(&sb)
	s.appendTopicValueMetrics(r, &sb)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(sb.String()))
}

func (s *Server) appendBrokerMetrics(sb *strings.Builder) {
	appendMetricComment(sb, "metrics", "gauge", "MonsterMQ broker metric")
	nodeID := s.nodeID()
	var snap metrics.BrokerSnapshot
	if s.deps.Collector != nil {
		snap = s.deps.Collector.Latest()
	}
	for _, label := range scrapeBrokerMetricLabels {
		val, _ := instantBrokerMetricValue(snap, label)
		appendMetric(sb, "metrics", []labelKV{
			{K: "node", V: nodeID},
			{K: "metric", V: label},
		}, val)
	}
}

func (s *Server) appendTopicValueMetrics(r *http.Request, sb *strings.Builder) {
	appendMetricComment(sb, "topics", "gauge", "Current value of MQTT topic")
	ctx := r.Context()
	for _, g := range s.groups() {
		lv := g.LastValue()
		if lv == nil {
			continue
		}
		filters := []string{"#"}
		if isWinCCOAGroup(g) {
			filters = s.scopedFilters(g)
			if len(filters) == 0 {
				continue
			}
		}
		groupName := g.Name()
		seen := map[string]struct{}{}
		for _, f := range filters {
			_ = lv.FindMatchingMessages(ctx, f, func(msg stores.BrokerMessage) bool {
				if len(filters) > 1 {
					if _, dup := seen[msg.TopicName]; dup {
						return true
					}
					seen[msg.TopicName] = struct{}{}
				}
				if !s.canRead(r, msg.TopicName) {
					return true
				}
				payload := strings.TrimSpace(string(msg.Payload))
				topicName := msg.TopicName
				if numVal, ok := parseJavaDouble(payload); ok {
					appendMetric(sb, "topics", []labelKV{
						{K: "group", V: groupName},
						{K: "topic", V: topicName},
						{K: "field", V: ""},
					}, numVal)
					return true
				}
				if fields, ok := numericFields(payload); ok {
					for _, fv := range fields {
						appendMetric(sb, "topics", []labelKV{
							{K: "group", V: groupName},
							{K: "topic", V: topicName},
							{K: "field", V: fv.Key},
						}, fv.Value)
					}
				}
				return true
			})
		}
	}
}

type labelKV struct {
	K string
	V string
}

func appendMetricComment(sb *strings.Builder, name, metricType, help string) {
	sb.WriteString("# HELP ")
	sb.WriteString(name)
	sb.WriteByte(' ')
	sb.WriteString(help)
	sb.WriteString("\n# TYPE ")
	sb.WriteString(name)
	sb.WriteByte(' ')
	sb.WriteString(metricType)
	sb.WriteByte('\n')
}

var labelEscaper = strings.NewReplacer(
	`\`, `\\`,
	`"`, `\"`,
	"\n", `\n`,
)

func appendMetric(sb *strings.Builder, name string, labels []labelKV, value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	sb.WriteString(name)
	sb.WriteByte('{')
	for i, kv := range labels {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(kv.K)
		sb.WriteString(`="`)
		sb.WriteString(labelEscaper.Replace(kv.V))
		sb.WriteByte('"')
	}
	sb.WriteString("} ")
	sb.WriteString(javaDouble(value))
	sb.WriteByte('\n')
}
