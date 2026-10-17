package prometheus

import (
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/stores"
)

var (
	allLabelNames = []string{"__name__", "group", "topic", "field", "node", "metric"}
	metricNames   = []string{"topics", "metrics"}
)

type listResponse struct {
	Status string `json:"status"`
	Data   any    `json:"data"`
}

type queryResponse struct {
	Status string    `json:"status"`
	Data   queryData `json:"data"`
}

type queryData struct {
	ResultType string `json:"resultType"`
	Result     []any  `json:"result"`
}

type topicMetricLabels struct {
	Name  string `json:"__name__"`
	Group string `json:"group"`
	Topic string `json:"topic"`
	Field string `json:"field"`
}

type brokerMetricLabelsObj struct {
	Name   string `json:"__name__"`
	Node   string `json:"node"`
	Metric string `json:"metric"`
}

type topicVectorSample struct {
	Metric topicMetricLabels `json:"metric"`
	Value  []any             `json:"value"`
}

type brokerVectorSample struct {
	Metric brokerMetricLabelsObj `json:"metric"`
	Value  []any                 `json:"value"`
}

type topicMatrixSeries struct {
	Metric topicMetricLabels `json:"metric"`
	Values [][]any           `json:"values"`
}

type brokerMatrixSeries struct {
	Metric brokerMetricLabelsObj `json:"metric"`
	Values [][]any               `json:"values"`
}

func (s *Server) nodeID() string {
	if s.deps.Cfg != nil && s.deps.Cfg.NodeID != "" {
		return s.deps.Cfg.NodeID
	}
	return "edge"
}

func (s *Server) rawQueryLimit() int {
	if s.deps.Cfg != nil && s.deps.Cfg.Prometheus.RawQueryLimit >= 1 {
		return s.deps.Cfg.Prometheus.RawQueryLimit
	}
	return 10000
}

func (s *Server) metricsIntervalSeconds() int {
	if s.deps.Collector != nil {
		secs := int(s.deps.Collector.Interval().Seconds())
		if secs >= 1 {
			return secs
		}
	}
	if s.deps.Cfg != nil && s.deps.Cfg.Metrics.CollectionIntervalSeconds >= 1 {
		return s.deps.Cfg.Metrics.CollectionIntervalSeconds
	}
	return 1
}

func (s *Server) groups() []*archive.Group {
	if s.deps.Archives == nil {
		return nil
	}
	gs := s.deps.Archives.Snapshot()
	sort.Slice(gs, func(i, j int) bool {
		return gs[i].Name() < gs[j].Name()
	})
	return gs
}

func (s *Server) handleLabels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, listResponse{
		Status: "success",
		Data:   allLabelNames,
	})
}

func (s *Server) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	labelName := chi.URLParam(r, "name")
	matchSelector := getMatchSelector(r)
	matchLabels := extractLabelMatchers(matchSelector)
	groupFilter := matchLabels["group"]
	topicFilter := matchLabels["topic"]

	switch labelName {
	case "__name__":
		writeJSON(w, http.StatusOK, listResponse{Status: "success", Data: metricNames})

	case "group":
		gs := s.groups()
		names := make([]string, 0, len(gs))
		for _, g := range gs {
			names = append(names, g.Name())
		}
		writeJSON(w, http.StatusOK, listResponse{Status: "success", Data: names})

	case "topic":
		ctx := r.Context()
		pattern := topicFilter
		if pattern == "" {
			pattern = "#"
		}
		topicSet := map[string]struct{}{}
		for _, g := range s.groups() {
			if groupFilter != "" && g.Name() != groupFilter {
				continue
			}
			if s.shouldSkipWinCCOAEnumeration(g, groupFilter, topicFilter) {
				continue
			}
			lv := g.LastValue()
			if lv == nil {
				continue
			}
			_ = lv.FindMatchingMessages(ctx, pattern, func(msg stores.BrokerMessage) bool {
				if s.canRead(r, msg.TopicName) {
					topicSet[msg.TopicName] = struct{}{}
				}
				return len(topicSet) < 5000
			})
		}
		topics := make([]string, 0, len(topicSet))
		for t := range topicSet {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		writeJSON(w, http.StatusOK, listResponse{Status: "success", Data: topics})

	case "field":
		ctx := r.Context()
		pattern := topicFilter
		if pattern == "" {
			pattern = "#"
		}
		fieldSet := map[string]struct{}{}
		for _, g := range s.groups() {
			if groupFilter != "" && g.Name() != groupFilter {
				continue
			}
			if s.shouldSkipWinCCOAEnumeration(g, groupFilter, topicFilter) {
				continue
			}
			lv := g.LastValue()
			if lv == nil {
				continue
			}
			_ = lv.FindMatchingMessages(ctx, pattern, func(msg stores.BrokerMessage) bool {
				if !s.canRead(r, msg.TopicName) {
					return true
				}
				payload := strings.TrimSpace(string(msg.Payload))
				if _, ok := parseJavaDouble(payload); ok {
					fieldSet[""] = struct{}{}
				} else if fields, ok := numericFields(payload); ok {
					for _, fv := range fields {
						fieldSet[fv.Key] = struct{}{}
					}
				}
				return len(fieldSet) < 1000
			})
		}
		fields := make([]string, 0, len(fieldSet))
		for f := range fieldSet {
			fields = append(fields, f)
		}
		sort.Strings(fields)
		writeJSON(w, http.StatusOK, listResponse{Status: "success", Data: fields})

	case "metric":
		writeJSON(w, http.StatusOK, listResponse{Status: "success", Data: brokerMetricLabels})

	case "node":
		writeJSON(w, http.StatusOK, listResponse{Status: "success", Data: []string{s.nodeID()}})

	default:
		writeJSON(w, http.StatusOK, listResponse{Status: "success", Data: []string{}})
	}
}

func (s *Server) handleSeries(w http.ResponseWriter, r *http.Request) {
	matchSelector := getMatchSelector(r)
	matchLabels := extractLabelMatchers(matchSelector)
	groupFilter := matchLabels["group"]
	topicFilter := matchLabels["topic"]
	metricName := seriesMetricName(matchSelector, matchLabels)

	data := make([]any, 0)
	ctx := r.Context()

	if metricName == "" || metricName == "topics" {
		pattern := topicFilter
		if pattern == "" {
			pattern = "#"
		}
		for _, g := range s.groups() {
			if groupFilter != "" && g.Name() != groupFilter {
				continue
			}
			if s.shouldSkipWinCCOAEnumeration(g, groupFilter, topicFilter) {
				continue
			}
			lv := g.LastValue()
			if lv == nil {
				continue
			}
			groupName := g.Name()
			_ = lv.FindMatchingMessages(ctx, pattern, func(msg stores.BrokerMessage) bool {
				if !s.canRead(r, msg.TopicName) {
					return true
				}
				payload := strings.TrimSpace(string(msg.Payload))
				var fields []string
				if _, ok := parseJavaDouble(payload); ok {
					fields = append(fields, "")
				} else if fvs, ok := numericFields(payload); ok {
					for _, fv := range fvs {
						fields = append(fields, fv.Key)
					}
				}
				if len(fields) == 0 {
					fields = append(fields, "")
				}
				for _, f := range fields {
					data = append(data, topicMetricLabels{
						Name:  "topics",
						Group: groupName,
						Topic: msg.TopicName,
						Field: f,
					})
				}
				return len(data) < 5000
			})
		}
	}

	if metricName == "" || metricName == "metrics" {
		nodeID := s.nodeID()
		for _, label := range brokerMetricLabels {
			data = append(data, brokerMetricLabelsObj{
				Name:   "metrics",
				Node:   nodeID,
				Metric: label,
			})
		}
	}

	writeJSON(w, http.StatusOK, listResponse{
		Status: "success",
		Data:   data,
	})
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	queryStr := getParam(r, "query")
	now := time.Now().Unix()

	if queryStr == "" {
		sendVectorResult(w, []any{})
		return
	}

	parsed := parsePromQL(queryStr)

	if parsed.Metric == "metrics" {
		metricLabel := parsed.Labels["metric"]
		nodeFilter, hasNode := parsed.Labels["node"]
		nodeID := s.nodeID()
		if hasNode && nodeFilter != nodeID {
			sendVectorResult(w, []any{})
			return
		}
		if s.deps.Collector == nil {
			sendVectorResult(w, []any{})
			return
		}
		snap := s.deps.Collector.Latest()
		val, ok := instantBrokerMetricValue(snap, metricLabel)
		if !ok {
			sendVectorResult(w, []any{})
			return
		}
		sendVectorResult(w, []any{
			brokerVectorSample{
				Metric: brokerMetricLabelsObj{
					Name:   "metrics",
					Node:   nodeID,
					Metric: metricLabel,
				},
				Value: []any{now, javaDouble(val)},
			},
		})
		return
	}

	topic := parsed.Labels["topic"]
	requestedGroup := parsed.Labels["group"]
	field := parsed.Labels["field"]

	result := make([]any, 0)
	if topic != "" {
		group, groupName := s.resolveGroup(requestedGroup, topic, false)
		if group != nil && group.LastValue() != nil {
			_ = group.LastValue().FindMatchingMessages(r.Context(), topic, func(msg stores.BrokerMessage) bool {
				if !s.canRead(r, msg.TopicName) {
					return true
				}
				payload := strings.TrimSpace(string(msg.Payload))
				if numVal, ok := extractNumericValue(payload, field); ok {
					result = append(result, topicVectorSample{
						Metric: topicMetricLabels{
							Name:  "topics",
							Group: groupName,
							Topic: msg.TopicName,
							Field: field,
						},
						Value: []any{now, javaDouble(numVal)},
					})
				}
				return true
			})
		}
	}

	sendVectorResult(w, result)
}

func (s *Server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	queryStr := getParam(r, "query")
	startStr := getParam(r, "start")
	endStr := getParam(r, "end")
	stepStr := getParam(r, "step")

	if queryStr == "" {
		sendRangeResult(w, []any{})
		return
	}

	now := time.Now().UTC()
	startTime, ok := parsePrometheusTime(startStr)
	if !ok {
		startTime = now.Add(-3600 * time.Second)
	}
	endTime, ok := parsePrometheusTime(endStr)
	if !ok {
		endTime = now
	}
	stepSeconds := parseStep(stepStr)

	parsed := parsePromQL(queryStr)

	if parsed.Metric == "metrics" {
		s.handleQueryRangeMetric(w, r, parsed, startTime, endTime, stepSeconds)
		return
	}

	intervalMinutes := snapToValidIntervalMinutes(stepSeconds)
	topic := parsed.Labels["topic"]
	requestedGroup := parsed.Labels["group"]
	field := parsed.Labels["field"]

	if topic == "" {
		sendRangeResult(w, []any{})
		return
	}

	if !strings.ContainsAny(topic, "+#") && !s.canRead(r, topic) {
		sendRangeResult(w, []any{})
		return
	}

	group, groupName := s.resolveGroup(requestedGroup, topic, true)
	if group == nil || group.Archive() == nil {
		sendRangeResult(w, []any{})
		return
	}
	archiveStore := group.Archive()
	ctx := r.Context()

	if stepSeconds < 60 && parsed.Function == "" {
		records, err := archiveStore.GetHistory(ctx, topic, &startTime, &endTime, s.rawQueryLimit())
		if err != nil {
			s.deps.Logger.Warn("prometheus query_range raw history error", "group", groupName, "topic", topic, "err", err)
			sendRangeResult(w, []any{})
			return
		}
		var raw []samplePoint
		for _, rec := range records {
			recTopic := rec.Topic
			if recTopic == "" {
				recTopic = topic
			}
			if !s.canRead(r, recTopic) {
				continue
			}
			numVal, ok := extractNumericValue(string(rec.Payload), field)
			if !ok {
				continue
			}
			raw = append(raw, samplePoint{
				ts:  rec.Timestamp.UnixMilli() / 1000,
				val: numVal,
			})
		}
		values := downsampleByStep(raw, startTime.Unix(), endTime.Unix(), stepSeconds)
		sendRangeResult(w, []any{
			topicMatrixSeries{
				Metric: topicMetricLabels{
					Name:  "topics",
					Group: groupName,
					Topic: topic,
					Field: field,
				},
				Values: values,
			},
		})
		return
	}

	aggFunction := parsed.Function
	if aggFunction == "" {
		aggFunction = "AVG"
	}
	var fields []string
	if field != "" {
		fields = []string{field}
	}
	aggResult, err := archiveStore.GetAggregatedHistory(ctx, []string{topic}, startTime, endTime, intervalMinutes, []string{aggFunction}, fields)
	if err != nil || aggResult == nil {
		if err != nil {
			s.deps.Logger.Warn("prometheus query_range aggregated history error", "group", groupName, "topic", topic, "err", err)
		}
		sendRangeResult(w, []any{})
		return
	}

	colName := stores.AggregatedColumnName(topic, field, aggFunction)
	colIndex := -1
	for i, c := range aggResult.Columns {
		if c == colName {
			colIndex = i
			break
		}
	}
	if colIndex < 0 {
		sendRangeResult(w, []any{})
		return
	}

	values := make([][]any, 0, len(aggResult.Rows))
	for _, row := range aggResult.Rows {
		if len(row) <= colIndex || len(row) == 0 {
			continue
		}
		tsStr, ok := row[0].(string)
		if !ok || tsStr == "" {
			continue
		}
		ts, err := time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			continue
		}
		numVal, ok := toFloat64(row[colIndex])
		if !ok {
			continue
		}
		values = append(values, []any{ts.Unix(), javaDouble(numVal)})
	}

	sendRangeResult(w, []any{
		topicMatrixSeries{
			Metric: topicMetricLabels{
				Name:  "topics",
				Group: groupName,
				Topic: topic,
				Field: field,
			},
			Values: values,
		},
	})
}

func (s *Server) handleQueryRangeMetric(w http.ResponseWriter, r *http.Request, parsed parsedQuery, startTime, endTime time.Time, stepSeconds int64) {
	metricLabel := parsed.Labels["metric"]
	nodeFilter, hasNode := parsed.Labels["node"]
	nodeID := s.nodeID()
	if hasNode && nodeFilter != nodeID {
		sendRangeResult(w, []any{})
		return
	}
	if s.deps.Metrics == nil {
		sendRangeResult(w, []any{})
		return
	}

	from, limit := metricsWindow(startTime, endTime, s.metricsIntervalSeconds())
	rows, err := s.deps.Metrics.GetHistory(r.Context(), stores.MetricBroker, nodeID, from, endTime, limit)
	if err != nil {
		s.deps.Logger.Warn("prometheus query_range metrics history error", "node", nodeID, "err", err)
		sendRangeResult(w, []any{})
		return
	}

	var raw []samplePoint
	for _, row := range rows {
		val, ok := extractHistoryBrokerMetricValue(row.Payload, metricLabel)
		if !ok {
			continue
		}
		raw = append(raw, samplePoint{
			ts:  row.Timestamp.Unix(),
			val: val,
		})
	}
	sort.Slice(raw, func(i, j int) bool { return raw[i].ts < raw[j].ts })

	values := downsampleByStep(raw, startTime.Unix(), endTime.Unix(), stepSeconds)
	sendRangeResult(w, []any{
		brokerMatrixSeries{
			Metric: brokerMetricLabelsObj{
				Name:   "metrics",
				Node:   nodeID,
				Metric: metricLabel,
			},
			Values: values,
		},
	})
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return 0, false
		}
		return n, true
	case float32:
		f := float64(n)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int16:
		return float64(n), true
	case int8:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint64:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint8:
		return float64(n), true
	default:
		return 0, false
	}
}

func sendRangeResult(w http.ResponseWriter, result []any) {
	if result == nil {
		result = []any{}
	}
	writeJSON(w, http.StatusOK, queryResponse{
		Status: "success",
		Data: queryData{
			ResultType: "matrix",
			Result:     result,
		},
	})
}

func sendVectorResult(w http.ResponseWriter, result []any) {
	if result == nil {
		result = []any{}
	}
	writeJSON(w, http.StatusOK, queryResponse{
		Status: "success",
		Data: queryData{
			ResultType: "vector",
			Result:     result,
		},
	})
}

func getParam(r *http.Request, name string) string {
	if vals, ok := r.URL.Query()[name]; ok && len(vals) > 0 {
		return vals[0]
	}
	_ = r.ParseForm()
	if vals, ok := r.PostForm[name]; ok && len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func getMatchSelector(r *http.Request) string {
	if vals, ok := r.URL.Query()["match[]"]; ok && len(vals) > 0 {
		return vals[0]
	}
	_ = r.ParseForm()
	if vals, ok := r.PostForm["match[]"]; ok && len(vals) > 0 {
		return vals[0]
	}
	return ""
}
