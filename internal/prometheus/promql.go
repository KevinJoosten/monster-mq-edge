package prometheus

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type parsedQuery struct {
	Metric   string
	Labels   map[string]string
	Function string // "AVG", "MIN", "MAX", "COUNT" or ""
	Window   string
}

var (
	funcPattern         = regexp.MustCompile(`(?i)^(avg|min|max|count)_over_time\((.+)\[(\w+)\]\s*\)$`)
	metricPattern       = regexp.MustCompile(`^(\w+)\s*(\{[^}]*\})?`)
	labelPattern        = regexp.MustCompile(`(\w+)="([^"]*?)"`)
	matchLabelPattern   = regexp.MustCompile(`(\w+)=~?"([^"]*?)"`)
	leadingMetricRegexp = regexp.MustCompile(`^(\w+)`)
)

func parsePromQL(query string) parsedQuery {
	q := strings.TrimSpace(query)

	var fn, window string
	inner := q
	if m := funcPattern.FindStringSubmatch(q); m != nil {
		fn = strings.ToUpper(m[1])
		inner = strings.TrimSpace(m[2])
		window = m[3]
	}

	var metric, labelsStr string
	if ml := metricPattern.FindStringSubmatch(inner); ml != nil {
		metric = ml[1]
		labelsStr = ml[2]
	}

	labels := map[string]string{}
	if labelsStr != "" {
		for _, m := range labelPattern.FindAllStringSubmatch(labelsStr, -1) {
			labels[m[1]] = m[2]
		}
	}

	return parsedQuery{
		Metric:   metric,
		Labels:   labels,
		Function: fn,
		Window:   window,
	}
}

func extractLabelMatchers(selector string) map[string]string {
	labels := map[string]string{}
	for _, m := range matchLabelPattern.FindAllStringSubmatch(selector, -1) {
		labels[m[1]] = m[2]
	}
	return labels
}

func seriesMetricName(selector string, matchLabels map[string]string) string {
	if name, ok := matchLabels["__name__"]; ok && name != "" {
		return name
	}
	if m := leadingMetricRegexp.FindStringSubmatch(strings.TrimSpace(selector)); m != nil {
		return m[1]
	}
	return ""
}

func parsePrometheusTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	if f, ok := parseJavaDouble(s); ok {
		ms := int64(f * 1000)
		return time.UnixMilli(ms).UTC(), true
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}

func parseStep(s string) int64 {
	if s == "" {
		return 60
	}
	if f, ok := parseJavaDouble(s); ok {
		return int64(f)
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(v)
	}
	return 60
}

func snapToValidIntervalMinutes(stepSeconds int64) int {
	minutes := stepSeconds / 60
	if minutes < 1 {
		minutes = 1
	}
	switch {
	case minutes < 5:
		return 1
	case minutes < 15:
		return 5
	case minutes < 60:
		return 15
	case minutes < 1440:
		return 60
	default:
		return 1440
	}
}

type samplePoint struct {
	ts  int64
	val float64
}

func downsampleByStep(raw []samplePoint, startEpoch, endEpoch, stepSeconds int64) [][]any {
	step := stepSeconds
	if step < 1 {
		step = 1
	}
	type bucketAcc struct {
		sum   float64
		count int
	}
	buckets := map[int64]*bucketAcc{}
	var keys []int64
	for _, pt := range raw {
		b := (pt.ts - startEpoch) / step
		acc, ok := buckets[b]
		if !ok {
			acc = &bucketAcc{}
			buckets[b] = acc
			keys = append(keys, b)
		}
		acc.sum += pt.val
		acc.count++
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([][]any, 0, len(keys))
	for _, b := range keys {
		ts := startEpoch + b*step
		if ts <= endEpoch {
			acc := buckets[b]
			avg := acc.sum / float64(acc.count)
			out = append(out, []any{ts, javaDouble(avg)})
		}
	}
	return out
}

func metricsWindow(start, end time.Time, intervalSeconds int) (from time.Time, limit int) {
	interval := int64(intervalSeconds)
	if interval < 1 {
		interval = 1
	}
	const maxPoints = 50000
	fromEpoch := start.Unix()
	endEpoch := end.Unix()
	if endEpoch-fromEpoch > maxPoints*interval {
		fromEpoch = endEpoch - maxPoints*interval
		from = time.Unix(fromEpoch, 0).UTC()
	} else {
		from = start
	}
	span := endEpoch - fromEpoch
	if span < 0 {
		span = 0
	}
	limit = int(span/interval) + 2
	return from, limit
}
