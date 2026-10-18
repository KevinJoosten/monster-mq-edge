package oastore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/winccoanative"
)

type aggAccum struct {
	count int64
	sum   float64
	min   float64
	max   float64
}

func (ac *aggAccum) add(v float64) {
	if ac.count == 0 {
		ac.min = v
		ac.max = v
	} else {
		if v < ac.min {
			ac.min = v
		}
		if v > ac.max {
			ac.max = v
		}
	}
	ac.count++
	ac.sum += v
}

func (a *MessageArchive) GetAggregatedHistory(
	ctx context.Context,
	topics []string,
	startTime, endTime time.Time,
	intervalMinutes int,
	functions []string,
	fields []string,
) (*stores.AggregatedResult, error) {
	if len(topics) == 0 {
		return &stores.AggregatedResult{
			Columns:    []string{"timestamp"},
			Rows:       [][]any{},
			Interval:   fmt.Sprintf("%d", intervalMinutes),
			StartTime:  startTime.UTC().Format(time.RFC3339),
			EndTime:    endTime.UTC().Format(time.RFC3339),
			TopicCount: 0,
			RowCount:   0,
		}, nil
	}

	if intervalMinutes <= 0 {
		intervalMinutes = 5
	}
	if len(functions) == 0 {
		functions = []string{"AVG"}
	}
	effectiveFields := fields
	if len(effectiveFields) == 0 {
		effectiveFields = []string{""}
	}

	interval := time.Duration(intervalMinutes) * time.Minute
	startUTC := startTime.UTC()
	endUTC := endTime.UTC()

	buckets := int64(0)
	if !endUTC.Before(startUTC) {
		buckets = int64(endUTC.Sub(startUTC)/interval) + 1
	}
	numCols := int64(len(topics) * len(effectiveFields) * len(functions))
	if buckets*numCols > maxAggregationCells {
		return nil, fmt.Errorf(
			"WinCC OA aggregation would produce %d buckets x %d columns (limit %d); narrow the range or use a longer interval",
			buckets, numCols, maxAggregationCells,
		)
	}

	columns := make([]string, 1, 1+int(numCols))
	columns[0] = "timestamp"
	for _, t := range topics {
		for _, field := range effectiveFields {
			for _, fn := range functions {
				columns = append(columns, stores.AggregatedColumnName(t, field, fn))
			}
		}
	}

	aggCtx, cancel := context.WithTimeout(ctx, aggTimeout)
	defer cancel()

	numPairs := len(topics) * len(effectiveFields)
	bucketData := map[string][]aggAccum{}

	if !endUTC.Before(startUTC) {
		for tIdx, topicName := range topics {
			f, _ := winccoanative.SplitShared(topicName)
			if winccoanative.HasWildcard(f) {
				continue
			}
			rt, err := a.r.resolveExact(aggCtx, f, true)
			if err != nil {
				return nil, err
			}
			if rt == nil {
				continue
			}
			dpe := rt.target.DPE(rt.system) + ":_offline.._value"
			resume := startUTC
			for !resume.After(endUTC) {
				res, err := a.r.opts.API.DpGetPeriod(aggCtx, oahost.PeriodRequest{
					Names:  []string{dpe},
					Start:  resume,
					End:    endUTC,
					Max:    a.r.aggPageSamples(),
					Newest: false,
				}, aggTimeout, func(samples []oahost.PeriodSample) error {
					for _, s := range samples {
						if s.Time.Before(startUTC) || s.Time.After(endUTC) {
							continue
						}
						bKey := s.Time.UTC().Truncate(interval).Format("2006-01-02T15:04:00Z")
						accs, ok := bucketData[bKey]
						if !ok {
							accs = make([]aggAccum, numPairs)
							bucketData[bKey] = accs
						}
						for fIdx, field := range effectiveFields {
							if val, ok := extractSampleNumber(s.Value, field); ok {
								accs[tIdx*len(effectiveFields)+fIdx].add(val)
							}
						}
					}
					return nil
				})
				if err != nil {
					return nil, err
				}
				if reason, failed := res.Failed[0]; failed {
					a.r.opts.Logger.Debug("winccoa aggregation DPE skipped", "dpe", dpe, "err", reason)
					break
				}
				if !res.Truncated || res.Resume.IsZero() {
					break
				}
				resume = res.Resume
			}
		}
	}

	bucketKeys := make([]string, 0, len(bucketData))
	for k := range bucketData {
		bucketKeys = append(bucketKeys, k)
	}
	sort.Strings(bucketKeys)

	rows := make([][]any, 0, len(bucketKeys))
	for _, bKey := range bucketKeys {
		accs := bucketData[bKey]
		row := make([]any, 1, 1+int(numCols))
		row[0] = bKey
		for tIdx := range topics {
			for fIdx := range effectiveFields {
				ac := accs[tIdx*len(effectiveFields)+fIdx]
				for _, fn := range functions {
					switch strings.ToUpper(fn) {
					case "COUNT":
						row = append(row, float64(ac.count))
					case "MIN":
						if ac.count == 0 {
							row = append(row, nil)
						} else {
							row = append(row, ac.min)
						}
					case "MAX":
						if ac.count == 0 {
							row = append(row, nil)
						} else {
							row = append(row, ac.max)
						}
					case "SUM":
						if ac.count == 0 {
							row = append(row, nil)
						} else {
							row = append(row, ac.sum)
						}
					default: // AVG
						if ac.count == 0 {
							row = append(row, nil)
						} else {
							row = append(row, ac.sum/float64(ac.count))
						}
					}
				}
			}
		}
		rows = append(rows, row)
	}

	return &stores.AggregatedResult{
		Columns:    columns,
		Rows:       rows,
		Interval:   fmt.Sprintf("%d", intervalMinutes),
		StartTime:  startUTC.Format(time.RFC3339),
		EndTime:    endUTC.Format(time.RFC3339),
		TopicCount: len(topics),
		RowCount:   len(rows),
	}, nil
}

func extractSampleNumber(v oahost.Value, field string) (float64, bool) {
	if field == "" || field == "value" {
		switch v.Kind {
		case oahost.KindFloat:
			if math.IsNaN(v.Float) || math.IsInf(v.Float, 0) {
				return 0, false
			}
			return v.Float, true
		case oahost.KindInt:
			return float64(v.Int), true
		case oahost.KindUint, oahost.KindBit32:
			return float64(v.Uint), true
		case oahost.KindBool:
			if v.Bool {
				return 1, true
			}
			return 0, true
		case oahost.KindString:
			return extractJSONNumber(v.Str, field == "")
		default:
			return 0, false
		}
	}
	if v.Kind != oahost.KindString {
		return 0, false
	}
	return extractJSONPathNumber(v.Str, strings.Split(field, "."))
}

func extractJSONNumber(raw string, allowBare bool) (float64, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, false
	}
	if trimmed[0] == '{' {
		return extractJSONPathNumber(trimmed, []string{"value"})
	}
	if !allowBare {
		// When field == "value" and string is a bare JSON number, also allow it
		// per §2.6 step 6 ("For field \"\" or \"value\": ... A String gives its JSON number, or $.value of a JSON object").
	}
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.UseNumber()
	var val any
	if err := dec.Decode(&val); err != nil {
		return 0, false
	}
	return toFiniteFloat(val)
}

func extractJSONPathNumber(raw string, path []string) (float64, bool) {
	trimmed := bytes.TrimSpace([]byte(raw))
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return 0, false
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	var cur any
	if err := dec.Decode(&cur); err != nil {
		return 0, false
	}
	for _, seg := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return 0, false
		}
		cur, ok = m[seg]
		if !ok {
			return 0, false
		}
	}
	return toFiniteFloat(cur)
}

func toFiniteFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		f, err := strconv.ParseFloat(x.String(), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, false
		}
		return x, true
	}
	return 0, false
}
