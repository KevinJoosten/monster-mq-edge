package archive

import (
	"fmt"
	"sort"
	"time"

	"monstermq.io/edge/internal/stores"
)

// MergeAggregated combines per-group AggregatedResult slices into one result
// whose columns follow topics x fields x functions in request order and whose
// rows are the sorted union of bucket timestamps across parts.
func MergeAggregated(
	topics []string,
	fields []string,
	functions []string,
	intervalMinutes int,
	startTime, endTime time.Time,
	parts []*stores.AggregatedResult,
) *stores.AggregatedResult {
	if intervalMinutes <= 0 {
		intervalMinutes = 5
	}
	effectiveFields := fields
	if len(effectiveFields) == 0 {
		effectiveFields = []string{""}
	}
	effectiveFns := functions
	if len(effectiveFns) == 0 {
		effectiveFns = []string{"AVG"}
	}

	numCols := len(topics) * len(effectiveFields) * len(effectiveFns)
	columns := make([]string, 1, 1+numCols)
	columns[0] = "timestamp"
	colIndex := make(map[string]int, numCols)
	for _, t := range topics {
		for _, field := range effectiveFields {
			for _, fn := range effectiveFns {
				col := stores.AggregatedColumnName(t, field, fn)
				if _, exists := colIndex[col]; !exists {
					colIndex[col] = len(columns)
				}
				columns = append(columns, col)
			}
		}
	}

	bucketRows := map[string][]any{}
	for _, part := range parts {
		if part == nil {
			continue
		}
		for _, r := range part.Rows {
			if len(r) == 0 {
				continue
			}
			bKey, ok := r[0].(string)
			if !ok {
				continue
			}
			mergedRow, exists := bucketRows[bKey]
			if !exists {
				mergedRow = make([]any, len(columns))
				mergedRow[0] = bKey
				bucketRows[bKey] = mergedRow
			}
			for cIdx := 1; cIdx < len(part.Columns) && cIdx < len(r); cIdx++ {
				colName := part.Columns[cIdx]
				for idx := 1; idx < len(columns); idx++ {
					if columns[idx] == colName {
						mergedRow[idx] = r[cIdx]
					}
				}
			}
		}
	}

	bucketKeys := make([]string, 0, len(bucketRows))
	for k := range bucketRows {
		bucketKeys = append(bucketKeys, k)
	}
	sort.Strings(bucketKeys)

	rows := make([][]any, 0, len(bucketKeys))
	for _, k := range bucketKeys {
		rows = append(rows, bucketRows[k])
	}

	return &stores.AggregatedResult{
		Columns:    columns,
		Rows:       rows,
		Interval:   fmt.Sprintf("%d", intervalMinutes),
		StartTime:  startTime.UTC().Format(time.RFC3339),
		EndTime:    endTime.UTC().Format(time.RFC3339),
		TopicCount: len(topics),
		RowCount:   len(rows),
	}
}
