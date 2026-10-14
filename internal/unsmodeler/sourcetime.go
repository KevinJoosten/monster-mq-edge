package unsmodeler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Source time formats of a topic binding (timeFormat).
const (
	TimeISO8601 = "iso8601"
	TimeEpochMs = "epochMs"
	TimeEpochS  = "epochS"
)

// Plausible source times: a time outside this window is a wrong format
// (seconds read as milliseconds or the other way round) or a broken device
// clock; the flow then uses the receive time.
var (
	minSourceTime     = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	maxSourceTimeSkew = 24 * time.Hour
)

var isoLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
}

// sourceTime reads the source time of a payload at timePath.
func sourceTime(payload []byte, timePath, format string) (time.Time, error) {
	raw, err := extractJSONPath(payload, timePath)
	if err != nil {
		return time.Time{}, err
	}
	return parseSourceTime(raw, format)
}

// parseSourceTime converts a JSON value to a time in the given format.
func parseSourceTime(raw json.RawMessage, format string) (time.Time, error) {
	raw = bytes.TrimSpace(raw)
	var text string
	if len(raw) > 0 && raw[0] == '"' {
		if err := json.Unmarshal(raw, &text); err != nil {
			return time.Time{}, err
		}
		text = strings.TrimSpace(text)
	} else {
		text = string(raw)
	}
	switch format {
	case TimeISO8601:
		for _, l := range isoLayouts {
			if t, err := time.Parse(l, text); err == nil {
				return t.UTC(), nil
			}
		}
		return time.Time{}, fmt.Errorf("%s is not an ISO 8601 time", short(text))
	case TimeEpochMs, TimeEpochS:
		f, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return time.Time{}, fmt.Errorf("%s is not a number", short(text))
		}
		if format == TimeEpochS {
			f *= 1000
		}
		if f < 0 || f > 1e15 {
			return time.Time{}, fmt.Errorf("%s is out of range for %s", short(text), format)
		}
		return time.UnixMilli(int64(math.Round(f))).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unknown time format %q", format)
}

// checkSourceTime rejects a time outside the plausible window around now.
func checkSourceTime(t, now time.Time) error {
	if t.Before(minSourceTime) {
		return fmt.Errorf("source time %s is before 2000", t.Format(time.RFC3339))
	}
	if t.After(now.Add(maxSourceTimeSkew)) {
		return fmt.Errorf("source time %s is more than a day in the future", t.Format(time.RFC3339))
	}
	return nil
}

func short(s string) string {
	if len(s) > 40 {
		return strconv.Quote(s[:40] + "…")
	}
	return strconv.Quote(s)
}
