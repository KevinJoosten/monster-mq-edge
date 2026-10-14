package unsmodeler

import (
	"strings"
	"testing"
	"time"
)

func TestSourceTime(t *testing.T) {
	want := time.Date(2026, 10, 14, 9, 12, 30, 250e6, time.UTC)
	ok := []struct {
		payload, path, format string
	}{
		{`{"TimeMS": 1791969150250, "Value": 10.0}`, "$.TimeMS", TimeEpochMs},
		{`{"ts": "1791969150250"}`, "$.ts", TimeEpochMs},
		{`{"ts": 1791969150.25}`, "$.ts", TimeEpochS},
		{`{"t": {"at": "2026-10-14T09:12:30.250Z"}}`, "$.t.at", TimeISO8601},
		{`{"t": "2026-10-14T11:12:30.25+02:00"}`, "$.t", TimeISO8601},
		{`{"t": "2026-10-14T09:12:30.250"}`, "$.t", TimeISO8601},
		{`{"t": "2026-10-14 09:12:30.250"}`, "$.t", TimeISO8601},
	}
	for _, c := range ok {
		got, err := sourceTime([]byte(c.payload), c.path, c.format)
		if err != nil || !got.Equal(want) {
			t.Errorf("%s %s %s: %v %v", c.payload, c.path, c.format, got, err)
		}
	}
	bad := []struct {
		payload, path, format, text string
	}{
		{`{"ts": "yesterday"}`, "$.ts", TimeISO8601, "not an ISO 8601"},
		{`{"ts": "2026-10-14T09:12:30Z"}`, "$.ts", TimeEpochMs, "not a number"},
		{`{"ts": -5}`, "$.ts", TimeEpochS, "out of range"},
		{`{"ts": true}`, "$.ts", TimeEpochMs, "not a number"},
		{`{"v": 1}`, "$.ts", TimeEpochMs, ""},
		{`{"ts": 1}`, "$.ts", "rfc", "unknown time format"},
	}
	for _, c := range bad {
		if _, err := sourceTime([]byte(c.payload), c.path, c.format); err == nil || !strings.Contains(err.Error(), c.text) {
			t.Errorf("%s %s %s: %v", c.payload, c.path, c.format, err)
		}
	}
	now := time.Now()
	if checkSourceTime(now, now) != nil || checkSourceTime(time.UnixMilli(1791964196), now) == nil || checkSourceTime(now.Add(25*time.Hour), now) == nil {
		t.Fatal("plausibility window")
	}
}
