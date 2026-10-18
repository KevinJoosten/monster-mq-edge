package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"monstermq.io/edge/internal/oahost"
)

func TestWinCCOaArchiveQueryPages(t *testing.T) {
	sim, client := newSim(256)
	defer func() { client.Close(); sim.Close() }()
	sim.PageBytes = 512

	for i := 0; i < 40; i++ {
		dp := fmt.Sprintf("Drive%02d", i)
		if err := sim.CreateDP("System1", dp, "AnalogDrive"); err != nil {
			t.Fatalf("create %s: %v", dp, err)
		}
		if err := sim.Set(fmt.Sprintf("System1:%s.speed:_online.._value", dp), oahost.Value{Kind: oahost.KindFloat, Float: float64(i)}); err != nil {
			t.Fatalf("set %s: %v", dp, err)
		}
	}

	api := oahost.API{C: client}
	var pages int
	var rows [][]oahost.Value
	trunc, err := api.DpQuery(context.Background(),
		`SELECT '_online.._value', '_online.._stime' FROM 'Drive*.speed' WHERE _DPT = "AnalogDrive"`,
		1000, 5*time.Second, func(p oahost.QueryPage) error {
			pages++
			if len(p.Header) != 3 || p.Header[1].Str != ":_online.._value" || p.Header[2].Str != ":_online.._stime" {
				t.Fatalf("page %d bad header: %+v", pages, p.Header)
			}
			rows = append(rows, p.Rows...)
			return nil
		})
	if err != nil {
		t.Fatalf("DpQuery: %v", err)
	}
	if trunc {
		t.Fatalf("unexpected truncated=true")
	}
	if pages < 2 {
		t.Fatalf("expected multiple pages at PageBytes=512, got %d", pages)
	}
	if len(rows) != 40 {
		t.Fatalf("rows = %d, want 40", len(rows))
	}
	if sim.OpenReads() != 0 {
		t.Fatalf("open reads = %d, want 0", sim.OpenReads())
	}
	if sim.Calls(oahost.OpCursorNext) != uint64(pages-1) {
		t.Fatalf("CursorNext calls = %d, want %d", sim.Calls(oahost.OpCursorNext), pages-1)
	}
}

func TestWinCCOaArchiveQueryMaxRows(t *testing.T) {
	sim, client := newSim(256)
	defer func() { client.Close(); sim.Close() }()

	api := oahost.API{C: client}
	var count int
	trunc, err := api.DpQuery(context.Background(),
		`SELECT '_online.._value' FROM '*.**' WHERE _DPT = "AnalogDrive"`,
		5, 5*time.Second, func(p oahost.QueryPage) error {
			count += len(p.Rows)
			return nil
		})
	if err != nil {
		t.Fatalf("DpQuery: %v", err)
	}
	if !trunc {
		t.Fatalf("expected truncated=true")
	}
	if count != 5 {
		t.Fatalf("count = %d, want 5", count)
	}
}

func TestWinCCOaArchiveQueryEmpty(t *testing.T) {
	sim, client := newSim(256)
	defer func() { client.Close(); sim.Close() }()

	api := oahost.API{C: client}
	var pages, count int
	trunc, err := api.DpQuery(context.Background(),
		`SELECT '_online.._value' FROM 'NoSuchPrefix*.**'`,
		100, 5*time.Second, func(p oahost.QueryPage) error {
			pages++
			count += len(p.Rows)
			return nil
		})
	if err != nil {
		t.Fatalf("DpQuery: %v", err)
	}
	if trunc || pages != 1 || count != 0 {
		t.Fatalf("trunc=%v pages=%d count=%d, want false/1/0", trunc, pages, count)
	}
}

func TestWinCCOaArchivePeriodNewest(t *testing.T) {
	sim, client := newSim(256)
	defer func() { client.Close(); sim.Close() }()
	sim.PageBytes = 400

	base := time.Date(2026, 10, 18, 6, 0, 0, 0, time.UTC)
	for i := 1; i <= 25; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		if err := sim.SetTimed("System1:Pump1.speed:_online.._value", oahost.Value{Kind: oahost.KindFloat, Float: float64(i)}, ts); err != nil {
			t.Fatalf("SetTimed %d: %v", i, err)
		}
		if err := sim.SetTimed("System1:Pump101.speed:_online.._value", oahost.Value{Kind: oahost.KindFloat, Float: float64(100 + i)}, ts); err != nil {
			t.Fatalf("SetTimed %d: %v", i, err)
		}
	}

	api := oahost.API{C: client}
	var got []oahost.PeriodSample
	var pages int
	res, err := api.DpGetPeriod(context.Background(), oahost.PeriodRequest{
		Names:  []string{"System1:Pump1.speed:_offline.._value", "System1:Pump101.speed:_offline.._value"},
		Start:  base,
		End:    base.Add(30 * time.Second),
		Max:    20,
		Newest: true,
	}, 5*time.Second, func(s []oahost.PeriodSample) error {
		pages++
		got = append(got, s...)
		return nil
	})
	if err != nil {
		t.Fatalf("DpGetPeriod: %v", err)
	}
	if !res.Truncated || res.Total != 20 || len(got) != 20 {
		t.Fatalf("res=%+v len=%d, want truncated=true total=20", res, len(got))
	}
	if pages < 2 {
		t.Fatalf("expected multiple pages at PageBytes=400, got %d", pages)
	}
	// Newest 20 across 2 DPEs at t=1..25s are t=25s down to t=16s (2 samples per second, index 0 then index 1).
	for i := 0; i < 10; i++ {
		wantTs := base.Add(time.Duration(25-i) * time.Second)
		s0, s1 := got[2*i], got[2*i+1]
		if !s0.Time.Equal(wantTs) || s0.Index != 0 || s0.Value.Float != float64(25-i) {
			t.Fatalf("item %d = %+v, want idx=0 ts=%v val=%v", 2*i, s0, wantTs, 25-i)
		}
		if !s1.Time.Equal(wantTs) || s1.Index != 1 || s1.Value.Float != float64(125-i) {
			t.Fatalf("item %d = %+v, want idx=1 ts=%v val=%v", 2*i+1, s1, wantTs, 125-i)
		}
	}
}

func TestWinCCOaArchivePeriodOldestResume(t *testing.T) {
	sim, client := newSim(256)
	defer func() { client.Close(); sim.Close() }()

	base := time.Date(2026, 10, 18, 7, 0, 0, 0, time.UTC)
	// Write samples at:
	// t+1ms (1), t+2ms (2), t+3ms (3, 4, 5 sharing one ms!), t+4ms (6), t+5ms (7)
	times := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
		3 * time.Millisecond,
		3 * time.Millisecond,
		4 * time.Millisecond,
		5 * time.Millisecond,
	}
	for i, d := range times {
		if err := sim.SetTimed("System1:Pump1.speed:_online.._value", oahost.Value{Kind: oahost.KindFloat, Float: float64(i + 1)}, base.Add(d)); err != nil {
			t.Fatalf("SetTimed %d: %v", i, err)
		}
	}

	api := oahost.API{C: client}
	var collected []float64
	resume := base
	end := base.Add(10 * time.Millisecond)
	var calls int
	for {
		calls++
		if calls > 5 {
			t.Fatalf("too many resume iterations")
		}
		res, err := api.DpGetPeriod(context.Background(), oahost.PeriodRequest{
			Names:  []string{"System1:Pump1.speed:_offline.._value"},
			Start:  resume,
			End:    end,
			Max:    4, // cuts inside the 3 samples at t+3ms on call 1!
			Newest: false,
		}, 5*time.Second, func(s []oahost.PeriodSample) error {
			for _, item := range s {
				collected = append(collected, item.Value.Float)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("call %d DpGetPeriod: %v", calls, err)
		}
		if !res.Truncated {
			break
		}
		resume = res.Resume
	}
	want := []float64{1, 2, 3, 4, 5, 6, 7}
	if len(collected) != len(want) {
		t.Fatalf("collected = %v, want %v", collected, want)
	}
	for i := range want {
		if collected[i] != want[i] {
			t.Fatalf("collected[%d] = %v, want %v (full: %v)", i, collected[i], want[i], collected)
		}
	}
}

func TestWinCCOaArchivePeriodNoArchive(t *testing.T) {
	sim, client := newSim(256)
	defer func() { client.Close(); sim.Close() }()

	base := time.Date(2026, 10, 18, 8, 0, 0, 0, time.UTC)
	if err := sim.SetArchived("System1:Pump101.speed", false); err != nil {
		t.Fatalf("SetArchived: %v", err)
	}
	if err := sim.SetArchiveError("System1:Pump1.running", "archive backend offline"); err != nil {
		t.Fatalf("SetArchiveError: %v", err)
	}
	_ = sim.SetTimed("System1:Pump1.speed:_online.._value", oahost.Value{Kind: oahost.KindFloat, Float: 42}, base.Add(time.Second))
	_ = sim.SetTimed("System1:Pump101.speed:_online.._value", oahost.Value{Kind: oahost.KindFloat, Float: 99}, base.Add(time.Second))

	api := oahost.API{C: client}
	var got []oahost.PeriodSample
	res, err := api.DpGetPeriod(context.Background(), oahost.PeriodRequest{
		Names: []string{
			"System1:Pump1.speed:_offline.._value",
			"System1:Pump101.speed:_offline.._value",
			"System1:Pump1.running:_offline.._value",
		},
		Start:  base,
		End:    base.Add(5 * time.Second),
		Max:    100,
		Newest: true,
	}, 5*time.Second, func(s []oahost.PeriodSample) error {
		got = append(got, s...)
		return nil
	})
	if err != nil {
		t.Fatalf("DpGetPeriod: %v", err)
	}
	if len(got) != 1 || got[0].Index != 0 || got[0].Value.Float != 42 {
		t.Fatalf("got samples = %+v, want only index 0 (42)", got)
	}
	if res.Failed[2] != "archive backend offline" {
		t.Fatalf("failed = %+v, want index 2 failed", res.Failed)
	}
	if _, ok := res.Failed[1]; ok {
		t.Fatalf("unarchived DPE (P2) must return empty group without error, got %+v", res.Failed)
	}
}

func TestWinCCOaArchivePeriodInvalid(t *testing.T) {
	sim, client := newSim(256)
	defer func() { client.Close(); sim.Close() }()
	api := oahost.API{C: client}
	now := time.Now().UTC()

	// 0 names
	if _, err := api.DpGetPeriod(context.Background(), oahost.PeriodRequest{Start: now, End: now, Max: 10, Newest: true}, time.Second, nil); !errors.Is(err, oahost.ErrInvalid) {
		t.Fatalf("0 names: got %v, want ErrInvalid", err)
	}
	// 101 names
	many := make([]string, 101)
	for i := range many {
		many[i] = "System1:Pump1.speed:_offline.._value"
	}
	if _, err := api.DpGetPeriod(context.Background(), oahost.PeriodRequest{Names: many, Start: now, End: now, Max: 10, Newest: true}, time.Second, nil); !errors.Is(err, oahost.ErrInvalid) {
		t.Fatalf("101 names: got %v, want ErrInvalid", err)
	}
	// Two systems in one request
	if _, err := api.DpGetPeriod(context.Background(), oahost.PeriodRequest{
		Names:  []string{"System1:Pump1.speed:_offline.._value", "SubstationA:Feeder1.voltage:_offline.._value"},
		Start:  now,
		End:    now,
		Max:    10,
		Newest: true,
	}, time.Second, nil); !errors.Is(err, oahost.ErrInvalid) {
		t.Fatalf("mixed systems: got %v, want ErrInvalid", err)
	}
	// Oldest mode with 2 names
	if _, err := api.DpGetPeriod(context.Background(), oahost.PeriodRequest{
		Names:  []string{"System1:Pump1.speed:_offline.._value", "System1:Pump101.speed:_offline.._value"},
		Start:  now,
		End:    now,
		Max:    10,
		Newest: false,
	}, time.Second, nil); !errors.Is(err, oahost.ErrInvalid) {
		t.Fatalf("oldest mode with 2 names: got %v, want ErrInvalid", err)
	}
	// Raw request missing two TagTimes
	var w oahost.Writer
	w.U32(oahost.TagOp, oahost.OpDpGetPeriod)
	w.U32(oahost.TagFlags, oahost.FlagNewest)
	w.U32(oahost.TagCount, 10)
	w.String(oahost.TagName, "System1:Pump1.speed:_offline.._value")
	if _, err := client.Call(context.Background(), w.Bytes(), time.Second); !errors.Is(err, oahost.ErrInvalid) {
		t.Fatalf("missing TagTime: got %v, want ErrInvalid", err)
	}
}

func TestWinCCOaArchiveTimeoutAbort(t *testing.T) {
	sim, client := newSim(256)
	defer func() { client.Close(); sim.Close() }()
	sim.PageBytes = 256
	for i := 0; i < 20; i++ {
		dp := fmt.Sprintf("Slow%02d", i)
		_ = sim.CreateDP("System1", dp, "AnalogDrive")
	}
	sim.ReadDelay = 250 * time.Millisecond

	api := oahost.API{C: client}
	_, err := api.DpQuery(context.Background(),
		`SELECT '_online.._value' FROM 'Slow*.speed'`,
		1000, 60*time.Millisecond, func(oahost.QueryPage) error { return nil })
	if !errors.Is(err, oahost.ErrTimeout) {
		t.Fatalf("DpQuery timeout: got %v, want ErrTimeout", err)
	}
	// Wait for ReadDelay to finish and verify no cursor is leaked.
	time.Sleep(250 * time.Millisecond)
	if sim.OpenReads() != 0 {
		t.Fatalf("open reads after timeout = %d, want 0", sim.OpenReads())
	}
}

func TestWinCCOaArchiveCursorClosedOnStop(t *testing.T) {
	sim, client := newSim(256)
	defer func() { client.Close(); sim.Close() }()
	sim.PageBytes = 400
	for i := 0; i < 30; i++ {
		dp := fmt.Sprintf("Stop%02d", i)
		_ = sim.CreateDP("System1", dp, "AnalogDrive")
	}

	api := oahost.API{C: client}
	stopErr := errors.New("stop after first page")
	_, err := api.DpQuery(context.Background(),
		`SELECT '_online.._value' FROM 'Stop*.speed'`,
		1000, 5*time.Second, func(oahost.QueryPage) error {
			return stopErr
		})
	if !errors.Is(err, stopErr) {
		t.Fatalf("DpQuery: got %v, want %v", err, stopErr)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sim.OpenReads() == 0 && sim.Calls(oahost.OpCursorClose) >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("open reads = %d, CursorClose calls = %d; want 0 and >=1", sim.OpenReads(), sim.Calls(oahost.OpCursorClose))
}
