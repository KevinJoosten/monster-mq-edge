package oahost

import (
	"context"
	"fmt"
	"time"
)

// MaxPeriodNames is the maximum number of DPEs per DP_GET_PERIOD request.
const MaxPeriodNames = 100

// QueryPage is one decoded page of a one-shot DpQuery.
type QueryPage struct {
	Header []Value
	Rows   [][]Value
}

// PeriodRequest describes a DP_GET_PERIOD read.
type PeriodRequest struct {
	Names      []string // Sys:DP.el:_offline.._value, one system, 1..MaxPeriodNames
	Start, End time.Time
	Max        int  // samples: 1..100000 (Newest), 1..50000 (oldest)
	Newest     bool // keep the newest Max; false: the oldest Max plus Resume (one name only)
}

// PeriodSample is one historical value sample returned by DpGetPeriod.
type PeriodSample struct {
	Index int
	Time  time.Time
	Value Value
}

// PeriodResult summarizes a completed DpGetPeriod call.
type PeriodResult struct {
	Total     int
	Truncated bool
	Resume    time.Time      // oldest mode, truncated: start of the next request
	Failed    map[int]string // name index -> WinCC OA error
}

func (a API) readDeadline(ctx context.Context, timeout time.Duration) time.Time {
	if timeout <= 0 && a.C != nil {
		timeout = a.C.Limits().DefaultTimeout
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return deadline
}

func remainingTimeout(ctx context.Context, deadline time.Time) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	rem := time.Until(deadline)
	if rem <= 0 {
		return 0, ErrTimeout
	}
	return rem, nil
}

// CursorClose drops an open read cursor on the host. With ref == 0 it acts as
// a no-op capability probe that returns nil on hosts supporting archive reads.
func (a API) CursorClose(ctx context.Context, ref uint64, timeout time.Duration) error {
	var w Writer
	w.U32(TagOp, OpCursorClose)
	w.U64(TagRef, ref)
	start := time.Now()
	_, err := a.call(ctx, &w, timeout)
	a.logCall("cursorClose", start, err, "ref", ref)
	return err
}

func (a API) closeCursorAsync(ref uint64) {
	if ref == 0 || a.C == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var w Writer
		w.U32(TagOp, OpCursorClose)
		w.U64(TagRef, ref)
		_, _ = a.call(ctx, &w, time.Second)
	}()
}

// DpQuery runs a one-shot dpQuery limited to maxRows data rows and calls page
// for every page in order. timeout bounds the whole read.
func (a API) DpQuery(ctx context.Context, query string, maxRows int, timeout time.Duration,
	page func(QueryPage) error) (truncated bool, err error) {
	if query == "" || maxRows < 1 || maxRows > 1_000_000 {
		return false, ErrInvalid
	}
	start := time.Now()
	deadline := a.readDeadline(ctx, timeout)
	var pages, totalRows int
	defer func() {
		a.logCall("dpQuery", start, err, "query", query, "maxRows", maxRows, "pages", pages, "rows", totalRows, "truncated", truncated)
	}()

	rem, err := remainingTimeout(ctx, deadline)
	if err != nil {
		return false, err
	}
	var w Writer
	w.U32(TagOp, OpDpQuery)
	w.String(TagQuery, query)
	w.U32(TagCount, uint32(maxRows))
	m, err := a.call(ctx, &w, rem)
	if err != nil {
		return false, err
	}

	expectedCount, ok := m.U32(TagCount)
	if !ok {
		if ref, hasRef := m.U64(TagRef); hasRef {
			a.closeCursorAsync(ref)
		}
		return false, ErrMalformed
	}

	var cursor uint64
	for {
		pages++
		flags, _ := m.U32(TagFlags)
		if flags&FlagTruncated != 0 {
			truncated = true
		}
		more := flags&FlagMore != 0
		if more {
			ref, hasRef := m.U64(TagRef)
			if !hasRef || ref == 0 {
				a.closeCursorAsync(cursor)
				return truncated, ErrMalformed
			}
			cursor = ref
		} else {
			cursor = 0
		}

		table, decErr := QueryRows(m)
		if decErr != nil {
			a.closeCursorAsync(cursor)
			return truncated, decErr
		}
		var qp QueryPage
		if len(table) > 0 {
			qp.Header = table[0]
			qp.Rows = table[1:]
		}
		totalRows += len(qp.Rows)
		if a.C != nil && len(qp.Rows) > 0 {
			a.C.recordValuesOut(len(qp.Rows))
		}
		if page != nil {
			if cbErr := page(qp); cbErr != nil {
				a.closeCursorAsync(cursor)
				return truncated, cbErr
			}
		}

		if !more {
			break
		}
		rem, err = remainingTimeout(ctx, deadline)
		if err != nil {
			a.closeCursorAsync(cursor)
			return truncated, err
		}
		var nw Writer
		nw.U32(TagOp, OpCursorNext)
		nw.U64(TagRef, cursor)
		m, err = a.call(ctx, &nw, rem)
		if err != nil {
			a.closeCursorAsync(cursor)
			return truncated, err
		}
	}

	if totalRows != int(expectedCount) {
		return truncated, fmt.Errorf("%w: dpQuery delivered %d rows, TagCount was %d", ErrMalformed, totalRows, expectedCount)
	}
	return truncated, nil
}

// DpGetPeriod reads historical samples for 1..MaxPeriodNames DPEs on one
// system and calls page for every page in order. timeout bounds the whole read.
func (a API) DpGetPeriod(ctx context.Context, req PeriodRequest, timeout time.Duration,
	page func([]PeriodSample) error) (res PeriodResult, err error) {
	if len(req.Names) == 0 || len(req.Names) > MaxPeriodNames {
		return PeriodResult{}, ErrInvalid
	}
	if req.Start.UnixMilli() < 0 || req.End.Before(req.Start) {
		return PeriodResult{}, ErrInvalid
	}
	maxAllowed := 50_000
	if req.Newest {
		maxAllowed = 100_000
	} else if len(req.Names) != 1 {
		return PeriodResult{}, ErrInvalid
	}
	if req.Max < 1 || req.Max > maxAllowed {
		return PeriodResult{}, ErrInvalid
	}

	start := time.Now()
	deadline := a.readDeadline(ctx, timeout)
	var pages int
	defer func() {
		a.logCall("dpGetPeriod", start, err, "count", len(req.Names), "names", nameList(req.Names),
			"newest", req.Newest, "max", req.Max, "pages", pages, "rows", res.Total, "truncated", res.Truncated)
	}()

	rem, err := remainingTimeout(ctx, deadline)
	if err != nil {
		return PeriodResult{}, err
	}
	var w Writer
	w.U32(TagOp, OpDpGetPeriod)
	if req.Newest {
		w.U32(TagFlags, FlagNewest)
	}
	w.I64(TagTime, req.Start.UnixMilli())
	w.I64(TagTime, req.End.UnixMilli())
	w.U32(TagCount, uint32(req.Max))
	for _, n := range req.Names {
		w.String(TagName, n)
	}

	m, err := a.call(ctx, &w, rem)
	if err != nil {
		return PeriodResult{}, err
	}

	expectedCount, ok := m.U32(TagCount)
	if !ok {
		if ref, hasRef := m.U64(TagRef); hasRef {
			a.closeCursorAsync(ref)
		}
		return PeriodResult{}, ErrMalformed
	}
	if resumeMs, hasResume := m.U64(TagTime); hasResume {
		res.Resume = time.UnixMilli(int64(resumeMs)).UTC()
	}

	var cursor uint64
	var delivered int
	for {
		pages++
		flags, _ := m.U32(TagFlags)
		if flags&FlagTruncated != 0 {
			res.Truncated = true
		}
		more := flags&FlagMore != 0
		if more {
			ref, hasRef := m.U64(TagRef)
			if !hasRef || ref == 0 {
				a.closeCursorAsync(cursor)
				return res, ErrMalformed
			}
			cursor = ref
		} else {
			cursor = 0
		}

		samples, failed, decErr := decodePeriodPage(m, len(req.Names), pages == 1)
		if decErr != nil {
			a.closeCursorAsync(cursor)
			return res, decErr
		}
		if len(failed) > 0 {
			if res.Failed == nil {
				res.Failed = make(map[int]string, len(failed))
			}
			for k, v := range failed {
				res.Failed[k] = v
			}
		}
		delivered += len(samples)
		res.Total = delivered
		if a.C != nil && len(samples) > 0 {
			a.C.recordValuesOut(len(samples))
		}
		if page != nil && len(samples) > 0 {
			if cbErr := page(samples); cbErr != nil {
				a.closeCursorAsync(cursor)
				return res, cbErr
			}
		}

		if !more {
			break
		}
		rem, err = remainingTimeout(ctx, deadline)
		if err != nil {
			a.closeCursorAsync(cursor)
			return res, err
		}
		var nw Writer
		nw.U32(TagOp, OpCursorNext)
		nw.U64(TagRef, cursor)
		m, err = a.call(ctx, &nw, rem)
		if err != nil {
			a.closeCursorAsync(cursor)
			return res, err
		}
	}

	if delivered != int(expectedCount) {
		return res, fmt.Errorf("%w: dpGetPeriod delivered %d samples, TagCount was %d", ErrMalformed, delivered, expectedCount)
	}
	if !req.Newest && res.Truncated && res.Resume.IsZero() {
		return res, fmt.Errorf("%w: truncated oldest-mode dpGetPeriod missing resume time", ErrMalformed)
	}
	return res, nil
}

func decodePeriodPage(m Message, nameCount int, firstPage bool) ([]PeriodSample, map[int]string, error) {
	rows := m.All(TagRow)
	var samples []PeriodSample
	var failed map[int]string
	for _, raw := range rows {
		rm, err := ParseMessage(raw)
		if err != nil {
			return nil, nil, err
		}
		idx, ok := rm.U32(TagIndex)
		if !ok || int(idx) >= nameCount {
			return nil, nil, ErrMalformed
		}
		if errField, hasErr := rm.First(TagError); hasErr {
			if !firstPage {
				return nil, nil, ErrMalformed
			}
			if failed == nil {
				failed = make(map[int]string)
			}
			failed[int(idx)] = string(errField)
			continue
		}
		ms, hasTime := rm.U64(TagTime)
		valRaw, hasVal := rm.First(TagValue)
		if !hasTime || !hasVal {
			return nil, nil, ErrMalformed
		}
		val, err := DecodeValue(valRaw)
		if err != nil {
			return nil, nil, err
		}
		samples = append(samples, PeriodSample{
			Index: int(idx),
			Time:  time.UnixMilli(int64(ms)).UTC(),
			Value: val,
		})
	}
	return samples, failed, nil
}
