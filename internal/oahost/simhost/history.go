package simhost

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"time"

	"monstermq.io/edge/internal/oahost"
)

const maxHistorySamples = 10000

type histSample struct {
	timeMs int64
	value  oahost.Value
}

type readCursor struct {
	pages    [][]byte
	bytes    int
	lastUsed time.Time
}

func (h *Host) recordHistoryLocked(d *dp, el string, v oahost.Value, stime time.Time) {
	if d.unarchived != nil && d.unarchived[el] {
		return
	}
	if d.history == nil {
		d.history = map[string][]histSample{}
	}
	ms := stime.UTC().UnixMilli()
	s := histSample{timeMs: ms, value: cloneValue(v)}
	ring := d.history[el]
	if len(ring) > 0 && ms < ring[len(ring)-1].timeMs {
		i := sort.Search(len(ring), func(i int) bool { return ring[i].timeMs > ms })
		ring = append(ring, histSample{})
		copy(ring[i+1:], ring[i:])
		ring[i] = s
	} else {
		ring = append(ring, s)
	}
	if len(ring) > maxHistorySamples {
		ring = append([]histSample(nil), ring[len(ring)-maxHistorySamples:]...)
	}
	d.history[el] = ring
}

func cloneValue(v oahost.Value) oahost.Value {
	cp := v
	if len(v.Bytes) > 0 {
		cp.Bytes = append([]byte(nil), v.Bytes...)
	}
	if len(v.Dyn) > 0 {
		cp.Dyn = make([]oahost.Value, len(v.Dyn))
		for i, item := range v.Dyn {
			cp.Dyn[i] = cloneValue(item)
		}
	}
	return cp
}

// SetArchived turns historical sample recording and retrieval on or off for a
// DPE. When off, DP_GET_PERIOD returns an empty group (0 items, no error),
// matching a WinCC OA DPE without an _archive config (probe P2).
func (h *Host) SetArchived(address string, on bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, d, el, _, err := h.lookup(address)
	if err != nil {
		return err
	}
	if d.unarchived == nil {
		d.unarchived = map[string]bool{}
	}
	if on {
		delete(d.unarchived, el)
	} else {
		d.unarchived[el] = true
		if d.history != nil {
			delete(d.history, el)
		}
	}
	return nil
}

// SetArchiveError configures a simulated WinCC OA archive error for a DPE in
// DP_GET_PERIOD (!AnswerGroup::wasOk()). An empty errText clears the error.
func (h *Host) SetArchiveError(address, errText string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, d, el, _, err := h.lookup(address)
	if err != nil {
		return err
	}
	if d.archiveErr == nil {
		d.archiveErr = map[string]string{}
	}
	if errText == "" {
		delete(d.archiveErr, el)
	} else {
		d.archiveErr[el] = errText
	}
	return nil
}

// OpenReads returns the number of open read cursors on the host.
func (h *Host) OpenReads() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.reads)
}

// ReadBytes returns the total buffered bytes across open read cursors.
func (h *Host) ReadBytes() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.readBytes
}

func (h *Host) eraseCursorLocked(ref uint64) {
	c, ok := h.reads[ref]
	if !ok {
		return
	}
	h.readBytes -= c.bytes
	if h.readBytes < 0 {
		h.readBytes = 0
	}
	delete(h.reads, ref)
}

func (h *Host) expireReadsLocked() {
	ttl := h.CursorTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	now := time.Now()
	for ref, c := range h.reads {
		if now.Sub(c.lastUsed) > ttl {
			h.eraseCursorLocked(ref)
		}
	}
}

func (h *Host) checkReadBudgetLocked() error {
	maxReads := h.MaxOpenReads
	if maxReads <= 0 {
		maxReads = 64
	}
	maxBytes := h.MaxReadBytes
	if maxBytes <= 0 {
		maxBytes = 64 * 1024 * 1024
	}
	if len(h.reads) >= maxReads || h.readBytes >= maxBytes {
		return fmt.Errorf("%w: too many concurrent archive reads", oahost.ErrOverload)
	}
	return nil
}

func (h *Host) maxPageBytesLocked() int {
	if h.PageBytes > 0 {
		return h.PageBytes
	}
	return oahost.MaxMessage / 2
}

func (h *Host) finishPagesLocked(rowChunks [][]byte, totalCount uint32, truncated bool, resumeMs *int64, out *oahost.Writer) (uint64, error) {
	maxBytes := h.MaxReadBytes
	if maxBytes <= 0 {
		maxBytes = 64 * 1024 * 1024
	}
	var cursorID uint64
	if len(rowChunks) > 1 {
		h.nextReadID++
		cursorID = h.nextReadID
	}
	pages := make([][]byte, len(rowChunks))
	var remBytes int
	for idx, chunk := range rowChunks {
		more := idx+1 < len(rowChunks)
		var flags uint32
		if more {
			flags |= oahost.FlagMore
		}
		if truncated {
			flags |= oahost.FlagTruncated
		}
		var pw oahost.Writer
		pw.U32(oahost.TagFlags, flags)
		if more {
			pw.U64(oahost.TagRef, cursorID)
		}
		if idx == 0 {
			pw.U32(oahost.TagCount, totalCount)
			if resumeMs != nil {
				pw.I64(oahost.TagTime, *resumeMs)
			}
		}
		b := append(pw.Bytes(), chunk...)
		pages[idx] = b
		if idx > 0 {
			remBytes += len(b)
		}
	}
	if h.readBytes+remBytes > maxBytes {
		return 0, fmt.Errorf("%w: archive read byte budget exceeded", oahost.ErrOverload)
	}
	*out = oahost.Writer{}
	for _, f := range mustParseFields(pages[0]) {
		out.Raw(f.Tag, f.Data)
	}
	if cursorID != 0 {
		h.reads[cursorID] = &readCursor{
			pages:    pages[1:],
			bytes:    remBytes,
			lastUsed: time.Now(),
		}
		h.readBytes += remBytes
	}
	return cursorID, nil
}

func mustParseFields(b []byte) []oahost.Field {
	f, _ := oahost.Parse(b)
	return f
}

// QueryLog returns a copy of all dpQuery strings executed by the host.
func (h *Host) QueryLog() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.queryLog...)
}

func isSimProtectedDP(dp string) bool {
	if dp == "" || strings.HasPrefix(dp, "_") {
		return true
	}
	for _, pfx := range []string{"MMQConfigs", "MMQSessions", "MMQRetained", "MMQUsers", "MMQTopic"} {
		if dp == pfx || strings.HasPrefix(dp, pfx+"_") {
			return true
		}
	}
	return false
}

func (h *Host) dpQueryLocked(m oahost.Message, out *oahost.Writer) (uint64, error) {
	qstr := m.String(oahost.TagQuery)
	maxCount, ok := m.U32(oahost.TagCount)
	if qstr == "" || !ok || maxCount < 1 || maxCount > 1_000_000 {
		return 0, fmt.Errorf("%w: invalid dpQuery request", oahost.ErrInvalid)
	}
	if err := h.checkReadBudgetLocked(); err != nil {
		return 0, err
	}
	h.queryLog = append(h.queryLog, qstr)
	sub := queryRe.FindStringSubmatch(qstr)
	if sub == nil {
		return 0, fmt.Errorf("%w: unsupported query", oahost.ErrOA)
	}
	pattern := sub[2]
	sysName := h.local
	if sub[3] != "" {
		sysName = sub[3]
	} else if i := strings.Index(pattern, ":"); i >= 0 && !strings.Contains(pattern[:i], ".") {
		sysName, pattern = pattern[:i], pattern[i+1:]
	}
	s := h.systems[sysName]
	if s == nil || !s.available {
		return 0, fmt.Errorf("%w: system %s", oahost.ErrUnavailable, sysName)
	}
	re, err := oaPatternRe(sysName, pattern)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", oahost.ErrOA, err)
	}
	q := &query{re: re, typ: sub[4], system: sysName}
	for _, a := range attrRe.FindAllStringSubmatch(sub[1], -1) {
		q.attrs = append(q.attrs, a[1])
	}
	if len(q.attrs) == 0 {
		return 0, fmt.Errorf("%w: no attributes selected", oahost.ErrOA)
	}

	onlyArchived := strings.Contains(qstr, "_archive.._class")
	var matches []string
	for name, d := range s.dps {
		if isSimProtectedDP(name) {
			continue
		}
		if q.typ != "" && q.typ != d.typ {
			continue
		}
		for el, k := range h.types[d.typ].Elements {
			if k == ElemStruct {
				continue
			}
			if onlyArchived && d.unarchived != nil && d.unarchived[el] {
				continue
			}
			full := s.name + ":" + name + "." + el
			if re.MatchString(full) {
				matches = append(matches, full)
			}
		}
	}
	sort.Strings(matches)

	truncated := false
	if len(matches) > int(maxCount) {
		matches = matches[:maxCount]
		truncated = true
	}

	var hdr oahost.Writer
	hdr.Value(oahost.TagValue, oahost.Value{Kind: oahost.KindString, Str: ""})
	for _, a := range q.attrs {
		hdr.Value(oahost.TagValue, oahost.Value{Kind: oahost.KindString, Str: ":" + a})
	}
	hdrBytes := hdr.Bytes()

	maxPage := h.maxPageBytesLocked()
	const pageHdrSlack = 64
	var rowChunks [][]byte
	var chunk oahost.Writer
	chunk.Raw(oahost.TagRow, hdrBytes)
	hasRows := false
	var totalBuffered int
	maxBytes := h.MaxReadBytes
	if maxBytes <= 0 {
		maxBytes = 64 * 1024 * 1024
	}

	for _, full := range matches {
		_, d, el, _, err := h.lookup(full)
		if err != nil {
			continue
		}
		var row oahost.Writer
		row.Value(oahost.TagValue, oahost.Value{Kind: oahost.KindString, Str: full})
		for _, a := range q.attrs {
			row.Value(oahost.TagValue, h.readAttr(d, el, a))
		}
		rb := row.Bytes()
		totalBuffered += len(rb) + 5
		if h.readBytes+totalBuffered > maxBytes {
			return 0, fmt.Errorf("%w: archive read byte budget exceeded", oahost.ErrOverload)
		}
		if hasRows && chunk.Len()+len(rb)+5+pageHdrSlack > maxPage {
			rowChunks = append(rowChunks, chunk.Bytes())
			chunk = oahost.Writer{}
			chunk.Raw(oahost.TagRow, hdrBytes)
			hasRows = false
		}
		chunk.Raw(oahost.TagRow, rb)
		hasRows = true
	}
	rowChunks = append(rowChunks, chunk.Bytes())
	return h.finishPagesLocked(rowChunks, uint32(len(matches)), truncated, nil, out)
}

type simSample struct {
	index  uint32
	timeMs int64
	value  oahost.Value
}

func (h *Host) dpGetPeriodLocked(m oahost.Message, out *oahost.Writer) (uint64, error) {
	times := m.All(oahost.TagTime)
	if len(times) != 2 || len(times[0]) != 8 || len(times[1]) != 8 {
		return 0, fmt.Errorf("%w: dpGetPeriod requires start and end times", oahost.ErrInvalid)
	}
	startMs := int64(binary.LittleEndian.Uint64(times[0]))
	endMs := int64(binary.LittleEndian.Uint64(times[1]))
	if startMs < 0 || endMs < startMs {
		return 0, fmt.Errorf("%w: invalid dpGetPeriod time range", oahost.ErrInvalid)
	}
	flags, _ := m.U32(oahost.TagFlags)
	newest := flags&oahost.FlagNewest != 0
	maxAllowed := uint32(50_000)
	if newest {
		maxAllowed = 100_000
	}
	maxCount, ok := m.U32(oahost.TagCount)
	if !ok || maxCount < 1 || maxCount > maxAllowed {
		return 0, fmt.Errorf("%w: invalid dpGetPeriod count", oahost.ErrInvalid)
	}
	names := m.All(oahost.TagName)
	if len(names) == 0 || len(names) > oahost.MaxPeriodNames || (!newest && len(names) != 1) {
		return 0, fmt.Errorf("%w: invalid dpGetPeriod name count", oahost.ErrInvalid)
	}
	if err := h.checkReadBudgetLocked(); err != nil {
		return 0, err
	}

	type target struct {
		d  *dp
		el string
	}
	targets := make([]target, len(names))
	var firstSys string
	for i, raw := range names {
		n := string(raw)
		sys, _, _ := splitAddress(n, h.local)
		if i == 0 {
			firstSys = sys
		} else if sys != firstSys {
			return 0, fmt.Errorf("%w: dpGetPeriod names must belong to one system", oahost.ErrInvalid)
		}
		_, d, el, _, err := h.lookup(n)
		if err != nil {
			return 0, err
		}
		targets[i] = target{d: d, el: el}
	}

	var all []simSample
	failed := map[uint32]string{}
	for i, t := range targets {
		if t.d.archiveErr != nil {
			if errText, hasErr := t.d.archiveErr[t.el]; hasErr {
				failed[uint32(i)] = errText
				continue
			}
		}
		if t.d.unarchived != nil && t.d.unarchived[t.el] {
			continue
		}
		for _, hs := range t.d.history[t.el] {
			if hs.timeMs < startMs || hs.timeMs > endMs {
				continue
			}
			all = append(all, simSample{index: uint32(i), timeMs: hs.timeMs, value: hs.value})
		}
	}

	truncated := false
	var resumeMs *int64
	if newest {
		sort.Slice(all, func(i, j int) bool {
			if all[i].timeMs != all[j].timeMs {
				return all[i].timeMs > all[j].timeMs
			}
			return all[i].index < all[j].index
		})
		if len(all) > int(maxCount) {
			all = all[:maxCount]
			truncated = true
		}
	} else {
		if len(all) > int(maxCount) {
			all = all[:maxCount]
			truncated = true
			lastMs := all[len(all)-1].timeMs
			for len(all) > 0 && all[len(all)-1].timeMs == lastMs {
				all = all[:len(all)-1]
			}
			if len(all) == 0 {
				return 0, fmt.Errorf("%w: all samples in page share one millisecond", oahost.ErrTooLarge)
			}
			resumeMs = &lastMs
		}
	}

	maxPage := h.maxPageBytesLocked()
	const pageHdrSlack = 64
	var rowChunks [][]byte
	var chunk oahost.Writer

	var failedIdxs []int
	for idx := range failed {
		failedIdxs = append(failedIdxs, int(idx))
	}
	sort.Ints(failedIdxs)
	for _, idx := range failedIdxs {
		var frow oahost.Writer
		frow.U32(oahost.TagIndex, uint32(idx))
		frow.String(oahost.TagError, failed[uint32(idx)])
		chunk.Raw(oahost.TagRow, frow.Bytes())
	}

	hasRows := false
	var totalBuffered int
	maxBytes := h.MaxReadBytes
	if maxBytes <= 0 {
		maxBytes = 64 * 1024 * 1024
	}
	for _, s := range all {
		var srow oahost.Writer
		srow.U32(oahost.TagIndex, s.index)
		srow.I64(oahost.TagTime, s.timeMs)
		srow.Value(oahost.TagValue, s.value)
		rb := srow.Bytes()
		totalBuffered += len(rb) + 5
		if h.readBytes+totalBuffered > maxBytes {
			return 0, fmt.Errorf("%w: archive read byte budget exceeded", oahost.ErrOverload)
		}
		if hasRows && chunk.Len()+len(rb)+5+pageHdrSlack > maxPage {
			rowChunks = append(rowChunks, chunk.Bytes())
			chunk = oahost.Writer{}
			hasRows = false
		}
		chunk.Raw(oahost.TagRow, rb)
		hasRows = true
	}
	rowChunks = append(rowChunks, chunk.Bytes())
	return h.finishPagesLocked(rowChunks, uint32(len(all)), truncated, resumeMs, out)
}

func (h *Host) cursorNextLocked(m oahost.Message, out *oahost.Writer) error {
	ref, _ := m.U64(oahost.TagRef)
	c, ok := h.reads[ref]
	if !ok || len(c.pages) == 0 {
		return fmt.Errorf("%w: unknown or expired cursor", oahost.ErrNotFound)
	}
	page := c.pages[0]
	c.pages = c.pages[1:]
	c.bytes -= len(page)
	h.readBytes -= len(page)
	if h.readBytes < 0 {
		h.readBytes = 0
	}
	c.lastUsed = time.Now()
	if len(c.pages) == 0 {
		delete(h.reads, ref)
	}
	*out = oahost.Writer{}
	for _, f := range mustParseFields(page) {
		out.Raw(f.Tag, f.Data)
	}
	return nil
}

func (h *Host) cursorCloseLocked(m oahost.Message) {
	ref, _ := m.U64(oahost.TagRef)
	h.eraseCursorLocked(ref)
}
