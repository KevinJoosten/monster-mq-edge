package nats

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/stores"
)

const (
	natsServerVersion = "2.10.0"
	maxControlLine    = 65536
	maxPayloadBytes   = 1048576
)

type sidState struct {
	sid         string
	subject     string
	topicFilter string
	subID       int
	delivered   int64
	maxMsgs     *int64
	stopCh      chan struct{}
	stopped     atomic.Bool
}

type natsSubDelivery struct {
	state *sidState
	msg   stores.BrokerMessage
}

// Conn represents a single client connection to the NATS server.
type Conn struct {
	server            *Server
	netConn           net.Conn
	clientID          string
	writer            *bufio.Writer
	writeMu           sync.Mutex
	mu                sync.Mutex
	authenticated     bool
	username          string
	verbose           bool
	headersEnabled    bool
	registered        bool
	closed            bool
	sids              map[string]*sidState
	subCh             chan natsSubDelivery
	subWriterOnce     sync.Once
	subStopCh         chan struct{}
	subOverloaded     atomic.Bool
	subDropped        atomic.Int64
	lastWriteDeadline int64
	js                *jetstream
}

func newConn(s *Server, nc net.Conn) *Conn {
	c := &Conn{
		server:    s,
		netConn:   nc,
		clientID:  "nats-" + uuid.NewString(),
		writer:    bufio.NewWriterSize(nc, 65536),
		sids:      make(map[string]*sidState),
		subStopCh: make(chan struct{}),
	}
	c.js = newJetstream(c)
	return c
}

func (c *Conn) serve() {
	defer c.close()

	// Send INFO line immediately on accept (NC:68-79)
	authReq := c.server.deps.UserManagementEnabled
	infoJSON := fmt.Sprintf(`{"server_id":"monstermq","server_name":"MonsterMQ","version":"%s","proto":1,"headers":true,"max_payload":%d,"jetstream":true,"auth_required":%t}`,
		natsServerVersion, maxPayloadBytes, authReq)
	if err := c.writeLine("INFO " + infoJSON); err != nil {
		return
	}

	br := bufio.NewReaderSize(c.netConn, 65536)
	var lineBuf []byte

	for {
		// Read lines terminated by CRLF (\r\n) with F7 maxControlLine limit
		lineBuf = lineBuf[:0]
		for {
			chunk, err := br.ReadSlice('\n')
			if err != nil {
				if errors.Is(err, bufio.ErrBufferFull) {
					lineBuf = append(lineBuf, chunk...)
					if len(lineBuf) > maxControlLine {
						_ = c.writeError("Maximum Control Line Exceeded")
						return
					}
					continue
				}
				// Any other read error or EOF: terminate connection
				return
			}
			lineBuf = append(lineBuf, chunk...)
			if len(lineBuf) > maxControlLine {
				_ = c.writeError("Maximum Control Line Exceeded")
				return
			}
			// Only CRLF terminates the line; a bare LF is part of the line (NC:73-75)
			if len(lineBuf) >= 2 && lineBuf[len(lineBuf)-2] == '\r' {
				lineBuf = lineBuf[:len(lineBuf)-2]
				break
			}
		}

		line := string(lineBuf)
		if len(line) == 0 {
			continue
		}

		spaceIdx := strings.IndexByte(line, ' ')
		var cmd, args string
		if spaceIdx > 0 {
			cmd = strings.ToUpper(line[:spaceIdx])
			args = line[spaceIdx+1:]
		} else {
			cmd = strings.ToUpper(line)
			args = ""
		}

		switch cmd {
		case "CONNECT":
			c.handleConnect(args)
		case "PING":
			_ = c.writeLine("PONG")
		case "PONG":
			// ignore
		case "SUB":
			c.handleSub(args)
		case "UNSUB":
			c.handleUnsub(args)
		case "PUB":
			if !c.handlePub(br, args, false) {
				return
			}
		case "HPUB":
			if !c.handlePub(br, args, true) {
				return
			}
		default:
			_ = c.writeError("Unknown Protocol Operation")
		}
	}
}

func (c *Conn) handleConnect(args string) {
	var req struct {
		Verbose bool   `json:"verbose"`
		Headers bool   `json:"headers"`
		User    string `json:"user"`
		Pass    string `json:"pass"`
	}
	if err := json.Unmarshal([]byte(args), &req); err != nil {
		_ = c.writeError("Invalid CONNECT JSON")
		return
	}

	c.mu.Lock()
	c.verbose = req.Verbose
	c.headersEnabled = req.Headers
	c.mu.Unlock()

	if !c.server.deps.UserManagementEnabled {
		c.mu.Lock()
		c.authenticated = true
		c.mu.Unlock()
		if req.Verbose {
			_ = c.writeLine("+OK")
		}
		c.registerSession()
		return
	}

	user := req.User
	pass := req.Pass
	if c.server.deps.AuthCache != nil && c.server.deps.AuthCache.Validate(user, pass) {
		c.mu.Lock()
		c.authenticated = true
		c.username = user
		c.mu.Unlock()
		if req.Verbose {
			_ = c.writeLine("+OK")
		}
		c.registerSession()
	} else {
		_ = c.writeError("Authorization Violation")
		c.close()
	}
}

func (c *Conn) registerSession() {
	c.mu.Lock()
	if c.registered || c.closed {
		c.mu.Unlock()
		return
	}
	c.registered = true
	clientID := c.clientID
	c.mu.Unlock()

	remoteAddr := c.netConn.RemoteAddr().String()
	localAddr := c.netConn.LocalAddr().String()
	infoJSON := fmt.Sprintf(`{"RemoteAddress":"%s","LocalAddress":"%s","ProtocolVersion":"NATS","SSL":false}`, remoteAddr, localAddr)

	if c.server.deps.Sessions != nil {
		_ = c.server.deps.Sessions.SetClient(context.Background(), stores.SessionInfo{
			ClientID:      clientID,
			NodeID:        c.server.deps.NodeID,
			CleanSession:  true,
			Connected:     true,
			UpdateTime:    time.Now(),
			ClientAddress: remoteAddr,
			Information:   infoJSON,
		})
	}
	if c.server.deps.ProtoSessions != nil {
		c.server.deps.ProtoSessions.Register(clientID, func() {
			c.close()
		})
	}
}

func (c *Conn) checkAuth() bool {
	if !c.server.deps.UserManagementEnabled {
		return true
	}
	c.mu.Lock()
	authed := c.authenticated
	c.mu.Unlock()
	if !authed {
		_ = c.writeError("Authorization Violation")
		return false
	}
	return true
}

func (c *Conn) canPublish(topic string) bool {
	if !c.server.deps.UserManagementEnabled {
		return true
	}
	c.mu.Lock()
	user := c.username
	c.mu.Unlock()
	if c.server.deps.AuthCache == nil {
		return true
	}
	return c.server.deps.AuthCache.Allow(user, topic, true)
}

func (c *Conn) canSubscribe(topic string) bool {
	if !c.server.deps.UserManagementEnabled {
		return true
	}
	c.mu.Lock()
	user := c.username
	c.mu.Unlock()
	if c.server.deps.AuthCache == nil {
		return true
	}
	return c.server.deps.AuthCache.Allow(user, topic, false)
}

func (c *Conn) handleSub(args string) {
	if !c.checkAuth() {
		return
	}

	parts := strings.Fields(args)
	if len(parts) < 2 {
		_ = c.writeError("Invalid SUB")
		return
	}
	subject := parts[0]
	sid := parts[len(parts)-1] // SID is always the last token (NC:215)

	// F2: If SID is already in use, replace its subscription
	c.removeSid(sid)

	topic := SubjectToTopic(subject)
	if !c.canSubscribe(topic) {
		_ = c.writeError(fmt.Sprintf("Permissions Violation for Subscription to \"%s\"", subject))
		return
	}

	// E2: Validate native filter
	if c.server.deps.Bus != nil {
		if err := c.server.deps.Bus.Validate([]string{topic}); err != nil {
			if c.server.deps.Logger != nil {
				c.server.deps.Logger.Warn("Invalid NATS subject", "subject", subject, "topic", topic, "err", err)
			}
			_ = c.writeError("Invalid Subject")
			return
		}
	}

	if c.server.deps.Bus == nil {
		return
	}

	c.ensureSubWriter()
	state := &sidState{
		sid:         sid,
		subject:     subject,
		topicFilter: topic,
		stopCh:      make(chan struct{}),
	}
	subID := c.server.deps.Bus.SubscribeFunc([]string{topic}, func(msg stores.BrokerMessage) {
		if state.stopped.Load() {
			return
		}
		if !dollarTopicMatches(msg.TopicName, []string{state.topicFilter}) {
			return
		}
		select {
		case c.subCh <- natsSubDelivery{state: state, msg: msg}:
		default:
			c.subDropped.Add(1)
			if c.subOverloaded.CompareAndSwap(false, true) && c.server.deps.Logger != nil {
				c.server.deps.Logger.Warn("nats subscriber outbound queue overloaded; dropping messages", "client", c.clientID)
			}
		}
	})
	state.subID = subID

	c.mu.Lock()
	c.sids[sid] = state
	c.mu.Unlock()

	c.js.onSubscribe()
}

func (c *Conn) ensureSubWriter() {
	c.subWriterOnce.Do(func() {
		c.subCh = make(chan natsSubDelivery, 65536)
		go c.subWriterLoop()
	})
}

func (c *Conn) subWriterLoop() {
	for {
		select {
		case <-c.subStopCh:
			return
		case d := <-c.subCh:
			if !c.writeSubBatch(d) {
				c.close()
				return
			}
			if c.subOverloaded.Load() && len(c.subCh) < cap(c.subCh)/4 {
				if c.subOverloaded.CompareAndSwap(true, false) {
					dropped := c.subDropped.Swap(0)
					if c.server.deps.Logger != nil {
						c.server.deps.Logger.Info("nats subscriber outbound queue overload cleared", "client", c.clientID, "dropped", dropped)
					}
				}
			}
		}
	}
}

func (c *Conn) writeSubBatch(first natsSubDelivery) bool {
	c.mu.Lock()
	headersEnabled := c.headersEnabled
	c.mu.Unlock()

	var toRemove []string
	sentCount := 0

	c.writeMu.Lock()
	if c.isClosed() {
		c.writeMu.Unlock()
		return false
	}

	var writeErr error
	writeOne := func(d natsSubDelivery) bool {
		if d.state.stopped.Load() {
			return true
		}
		if !c.canSubscribe(d.msg.TopicName) {
			return true
		}

		c.mu.Lock()
		if d.state.maxMsgs != nil && d.state.delivered >= *d.state.maxMsgs {
			c.mu.Unlock()
			return true
		}
		d.state.delivered++
		reachedMax := d.state.maxMsgs != nil && d.state.delivered >= *d.state.maxMsgs
		c.mu.Unlock()

		subject := TopicToSubject(d.msg.TopicName)
		replyTo := ""
		if d.msg.ResponseTopic != "" {
			replyTo = TopicToSubject(d.msg.ResponseTopic)
		}
		var headerBlock string
		if headersEnabled && len(d.msg.UserProperties) > 0 {
			headerBlock = formatHeaders("", d.msg.UserProperties, nil)
		}

		if writeErr = writeNatsMsgFrame(c.writer, subject, d.state.sid, replyTo, headerBlock, d.msg.Payload); writeErr != nil {
			return false
		}
		sentCount++
		if reachedMax {
			d.state.stopped.Store(true)
			toRemove = append(toRemove, d.state.sid)
		}
		return true
	}

	if writeOne(first) {
		for i := 0; i < 256 && c.writer.Buffered() < 65536; i++ {
			select {
			case d := <-c.subCh:
				if !writeOne(d) {
					i = 256
				}
			default:
				i = 256
			}
		}
	}

	if writeErr == nil && sentCount > 0 {
		writeErr = c.flushLocked()
	}
	c.writeMu.Unlock()

	for _, sid := range toRemove {
		c.removeSid(sid)
	}

	if writeErr != nil {
		return false
	}
	if c.server.deps.Counter != nil && sentCount > 0 {
		for i := 0; i < sentCount; i++ {
			c.server.deps.Counter.IncClientOut(c.clientID)
		}
	}
	return true
}

func writeNatsMsgFrame(w *bufio.Writer, subject, sid, replyTo, headerBlock string, payload []byte) error {
	var scratch [64]byte
	if headerBlock != "" {
		if _, err := w.WriteString("HMSG "); err != nil {
			return err
		}
		if _, err := w.WriteString(subject); err != nil {
			return err
		}
		if err := w.WriteByte(' '); err != nil {
			return err
		}
		if _, err := w.WriteString(sid); err != nil {
			return err
		}
		if err := w.WriteByte(' '); err != nil {
			return err
		}
		if replyTo != "" {
			if _, err := w.WriteString(replyTo); err != nil {
				return err
			}
			if err := w.WriteByte(' '); err != nil {
				return err
			}
		}
		b := strconv.AppendInt(scratch[:0], int64(len(headerBlock)), 10)
		b = append(b, ' ')
		b = strconv.AppendInt(b, int64(len(headerBlock)+len(payload)), 10)
		b = append(b, '\r', '\n')
		if _, err := w.Write(b); err != nil {
			return err
		}
		if _, err := w.WriteString(headerBlock); err != nil {
			return err
		}
	} else {
		if _, err := w.WriteString("MSG "); err != nil {
			return err
		}
		if _, err := w.WriteString(subject); err != nil {
			return err
		}
		if err := w.WriteByte(' '); err != nil {
			return err
		}
		if _, err := w.WriteString(sid); err != nil {
			return err
		}
		if err := w.WriteByte(' '); err != nil {
			return err
		}
		if replyTo != "" {
			if _, err := w.WriteString(replyTo); err != nil {
				return err
			}
			if err := w.WriteByte(' '); err != nil {
				return err
			}
		}
		b := strconv.AppendInt(scratch[:0], int64(len(payload)), 10)
		b = append(b, '\r', '\n')
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	_, err := w.WriteString("\r\n")
	return err
}

func (c *Conn) handleUnsub(args string) {
	if !c.checkAuth() {
		return
	}

	parts := strings.Fields(args)
	if len(parts) == 0 {
		_ = c.writeError("Invalid UNSUB")
		return
	}
	sid := parts[0]
	var maxMsgs *int64
	if len(parts) > 1 {
		if val, err := strconv.ParseInt(parts[1], 10, 64); err == nil && val > 0 {
			maxMsgs = &val
		}
	}

	if maxMsgs == nil {
		c.removeSid(sid)
		return
	}

	// F4: if delivered >= max, remove now, otherwise set maxMsgs
	c.mu.Lock()
	state := c.sids[sid]
	var shouldRemove bool
	if state != nil {
		if state.delivered >= *maxMsgs {
			shouldRemove = true
		} else {
			state.maxMsgs = maxMsgs
		}
	}
	c.mu.Unlock()

	if shouldRemove {
		c.removeSid(sid)
	}
}

func (c *Conn) removeSid(sid string) {
	c.mu.Lock()
	state, exists := c.sids[sid]
	if exists {
		delete(c.sids, sid)
	}
	c.mu.Unlock()

	if !exists || state == nil {
		return
	}

	if state.stopped.CompareAndSwap(false, true) {
		close(state.stopCh)
	}
	if c.server.deps.Bus != nil {
		c.server.deps.Bus.Unsubscribe(state.subID)
	}
	c.js.onUnsubscribe()
}

func (c *Conn) handlePub(br *bufio.Reader, args string, withHeaders bool) bool {
	op := "PUB"
	minParts := 2
	if withHeaders {
		op = "HPUB"
		minParts = 3
	}
	parts := strings.Fields(args)
	if len(parts) < minParts || len(parts) > minParts+1 {
		_ = c.writeError("Invalid " + op)
		return true
	}

	subject := parts[0]
	numBytesStr := parts[len(parts)-1]
	numBytes, err := strconv.Atoi(numBytesStr)
	var headerBytes int
	var errH error
	if withHeaders {
		headerBytesStr := parts[len(parts)-2]
		headerBytes, errH = strconv.Atoi(headerBytesStr)
	}

	if err != nil || errH != nil || numBytes < 0 || headerBytes < 0 || headerBytes > numBytes {
		_ = c.writeError("Invalid " + op + " byte count")
		return true
	}

	// F7: Maximum payload violation closes connection
	if numBytes > maxPayloadBytes {
		_ = c.writeError("Maximum Payload Violation")
		c.close()
		return false
	}

	isJS := isJetStreamSubject(subject)
	var mqttTopic string
	if !isJS {
		mqttTopic = SubjectToTopic(subject)
	}

	discard := false
	if !c.checkAuth() {
		discard = true
	} else if !isJS && !c.canPublish(mqttTopic) {
		_ = c.writeError(fmt.Sprintf("Permissions Violation for Publish to \"%s\"", subject))
		discard = true
	}

	replyTo := ""
	if len(parts) == minParts+1 {
		replyTo = parts[1]
	}

	// Read exact payload bytes + 2 for trailing \r\n (NC:296-297)
	buf := make([]byte, numBytes+2)
	if _, err := io.ReadFull(br, buf); err != nil {
		return false
	}

	if discard {
		return true
	}

	payloadWithHeaders := buf[:numBytes]
	var headers []stores.UserProperty
	if headerBytes > 0 {
		headers = parseHeaders(string(payloadWithHeaders[:headerBytes]))
	}
	payload := payloadWithHeaders[headerBytes:]

	if isJS {
		c.js.handlePublish(subject, replyTo, headers, payload)
		return true
	}
	var respTopic string
	if replyTo != "" {
		respTopic = SubjectToTopic(replyTo)
	}

	msg := stores.BrokerMessage{
		TopicName:      mqttTopic,
		Payload:        payload,
		QoS:            0,
		IsRetain:       false,
		ClientID:       c.clientID,
		ResponseTopic:  respTopic,
		UserProperties: headers,
		Time:           time.Now().UTC(),
	}

	if c.server.deps.Publish != nil {
		if err := c.server.deps.Publish(c.clientID, msg); err != nil {
			// E4: ErrNotAuthorized returns permissions violation
			if isErrNotAuthorized(err) {
				_ = c.writeError(fmt.Sprintf("Permissions Violation for Publish to \"%s\"", subject))
			} else if c.server.deps.Logger != nil {
				c.server.deps.Logger.Warn("NATS publish failed", "err", err, "topic", mqttTopic)
			}
		} else {
			if c.server.deps.Counter != nil {
				c.server.deps.Counter.IncClientIn(c.clientID)
			}
		}
	}
	return true
}

func (c *Conn) sidsForSubject(subject string) []string {
	topic := SubjectToTopic(subject)
	c.mu.Lock()
	defer c.mu.Unlock()
	var result []string
	for sid, state := range c.sids {
		if topicMatchesFilter(topic, state.topicFilter) {
			result = append(result, sid)
		}
	}
	return result
}

func (c *Conn) deliverLocal(subject, replyTo string, headerBlock string, payload []byte) {
	for _, sid := range c.sidsForSubject(subject) {
		c.deliverMsgRaw(subject, sid, replyTo, headerBlock, payload)
	}
}

func (c *Conn) deliverVia(deliverSubject, subject, replyTo string, headerBlock string, payload []byte) {
	for _, sid := range c.sidsForSubject(deliverSubject) {
		c.deliverMsgRaw(subject, sid, replyTo, headerBlock, payload)
		if c.server.deps.Counter != nil {
			c.server.deps.Counter.IncClientOut(c.clientID)
		}
	}
}

func (c *Conn) deliverMsg(subject, sid, replyTo string, userProps []stores.UserProperty, payload []byte) {
	var headerBlock string
	if len(userProps) > 0 {
		headerBlock = formatHeaders("", userProps, nil)
	}
	c.deliverMsgRaw(subject, sid, replyTo, headerBlock, payload)
}

func (c *Conn) deliverMsgRaw(subject, sid, replyTo string, headerBlock string, payload []byte) {
	c.mu.Lock()
	headersEnabled := c.headersEnabled
	c.mu.Unlock()

	var reply string
	if replyTo != "" {
		reply = replyTo + " "
	}

	if headerBlock != "" && headersEnabled {
		headerBytes := []byte(headerBlock)
		line := fmt.Sprintf("HMSG %s %s %s%d %d\r\n", subject, sid, reply, len(headerBytes), len(headerBytes)+len(payload))
		_ = c.writeRaw([][]byte{[]byte(line), headerBytes, payload, []byte("\r\n")})
	} else {
		line := fmt.Sprintf("MSG %s %s %s%d\r\n", subject, sid, reply, len(payload))
		_ = c.writeRaw([][]byte{[]byte(line), payload, []byte("\r\n")})
	}
}

func (c *Conn) writeLine(line string) error {
	return c.writeRaw([][]byte{[]byte(line), []byte("\r\n")})
}

func (c *Conn) writeError(msg string) error {
	return c.writeLine(fmt.Sprintf("-ERR '%s'", msg))
}

func (c *Conn) flushLocked() error {
	now := time.Now().UnixNano()
	if now-c.lastWriteDeadline >= int64(time.Second) {
		c.lastWriteDeadline = now
		_ = c.netConn.SetWriteDeadline(time.Unix(0, now+int64(10*time.Second)))
	}
	return c.writer.Flush()
}

func (c *Conn) writeRaw(chunks [][]byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.isClosed() {
		return net.ErrClosed
	}

	for _, chunk := range chunks {
		if _, err := c.writer.Write(chunk); err != nil {
			return err
		}
	}
	return c.flushLocked()
}

func (c *Conn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Conn) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.subStopCh)
	clientID := c.clientID
	sids := make([]*sidState, 0, len(c.sids))
	for _, s := range c.sids {
		sids = append(sids, s)
	}
	c.sids = nil
	c.mu.Unlock()

	c.js.close()

	for _, s := range sids {
		if s.stopped.CompareAndSwap(false, true) {
			close(s.stopCh)
		}
		if c.server.deps.Bus != nil {
			c.server.deps.Bus.Unsubscribe(s.subID)
		}
	}

	if c.server.deps.ProtoSessions != nil {
		c.server.deps.ProtoSessions.Unregister(clientID)
	}
	if c.server.deps.Sessions != nil {
		_ = c.server.deps.Sessions.DelClient(context.Background(), clientID)
	}
	if c.server.deps.Release != nil {
		c.server.deps.Release(clientID)
	}
	if c.server.deps.Counter != nil {
		c.server.deps.Counter.ForgetClient(clientID)
	}

	_ = c.netConn.Close()
}

func dollarTopicMatches(topic string, filters []string) bool {
	if !strings.HasPrefix(topic, "$") {
		return true
	}
	for _, f := range filters {
		if !strings.HasPrefix(f, "#") && !strings.HasPrefix(f, "+") {
			return true
		}
	}
	return false
}

func isErrNotAuthorized(err error) bool {
	if errors.Is(err, packets.ErrNotAuthorized) {
		return true
	}
	var c packets.Code
	if errors.As(err, &c) && c.Code == packets.ErrNotAuthorized.Code {
		return true
	}
	return false
}

