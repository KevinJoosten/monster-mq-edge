package nats

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/stores"
)

type Consumer struct {
	name          string
	bucket        string
	group         *archive.Group
	deliverSubj   string
	filters       []string
	headersOnly   bool
	config        map[string]any
	created       time.Time
	heartbeatMs   int64
	busSubID      int
	deliveredSeq  int64
	lastStreamSeq int64
	ready         bool
	bound         bool
	loaded        bool
	snapshot      []stores.BrokerMessage
	pendingLive   []stores.BrokerMessage
	waitingInfo   []string
	ticker        *time.Ticker
	stopCh        chan struct{}
	stopped       atomic.Bool
	mu            sync.Mutex
}

func (c *Consumer) stop() {
	if c.stopped.CompareAndSwap(false, true) {
		close(c.stopCh)
	}
	c.mu.Lock()
	if c.ticker != nil {
		c.ticker.Stop()
		c.ticker = nil
	}
	c.mu.Unlock()
}

func (j *jetstream) consumerCreate(path string, request map[string]any, replyTo string) {
	respType := "io.nats.jetstream.api.v1.consumer_create_response"
	tokens := strings.SplitN(path, ".", 3)
	stream, _ := request["stream_name"].(string)
	if stream == "" {
		stream = tokens[0]
	}
	bucket := strings.TrimPrefix(stream, streamPrefix)
	group := j.buckets()[bucket]
	if group == nil {
		j.reply(replyTo, j.streamNotFound(respType))
		return
	}

	config, _ := request["config"].(map[string]any)
	if config == nil {
		config = make(map[string]any)
	}

	deliverSubject, _ := config["deliver_subject"].(string)
	if deliverSubject == "" {
		j.reply(replyTo, j.errorJson(&respType, 400, 10025, "pull consumers are not supported by MonsterMQ, use a push consumer"))
		return
	}

	prefix := fmt.Sprintf("$KV.%s.", bucket)
	var filterSubjects []string
	if fs, ok := config["filter_subject"].(string); ok && fs != "" {
		filterSubjects = append(filterSubjects, fs)
	}
	if fss, ok := config["filter_subjects"].([]any); ok {
		for _, item := range fss {
			if s, ok := item.(string); ok && s != "" {
				filterSubjects = append(filterSubjects, s)
			}
		}
	}
	if len(filterSubjects) == 0 {
		filterSubjects = append(filterSubjects, prefix+">")
	}

	var filters []string
	for _, fs := range filterSubjects {
		if !strings.HasPrefix(fs, prefix) || len(fs) == len(prefix) {
			j.reply(replyTo, j.errorJson(&respType, 400, 10093, fmt.Sprintf("filter subject [%s] is not part of bucket [%s]", fs, bucket)))
			return
		}
		topicFilter := SubjectToTopic(strings.TrimPrefix(fs, prefix))

		// E2: bus.Validate error
		if j.conn.server.deps.Bus != nil {
			if err := j.conn.server.deps.Bus.Validate([]string{topicFilter}); err != nil {
				j.reply(replyTo, j.errorJson(&respType, 400, 10093, fmt.Sprintf("filter subject [%s] is not valid: %s", fs, err.Error())))
				return
			}
		}

		if !j.conn.canSubscribe(topicFilter) {
			j.reply(replyTo, j.errorJson(&respType, 403, 10101, fmt.Sprintf("Permissions Violation for Subscription to \"%s\"", fs)))
			return
		}
		filters = append(filters, topicFilter)
	}

	name, _ := config["name"].(string)
	if name == "" {
		name, _ = config["durable_name"].(string)
	}
	if name == "" && len(tokens) > 1 && tokens[1] != "" {
		name = tokens[1]
	}
	if name == "" {
		rb := make([]byte, 4)
		_, _ = rand.Read(rb)
		name = hex.EncodeToString(rb)
	}

	j.mu.Lock()
	if existing, ok := j.consumers[name]; ok {
		j.mu.Unlock()
		j.removeConsumer(existing)
		j.mu.Lock()
	}

	deliverPolicy, _ := config["deliver_policy"].(string)
	if deliverPolicy == "" {
		deliverPolicy = "all"
	}

	var optStartSeq int64
	if seqVal, ok := config["opt_start_seq"].(float64); ok {
		optStartSeq = int64(seqVal)
	}

	var heartbeatMs int64
	if hbVal, ok := config["idle_heartbeat"].(float64); ok && hbVal > 0 {
		heartbeatMs = int64(hbVal) / 1000000
	}

	headersOnly, _ := config["headers_only"].(bool)
	cfgCopy := make(map[string]any)
	for k, v := range config {
		cfgCopy[k] = v
	}
	cfgCopy["name"] = name

	consumer := &Consumer{
		name:        name,
		bucket:      bucket,
		group:       group,
		deliverSubj: deliverSubject,
		filters:     filters,
		headersOnly: headersOnly,
		config:      cfgCopy,
		created:     time.Now().UTC(),
		heartbeatMs: heartbeatMs,
		stopCh:      make(chan struct{}),
	}

	if j.conn.server.deps.Bus == nil {
		j.mu.Unlock()
		return
	}

	subID, ch, overflow := j.conn.server.deps.Bus.SubscribeWithOverflow(filters, 1024)
	consumer.busSubID = subID
	j.consumers[name] = consumer
	j.mu.Unlock()

	go j.runConsumerBusReader(consumer, ch, overflow)

	go func() {
		snapshotMap := make(map[string]stores.BrokerMessage)
		var scanErr error
		if deliverPolicy != "new" && group.LastValue() != nil {
			for _, f := range filters {
				err := group.LastValue().FindMatchingMessages(context.Background(), f, func(msg stores.BrokerMessage) bool {
					if len(msg.Payload) > 0 && revision(msg) >= optStartSeq && j.conn.canSubscribe(msg.TopicName) {
						snapshotMap[msg.TopicName] = msg
					}
					return true
				})
				if err != nil {
					scanErr = err
					break
				}
			}
		}

		j.mu.Lock()
		active := j.consumers[name] == consumer
		j.mu.Unlock()
		if !active {
			return
		}

		if scanErr != nil {
			j.removeConsumer(consumer)
			j.reply(replyTo, j.errorJson(&respType, 500, 10010, fmt.Sprintf("last value store error: %s", scanErr.Error())))
			return
		}

		var snapshotList []stores.BrokerMessage
		for _, msg := range snapshotMap {
			snapshotList = append(snapshotList, msg)
		}

		consumer.mu.Lock()
		consumer.snapshot = snapshotList
		consumer.loaded = true
		waitingReplies := consumer.waitingInfo
		consumer.waitingInfo = nil
		consumer.mu.Unlock()

		infoResp := j.consumerInfoJson(consumer, respType, int64(len(snapshotList)))
		b, _ := json.Marshal(infoResp)
		j.reply(replyTo, b)

		j.startDelivery(consumer)

		for _, rep := range waitingReplies {
			info := j.consumerInfoJson(consumer, "io.nats.jetstream.api.v1.consumer_info_response", int64(len(snapshotList)))
			b, _ := json.Marshal(info)
			j.reply(rep, b)
		}
	}()
}

func (j *jetstream) runConsumerBusReader(c *Consumer, ch <-chan stores.BrokerMessage, overflow <-chan struct{}) {
	for {
		select {
		case <-c.stopCh:
			return
		case <-overflow:
			_ = j.conn.writeError("Slow Consumer")
			j.conn.close()
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// E6: KV consumers drop Initial messages
			if msg.Initial {
				continue
			}
			if !c.group.MatchesFilter(msg.TopicName) {
				continue
			}
			// F6: Delivery-time ACL check
			if !j.conn.canSubscribe(msg.TopicName) {
				continue
			}

			c.mu.Lock()
			if !c.ready {
				if len(c.pendingLive) >= 1024 {
					c.mu.Unlock()
					_ = j.conn.writeError("Slow Consumer")
					j.conn.close()
					return
				}
				c.pendingLive = append(c.pendingLive, msg)
				c.mu.Unlock()
			} else {
				c.mu.Unlock()
				j.deliverConsumerMsg(c, msg, 0)
			}
		}
	}
}

func (j *jetstream) startDelivery(c *Consumer) {
	c.mu.Lock()
	if !c.loaded || c.ready {
		c.mu.Unlock()
		return
	}
	sids := j.conn.sidsForSubject(c.deliverSubj)
	if len(sids) == 0 {
		c.mu.Unlock()
		return
	}
	c.bound = true
	snapshot := c.snapshot
	c.snapshot = nil
	pendingLive := c.pendingLive
	c.pendingLive = nil
	c.ready = true
	c.mu.Unlock()

	for i, msg := range snapshot {
		pending := int64(len(snapshot) - i - 1)
		j.deliverConsumerMsg(c, msg, pending)
	}

	for _, msg := range pendingLive {
		j.deliverConsumerMsg(c, msg, 0)
	}

	if c.heartbeatMs > 0 {
		c.mu.Lock()
		c.ticker = time.NewTicker(time.Duration(c.heartbeatMs) * time.Millisecond)
		ticker := c.ticker
		c.mu.Unlock()

		go func() {
			for {
				select {
				case <-c.stopCh:
					return
				case <-ticker.C:
					j.sendHeartbeat(c)
				}
			}
		}()
	}
}

func (j *jetstream) deliverConsumerMsg(c *Consumer, msg stores.BrokerMessage, pending int64) {
	c.mu.Lock()
	c.deliveredSeq++
	c.lastStreamSeq = revision(msg)
	deliveredSeq := c.deliveredSeq
	streamSeq := c.lastStreamSeq
	headersOnly := c.headersOnly
	c.mu.Unlock()

	subject := fmt.Sprintf("$KV.%s.%s", c.bucket, TopicToSubject(msg.TopicName))
	ackSubject := fmt.Sprintf("$JS.ACK.KV_%s.%s.1.%d.%d.%d.%d", c.bucket, c.name, streamSeq, deliveredSeq, nanos(msg.Time), pending)

	if len(msg.Payload) == 0 {
		headerBlock := formatHeadersMap("", map[string]string{"KV-Operation": "DEL"})
		j.conn.deliverVia(c.deliverSubj, subject, ackSubject, headerBlock, nil)
	} else if headersOnly {
		headerBlock := formatHeadersMap("", map[string]string{"Nats-Msg-Size": strconv.Itoa(len(msg.Payload))})
		j.conn.deliverVia(c.deliverSubj, subject, ackSubject, headerBlock, nil)
	} else {
		j.conn.deliverVia(c.deliverSubj, subject, ackSubject, "", msg.Payload)
	}
}

func (j *jetstream) sendHeartbeat(c *Consumer) {
	c.mu.Lock()
	delSeq := c.deliveredSeq
	lastSeq := c.lastStreamSeq
	c.mu.Unlock()

	headerBlock := formatHeadersMap("100 Idle Heartbeat", map[string]string{
		"Nats-Last-Consumer": strconv.FormatInt(delSeq, 10),
		"Nats-Last-Stream":   strconv.FormatInt(lastSeq, 10),
	})
	for _, sid := range j.conn.sidsForSubject(c.deliverSubj) {
		j.conn.deliverMsgRaw(c.deliverSubj, sid, "", headerBlock, nil)
	}
}

func (j *jetstream) consumerInfoJson(c *Consumer, respType string, pending int64) map[string]any {
	c.mu.Lock()
	delSeq := c.deliveredSeq
	lastSeq := c.lastStreamSeq
	cfg := c.config
	created := c.created
	name := c.name
	bucket := c.bucket
	c.mu.Unlock()

	return map[string]any{
		"type":        respType,
		"stream_name": streamPrefix + bucket,
		"name":        name,
		"created":     isoTime(created),
		"config":      cfg,
		"delivered": map[string]any{
			"consumer_seq": delSeq,
			"stream_seq":   lastSeq,
		},
		"ack_floor": map[string]any{
			"consumer_seq": 0,
			"stream_seq":   0,
		},
		"num_ack_pending": 0,
		"num_redelivered": 0,
		"num_waiting":     0,
		"num_pending":     pending,
		"push_bound":      true,
		"ts":              isoTime(time.Now()),
	}
}

func (j *jetstream) consumerDelete(path, replyTo string) {
	respType := "io.nats.jetstream.api.v1.consumer_delete_response"
	name := path
	if idx := strings.LastIndexByte(path, '.'); idx >= 0 {
		name = path[idx+1:]
	}
	j.mu.Lock()
	c := j.consumers[name]
	j.mu.Unlock()

	if c == nil {
		j.reply(replyTo, j.errorJson(&respType, 404, 10014, "consumer not found"))
		return
	}
	j.removeConsumer(c)
	resp := map[string]any{
		"type":    respType,
		"success": true,
	}
	b, _ := json.Marshal(resp)
	j.reply(replyTo, b)
}

func (j *jetstream) consumerInfo(path, replyTo string) {
	respType := "io.nats.jetstream.api.v1.consumer_info_response"
	name := path
	if idx := strings.LastIndexByte(path, '.'); idx >= 0 {
		name = path[idx+1:]
	}
	j.mu.Lock()
	c := j.consumers[name]
	j.mu.Unlock()

	if c == nil {
		j.reply(replyTo, j.errorJson(&respType, 404, 10014, "consumer not found"))
		return
	}

	c.mu.Lock()
	if !c.loaded {
		c.waitingInfo = append(c.waitingInfo, replyTo)
		c.mu.Unlock()
		return
	}
	pending := int64(len(c.snapshot))
	c.mu.Unlock()

	info := j.consumerInfoJson(c, respType, pending)
	b, _ := json.Marshal(info)
	j.reply(replyTo, b)
}

func (j *jetstream) removeConsumer(c *Consumer) {
	j.mu.Lock()
	if j.consumers[c.name] == c {
		delete(j.consumers, c.name)
	}
	j.mu.Unlock()

	c.stop()
	if j.conn.server.deps.Bus != nil {
		j.conn.server.deps.Bus.Unsubscribe(c.busSubID)
	}
}
