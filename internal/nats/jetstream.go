package nats

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/stores"
)

const (
	jsApiPrefix  = "$JS.API."
	kvPrefix     = "$KV."
	streamPrefix = "KV_"
	countLimit   = 1000000
)

var bucketNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func isJetStreamSubject(subject string) bool {
	return strings.HasPrefix(subject, "$JS.") || strings.HasPrefix(subject, kvPrefix)
}

func revision(msg stores.BrokerMessage) int64 {
	return msg.Time.UnixNano() / int64(time.Millisecond)
}

func isoTime(t time.Time) string {
	if t.IsZero() {
		return "0001-01-01T00:00:00Z"
	}
	return t.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
}

func nanos(t time.Time) int64 {
	return t.UnixNano()
}

func keyToTopic(key string) (string, bool) {
	if key == "" || strings.HasPrefix(key, ".") || strings.HasSuffix(key, ".") || strings.Contains(key, "..") {
		return "", false
	}
	if strings.Contains(key, "*") || strings.Contains(key, ">") {
		return "", false
	}
	return SubjectToTopic(key), true
}

type jetstream struct {
	conn      *Conn
	mu        sync.Mutex
	consumers map[string]*Consumer
}

func newJetstream(c *Conn) *jetstream {
	return &jetstream{
		conn:      c,
		consumers: make(map[string]*Consumer),
	}
}

func (j *jetstream) buckets() map[string]*archive.Group {
	res := make(map[string]*archive.Group)
	if j.conn.server.deps.ArchiveManager == nil {
		return res
	}
	for _, g := range j.conn.server.deps.ArchiveManager.Snapshot() {
		name := g.Name()
		if !bucketNameRegex.MatchString(name) {
			continue
		}
		cfg := g.Config()
		lastValType := strings.ToUpper(strings.TrimSpace(string(cfg.LastValType)))
		if lastValType == "" || lastValType == "NONE" || g.LastValue() == nil {
			continue
		}
		res[name] = g
	}
	return res
}

func (j *jetstream) consumersCountForBucket(bucket string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	count := 0
	for _, c := range j.consumers {
		if c.bucket == bucket {
			count++
		}
	}
	return count
}

func (j *jetstream) onSubscribe() {
	j.mu.Lock()
	consumers := make([]*Consumer, 0, len(j.consumers))
	for _, c := range j.consumers {
		consumers = append(consumers, c)
	}
	j.mu.Unlock()

	for _, c := range consumers {
		j.startDelivery(c)
	}
}

func (j *jetstream) onUnsubscribe() {
	j.mu.Lock()
	toRemove := make([]*Consumer, 0)
	for _, c := range j.consumers {
		if c.bound && len(j.conn.sidsForSubject(c.deliverSubj)) == 0 {
			toRemove = append(toRemove, c)
		}
	}
	j.mu.Unlock()

	for _, c := range toRemove {
		j.removeConsumer(c)
	}
}

func (j *jetstream) close() {
	j.mu.Lock()
	consumers := make([]*Consumer, 0, len(j.consumers))
	for _, c := range j.consumers {
		consumers = append(consumers, c)
	}
	j.consumers = make(map[string]*Consumer)
	j.mu.Unlock()

	for _, c := range consumers {
		c.stop()
	}
}

func (j *jetstream) handlePublish(subject, replyTo string, headers []stores.UserProperty, payload []byte) {
	if strings.HasPrefix(subject, kvPrefix) {
		j.handlePut(subject, replyTo, headers, payload)
	} else if strings.HasPrefix(subject, jsApiPrefix) {
		if replyTo == "" {
			return
		}
		api := strings.TrimPrefix(subject, jsApiPrefix)
		j.handleApi(api, replyTo, payload)
	}
}

func (j *jetstream) reply(replyTo string, body []byte) {
	j.conn.deliverLocal(replyTo, "", "", body)
}

func (j *jetstream) errorJson(respType *string, code, errCode int, desc string) []byte {
	m := map[string]any{
		"error": map[string]any{
			"code":        code,
			"err_code":    errCode,
			"description": desc,
		},
	}
	if respType != nil {
		m["type"] = *respType
	}
	b, _ := json.Marshal(m)
	return b
}

func (j *jetstream) streamNotFound(respType string) []byte {
	return j.errorJson(&respType, 404, 10059, "stream not found")
}

func (j *jetstream) noMessageFound(respType string) []byte {
	return j.errorJson(&respType, 404, 10037, "no message found")
}

func (j *jetstream) handleApi(api, replyTo string, payload []byte) {
	switch {
	case api == "INFO":
		j.accountInfo(replyTo)
	case strings.HasPrefix(api, "STREAM.INFO."):
		j.streamInfo(strings.TrimPrefix(api, "STREAM.INFO."), replyTo)
	case api == "STREAM.NAMES":
		var req map[string]any
		_ = json.Unmarshal(payload, &req)
		j.streamNames(req, replyTo)
	case api == "STREAM.LIST":
		var req map[string]any
		_ = json.Unmarshal(payload, &req)
		j.streamList(req, replyTo)
	case strings.HasPrefix(api, "STREAM.MSG.GET."):
		var req map[string]any
		_ = json.Unmarshal(payload, &req)
		j.msgGet(strings.TrimPrefix(api, "STREAM.MSG.GET."), req, replyTo)
	case strings.HasPrefix(api, "CONSUMER.CREATE."):
		var req map[string]any
		_ = json.Unmarshal(payload, &req)
		j.consumerCreate(strings.TrimPrefix(api, "CONSUMER.CREATE."), req, replyTo)
	case strings.HasPrefix(api, "CONSUMER.DURABLE.CREATE."):
		var req map[string]any
		_ = json.Unmarshal(payload, &req)
		j.consumerCreate(strings.TrimPrefix(api, "CONSUMER.DURABLE.CREATE."), req, replyTo)
	case strings.HasPrefix(api, "CONSUMER.DELETE."):
		j.consumerDelete(strings.TrimPrefix(api, "CONSUMER.DELETE."), replyTo)
	case strings.HasPrefix(api, "CONSUMER.INFO."):
		j.consumerInfo(strings.TrimPrefix(api, "CONSUMER.INFO."), replyTo)
	default:
		msg := fmt.Sprintf("JetStream API [%s] is not supported by MonsterMQ; only Key-Value buckets backed by archive groups are available", api)
		j.reply(replyTo, j.errorJson(nil, 400, 10025, msg))
	}
}

func (j *jetstream) accountInfo(replyTo string) {
	j.mu.Lock()
	consumerCount := len(j.consumers)
	j.mu.Unlock()

	bucketCount := len(j.buckets())
	resp := map[string]any{
		"type":             "io.nats.jetstream.api.v1.account_info_response",
		"memory":           0,
		"storage":          0,
		"reserved_memory":  0,
		"reserved_storage": 0,
		"streams":          bucketCount,
		"consumers":        consumerCount,
		"limits": map[string]any{
			"max_memory":               -1,
			"max_storage":              -1,
			"max_streams":              -1,
			"max_consumers":            -1,
			"max_ack_pending":          -1,
			"memory_max_stream_bytes":  -1,
			"storage_max_stream_bytes": -1,
			"max_bytes_required":       false,
		},
		"api": map[string]any{
			"total":  0,
			"errors": 0,
		},
	}
	b, _ := json.Marshal(resp)
	j.reply(replyTo, b)
}

func streamConfig(bucket string, group *archive.Group) map[string]any {
	cfg := group.Config()
	storage := "file"
	if strings.ToUpper(strings.TrimSpace(string(cfg.LastValType))) == "MEMORY" {
		storage = "memory"
	}
	return map[string]any{
		"name":                 streamPrefix + bucket,
		"description":          fmt.Sprintf("MonsterMQ archive group %s", bucket),
		"subjects":             []string{fmt.Sprintf("$KV.%s.>", bucket)},
		"retention":            "limits",
		"max_consumers":        -1,
		"max_msgs":             -1,
		"max_bytes":            -1,
		"max_age":              0,
		"max_msgs_per_subject": 1,
		"max_msg_size":         -1,
		"discard":              "new",
		"storage":              storage,
		"num_replicas":         1,
		"duplicate_window":     int64(120000000000),
		"compression":          "none",
		"allow_direct":         false,
		"mirror_direct":        false,
		"sealed":               cfg.LastValReadOnly,
		"deny_delete":          true,
		"deny_purge":           false,
		"allow_rollup_hdrs":    true,
	}
}

type streamStats struct {
	messages int
	bytes    int64
	last     time.Time
}

func (j *jetstream) buildStreamInfo(bucket string, group *archive.Group, done func(map[string]any)) {
	go func() {
		var stats streamStats
		if store := group.LastValue(); store != nil {
			_ = store.FindMatchingMessages(context.Background(), "#", func(msg stores.BrokerMessage) bool {
				if len(msg.Payload) > 0 {
					stats.messages++
					stats.bytes += int64(len(msg.Payload))
					if stats.last.IsZero() || msg.Time.After(stats.last) {
						stats.last = msg.Time
					}
				}
				return stats.messages < countLimit
			})
		}
		zero := "0001-01-01T00:00:00Z"
		var firstSeq, lastSeq int64
		lastTs := zero
		if stats.messages > 0 {
			firstSeq = 1
		}
		if !stats.last.IsZero() {
			lastSeq = stats.last.UnixNano() / int64(time.Millisecond)
			lastTs = isoTime(stats.last)
		}
		consumerCount := j.consumersCountForBucket(bucket)
		state := map[string]any{
			"messages":       stats.messages,
			"bytes":          stats.bytes,
			"first_seq":      firstSeq,
			"first_ts":       zero,
			"last_seq":       lastSeq,
			"last_ts":        lastTs,
			"num_subjects":   stats.messages,
			"consumer_count": consumerCount,
		}
		info := map[string]any{
			"config":  streamConfig(bucket, group),
			"created": zero,
			"state":   state,
			"ts":      isoTime(time.Now()),
		}
		done(info)
	}()
}

func (j *jetstream) streamInfo(stream, replyTo string) {
	respType := "io.nats.jetstream.api.v1.stream_info_response"
	bucket := strings.TrimPrefix(stream, streamPrefix)
	group := j.buckets()[bucket]
	if group == nil {
		j.reply(replyTo, j.streamNotFound(respType))
		return
	}
	j.buildStreamInfo(bucket, group, func(info map[string]any) {
		info["type"] = respType
		info["total"] = 0
		info["offset"] = 0
		info["limit"] = 0
		b, _ := json.Marshal(info)
		j.reply(replyTo, b)
	})
}

func (j *jetstream) selectBuckets(req map[string]any) map[string]*archive.Group {
	all := j.buckets()
	subject, _ := req["subject"].(string)
	if subject == "" || subject == ">" || subject == "$KV.*.>" || subject == "$KV.>" {
		return all
	}
	if !strings.HasPrefix(subject, kvPrefix) {
		return nil
	}
	rest := strings.TrimPrefix(subject, kvPrefix)
	dot := strings.IndexByte(rest, '.')
	bucket := rest
	if dot > 0 {
		bucket = rest[:dot]
	}
	if g, ok := all[bucket]; ok {
		return map[string]*archive.Group{bucket: g}
	}
	return nil
}

func (j *jetstream) streamNames(req map[string]any, replyTo string) {
	sel := j.selectBuckets(req)
	var names []string
	for b := range sel {
		names = append(names, streamPrefix+b)
	}
	sort.Strings(names)
	if names == nil {
		names = []string{}
	}
	resp := map[string]any{
		"type":    "io.nats.jetstream.api.v1.stream_names_response",
		"total":   len(names),
		"offset":  0,
		"limit":   1024,
		"streams": names,
	}
	b, _ := json.Marshal(resp)
	j.reply(replyTo, b)
}

func (j *jetstream) streamList(req map[string]any, replyTo string) {
	sel := j.selectBuckets(req)
	var buckets []string
	for b := range sel {
		buckets = append(buckets, b)
	}
	sort.Strings(buckets)

	if len(buckets) == 0 {
		resp := map[string]any{
			"type":    "io.nats.jetstream.api.v1.stream_list_response",
			"total":   0,
			"offset":  0,
			"limit":   256,
			"streams": []any{},
			"missing": []any{},
		}
		b, _ := json.Marshal(resp)
		j.reply(replyTo, b)
		return
	}

	infos := make([]map[string]any, len(buckets))
	var wg sync.WaitGroup
	for idx, b := range buckets {
		wg.Add(1)
		index := idx
		bucket := b
		group := sel[bucket]
		j.buildStreamInfo(bucket, group, func(info map[string]any) {
			infos[index] = info
			wg.Done()
		})
	}

	go func() {
		wg.Wait()
		resp := map[string]any{
			"type":    "io.nats.jetstream.api.v1.stream_list_response",
			"total":   len(infos),
			"offset":  0,
			"limit":   256,
			"streams": infos,
			"missing": []any{},
		}
		b, _ := json.Marshal(resp)
		j.reply(replyTo, b)
	}()
}

func (j *jetstream) msgGet(stream string, request map[string]any, replyTo string) {
	respType := "io.nats.jetstream.api.v1.stream_msg_get_response"
	bucket := strings.TrimPrefix(stream, streamPrefix)
	group := j.buckets()[bucket]
	if group == nil {
		j.reply(replyTo, j.streamNotFound(respType))
		return
	}
	subject, _ := request["last_by_subj"].(string)
	prefix := fmt.Sprintf("$KV.%s.", bucket)
	if subject == "" || !strings.HasPrefix(subject, prefix) {
		j.reply(replyTo, j.noMessageFound(respType))
		return
	}
	key := strings.TrimPrefix(subject, prefix)
	topic, ok := keyToTopic(key)
	if !ok {
		j.reply(replyTo, j.errorJson(&respType, 400, 10052, "invalid key"))
		return
	}
	if !j.conn.canSubscribe(topic) {
		j.reply(replyTo, j.errorJson(&respType, 403, 10101, fmt.Sprintf("Permissions Violation for Subscription to \"%s\"", subject)))
		return
	}

	go func() {
		var msg *stores.BrokerMessage
		var err error
		if group.LastValue() != nil {
			msg, err = group.LastValue().Get(context.Background(), topic)
		}
		if err != nil {
			j.reply(replyTo, j.errorJson(&respType, 500, 10010, fmt.Sprintf("last value store error: %s", err.Error())))
			return
		}
		if msg == nil || len(msg.Payload) == 0 {
			j.reply(replyTo, j.noMessageFound(respType))
			return
		}
		resp := map[string]any{
			"type": respType,
			"message": map[string]any{
				"subject": subject,
				"seq":     revision(*msg),
				"data":    base64.StdEncoding.EncodeToString(msg.Payload),
				"time":    isoTime(msg.Time),
			},
		}
		b, _ := json.Marshal(resp)
		j.reply(replyTo, b)
	}()
}

func (j *jetstream) handlePut(subject, replyTo string, headers []stores.UserProperty, payload []byte) {
	ack := func(data []byte) {
		if replyTo != "" {
			j.reply(replyTo, data)
		}
	}

	rest := strings.TrimPrefix(subject, kvPrefix)
	dotIdx := strings.IndexByte(rest, '.')
	if dotIdx <= 0 {
		ack(j.streamNotFound("io.nats.jetstream.api.v1.pub_ack_response"))
		return
	}
	bucket := rest[:dotIdx]
	key := rest[dotIdx+1:]

	group := j.buckets()[bucket]
	if group == nil {
		ack(j.streamNotFound("io.nats.jetstream.api.v1.pub_ack_response"))
		return
	}

	topic, ok := keyToTopic(key)
	if !ok {
		ack(j.errorJson(nil, 400, 10052, "invalid key"))
		return
	}

	headerMap := make(map[string]string)
	for _, h := range headers {
		headerMap[h.Key] = h.Value
	}
	op := headerMap["KV-Operation"]
	isDelete := op == "DEL" || op == "PURGE"

	isNative := false
	if !isDelete && j.conn.server.deps.NativeValueWrite != nil {
		if setTopic, ok := j.conn.server.deps.NativeValueWrite(topic); ok {
			topic = setTopic
			isNative = true
		}
	}

	if !isNative {
		if !group.MatchesFilter(topic) {
			ack(j.errorJson(nil, 400, 10052, fmt.Sprintf("key [%s] does not match the topic filter of archive group [%s]", topic, bucket)))
			return
		}
		if group.Config().LastValReadOnly {
			ack(j.errorJson(nil, 400, 10039, fmt.Sprintf("last value store of archive group [%s] is read-only", bucket)))
			return
		}
	}

	if !j.conn.canPublish(topic) {
		ack(j.errorJson(nil, 403, 10101, fmt.Sprintf("Permissions Violation for Publish to \"%s\"", subject)))
		return
	}

	var value []byte
	if isDelete {
		value = []byte{}
	} else {
		value = payload
	}

	retain := true
	if isNative {
		retain = false
	}

	publish := func() {
		msg := stores.BrokerMessage{
			TopicName: topic,
			Payload:   value,
			QoS:       0,
			IsRetain:  retain,
			ClientID:  j.conn.clientID,
			Time:      time.Now().UTC(),
		}
		if j.conn.server.deps.Publish != nil {
			if err := j.conn.server.deps.Publish(j.conn.clientID, msg); err != nil {
				if isErrNotAuthorized(err) {
					ack(j.errorJson(nil, 403, 10101, fmt.Sprintf("Permissions Violation for Publish to \"%s\"", subject)))
				} else {
					ack(j.errorJson(nil, 400, 10052, err.Error()))
				}
				return
			}
		}
		if j.conn.server.deps.Counter != nil {
			j.conn.server.deps.Counter.IncClientIn(j.conn.clientID)
		}
		seq := revision(msg)
		ackResp, _ := json.Marshal(map[string]any{
			"stream": streamPrefix + bucket,
			"seq":    seq,
		})
		ack(ackResp)
	}

	expectedSeqStr, hasExpected := headerMap["Nats-Expected-Last-Subject-Sequence"]
	if !hasExpected {
		publish()
		return
	}
	expectedSeq, err := strconv.ParseInt(expectedSeqStr, 10, 64)
	if err != nil {
		publish()
		return
	}

	go func() {
		var current *stores.BrokerMessage
		var storeErr error
		if group.LastValue() != nil {
			current, storeErr = group.LastValue().Get(context.Background(), topic)
		}
		if storeErr != nil {
			ack(j.errorJson(nil, 500, 10010, fmt.Sprintf("last value store error: %s", storeErr.Error())))
			return
		}
		var currentRevision int64
		if current != nil && len(current.Payload) > 0 {
			currentRevision = revision(*current)
		}
		if currentRevision != expectedSeq {
			ack(j.errorJson(nil, 400, 10071, fmt.Sprintf("wrong last sequence: %d", currentRevision)))
			return
		}
		publish()
	}()
}
