package redisserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"monstermq.io/edge/internal/archive"
)

const errWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"

type redisStoredValue struct {
	Type  string
	Value any
}

type jsonEnvelope struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

func decodeStoredValue(payload []byte) *redisStoredValue {
	if payload == nil {
		return nil
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err == nil {
		if t, ok := raw["type"].(string); ok {
			if v, exists := raw["value"]; exists {
				return &redisStoredValue{Type: t, Value: v}
			}
		}
	}
	return &redisStoredValue{Type: "string", Value: payload}
}

func encodeEnvelope(typ string, val any) ([]byte, error) {
	return json.Marshal(jsonEnvelope{Type: typ, Value: val})
}

func (c *Conn) readStored(group *archive.Group, topic string) (*redisStoredValue, error) {
	if group == nil || group.LastValue() == nil {
		return nil, nil
	}
	msg, err := group.LastValue().Get(context.Background(), topic)
	if err != nil || msg == nil {
		return nil, err
	}
	return decodeStoredValue(msg.Payload), nil
}

// -------------------------------------------------------------
// Hash Commands
// -------------------------------------------------------------

func (c *Conn) handleHset(args [][]byte) error {
	if len(args) < 4 || len(args)%2 != 0 {
		return c.writeError("ERR wrong number of arguments for 'hset' command")
	}
	topic := string(args[1])

	vw, err := c.validateWrite("HSET", topic, false)
	if err != nil {
		return c.writeError(err.Error())
	}

	unlock := c.server.lockStripes(vw.group.Name(), []string{topic})
	defer unlock()

	stored, _ := c.readStored(vw.group, topic)
	var hash map[string]any
	if stored == nil {
		hash = make(map[string]any)
	} else if stored.Type != "hash" {
		return c.writeError(errWrongType)
	} else {
		if m, ok := stored.Value.(map[string]any); ok {
			hash = m
		} else {
			return c.writeError(errWrongType)
		}
	}

	var added int64
	for i := 2; i < len(args); i += 2 {
		f := string(args[i])
		v := string(args[i+1])
		if _, exists := hash[f]; !exists {
			added++
		}
		hash[f] = v
	}

	payload, err := encodeEnvelope("hash", hash)
	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}

	if err := c.publishMessage(topic, payload, false); err != nil {
		return c.writeError(err.Error())
	}
	return c.writeInteger(added)
}

func (c *Conn) handleHget(args [][]byte) error {
	if len(args) != 3 {
		return c.writeError("ERR wrong number of arguments for 'hget' command")
	}
	topic := string(args[1])
	field := string(args[2])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeNullBulk()
	}
	if stored.Type != "hash" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}
	val, exists := m[field]
	if !exists {
		return c.writeNullBulk()
	}
	return c.writeBulkStr(fmt.Sprintf("%v", val))
}

func (c *Conn) handleHmget(args [][]byte) error {
	if len(args) < 3 {
		return c.writeError("ERR wrong number of arguments for 'hmget' command")
	}
	topic := string(args[1])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	fields := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		fields[i-2] = string(args[i])
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		items := make([]respValue, len(fields))
		for i := range items {
			items[i] = respBulk(nil)
		}
		return c.writeArray(items)
	}
	if stored.Type != "hash" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	items := make([]respValue, len(fields))
	for i, f := range fields {
		if val, exists := m[f]; exists {
			items[i] = respBulk([]byte(fmt.Sprintf("%v", val)))
		} else {
			items[i] = respBulk(nil)
		}
	}
	return c.writeArray(items)
}

func (c *Conn) handleHgetall(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'hgetall' command")
	}
	topic := string(args[1])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeArray(nil)
	}
	if stored.Type != "hash" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	items := make([]respValue, 0, len(keys)*2)
	for _, k := range keys {
		items = append(items, respBulk([]byte(k)))
		items = append(items, respBulk([]byte(fmt.Sprintf("%v", m[k]))))
	}
	return c.writeArray(items)
}

func (c *Conn) handleHdel(args [][]byte) error {
	if len(args) < 3 {
		return c.writeError("ERR wrong number of arguments for 'hdel' command")
	}
	topic := string(args[1])

	vw, err := c.validateWrite("HDEL", topic, false)
	if err != nil {
		return c.writeError(err.Error())
	}

	unlock := c.server.lockStripes(vw.group.Name(), []string{topic})
	defer unlock()

	stored, _ := c.readStored(vw.group, topic)
	if stored == nil {
		return c.writeInteger(0)
	}
	if stored.Type != "hash" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	var removed int64
	for i := 2; i < len(args); i++ {
		f := string(args[i])
		if _, exists := m[f]; exists {
			delete(m, f)
			removed++
		}
	}

	payload, err := encodeEnvelope("hash", m)
	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	if err := c.publishMessage(topic, payload, false); err != nil {
		return c.writeError(err.Error())
	}
	return c.writeInteger(removed)
}

func (c *Conn) handleHexists(args [][]byte) error {
	if len(args) != 3 {
		return c.writeError("ERR wrong number of arguments for 'hexists' command")
	}
	topic := string(args[1])
	field := string(args[2])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeInteger(0)
	}
	if stored.Type != "hash" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}
	if _, exists := m[field]; exists {
		return c.writeInteger(1)
	}
	return c.writeInteger(0)
}

func (c *Conn) handleHlen(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'hlen' command")
	}
	topic := string(args[1])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeInteger(0)
	}
	if stored.Type != "hash" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}
	return c.writeInteger(int64(len(m)))
}

func (c *Conn) handleHkeys(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'hkeys' command")
	}
	topic := string(args[1])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeArray(nil)
	}
	if stored.Type != "hash" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	items := make([]respValue, len(keys))
	for i, k := range keys {
		items[i] = respBulk([]byte(k))
	}
	return c.writeArray(items)
}

func (c *Conn) handleHvals(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'hvals' command")
	}
	topic := string(args[1])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeArray(nil)
	}
	if stored.Type != "hash" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	items := make([]respValue, len(keys))
	for i, k := range keys {
		items[i] = respBulk([]byte(fmt.Sprintf("%v", m[k])))
	}
	return c.writeArray(items)
}

func (c *Conn) handleHscan(args [][]byte) error {
	if len(args) < 3 {
		return c.writeError("ERR wrong number of arguments for 'hscan' command")
	}
	topic := string(args[1])
	cursor, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil || cursor < 0 {
		return c.writeError("ERR invalid cursor")
	}

	var match string
	count := -1
	for i := 3; i+1 < len(args); i += 2 {
		opt := string(args[i])
		if strings.EqualFold(opt, "MATCH") {
			match = string(args[i+1])
		} else if strings.EqualFold(opt, "COUNT") {
			if n, err := strconv.Atoi(string(args[i+1])); err == nil {
				count = n
			}
		}
	}

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeArray([]respValue{
			respBulk([]byte("0")),
			respArray(nil),
		})
	}
	if stored.Type != "hash" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	var keys []string
	for k := range m {
		if match == "" || globMatch(match, k) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var nextCursor int64
	var selectedKeys []string
	if int(cursor) < len(keys) {
		end := len(keys)
		if count > 0 && int(cursor)+count < end {
			end = int(cursor) + count
			nextCursor = int64(end)
		}
		selectedKeys = keys[cursor:end]
	}

	items := make([]respValue, 0, len(selectedKeys)*2)
	for _, k := range selectedKeys {
		items = append(items, respBulk([]byte(k)))
		items = append(items, respBulk([]byte(fmt.Sprintf("%v", m[k]))))
	}

	return c.writeArray([]respValue{
		respBulk([]byte(strconv.FormatInt(nextCursor, 10))),
		respArray(items),
	})
}

// -------------------------------------------------------------
// Set Commands
// -------------------------------------------------------------

func (c *Conn) handleSadd(args [][]byte) error {
	if len(args) < 3 {
		return c.writeError("ERR wrong number of arguments for 'sadd' command")
	}
	topic := string(args[1])

	vw, err := c.validateWrite("SADD", topic, false)
	if err != nil {
		return c.writeError(err.Error())
	}

	unlock := c.server.lockStripes(vw.group.Name(), []string{topic})
	defer unlock()

	stored, _ := c.readStored(vw.group, topic)
	var set []string
	seen := make(map[string]bool)
	if stored == nil {
		set = []string{}
	} else if stored.Type != "set" {
		return c.writeError(errWrongType)
	} else {
		if arr, ok := stored.Value.([]any); ok {
			for _, item := range arr {
				str := fmt.Sprintf("%v", item)
				if !seen[str] {
					seen[str] = true
					set = append(set, str)
				}
			}
		} else {
			return c.writeError(errWrongType)
		}
	}

	var added int64
	for i := 2; i < len(args); i++ {
		member := string(args[i])
		if !seen[member] {
			seen[member] = true
			set = append(set, member)
			added++
		}
	}

	payload, err := encodeEnvelope("set", set)
	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	if err := c.publishMessage(topic, payload, false); err != nil {
		return c.writeError(err.Error())
	}
	return c.writeInteger(added)
}

func (c *Conn) handleSrem(args [][]byte) error {
	if len(args) < 3 {
		return c.writeError("ERR wrong number of arguments for 'srem' command")
	}
	topic := string(args[1])

	vw, err := c.validateWrite("SREM", topic, false)
	if err != nil {
		return c.writeError(err.Error())
	}

	unlock := c.server.lockStripes(vw.group.Name(), []string{topic})
	defer unlock()

	stored, _ := c.readStored(vw.group, topic)
	if stored == nil {
		return c.writeInteger(0)
	}
	if stored.Type != "set" {
		return c.writeError(errWrongType)
	}
	arr, ok := stored.Value.([]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	toRemove := make(map[string]bool)
	for i := 2; i < len(args); i++ {
		toRemove[string(args[i])] = true
	}

	var newSet []string
	var removed int64
	for _, item := range arr {
		str := fmt.Sprintf("%v", item)
		if toRemove[str] {
			removed++
		} else {
			newSet = append(newSet, str)
		}
	}

	payload, err := encodeEnvelope("set", newSet)
	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	if err := c.publishMessage(topic, payload, false); err != nil {
		return c.writeError(err.Error())
	}
	return c.writeInteger(removed)
}

func (c *Conn) handleSmembers(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'smembers' command")
	}
	topic := string(args[1])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeArray(nil)
	}
	if stored.Type != "set" {
		return c.writeError(errWrongType)
	}
	arr, ok := stored.Value.([]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	items := make([]respValue, len(arr))
	for i, item := range arr {
		items[i] = respBulk([]byte(fmt.Sprintf("%v", item)))
	}
	return c.writeArray(items)
}

func (c *Conn) handleSismember(args [][]byte) error {
	if len(args) != 3 {
		return c.writeError("ERR wrong number of arguments for 'sismember' command")
	}
	topic := string(args[1])
	target := string(args[2])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeInteger(0)
	}
	if stored.Type != "set" {
		return c.writeError(errWrongType)
	}
	arr, ok := stored.Value.([]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	for _, item := range arr {
		if fmt.Sprintf("%v", item) == target {
			return c.writeInteger(1)
		}
	}
	return c.writeInteger(0)
}

func (c *Conn) handleScard(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'scard' command")
	}
	topic := string(args[1])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeInteger(0)
	}
	if stored.Type != "set" {
		return c.writeError(errWrongType)
	}
	arr, ok := stored.Value.([]any)
	if !ok {
		return c.writeError(errWrongType)
	}
	return c.writeInteger(int64(len(arr)))
}

// -------------------------------------------------------------
// List Commands
// -------------------------------------------------------------

func (c *Conn) handleListPush(args [][]byte, isLeft bool) error {
	cmdName := strings.ToLower(string(args[0]))
	if len(args) < 3 {
		return c.writeError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmdName))
	}
	topic := string(args[1])

	vw, err := c.validateWrite(cmdName, topic, false)
	if err != nil {
		return c.writeError(err.Error())
	}

	unlock := c.server.lockStripes(vw.group.Name(), []string{topic})
	defer unlock()

	stored, _ := c.readStored(vw.group, topic)
	var list []string
	if stored == nil {
		list = []string{}
	} else if stored.Type != "list" {
		return c.writeError(errWrongType)
	} else {
		if arr, ok := stored.Value.([]any); ok {
			for _, item := range arr {
				list = append(list, fmt.Sprintf("%v", item))
			}
		} else {
			return c.writeError(errWrongType)
		}
	}

	if isLeft {
		// LPUSH k a b -> prepend a, then prepend b -> [b, a, ...]
		for i := 2; i < len(args); i++ {
			val := string(args[i])
			list = append([]string{val}, list...)
		}
	} else {
		// RPUSH k a b -> append a, then append b
		for i := 2; i < len(args); i++ {
			list = append(list, string(args[i]))
		}
	}

	payload, err := encodeEnvelope("list", list)
	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	if err := c.publishMessage(topic, payload, false); err != nil {
		return c.writeError(err.Error())
	}
	return c.writeInteger(int64(len(list)))
}

func (c *Conn) handleListPop(args [][]byte, isLeft bool) error {
	cmdName := strings.ToLower(string(args[0]))
	if len(args) != 2 {
		return c.writeError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmdName))
	}
	topic := string(args[1])

	vw, err := c.validateWrite(cmdName, topic, false)
	if err != nil {
		return c.writeError(err.Error())
	}

	unlock := c.server.lockStripes(vw.group.Name(), []string{topic})
	defer unlock()

	stored, _ := c.readStored(vw.group, topic)
	if stored == nil {
		return c.writeNullBulk()
	}
	if stored.Type != "list" {
		return c.writeError(errWrongType)
	}
	arr, ok := stored.Value.([]any)
	if !ok {
		return c.writeError(errWrongType)
	}
	if len(arr) == 0 {
		return c.writeNullBulk()
	}

	var popped string
	var remaining []string
	if isLeft {
		popped = fmt.Sprintf("%v", arr[0])
		for _, item := range arr[1:] {
			remaining = append(remaining, fmt.Sprintf("%v", item))
		}
	} else {
		popped = fmt.Sprintf("%v", arr[len(arr)-1])
		for _, item := range arr[:len(arr)-1] {
			remaining = append(remaining, fmt.Sprintf("%v", item))
		}
	}

	payload, err := encodeEnvelope("list", remaining)
	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	if err := c.publishMessage(topic, payload, false); err != nil {
		return c.writeError(err.Error())
	}
	return c.writeBulkStr(popped)
}

func (c *Conn) handleLrange(args [][]byte) error {
	if len(args) != 4 {
		return c.writeError("ERR wrong number of arguments for 'lrange' command")
	}
	topic := string(args[1])
	start, err1 := strconv.ParseInt(string(args[2]), 10, 64)
	end, err2 := strconv.ParseInt(string(args[3]), 10, 64)
	if err1 != nil || err2 != nil {
		return c.writeError("ERR value is not an integer or out of range")
	}

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeArray(nil)
	}
	if stored.Type != "list" {
		return c.writeError(errWrongType)
	}
	arr, ok := stored.Value.([]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	sliced := sliceAnyList(arr, start, end)
	items := make([]respValue, len(sliced))
	for i, item := range sliced {
		items[i] = respBulk([]byte(fmt.Sprintf("%v", item)))
	}
	return c.writeArray(items)
}

func sliceAnyList(arr []any, rawStart, rawEnd int64) []any {
	if len(arr) == 0 {
		return []any{}
	}
	size := int64(len(arr))
	start := rawStart
	if start < 0 {
		start = size + start
	}
	end := rawEnd
	if end < 0 {
		end = size + end
	}
	if start < 0 {
		start = 0
	}
	if end < 0 || start >= size || start > end {
		return []any{}
	}
	if end >= size {
		end = size - 1
	}
	return arr[start : end+1]
}

func (c *Conn) handleLlen(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'llen' command")
	}
	topic := string(args[1])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeInteger(0)
	}
	if stored.Type != "list" {
		return c.writeError(errWrongType)
	}
	arr, ok := stored.Value.([]any)
	if !ok {
		return c.writeError(errWrongType)
	}
	return c.writeInteger(int64(len(arr)))
}

func (c *Conn) handleLindex(args [][]byte) error {
	if len(args) != 3 {
		return c.writeError("ERR wrong number of arguments for 'lindex' command")
	}
	topic := string(args[1])
	idx, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return c.writeError("ERR value is not an integer or out of range")
	}

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeNullBulk()
	}
	if stored.Type != "list" {
		return c.writeError(errWrongType)
	}
	arr, ok := stored.Value.([]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	size := int64(len(arr))
	if idx < 0 {
		idx = size + idx
	}
	if idx < 0 || idx >= size {
		return c.writeNullBulk()
	}
	return c.writeBulkStr(fmt.Sprintf("%v", arr[idx]))
}

// -------------------------------------------------------------
// Sorted Set (ZSET) Commands
// -------------------------------------------------------------

func formatScore(score float64) string {
	if score == float64(int64(score)) {
		return strconv.FormatInt(int64(score), 10)
	}
	return strconv.FormatFloat(score, 'f', -1, 64)
}

type zsetEntry struct {
	member string
	score  float64
}

func (c *Conn) handleZadd(args [][]byte) error {
	if len(args) < 4 || len(args)%2 != 0 {
		return c.writeError("ERR wrong number of arguments for 'zadd' command")
	}
	topic := string(args[1])

	// Validate all scores first
	pairs := make([]zsetEntry, (len(args)-2)/2)
	for i := 0; i < len(pairs); i++ {
		scoreStr := string(args[2+i*2])
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return c.writeError("ERR value is not a valid float")
		}
		pairs[i] = zsetEntry{
			score:  score,
			member: string(args[3+i*2]),
		}
	}

	vw, err := c.validateWrite("ZADD", topic, false)
	if err != nil {
		return c.writeError(err.Error())
	}

	unlock := c.server.lockStripes(vw.group.Name(), []string{topic})
	defer unlock()

	stored, _ := c.readStored(vw.group, topic)
	zset := make(map[string]float64)
	if stored != nil {
		if stored.Type != "zset" {
			return c.writeError(errWrongType)
		}
		if m, ok := stored.Value.(map[string]any); ok {
			for k, v := range m {
				if num, ok := v.(float64); ok {
					zset[k] = num
				}
			}
		} else {
			return c.writeError(errWrongType)
		}
	}

	var added int64
	for _, p := range pairs {
		if _, exists := zset[p.member]; !exists {
			added++
		}
		zset[p.member] = p.score
	}

	payload, err := encodeEnvelope("zset", zset)
	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	if err := c.publishMessage(topic, payload, false); err != nil {
		return c.writeError(err.Error())
	}
	return c.writeInteger(added)
}

func (c *Conn) handleZrange(args [][]byte) error {
	if len(args) < 4 || len(args) > 5 {
		return c.writeError("ERR wrong number of arguments for 'zrange' command")
	}
	topic := string(args[1])
	start, err1 := strconv.ParseInt(string(args[2]), 10, 64)
	end, err2 := strconv.ParseInt(string(args[3]), 10, 64)
	if err1 != nil || err2 != nil {
		return c.writeError("ERR value is not an integer or out of range")
	}
	withScores := false
	if len(args) == 5 {
		if !strings.EqualFold(string(args[4]), "WITHSCORES") {
			return c.writeError("ERR syntax error")
		}
		withScores = true
	}

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeArray(nil)
	}
	if stored.Type != "zset" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	var entries []zsetEntry
	for k, v := range m {
		if num, ok := v.(float64); ok {
			entries = append(entries, zsetEntry{member: k, score: num})
		}
	}

	// Sort by score ascending, then member ascending (lexicographical)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].score != entries[j].score {
			return entries[i].score < entries[j].score
		}
		return entries[i].member < entries[j].member
	})

	size := int64(len(entries))
	if start < 0 {
		start = size + start
	}
	if end < 0 {
		end = size + end
	}
	if start < 0 {
		start = 0
	}
	var sliced []zsetEntry
	if !(end < 0 || start >= size || start > end) {
		if end >= size {
			end = size - 1
		}
		sliced = entries[start : end+1]
	}

	if withScores {
		items := make([]respValue, 0, len(sliced)*2)
		for _, e := range sliced {
			items = append(items, respBulk([]byte(e.member)))
			items = append(items, respBulk([]byte(formatScore(e.score))))
		}
		return c.writeArray(items)
	}

	items := make([]respValue, len(sliced))
	for i, e := range sliced {
		items[i] = respBulk([]byte(e.member))
	}
	return c.writeArray(items)
}

func (c *Conn) handleZrem(args [][]byte) error {
	if len(args) < 3 {
		return c.writeError("ERR wrong number of arguments for 'zrem' command")
	}
	topic := string(args[1])

	vw, err := c.validateWrite("ZREM", topic, false)
	if err != nil {
		return c.writeError(err.Error())
	}

	unlock := c.server.lockStripes(vw.group.Name(), []string{topic})
	defer unlock()

	stored, _ := c.readStored(vw.group, topic)
	if stored == nil {
		return c.writeInteger(0)
	}
	if stored.Type != "zset" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}

	var removed int64
	for i := 2; i < len(args); i++ {
		mem := string(args[i])
		if _, exists := m[mem]; exists {
			delete(m, mem)
			removed++
		}
	}

	payload, err := encodeEnvelope("zset", m)
	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	if err := c.publishMessage(topic, payload, false); err != nil {
		return c.writeError(err.Error())
	}
	return c.writeInteger(removed)
}

func (c *Conn) handleZscore(args [][]byte) error {
	if len(args) != 3 {
		return c.writeError("ERR wrong number of arguments for 'zscore' command")
	}
	topic := string(args[1])
	member := string(args[2])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeNullBulk()
	}
	if stored.Type != "zset" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}
	if v, exists := m[member]; exists {
		if score, ok := v.(float64); ok {
			return c.writeBulkStr(formatScore(score))
		}
	}
	return c.writeNullBulk()
}

func (c *Conn) handleZcard(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'zcard' command")
	}
	topic := string(args[1])

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeInteger(0)
	}
	if stored.Type != "zset" {
		return c.writeError(errWrongType)
	}
	m, ok := stored.Value.(map[string]any)
	if !ok {
		return c.writeError(errWrongType)
	}
	return c.writeInteger(int64(len(m)))
}

// -------------------------------------------------------------
// JSON Commands
// -------------------------------------------------------------

func isRootJsonPath(p string) bool {
	return p == "$" || p == "."
}

func (c *Conn) handleJsonSet(args [][]byte) error {
	if len(args) != 4 {
		return c.writeError("ERR wrong number of arguments for 'json.set' command")
	}
	topic := string(args[1])
	path := string(args[2])
	if !isRootJsonPath(path) {
		return c.writeError("ERR only root JSON path '$' or '.' is supported")
	}

	var parsed any
	if err := json.Unmarshal(args[3], &parsed); err != nil {
		return c.writeError("ERR invalid JSON")
	}

	vw, err := c.validateWrite("JSON.SET", topic, false)
	if err != nil {
		return c.writeError(err.Error())
	}

	unlock := c.server.lockStripes(vw.group.Name(), []string{topic})
	defer unlock()

	payload, err := encodeEnvelope("json", parsed)
	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	if err := c.publishMessage(topic, payload, false); err != nil {
		return c.writeError(err.Error())
	}
	return c.writeBulkStr("OK")
}

func (c *Conn) handleJsonGet(args [][]byte) error {
	if len(args) < 2 || len(args) > 3 {
		return c.writeError("ERR wrong number of arguments for 'json.get' command")
	}
	topic := string(args[1])
	if len(args) == 3 && !isRootJsonPath(string(args[2])) {
		return c.writeError("ERR only root JSON path '$' or '.' is supported")
	}

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeNullBulk()
	}
	if stored.Type != "json" {
		return c.writeError(errWrongType)
	}

	raw, err := json.Marshal(stored.Value)
	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}
	return c.writeBulk(raw)
}

func (c *Conn) handleJsonDel(args [][]byte) error {
	if len(args) < 2 || len(args) > 3 {
		return c.writeError("ERR wrong number of arguments for 'json.del' command")
	}
	topic := string(args[1])
	if len(args) == 3 && !isRootJsonPath(string(args[2])) {
		return c.writeError("ERR only root JSON path '$' or '.' is supported")
	}

	if err := c.checkNativeTypedWrite("json.del", topic); err != nil {
		return c.writeError(err.Error())
	}

	if !c.canWrite(topic) {
		return c.writeError("NOPERM no permission to delete topic")
	}

	group := c.selectedArchiveGroup()
	if group == nil {
		return nil
	}

	if isGroupReadOnly(group) {
		return c.writeError(fmt.Sprintf("ERR last value store of archive group [%s] is read-only", group.Name()))
	}

	store := group.LastValue()
	if store == nil {
		return c.writeInteger(0)
	}

	unlock := c.server.lockStripes(group.Name(), []string{topic})
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		unlock()
		return c.writeInteger(0)
	}
	if stored.Type != "json" {
		unlock()
		return c.writeError(errWrongType)
	}

	delErr := store.DelAll(context.Background(), []string{topic})
	unlock()

	if delErr != nil {
		return c.writeError(fmt.Sprintf("ERR %s", delErr.Error()))
	}
	return c.writeInteger(1)
}

func (c *Conn) handleJsonType(args [][]byte) error {
	if len(args) < 2 || len(args) > 3 {
		return c.writeError("ERR wrong number of arguments for 'json.type' command")
	}
	topic := string(args[1])
	if len(args) == 3 && !isRootJsonPath(string(args[2])) {
		return c.writeError("ERR only root JSON path '$' or '.' is supported")
	}

	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	stored, _ := c.readStored(group, topic)
	if stored == nil {
		return c.writeNullBulk()
	}
	if stored.Type != "json" {
		return c.writeError(errWrongType)
	}

	typeName := getJsonTypeName(stored.Value)
	return c.writeBulkStr(typeName)
}

func getJsonTypeName(v any) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case bool:
		return "boolean"
	case float64, float32, int, int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8:
		return "number"
	default:
		return "object"
	}
}
