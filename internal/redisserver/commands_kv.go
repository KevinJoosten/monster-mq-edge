package redisserver

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

func (c *Conn) handleGet(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'get' command")
	}
	topic := string(args[1])
	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}

	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	if group == nil || group.LastValue() == nil {
		return c.writeNullBulk()
	}

	msg, err := group.LastValue().Get(context.Background(), topic)
	if err != nil || msg == nil {
		return c.writeNullBulk()
	}
	return c.writeBulk(msg.Payload)
}

func (c *Conn) handleGetRange(args [][]byte) error {
	cmdName := strings.ToLower(string(args[0]))
	if len(args) != 4 {
		return c.writeError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmdName))
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
	if group == nil || group.LastValue() == nil {
		return c.writeBulk([]byte{})
	}

	msg, err := group.LastValue().Get(context.Background(), topic)
	if err != nil || msg == nil {
		return c.writeBulk([]byte{})
	}

	sliced := sliceRange(msg.Payload, start, end)
	return c.writeBulk(sliced)
}

func sliceRange(payload []byte, rawStart, rawEnd int64) []byte {
	if len(payload) == 0 {
		return []byte{}
	}
	size := int64(len(payload))
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
		return []byte{}
	}
	if end >= size {
		end = size - 1
	}
	return payload[start : end+1]
}

func (c *Conn) handleMget(args [][]byte) error {
	if len(args) < 2 {
		return c.writeError("ERR wrong number of arguments for 'mget' command")
	}

	topics := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		topics[i-1] = string(args[i])
	}

	for _, t := range topics {
		if !c.canRead(t) {
			return c.writeError("NOPERM no permission to read one or more topics")
		}
	}

	if c.selectedArchiveGroup() == nil {
		return nil
	}

	items := make([]respValue, len(topics))
	for i, t := range topics {
		group := c.resolveGroupForRead(t)
		if group == nil || group.LastValue() == nil {
			items[i] = respBulk(nil)
			continue
		}
		msg, err := group.LastValue().Get(context.Background(), t)
		if err != nil || msg == nil {
			items[i] = respBulk(nil)
		} else {
			items[i] = respBulk(msg.Payload)
		}
	}
	return c.writeArray(items)
}

func (c *Conn) handleSet(args [][]byte) error {
	if len(args) < 3 {
		return c.writeError("ERR wrong number of arguments for 'set' command")
	}
	topic := string(args[1])
	payload := args[2]

	vw, err := c.validateWrite("SET", topic, false)
	if err != nil {
		return c.writeError(err.Error())
	}

	if vw.isNative {
		if err := c.publishMessage(vw.setTopic, payload, true); err != nil {
			return c.writeError(err.Error())
		}
		return c.writeSimple("OK")
	}

	groupName := vw.group.Name()
	unlock := c.server.lockStripes(groupName, []string{topic})
	pubErr := c.publishMessage(topic, payload, false)
	unlock()

	if pubErr != nil {
		return c.writeError(pubErr.Error())
	}
	return c.writeSimple("OK")
}

func (c *Conn) handleMset(args [][]byte) error {
	if len(args) < 3 || len(args)%2 == 0 {
		return c.writeError("ERR wrong number of arguments for 'mset' command")
	}

	count := (len(args) - 1) / 2
	topics := make([]string, count)
	payloads := make([][]byte, count)
	for i := 0; i < count; i++ {
		topics[i] = string(args[1+i*2])
		payloads[i] = args[2+i*2]
	}

	// F8: validate all pairs before publishing any
	vws := make([]*validatedWrite, count)
	for i := 0; i < count; i++ {
		vw, err := c.validateWrite("MSET", topics[i], false)
		if err != nil {
			return c.writeError(err.Error())
		}
		vws[i] = vw
	}

	// Group keys by groupName for stripe locking
	groupKeys := make(map[string][]string)
	for i, vw := range vws {
		if !vw.isNative {
			gName := vw.group.Name()
			groupKeys[gName] = append(groupKeys[gName], topics[i])
		}
	}

	var unlocks []func()
	for gName, keys := range groupKeys {
		unlocks = append(unlocks, c.server.lockStripes(gName, keys))
	}

	var pubErr error
	for i := 0; i < count; i++ {
		vw := vws[i]
		if vw.isNative {
			if err := c.publishMessage(vw.setTopic, payloads[i], true); err != nil {
				pubErr = err
				break
			}
		} else {
			if err := c.publishMessage(topics[i], payloads[i], false); err != nil {
				pubErr = err
				break
			}
		}
	}

	for _, u := range unlocks {
		u()
	}

	if pubErr != nil {
		return c.writeError(pubErr.Error())
	}
	return c.writeSimple("OK")
}

func (c *Conn) handleDel(args [][]byte) error {
	if len(args) < 2 {
		return c.writeError("ERR wrong number of arguments for 'del' command")
	}

	topics := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		topics[i-1] = string(args[i])
	}

	// Step 2 native rule check (E4)
	for _, t := range topics {
		if err := c.checkNativeTypedWrite("del", t); err != nil {
			return c.writeError(err.Error())
		}
	}

	for _, t := range topics {
		if !c.canWrite(t) {
			return c.writeError("NOPERM no permission to delete one or more topics")
		}
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

	unlock := c.server.lockStripes(group.Name(), topics)
	var count int64
	for _, t := range topics {
		msg, err := store.Get(context.Background(), t)
		if err == nil && msg != nil {
			count++
		}
	}
	delErr := store.DelAll(context.Background(), topics)
	unlock()

	if delErr != nil {
		return c.writeError(fmt.Sprintf("ERR %s", delErr.Error()))
	}
	return c.writeInteger(count)
}

func (c *Conn) handleExists(args [][]byte) error {
	if len(args) < 2 {
		return c.writeError("ERR wrong number of arguments for 'exists' command")
	}

	topics := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		topics[i-1] = string(args[i])
	}

	for _, t := range topics {
		if !c.canRead(t) {
			return c.writeError("NOPERM no permission to read one or more topics")
		}
	}

	if c.selectedArchiveGroup() == nil {
		return nil
	}

	var count int64
	for _, t := range topics {
		group := c.resolveGroupForRead(t)
		if group == nil || group.LastValue() == nil {
			continue
		}
		msg, err := group.LastValue().Get(context.Background(), t)
		if err == nil && msg != nil {
			count++
		}
	}
	return c.writeInteger(count)
}

func (c *Conn) handleTtl(args [][]byte) error {
	cmdName := strings.ToLower(string(args[0]))
	if len(args) != 2 {
		return c.writeError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmdName))
	}
	topic := string(args[1])
	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}

	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	if group == nil || group.LastValue() == nil {
		return c.writeInteger(-2)
	}

	msg, err := group.LastValue().Get(context.Background(), topic)
	if err != nil || msg == nil {
		return c.writeInteger(-2)
	}
	return c.writeInteger(-1)
}

func (c *Conn) handleScan(args [][]byte) error {
	match := "#"
	for i := 2; i+1 < len(args); i += 2 {
		if strings.EqualFold(string(args[i]), "MATCH") {
			match = string(args[i+1])
		}
	}

	if c.selectedArchiveGroup() == nil {
		return nil
	}

	topicPattern, postFilter := redisPatternToMqtt(match)
	group := c.resolveGroupForRead(topicPattern)
	if group == nil || group.LastValue() == nil {
		return c.writeArray([]respValue{
			respBulk([]byte("0")),
			respArray(nil),
		})
	}

	keysLimit := c.server.deps.Cfg.RedisServer.EffectiveKeysLimit()
	store := group.LastValue()

	var topics []string
	err := store.FindMatchingTopics(context.Background(), topicPattern, func(t string) bool {
		if postFilter == nil || postFilter(t) {
			if c.canRead(t) {
				topics = append(topics, t)
			}
		}
		return len(topics) < keysLimit
	})

	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}

	items := make([]respValue, len(topics))
	for i, t := range topics {
		items[i] = respBulk([]byte(t))
	}

	return c.writeArray([]respValue{
		respBulk([]byte("0")),
		respArray(items),
	})
}

func (c *Conn) handleKeys(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'keys' command")
	}

	topicPattern, postFilter := redisPatternToMqtt(string(args[1]))
	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topicPattern)
	if group == nil || group.LastValue() == nil {
		return c.writeArray(nil)
	}

	keysLimit := c.server.deps.Cfg.RedisServer.EffectiveKeysLimit()
	store := group.LastValue()

	var topics []string
	err := store.FindMatchingTopics(context.Background(), topicPattern, func(t string) bool {
		if postFilter == nil || postFilter(t) {
			if c.canRead(t) {
				topics = append(topics, t)
			}
		}
		return len(topics) < keysLimit
	})

	if err != nil {
		return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
	}

	items := make([]respValue, len(topics))
	for i, t := range topics {
		items[i] = respBulk([]byte(t))
	}
	return c.writeArray(items)
}

func (c *Conn) handleType(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'type' command")
	}
	topic := string(args[1])
	if !c.canRead(topic) {
		return c.writeError("NOPERM no permission to read topic")
	}

	if c.selectedArchiveGroup() == nil {
		return nil
	}

	group := c.resolveGroupForRead(topic)
	if group == nil || group.LastValue() == nil {
		return c.writeSimple("none")
	}

	msg, err := group.LastValue().Get(context.Background(), topic)
	if err != nil || msg == nil {
		return c.writeSimple("none")
	}

	stored := decodeStoredValue(msg.Payload)
	if stored == nil {
		return c.writeSimple("none")
	}
	return c.writeSimple(stored.Type)
}
