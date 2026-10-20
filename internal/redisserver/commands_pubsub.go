package redisserver

import (
	"bufio"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"monstermq.io/edge/internal/stores"
)

func (c *Conn) handlePublish(args [][]byte) error {
	if len(args) != 3 {
		return c.writeError("ERR wrong number of arguments for 'publish' command")
	}
	topic := string(args[1])
	payload := args[2]

	vw, err := c.validateWrite("PUBLISH", topic, true)
	if err != nil {
		return c.writeError(err.Error())
	}

	if err := c.publishMessage(topic, payload, vw.isNative); err != nil {
		return c.writeError(err.Error())
	}
	return c.writeInteger(0)
}

func (c *Conn) handleSubscribe(args [][]byte, isPattern bool) error {
	cmdUpper := strings.ToUpper(string(args[0]))
	if len(args) < 2 {
		return c.writeError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmdUpper)))
	}

	cmdName := "subscribe"
	if isPattern {
		cmdName = "psubscribe"
	}

	filters := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		filters[i-1] = string(args[i])
	}

	// ACL check for every filter first
	type subItem struct {
		requested   string
		mqttFilter  string
		postFilter  func(string) bool
	}
	items := make([]subItem, len(filters))

	for i, f := range filters {
		mqttFilter := f
		var postFilter func(string) bool
		if isPattern {
			mqttFilter, postFilter = redisPatternToMqtt(f)
		}
		if !c.canRead(mqttFilter) {
			return c.writeError("NOPERM no permission to subscribe to one or more topics")
		}
		items[i] = subItem{
			requested:  f,
			mqttFilter: mqttFilter,
			postFilter: postFilter,
		}
	}

	// Validate bus filters (S3.1, E7)
	if c.server.deps.Bus != nil {
		toValidate := make([]string, len(items))
		for i, item := range items {
			toValidate[i] = item.mqttFilter
		}
		if err := c.server.deps.Bus.Validate(toValidate); err != nil {
			return c.writeError(fmt.Sprintf("ERR %s", err.Error()))
		}
	}

	// Subscribe each filter in request order
	for _, item := range items {
		key := subKey{isPattern: isPattern, name: item.requested}

		c.writeMu.Lock()
		c.subsMu.Lock()

		if _, exists := c.subs[key]; exists {
			total := len(c.subs)
			c.subsMu.Unlock()
			_ = respArray([]respValue{
				respBulk([]byte(cmdName)),
				respBulk([]byte(item.requested)),
				respInteger(int64(total)),
			}).writeResp(c.writer)
			c.writeMu.Unlock()
			continue
		}

		if c.server.deps.Bus == nil {
			total := len(c.subs)
			c.subsMu.Unlock()
			_ = respArray([]respValue{
				respBulk([]byte(cmdName)),
				respBulk([]byte(item.requested)),
				respInteger(int64(total)),
			}).writeResp(c.writer)
			c.writeMu.Unlock()
			continue
		}

		c.ensureSubWriter()
		holder := &subscriptionHolder{}
		holder.active.Store(true)
		reqName := item.requested
		pf := item.postFilter
		subID := c.server.deps.Bus.SubscribeFunc([]string{item.mqttFilter}, func(msg stores.BrokerMessage) {
			if !holder.active.Load() || c.closed.Load() {
				return
			}
			if pf != nil && !pf(msg.TopicName) {
				return
			}
			select {
			case c.subCh <- pubSubDelivery{holder: holder, isPattern: isPattern, pattern: reqName, msg: msg}:
			default:
				c.subDropped.Add(1)
				if c.subOverloaded.CompareAndSwap(false, true) && c.server.deps.Logger != nil {
					c.server.deps.Logger.Warn("redis subscriber outbound queue overloaded; dropping messages", "client", c.clientID)
				}
			}
		})
		holder.id = uint64(subID)
		holder.cancel = func() {
			if holder.active.CompareAndSwap(true, false) && c.server.deps.Bus != nil {
				c.server.deps.Bus.Unsubscribe(subID)
			}
		}
		c.subs[key] = holder
		total := len(c.subs)
		c.subsMu.Unlock()

		_ = respArray([]respValue{
			respBulk([]byte(cmdName)),
			respBulk([]byte(item.requested)),
			respInteger(int64(total)),
		}).writeResp(c.writer)
		c.writeMu.Unlock()
	}

	return nil
}

func (c *Conn) ensureSubWriter() {
	c.subWriterOnce.Do(func() {
		c.subCh = make(chan pubSubDelivery, 65536)
		go c.subWriterLoop()
	})
}

func (c *Conn) subWriterLoop() {
	for {
		select {
		case <-c.subStopCh:
			return
		case d := <-c.subCh:
			if !c.writePubSubBatch(d) {
				c.Close()
				return
			}
			if c.subOverloaded.Load() && len(c.subCh) < cap(c.subCh)/4 {
				if c.subOverloaded.CompareAndSwap(true, false) {
					dropped := c.subDropped.Swap(0)
					if c.server.deps.Logger != nil {
						c.server.deps.Logger.Info("redis subscriber outbound queue overload cleared", "client", c.clientID, "dropped", dropped)
					}
				}
			}
		}
	}
}

func (c *Conn) writePubSubBatch(first pubSubDelivery) bool {
	c.writeMu.Lock()
	var err error
	sentCount := 0

	writeOne := func(d pubSubDelivery) bool {
		if !d.holder.active.Load() || !c.canRead(d.msg.TopicName) {
			return true
		}
		if err = writePubSubFrame(c.writer, d.isPattern, d.pattern, d.msg.TopicName, d.msg.Payload); err != nil {
			return false
		}
		sentCount++
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

	if err == nil && sentCount > 0 {
		err = c.flush()
	}
	c.writeMu.Unlock()

	if err != nil {
		return false
	}
	if c.server.deps.Collector != nil && sentCount > 0 {
		for i := 0; i < sentCount; i++ {
			c.server.deps.Collector.IncClientOut(c.clientID)
		}
	}
	return true
}

func writePubSubFrame(w *bufio.Writer, isPattern bool, pattern, topic string, payload []byte) error {
	var scratch [64]byte
	if isPattern {
		b := append(scratch[:0], "*4\r\n$8\r\npmessage\r\n$"...)
		b = strconv.AppendInt(b, int64(len(pattern)), 10)
		b = append(b, '\r', '\n')
		if _, err := w.Write(b); err != nil {
			return err
		}
		if _, err := w.WriteString(pattern); err != nil {
			return err
		}
		b = append(scratch[:0], "\r\n$"...)
		b = strconv.AppendInt(b, int64(len(topic)), 10)
		b = append(b, '\r', '\n')
		if _, err := w.Write(b); err != nil {
			return err
		}
		if _, err := w.WriteString(topic); err != nil {
			return err
		}
	} else {
		b := append(scratch[:0], "*3\r\n$7\r\nmessage\r\n$"...)
		b = strconv.AppendInt(b, int64(len(topic)), 10)
		b = append(b, '\r', '\n')
		if _, err := w.Write(b); err != nil {
			return err
		}
		if _, err := w.WriteString(topic); err != nil {
			return err
		}
	}
	b := append(scratch[:0], "\r\n$"...)
	b = strconv.AppendInt(b, int64(len(payload)), 10)
	b = append(b, '\r', '\n')
	if _, err := w.Write(b); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	_, err := w.WriteString("\r\n")
	return err
}

func (c *Conn) handleUnsubscribe(args [][]byte, isPattern bool) error {
	cmdName := "unsubscribe"
	if isPattern {
		cmdName = "punsubscribe"
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if len(args) > 1 {
		for i := 1; i < len(args); i++ {
			target := string(args[i])
			key := subKey{isPattern: isPattern, name: target}

			c.subsMu.Lock()
			if holder, exists := c.subs[key]; exists {
				holder.cancel()
				delete(c.subs, key)
			}
			remaining := len(c.subs)
			c.subsMu.Unlock()

			if err := (respArray([]respValue{
				respBulk([]byte(cmdName)),
				respBulk([]byte(target)),
				respInteger(int64(remaining)),
			}).writeResp(c.writer)); err != nil {
				return err
			}
		}
		return nil
	}

	// No arguments: unsubscribe from all of this kind
	c.subsMu.Lock()
	var toRemove []subKey
	for k := range c.subs {
		if k.isPattern == isPattern {
			toRemove = append(toRemove, k)
		}
	}

	if len(toRemove) == 0 {
		remaining := len(c.subs)
		c.subsMu.Unlock()
		return respArray([]respValue{
			respBulk([]byte(cmdName)),
			respBulk(nil),
			respInteger(int64(remaining)),
		}).writeResp(c.writer)
	}

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i].name < toRemove[j].name
	})

	for _, k := range toRemove {
		if holder, exists := c.subs[k]; exists {
			holder.cancel()
			delete(c.subs, k)
		}
		remaining := len(c.subs)
		if err := (respArray([]respValue{
			respBulk([]byte(cmdName)),
			respBulk([]byte(k.name)),
			respInteger(int64(remaining)),
		}).writeResp(c.writer)); err != nil {
			c.subsMu.Unlock()
			return err
		}
	}
	c.subsMu.Unlock()
	return nil
}
