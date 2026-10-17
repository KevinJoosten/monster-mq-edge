package redisserver

import (
	"context"
	"fmt"
	"sort"
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

		subID, msgCh, overflowCh := c.server.deps.Bus.SubscribeWithOverflow([]string{item.mqttFilter}, 1024)
		ctx, cancel := context.WithCancel(context.Background())
		holder := &subscriptionHolder{
			id:     uint64(subID),
			cancel: cancel,
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

		go c.runSubscription(ctx, subID, key, item.postFilter, msgCh, overflowCh)
	}

	return nil
}

func (c *Conn) runSubscription(
	ctx context.Context,
	subID int,
	key subKey,
	postFilter func(string) bool,
	msgCh <-chan stores.BrokerMessage,
	overflowCh <-chan struct{},
) {
	defer func() {
		if c.server.deps.Bus != nil {
			c.server.deps.Bus.Unsubscribe(subID)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-overflowCh:
			// E7: subscriber overflow, close connection
			if c.server.deps.Logger != nil {
				c.server.deps.Logger.Warn("redis subscriber overflow, closing connection", "client", c.clientID)
			}
			c.writeMu.Lock()
			_ = c.writeError("ERR slow subscriber, messages dropped, closing connection")
			_ = c.flush()
			c.writeMu.Unlock()
			c.Close()
			return
		case msg, ok := <-msgCh:
			if !ok {
				return
			}
			if postFilter != nil && !postFilter(msg.TopicName) {
				continue
			}
			// F12: delivery-time ACL check
			if !c.canRead(msg.TopicName) {
				continue
			}

			c.writeMu.Lock()
			var err error
			if key.isPattern {
				err = respArray([]respValue{
					respBulk([]byte("pmessage")),
					respBulk([]byte(key.name)),
					respBulk([]byte(msg.TopicName)),
					respBulk(msg.Payload),
				}).writeResp(c.writer)
			} else {
				err = respArray([]respValue{
					respBulk([]byte("message")),
					respBulk([]byte(msg.TopicName)),
					respBulk(msg.Payload),
				}).writeResp(c.writer)
			}
			if err == nil {
				_ = c.flush()
			}
			c.writeMu.Unlock()

			if err != nil {
				c.Close()
				return
			}

			// F11: out-counter
			if c.server.deps.Collector != nil {
				c.server.deps.Collector.IncClientOut(c.clientID)
			}
		}
	}
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
