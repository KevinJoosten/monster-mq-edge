package redisserver

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/winccoanative"
)

// resolveGroupForRead resolves the archive group that serves a read for topic.
// If the selected DB is DefaultDb, it applies E2 fallback via ResolveGroupForTopic.
func (c *Conn) resolveGroupForRead(topic string) *archive.Group {
	if c.server.deps.Archives == nil {
		return nil
	}
	defaultDb := c.server.deps.Cfg.RedisServer.DefaultDb
	group := c.server.deps.Archives.ByRedisDB(c.selectedDb)
	if c.selectedDb == defaultDb {
		groupName := ""
		if group != nil {
			groupName = group.Name()
		}
		if resolved := c.server.deps.Archives.ResolveGroupForTopic(groupName, topic, false); resolved != nil {
			return resolved
		}
	}
	return group
}

// selectedArchiveGroup returns the archive group for the selected DB without redirection.
// If not mapped, writes "ERR selected DB is not mapped to an archive group" and returns nil.
func (c *Conn) selectedArchiveGroup() *archive.Group {
	if c.server.deps.Archives == nil {
		_ = c.writeError("ERR selected DB is not mapped to an archive group")
		return nil
	}
	group := c.server.deps.Archives.ByRedisDB(c.selectedDb)
	if group == nil {
		_ = c.writeError("ERR selected DB is not mapped to an archive group")
		return nil
	}
	return group
}

// isGroupReadOnly returns true if the archive group has LastValReadOnly set
// or uses the WINCCOA last-value store (E10).
func isGroupReadOnly(group *archive.Group) bool {
	if group == nil {
		return false
	}
	cfg := group.Config()
	if cfg.LastValReadOnly || cfg.LastValType == stores.MessageStoreWinCCOA {
		return true
	}
	if lv := group.LastValue(); lv != nil && lv.Type() == stores.MessageStoreWinCCOA {
		return true
	}
	return false
}

// checkNativeTypedWrite checks rule E4: typed writes and DEL are not supported on WinCC OA topics.
func (c *Conn) checkNativeTypedWrite(cmd, topic string) error {
	if c.server.deps.NativeNames != nil {
		names := c.server.deps.NativeNames()
		if names.Classify(topic) != winccoanative.KindOther {
			return fmt.Errorf("ERR '%s' is not supported on WinCC OA topics, use SET or MSET", strings.ToLower(cmd))
		}
	}
	return nil
}

type validatedWrite struct {
	topic    string
	isNative bool
	setTopic string
	group    *archive.Group
}

// validateWrite performs steps 1-6 of the write checks (Section 260 of plan).
func (c *Conn) validateWrite(cmd, topic string, isPublish bool) (*validatedWrite, error) {
	// Step 1: Wildcards
	if strings.Contains(topic, "+") || strings.Contains(topic, "#") {
		return nil, errors.New("ERR topic must not contain wildcard characters")
	}

	// Step 2: Native rule
	upperCmd := strings.ToUpper(cmd)
	if upperCmd == "SET" || upperCmd == "MSET" {
		if c.server.deps.NativeValueWrite != nil {
			if setTopic, ok := c.server.deps.NativeValueWrite(topic); ok {
				// S2 path: check ACL on /set topic, skip steps 3-6, force retain to false
				if !c.canWrite(setTopic) {
					return nil, errors.New("NOPERM no permission to publish topic")
				}
				return &validatedWrite{
					topic:    topic,
					isNative: true,
					setTopic: setTopic,
				}, nil
			}
		}
	} else if upperCmd != "PUBLISH" {
		// Typed write
		if err := c.checkNativeTypedWrite(cmd, topic); err != nil {
			return nil, err
		}
	}

	// Step 3: DB group
	var group *archive.Group
	if c.server.deps.Archives != nil {
		group = c.server.deps.Archives.ByRedisDB(c.selectedDb)
	}
	if group == nil {
		if !isPublish {
			return nil, errors.New("ERR selected DB is not mapped to an archive group")
		}
	} else {
		// Step 4: Topic filter
		if !group.MatchesFilter(topic) {
			return nil, errors.New("ERR topic is outside selected archive group topic filter")
		}

		// Step 5: Read-only check (F7; skipped for PUBLISH)
		if !isPublish && isGroupReadOnly(group) {
			return nil, fmt.Errorf("ERR last value store of archive group [%s] is read-only", group.Name())
		}
	}

	// Step 6: Publish ACL
	if !c.canWrite(topic) {
		return nil, errors.New("NOPERM no permission to publish topic")
	}

	return &validatedWrite{
		topic: topic,
		group: group,
	}, nil
}

// publishMessage publishes msg via S1 ProtocolPublish, handles E5 error formatting and F11 in-counter.
func (c *Conn) publishMessage(topic string, payload []byte, isNative bool) error {
	if c.server.deps.Publish == nil {
		return errors.New("ERR publish not configured")
	}
	retain := c.server.deps.Cfg.RedisServer.SetRetained
	if isNative {
		retain = false
	}
	msg := stores.BrokerMessage{
		TopicName: topic,
		Payload:   payload,
		QoS:       byte(c.server.deps.Cfg.RedisServer.EffectivePublishQos()),
		IsRetain:  retain,
		ClientID:  c.publisherID,
		Time:      time.Now(),
	}
	if err := c.server.deps.Publish(c.publisherID, msg); err != nil {
		// E5: replace CRLF with spaces
		reason := strings.ReplaceAll(strings.ReplaceAll(err.Error(), "\r", " "), "\n", " ")
		return fmt.Errorf("ERR %s", reason)
	}
	if c.server.deps.Collector != nil {
		c.server.deps.Collector.IncClientIn(c.clientID)
	}
	return nil
}
