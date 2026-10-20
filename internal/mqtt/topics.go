package mqtt

import (
	"strings"
	"sync"
	"sync/atomic"

	"monstermq.io/edge/internal/mqtt/packets"
)

var (
	SharePrefix = "$SHARE" // the prefix indicating a share topic
	SysPrefix   = "$SYS"   // the prefix indicating a system info topic
)

// TopicAliases contains inbound and outbound topic alias registrations.
type TopicAliases struct {
	Inbound  *InboundTopicAliases
	Outbound *OutboundTopicAliases
}

// NewTopicAliases returns an instance of TopicAliases.
func NewTopicAliases(topicAliasMaximum uint16) TopicAliases {
	return TopicAliases{
		Inbound:  NewInboundTopicAliases(topicAliasMaximum),
		Outbound: NewOutboundTopicAliases(topicAliasMaximum),
	}
}

// NewInboundTopicAliases returns a pointer to InboundTopicAliases.
func NewInboundTopicAliases(topicAliasMaximum uint16) *InboundTopicAliases {
	return &InboundTopicAliases{
		maximum:  topicAliasMaximum,
		internal: map[uint16]string{},
	}
}

// InboundTopicAliases contains a map of topic aliases received from the client.
type InboundTopicAliases struct {
	internal map[uint16]string
	sync.RWMutex
	maximum uint16
}

// Set sets a new alias for a specific topic.
func (a *InboundTopicAliases) Set(id uint16, topic string) string {
	a.Lock()
	defer a.Unlock()

	if a.maximum == 0 {
		return topic
	}

	if topic == "" {
		return a.internal[id] // empty when the alias is unknown; never store an empty mapping
	}

	a.internal[id] = topic
	return topic
}

// OutboundTopicAliases contains a map of topic aliases sent from the broker to the client.
type OutboundTopicAliases struct {
	internal map[string]uint16
	sync.RWMutex
	cursor  uint32
	maximum uint16
}

// NewOutboundTopicAliases returns a pointer to OutboundTopicAliases.
func NewOutboundTopicAliases(topicAliasMaximum uint16) *OutboundTopicAliases {
	return &OutboundTopicAliases{
		maximum:  topicAliasMaximum,
		internal: map[string]uint16{},
	}
}

// Set sets a new topic alias for a topic and returns the alias value, and a boolean
// indicating if the alias already existed.
func (a *OutboundTopicAliases) Set(topic string) (uint16, bool) {
	a.Lock()
	defer a.Unlock()

	if a.maximum == 0 {
		return 0, false
	}

	if i, ok := a.internal[topic]; ok {
		return i, true
	}

	i := atomic.LoadUint32(&a.cursor)
	if i+1 > uint32(a.maximum) {
		return 0, false
	}

	a.internal[topic] = uint16(i) + 1
	atomic.StoreUint32(&a.cursor, i+1)
	return uint16(i) + 1, false
}

// InlineSubFn is the signature for a callback function which will be called
// when an inline client receives a message on a topic it is subscribed to.
// The sub argument contains information about the subscription that was matched for any filters.
type InlineSubFn func(cl *Client, sub packets.Subscription, pk packets.Packet)

type InlineSubscription struct {
	packets.Subscription
	Handler InlineSubFn
}

// Subscriptions is a map of subscriptions keyed on filter (for client state).
type Subscriptions struct {
	internal map[string]packets.Subscription
	snap     atomic.Pointer[map[string]packets.Subscription]
	sync.RWMutex
}

// NewSubscriptions returns a new instance of Subscriptions.
func NewSubscriptions() *Subscriptions {
	return &Subscriptions{
		internal: map[string]packets.Subscription{},
	}
}

func (s *Subscriptions) refreshSnapLocked() {
	if len(s.internal) == 0 {
		s.snap.Store(nil)
		return
	}
	m := make(map[string]packets.Subscription, len(s.internal))
	for k, v := range s.internal {
		m[k] = v
	}
	s.snap.Store(&m)
}

// Add adds a new subscription for a client or filter id.
func (s *Subscriptions) Add(id string, val packets.Subscription) {
	s.Lock()
	s.internal[id] = val
	s.refreshSnapLocked()
	s.Unlock()
}

// Each calls fn for every subscription using the lock-free snapshot.
func (s *Subscriptions) Each(fn func(id string, sub packets.Subscription)) {
	m := s.snap.Load()
	if m == nil {
		return
	}
	for k, v := range *m {
		fn(k, v)
	}
}

// GetAll returns all subscriptions.
func (s *Subscriptions) GetAll() map[string]packets.Subscription {
	s.RLock()
	defer s.RUnlock()
	m := map[string]packets.Subscription{}
	for k, v := range s.internal {
		m[k] = v
	}
	return m
}

// Get returns a subscription for a specific client or filter id.
func (s *Subscriptions) Get(id string) (val packets.Subscription, ok bool) {
	s.RLock()
	defer s.RUnlock()
	val, ok = s.internal[id]
	return val, ok
}

// Len returns the number of subscriptions.
func (s *Subscriptions) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.internal)
}

// Delete removes a subscription by client or filter id.
func (s *Subscriptions) Delete(id string) {
	s.Lock()
	delete(s.internal, id)
	s.refreshSnapLocked()
	s.Unlock()
}

// ClientSubscriptions is a map of aggregated subscriptions for a client.
type ClientSubscriptions map[string]packets.Subscription

// Subscribers contains the shared and non-shared subscribers matching a topic.
type Subscribers struct {
	Shared              map[string]map[string]packets.Subscription
	SharedSelected      map[string]packets.Subscription
	Subscriptions       map[string]packets.Subscription
	InlineSubscriptions map[int]InlineSubscription
}

// SelectShared returns one subscriber for each shared subscription group.
func (s *Subscribers) SelectShared() {
	s.SharedSelected = map[string]packets.Subscription{}
	for _, subs := range s.Shared {
		for client, sub := range subs {
			cls, ok := s.SharedSelected[client]
			if !ok {
				cls = sub
			}

			s.SharedSelected[client] = cls.Merge(sub)
			break
		}
	}
}

// MergeSharedSelected merges the selected subscribers for a shared subscription group
// and the non-shared subscribers, to ensure that no subscriber gets multiple messages
// due to having both types of subscription matching the same filter.
func (s *Subscribers) MergeSharedSelected() {
	if s.Subscriptions == nil && len(s.SharedSelected) > 0 {
		s.Subscriptions = map[string]packets.Subscription{}
	}
	for client, sub := range s.SharedSelected {
		cls, ok := s.Subscriptions[client]
		if !ok {
			cls = sub
		}

		s.Subscriptions[client] = cls.Merge(sub)
	}
}

type exactClientSub struct {
	client string
	sub    packets.Subscription
}

type exactTopicEntry struct {
	subs   []exactClientSub
	inline []InlineSubscription
}

type exactShard struct {
	internal map[string]*exactTopicEntry
	snap     atomic.Pointer[map[string]*exactTopicEntry]
}

func exactShardIdx(topic string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(topic); i++ {
		h ^= uint32(topic[i])
		h *= 16777619
	}
	return h & 63
}

func isWildcardFilter(filter string) bool {
	return strings.IndexByte(filter, '+') >= 0 || strings.IndexByte(filter, '#') >= 0
}

type wildcardClientEntry struct {
	client string
	subs   []packets.Subscription
}

type sharedFilterEntry struct {
	plainFilter string
	clients     map[string]packets.Subscription
}

type wildcardSnapshot struct {
	clients []wildcardClientEntry
	inline  []InlineSubscription
	shared  []sharedFilterEntry
}

// TopicsIndex manages exact and wildcard subscriptions and retained messages.
type TopicsIndex struct {
	mu              sync.Mutex
	Retained        *packets.Packets
	exact           [64]exactShard
	wildcardClients map[string]map[string]packets.Subscription
	wildcardInline  map[string]map[int]InlineSubscription
	shared          map[string]map[string]packets.Subscription // keyed by group + "\x00" + plainFilter
	wildcardSnap    atomic.Pointer[wildcardSnapshot]
	observer        InterestObserver
}

// NewTopicsIndex returns a pointer to a new instance of TopicsIndex.
func NewTopicsIndex() *TopicsIndex {
	return &TopicsIndex{
		Retained:        packets.NewPackets(),
		wildcardClients: make(map[string]map[string]packets.Subscription),
		wildcardInline:  make(map[string]map[int]InlineSubscription),
		shared:          make(map[string]map[string]packets.Subscription),
	}
}

func (x *TopicsIndex) rebuildWildcardSnapLocked() {
	if len(x.wildcardClients) == 0 && len(x.wildcardInline) == 0 && len(x.shared) == 0 {
		x.wildcardSnap.Store(nil)
		return
	}
	snap := &wildcardSnapshot{}
	if len(x.wildcardInline) > 0 {
		for _, byID := range x.wildcardInline {
			for _, inl := range byID {
				snap.inline = append(snap.inline, inl)
			}
		}
	}
	if len(x.wildcardClients) > 0 {
		snap.clients = make([]wildcardClientEntry, 0, len(x.wildcardClients))
		for client, subsMap := range x.wildcardClients {
			if len(subsMap) == 0 {
				continue
			}
			subs := make([]packets.Subscription, 0, len(subsMap))
			for _, sub := range subsMap {
				subs = append(subs, sub)
			}
			snap.clients = append(snap.clients, wildcardClientEntry{
				client: client,
				subs:   subs,
			})
		}
	}
	if len(x.shared) > 0 {
		snap.shared = make([]sharedFilterEntry, 0, len(x.shared))
		for key, clientsMap := range x.shared {
			if len(clientsMap) == 0 {
				continue
			}
			sep := strings.IndexByte(key, 0)
			if sep < 0 {
				continue
			}
			plain := key[sep+1:]
			cp := make(map[string]packets.Subscription, len(clientsMap))
			for cl, sub := range clientsMap {
				cp[cl] = sub
			}
			snap.shared = append(snap.shared, sharedFilterEntry{
				plainFilter: plain,
				clients:     cp,
			})
		}
	}
	x.wildcardSnap.Store(snap)
}

func matchMQTTTopic(pattern, topic string) bool {
	if len(topic) > 0 && topic[0] == '$' && len(pattern) > 0 && (pattern[0] == '+' || pattern[0] == '#') {
		return false // [MQTT-4.7.1-1] [MQTT-4.7.1-2]
	}
	if pattern == "#" || pattern == topic {
		return true
	}
	pi, ti := 0, 0
	plen, tlen := len(pattern), len(topic)
	for pi <= plen {
		ps := pi
		for pi < plen && pattern[pi] != '/' {
			pi++
		}
		pseg := pattern[ps:pi]
		if pseg == "#" {
			return true
		}
		if ti > tlen {
			return false
		}
		ts := ti
		for ti < tlen && topic[ti] != '/' {
			ti++
		}
		tseg := topic[ts:ti]
		if pseg != "+" && pseg != tseg {
			return false
		}
		pi++
		ti++
	}
	return ti > tlen
}

func (x *TopicsIndex) refreshExactShardLocked(s *exactShard) {
	if len(s.internal) == 0 {
		s.snap.Store(nil)
		return
	}
	m := make(map[string]*exactTopicEntry, len(s.internal))
	for k, v := range s.internal {
		m[k] = v
	}
	s.snap.Store(&m)
}

func (x *TopicsIndex) upsertExactSubLocked(topic, client string, sub packets.Subscription) (existed bool) {
	s := &x.exact[exactShardIdx(topic)]
	if s.internal == nil {
		s.internal = make(map[string]*exactTopicEntry)
	}
	prev := s.internal[topic]
	var nextSubs []exactClientSub
	var nextInline []InlineSubscription
	if prev != nil {
		nextInline = prev.inline
		nextSubs = make([]exactClientSub, len(prev.subs))
		for i, cs := range prev.subs {
			if cs.client == client {
				nextSubs[i] = exactClientSub{client: client, sub: sub}
				existed = true
			} else {
				nextSubs[i] = cs
			}
		}
		if !existed {
			nextSubs = append(nextSubs, exactClientSub{client: client, sub: sub})
		}
	} else {
		nextSubs = []exactClientSub{{client: client, sub: sub}}
	}
	s.internal[topic] = &exactTopicEntry{subs: nextSubs, inline: nextInline}
	x.refreshExactShardLocked(s)
	return existed
}

func (x *TopicsIndex) deleteExactSubLocked(topic, client string) (existed bool) {
	s := &x.exact[exactShardIdx(topic)]
	if s.internal == nil {
		return false
	}
	prev := s.internal[topic]
	if prev == nil {
		return false
	}
	nextSubs := make([]exactClientSub, 0, len(prev.subs))
	for _, cs := range prev.subs {
		if cs.client == client {
			existed = true
		} else {
			nextSubs = append(nextSubs, cs)
		}
	}
	if !existed {
		return false
	}
	if len(nextSubs) == 0 && len(prev.inline) == 0 {
		delete(s.internal, topic)
	} else {
		s.internal[topic] = &exactTopicEntry{subs: nextSubs, inline: prev.inline}
	}
	x.refreshExactShardLocked(s)
	return true
}

func (x *TopicsIndex) upsertExactInlineLocked(topic string, sub InlineSubscription) (existed bool) {
	s := &x.exact[exactShardIdx(topic)]
	if s.internal == nil {
		s.internal = make(map[string]*exactTopicEntry)
	}
	prev := s.internal[topic]
	var nextSubs []exactClientSub
	var nextInline []InlineSubscription
	if prev != nil {
		nextSubs = prev.subs
		nextInline = make([]InlineSubscription, len(prev.inline))
		for i, inl := range prev.inline {
			if inl.Identifier == sub.Identifier {
				nextInline[i] = sub
				existed = true
			} else {
				nextInline[i] = inl
			}
		}
		if !existed {
			nextInline = append(nextInline, sub)
		}
	} else {
		nextInline = []InlineSubscription{sub}
	}
	s.internal[topic] = &exactTopicEntry{subs: nextSubs, inline: nextInline}
	x.refreshExactShardLocked(s)
	return existed
}

func (x *TopicsIndex) deleteExactInlineLocked(topic string, id int) (existed bool) {
	s := &x.exact[exactShardIdx(topic)]
	if s.internal == nil {
		return false
	}
	prev := s.internal[topic]
	if prev == nil {
		return false
	}
	nextInline := make([]InlineSubscription, 0, len(prev.inline))
	for _, inl := range prev.inline {
		if inl.Identifier == id {
			existed = true
		} else {
			nextInline = append(nextInline, inl)
		}
	}
	if !existed {
		return false
	}
	if len(prev.subs) == 0 && len(nextInline) == 0 {
		delete(s.internal, topic)
	} else {
		s.internal[topic] = &exactTopicEntry{subs: prev.subs, inline: nextInline}
	}
	x.refreshExactShardLocked(s)
	return true
}

func (x *TopicsIndex) getExact(topic string) *exactTopicEntry {
	s := &x.exact[exactShardIdx(topic)]
	m := s.snap.Load()
	if m == nil {
		return nil
	}
	return (*m)[topic]
}

// InterestObserver is notified after a subscription is added to or removed from the index.
// Calls happen after the index lock is released. For shared subscriptions filter is the
// filter without the $share/group prefix.
type InterestObserver interface {
	SubscriptionAdded(client string, filter string, shareGroup string, inline bool, inlineID int)
	SubscriptionRemoved(client string, filter string, shareGroup string, inline bool, inlineID int)
}

// SetObserver installs the interest observer. Call it before the server starts.
func (x *TopicsIndex) SetObserver(o InterestObserver) {
	x.observer = o
}

// SplitSharedFilter returns the group and plain filter of a $share filter.
func SplitSharedFilter(filter string) (group, plain string) { return splitShared(filter) }

func splitShared(filter string) (group, plain string) {
	i := strings.IndexByte(filter, '/')
	if i < 0 {
		return "", ""
	}
	rest := filter[i+1:]
	j := strings.IndexByte(rest, '/')
	if j < 0 {
		return rest, ""
	}
	return rest[:j], rest[j+1:]
}

// InlineSubscribe adds a new internal subscription for a topic filter, returning
// true if the subscription was new.
func (x *TopicsIndex) InlineSubscribe(subscription InlineSubscription) bool {
	var existed bool
	x.mu.Lock()
	if isWildcardFilter(subscription.Filter) {
		byID := x.wildcardInline[subscription.Filter]
		if byID == nil {
			byID = make(map[int]InlineSubscription)
			x.wildcardInline[subscription.Filter] = byID
		} else {
			_, existed = byID[subscription.Identifier]
		}
		byID[subscription.Identifier] = subscription
		x.rebuildWildcardSnapLocked()
	} else {
		existed = x.upsertExactInlineLocked(subscription.Filter, subscription)
	}
	x.mu.Unlock()

	if !existed && x.observer != nil {
		x.observer.SubscriptionAdded("", subscription.Filter, "", true, subscription.Identifier)
	}
	return !existed
}

// InlineUnsubscribe removes an internal subscription for a topic filter associated with a specific client,
// returning true if the subscription existed.
func (x *TopicsIndex) InlineUnsubscribe(id int, filter string) bool {
	var existed bool
	x.mu.Lock()
	if isWildcardFilter(filter) {
		if byID := x.wildcardInline[filter]; byID != nil {
			if _, existed = byID[id]; existed {
				delete(byID, id)
				if len(byID) == 0 {
					delete(x.wildcardInline, filter)
				}
				x.rebuildWildcardSnapLocked()
			}
		}
	} else {
		existed = x.deleteExactInlineLocked(filter, id)
	}
	x.mu.Unlock()

	if !existed {
		return false
	}
	if x.observer != nil {
		x.observer.SubscriptionRemoved("", filter, "", true, id)
	}
	return true
}

// Subscribe adds a new subscription for a client to a topic filter, returning
// true if the subscription was new.
func (x *TopicsIndex) Subscribe(client string, subscription packets.Subscription) bool {
	var existed bool
	prefix, _ := isolateParticle(subscription.Filter, 0)
	shared := strings.EqualFold(prefix, SharePrefix)

	x.mu.Lock()
	if shared {
		group, plain := splitShared(subscription.Filter)
		key := group + "\x00" + plain
		clientsMap := x.shared[key]
		if clientsMap == nil {
			clientsMap = make(map[string]packets.Subscription)
			x.shared[key] = clientsMap
		} else {
			_, existed = clientsMap[client]
		}
		clientsMap[client] = subscription
		x.rebuildWildcardSnapLocked()
	} else if isWildcardFilter(subscription.Filter) {
		cm := x.wildcardClients[client]
		if cm == nil {
			cm = make(map[string]packets.Subscription)
			x.wildcardClients[client] = cm
		} else {
			_, existed = cm[subscription.Filter]
		}
		cm[subscription.Filter] = subscription
		x.rebuildWildcardSnapLocked()
	} else {
		existed = x.upsertExactSubLocked(subscription.Filter, client, subscription)
	}
	x.mu.Unlock()

	if !existed && x.observer != nil {
		if shared {
			group, plain := splitShared(subscription.Filter)
			x.observer.SubscriptionAdded(client, plain, group, false, 0)
		} else {
			x.observer.SubscriptionAdded(client, subscription.Filter, "", false, 0)
		}
	}
	return !existed
}

// Unsubscribe removes a subscription filter for a client, returning true if the
// subscription existed.
func (x *TopicsIndex) Unsubscribe(filter, client string) bool {
	var existed bool
	prefix, _ := isolateParticle(filter, 0)
	shareSub := strings.EqualFold(prefix, SharePrefix)

	x.mu.Lock()
	if shareSub {
		group, plain := splitShared(filter)
		key := group + "\x00" + plain
		if clientsMap := x.shared[key]; clientsMap != nil {
			if _, existed = clientsMap[client]; existed {
				delete(clientsMap, client)
				if len(clientsMap) == 0 {
					delete(x.shared, key)
				}
				x.rebuildWildcardSnapLocked()
			}
		}
	} else if isWildcardFilter(filter) {
		if cm := x.wildcardClients[client]; cm != nil {
			if _, existed = cm[filter]; existed {
				delete(cm, filter)
				if len(cm) == 0 {
					delete(x.wildcardClients, client)
				}
				x.rebuildWildcardSnapLocked()
			}
		}
	} else {
		existed = x.deleteExactSubLocked(filter, client)
	}
	x.mu.Unlock()

	if !existed {
		return false
	}
	if x.observer != nil {
		if shareSub {
			group, plain := splitShared(filter)
			x.observer.SubscriptionRemoved(client, plain, group, false, 0)
		} else {
			x.observer.SubscriptionRemoved(client, filter, "", false, 0)
		}
	}
	return true
}

// RetainMessage saves a message payload to a topic. Returns 1 if a retained
// message was added, and -1 if the retained message was removed.
// 0 is returned if sequential empty payloads are received.
func (x *TopicsIndex) RetainMessage(pk packets.Packet) int64 {
	x.mu.Lock()
	defer x.mu.Unlock()

	if len(pk.Payload) > 0 {
		x.Retained.Add(pk.TopicName, pk)
		return 1
	}

	var out int64
	if pke, ok := x.Retained.Get(pk.TopicName); ok && len(pke.Payload) > 0 && pke.FixedHeader.Retain {
		out = -1
	}
	x.Retained.Delete(pk.TopicName) // [MQTT-3.3.1-6] [MQTT-3.3.1-7]
	return out
}

// Messages returns a slice of any retained messages which match a filter.
func (x *TopicsIndex) Messages(filter string) []packets.Packet {
	pks := []packets.Packet{}
	if len(filter) == 0 || x.Retained.Len() == 0 {
		return pks
	}

	if !isWildcardFilter(filter) {
		if pk, ok := x.Retained.Get(filter); ok {
			pks = append(pks, pk)
		}
		return pks
	}

	for topic, pk := range x.Retained.GetAll() {
		if strings.HasPrefix(topic, SysPrefix) && !strings.HasPrefix(filter, SysPrefix) {
			continue
		}
		if matchMQTTTopic(filter, topic) {
			pks = append(pks, pk)
		}
	}
	return pks
}

// Subscribers returns a map of clients who are subscribed to matching filters,
// their subscription ids and highest qos.
func (x *TopicsIndex) Subscribers(topic string) *Subscribers {
	subs := &Subscribers{
		Subscriptions: map[string]packets.Subscription{},
	}
	if len(topic) == 0 {
		return subs
	}

	if entry := x.getExact(topic); entry != nil {
		for _, cs := range entry.subs {
			subs.Subscriptions[cs.client] = cs.sub
		}
		if len(entry.inline) > 0 {
			subs.InlineSubscriptions = make(map[int]InlineSubscription, len(entry.inline))
			for _, inl := range entry.inline {
				subs.InlineSubscriptions[inl.Identifier] = inl
			}
		}
	}

	if wSnap := x.wildcardSnap.Load(); wSnap != nil {
		for i := range wSnap.inline {
			inl := &wSnap.inline[i]
			if matchMQTTTopic(inl.Filter, topic) {
				if subs.InlineSubscriptions == nil {
					subs.InlineSubscriptions = make(map[int]InlineSubscription)
				}
				subs.InlineSubscriptions[inl.Identifier] = *inl
			}
		}
		for i := range wSnap.clients {
			wc := &wSnap.clients[i]
			for j := range wc.subs {
				sub := wc.subs[j]
				if matchMQTTTopic(sub.Filter, topic) {
					if cls, ok := subs.Subscriptions[wc.client]; ok {
						subs.Subscriptions[wc.client] = cls.Merge(sub)
					} else {
						subs.Subscriptions[wc.client] = sub
					}
				}
			}
		}
		for i := range wSnap.shared {
			sf := &wSnap.shared[i]
			if matchMQTTTopic(sf.plainFilter, topic) {
				if subs.Shared == nil {
					subs.Shared = make(map[string]map[string]packets.Subscription)
				}
				for client, sub := range sf.clients {
					if subs.Shared[sub.Filter] == nil {
						subs.Shared[sub.Filter] = make(map[string]packets.Subscription)
					}
					subs.Shared[sub.Filter][client] = sub
				}
			}
		}
	}

	return subs
}

// isolateParticle extracts a particle between d / and d+1 / without allocations.
func isolateParticle(filter string, d int) (particle string, hasNext bool) {
	var next, end int
	for i := 0; end > -1 && i <= d; i++ {
		end = strings.IndexRune(filter, '/')

		switch {
		case d > -1 && i == d && end > -1:
			hasNext = true
			particle = filter[next:end]
		case end > -1:
			hasNext = false
			filter = filter[end+1:]
		default:
			hasNext = false
			particle = filter[next:]
		}
	}

	return
}

// IsSharedFilter returns true if the filter uses the share prefix.
func IsSharedFilter(filter string) bool {
	prefix, _ := isolateParticle(filter, 0)
	return strings.EqualFold(prefix, SharePrefix)
}

// IsValidFilter returns true if the filter is valid.
func IsValidFilter(filter string, forPublish bool) bool {
	if !forPublish && len(filter) == 0 { // publishing can accept zero-length topic filter if topic alias exists, so we don't enforce for publish.
		return false // [MQTT-4.7.3-1]
	}

	if forPublish {
		if strings.IndexByte(filter, '+') >= 0 || strings.IndexByte(filter, '#') >= 0 {
			return false //[MQTT-3.3.2-2]
		}
		if len(filter) == 0 || (filter[0] != '$' && filter[0] != '/') {
			return true
		}
		if len(filter) >= len(SysPrefix) && strings.EqualFold(filter[0:len(SysPrefix)], SysPrefix) {
			// 4.7.2 Non-normative - The Server SHOULD prevent Clients from using such Topic Names [$SYS] to exchange messages with other Clients.
			return false
		}
	}

	wildhash := strings.IndexRune(filter, '#')
	if wildhash >= 0 && wildhash != len(filter)-1 { // [MQTT-4.7.1-2]
		return false
	}

	prefix, hasNext := isolateParticle(filter, 0)
	if !hasNext && strings.EqualFold(prefix, SharePrefix) {
		return false // [MQTT-4.8.2-1]
	}

	if hasNext && strings.EqualFold(prefix, SharePrefix) {
		group, hasNext := isolateParticle(filter, 1)
		if !hasNext {
			return false // [MQTT-4.8.2-1]
		}

		if group == "" || strings.ContainsRune(group, '+') || strings.ContainsRune(group, '#') {
			return false // [MQTT-4.8.2-1] [MQTT-4.8.2-2]
		}

		// the nested filter after $share/<group>/ must be non-empty
		if len(filter) <= len(prefix)+1+len(group)+1 {
			return false
		}
	}

	// wildcards must occupy an entire topic level [MQTT-4.7.1-2] [MQTT-4.7.1-3]
	rem := filter
	for {
		var level string
		slash := strings.IndexByte(rem, '/')
		if slash >= 0 {
			level = rem[:slash]
		} else {
			level = rem
		}
		if (strings.IndexByte(level, '+') >= 0 && level != "+") || (strings.IndexByte(level, '#') >= 0 && level != "#") {
			return false
		}
		if slash < 0 {
			break
		}
		rem = rem[slash+1:]
	}

	return true
}
