package mqtt

import (
	"sort"
	"sync"
	"sync/atomic"

	"monstermq.io/edge/internal/mqtt/packets"
)

// inflightEntry is an inflight packet with its arrival sequence in the session,
// kept in one map entry so the hot path does a single map write per packet.
type inflightEntry struct {
	pk    packets.Packet
	order uint64 // for in-order resend and claims; an updated entry keeps its place
}

// Inflight is a map of InflightMessage keyed on packet id.
type Inflight struct {
	sync.RWMutex
	internal            map[uint16]inflightEntry // internal contains the inflight packets
	nextOrder           uint64                   // last sequence handed out
	deferredCount       atomic.Int32             // number of entries with Expiry < 0
	quotaMu             sync.Mutex               // protects quota fields
	receiveQuota        int32                    // remaining inbound qos quota for flow control
	sendQuota           int32                    // remaining outbound qos quota for flow control
	maximumReceiveQuota int32                    // maximum allowed receive quota
	maximumSendQuota    int32                    // maximum allowed send quota
}

// NewInflights returns a new instance of an Inflight packets map.
func NewInflights() *Inflight {
	return &Inflight{
		internal: map[uint16]inflightEntry{},
	}
}

// putLocked stores m, keeping the arrival sequence of an entry it replaces
// (e.g. PUBLISH replaced by PUBREC/PUBREL), and returns whether one existed.
// Callers hold the lock.
func (i *Inflight) putLocked(m packets.Packet) bool {
	prev, ok := i.internal[m.PacketID]
	if ok && prev.pk.Expiry < 0 {
		i.deferredCount.Add(-1)
	}
	if m.Expiry < 0 {
		i.deferredCount.Add(1)
	}
	order := prev.order
	if !ok {
		i.nextOrder++
		order = i.nextOrder
	}
	i.internal[m.PacketID] = inflightEntry{pk: m, order: order}
	return ok
}

// Set adds or updates an inflight packet by packet id.
func (i *Inflight) Set(m packets.Packet) bool {
	i.Lock()
	defer i.Unlock()
	return !i.putLocked(m)
}

// Get returns an inflight packet by packet id.
func (i *Inflight) Get(id uint16) (packets.Packet, bool) {
	i.RLock()
	defer i.RUnlock()

	if e, ok := i.internal[id]; ok {
		return e.pk, true
	}

	return packets.Packet{}, false
}

// Len returns the size of the inflight messages map.
func (i *Inflight) Len() int {
	i.RLock()
	defer i.RUnlock()
	return len(i.internal)
}

// Clone returns a new instance of Inflight with the same message data.
// This is used when transferring inflights from a taken-over session.
func (i *Inflight) Clone() *Inflight {
	c := NewInflights()
	i.RLock()
	defer i.RUnlock()
	for k, v := range i.internal {
		c.internal[k] = v
		if v.pk.Expiry < 0 {
			c.deferredCount.Add(1)
		}
	}
	c.nextOrder = i.nextOrder
	return c
}

// GetAll returns all the inflight messages.
func (i *Inflight) GetAll(immediate bool) []packets.Packet {
	i.RLock()
	defer i.RUnlock()

	es := make([]inflightEntry, 0, len(i.internal))
	for _, v := range i.internal {
		if !immediate || (immediate && v.pk.Expiry < 0) {
			es = append(es, v)
		}
	}

	// Oldest first, by arrival in the session: resends must keep the original
	// order of the messages [MQTT-4.6.0-1], also when packet ids wrap around.
	sort.Slice(es, func(a, b int) bool { return es[a].order < es[b].order })

	m := make([]packets.Packet, len(es))
	for k := range es {
		m[k] = es[k].pk
	}
	return m
}

// NextImmediate returns the next inflight packet which is indicated to be sent immediately.
// This typically occurs when the quota has been exhausted, and we need to wait until new quota
// is free to continue sending.
func (i *Inflight) NextImmediate() (packets.Packet, bool) {
	m := i.GetAll(true) // GetAll takes the read lock; locking here too would recursively RLock
	if len(m) > 0 {
		return m[0], true
	}

	return packets.Packet{}, false
}

// SetOutbound atomically checks and reserves send quota (when a maximum send quota is configured)
// and stores an outbound QoS > 0 packet in the inflight map. If the send quota is exhausted,
// the packet is marked as deferred (Expiry = -1) before storing.
func (i *Inflight) SetOutbound(m packets.Packet) (added bool, deferred bool) {
	i.Lock()
	defer i.Unlock()
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	if i.maximumSendQuota > 0 {
		// While older messages wait for quota, a new one queues behind them even
		// if quota is free right now: messages must reach the client in the
		// order they were published [MQTT-4.6.0-6]. The caller claims deferred
		// packets as quota allows.
		if i.sendQuota <= 0 || i.deferredCount.Load() > 0 {
			m.Expiry = -1
			deferred = true
		} else {
			i.sendQuota--
		}
	}

	return !i.putLocked(m), deferred
}

// HasDeferred reports whether any inflight packet is currently deferred waiting on send quota.
func (i *Inflight) HasDeferred() bool {
	return i.deferredCount.Load() > 0
}

// ClaimNextImmediate atomically finds the next deferred inflight packet (Expiry < 0),
// reserves one unit of send quota, restores the packet's normal expiry using expiryFn,
// updates it in-place in the inflight map, and returns it.
func (i *Inflight) ClaimNextImmediate(expiryFn func(packets.Packet) int64) (packets.Packet, bool) {
	if i.deferredCount.Load() == 0 {
		return packets.Packet{}, false
	}

	i.Lock()
	defer i.Unlock()
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	if i.maximumSendQuota > 0 && i.sendQuota <= 0 {
		return packets.Packet{}, false
	}

	var next inflightEntry
	found := false
	for _, v := range i.internal {
		if v.pk.Expiry >= 0 {
			continue
		}
		if !found || v.order < next.order {
			next = v
			found = true
		}
	}
	if !found {
		return packets.Packet{}, false
	}

	if i.maximumSendQuota > 0 {
		i.sendQuota--
	}
	i.deferredCount.Add(-1)
	next.pk.Expiry = 0
	if expiryFn != nil {
		if exp := expiryFn(next.pk); exp > 0 {
			next.pk.Expiry = exp
		}
	}
	i.internal[next.pk.PacketID] = next
	return next.pk, true
}

// Delete removes an in-flight message from the map. Returns true if the message existed.
func (i *Inflight) Delete(id uint16) bool {
	i.Lock()
	defer i.Unlock()

	prev, ok := i.internal[id]
	if ok && prev.pk.Expiry < 0 {
		i.deferredCount.Add(-1)
	}
	delete(i.internal, id)

	return ok
}

// ReceiveQuota returns the remaining inbound qos quota.
func (i *Inflight) ReceiveQuota() int32 {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()
	return i.receiveQuota
}

// SendQuota returns the remaining outbound qos quota.
func (i *Inflight) SendQuota() int32 {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()
	return i.sendQuota
}

// MaximumSendQuota returns the maximum outbound qos quota.
func (i *Inflight) MaximumSendQuota() int32 {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()
	return i.maximumSendQuota
}

// TryReserveReceiveQuota atomically checks and reduces the receive quota by 1.
// Returns true if quota was reserved, false if quota was already 0.
func (i *Inflight) TryReserveReceiveQuota() bool {
	return i.DecreaseReceiveQuota()
}

// DecreaseReceiveQuota reduces the receive quota by 1.
// Returns true if quota was successfully decreased, false if quota was already 0.
func (i *Inflight) DecreaseReceiveQuota() bool {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	if i.receiveQuota <= 0 {
		return false
	}
	i.receiveQuota--
	return true
}

// IncreaseReceiveQuota increases the receive quota by 1.
// Returns true if quota was successfully increased, false if already at maximum.
func (i *Inflight) IncreaseReceiveQuota() bool {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	if i.receiveQuota >= i.maximumReceiveQuota {
		return false
	}
	i.receiveQuota++
	return true
}

// ResetReceiveQuota resets the receive quota to the maximum allowed value.
func (i *Inflight) ResetReceiveQuota(n int32) {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	i.maximumReceiveQuota = n
	i.receiveQuota = n
}

// TryReserveSendQuota atomically checks and reduces the send quota by 1.
// Returns true if quota was reserved, false if quota was already 0.
func (i *Inflight) TryReserveSendQuota() bool {
	return i.DecreaseSendQuota()
}

// DecreaseSendQuota reduces the send quota by 1.
// Returns true if quota was successfully decreased, false if quota was already 0.
func (i *Inflight) DecreaseSendQuota() bool {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	if i.sendQuota <= 0 {
		return false
	}
	i.sendQuota--
	return true
}

// IncreaseSendQuota increases the send quota by 1.
// Returns true if quota was successfully increased, false if already at maximum.
func (i *Inflight) IncreaseSendQuota() bool {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	if i.sendQuota >= i.maximumSendQuota {
		return false
	}
	i.sendQuota++
	return true
}

// ResetSendQuota resets the send quota to the maximum allowed value.
func (i *Inflight) ResetSendQuota(n int32) {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	i.maximumSendQuota = n
	i.sendQuota = n
}
