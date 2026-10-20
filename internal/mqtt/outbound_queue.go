package mqtt

import (
	"sync"

	"monstermq.io/edge/internal/mqtt/packets"
)

const (
	outboundQueueMinCap  = 16  // first allocation of a queue's ring
	outboundQueueIdleCap = 256 // a drained ring larger than this is released
)

// outboundQueue holds the live publishes waiting for a client's writer. It
// grows on demand up to max entries and releases its ring once drained, so an
// idle connection costs almost nothing while a busy one can absorb a burst. A
// buffered channel of the same capacity would be allocated in full for every
// connection, however idle.
type outboundQueue struct {
	mu     sync.Mutex
	ring   []*packets.Packet
	head   int
	n      int
	max    int
	notify chan struct{} // holds a token while the queue may be non-empty
}

func newOutboundQueue(max int) *outboundQueue {
	return &outboundQueue{max: max, notify: make(chan struct{}, 1)}
}

// push appends pk and reports whether there was room for it.
func (q *outboundQueue) push(pk *packets.Packet) bool {
	q.mu.Lock()
	if q.n >= q.max {
		q.mu.Unlock()
		return false
	}
	if q.n == len(q.ring) {
		q.grow()
	}
	q.ring[(q.head+q.n)%len(q.ring)] = pk
	q.n++
	q.mu.Unlock()
	q.signal()
	return true
}

func (q *outboundQueue) grow() {
	size := max(outboundQueueMinCap, 2*len(q.ring))
	size = min(size, max(q.max, q.n+1))
	ring := make([]*packets.Packet, size)
	for i := 0; i < q.n; i++ {
		ring[i] = q.ring[(q.head+i)%len(q.ring)]
	}
	q.ring = ring
	q.head = 0
}

// pushFront puts back a packet just taken by pop, ahead of everything else. It
// ignores the limit, since the packet was counted when it was first pushed.
func (q *outboundQueue) pushFront(pk *packets.Packet) {
	q.mu.Lock()
	if q.n == len(q.ring) {
		q.grow()
	}
	q.head = (q.head - 1 + len(q.ring)) % len(q.ring)
	q.ring[q.head] = pk
	q.n++
	q.mu.Unlock()
	q.signal()
}

// pop removes and returns the oldest packet.
func (q *outboundQueue) pop() (*packets.Packet, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.n == 0 {
		return nil, false
	}
	pk := q.ring[q.head]
	q.ring[q.head] = nil
	q.head = (q.head + 1) % len(q.ring)
	q.n--
	if q.n == 0 {
		q.head = 0
		if len(q.ring) > outboundQueueIdleCap {
			q.ring = nil
		}
	}
	return pk, true
}

// len returns the number of queued packets.
func (q *outboundQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.n
}

// signal makes sure a token is waiting on notify.
func (q *outboundQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
