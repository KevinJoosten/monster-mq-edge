package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/xid"

	"monstermq.io/edge/internal/mqtt/mempool"
	"monstermq.io/edge/internal/mqtt/packets"
)

const (
	defaultKeepalive             uint16 = 10 // the default connection keepalive value in seconds.
	defaultClientProtocolVersion byte   = 4  // the default mqtt protocol version of connecting clients (if somehow unspecified).
	minimumKeepalive             uint16 = 5  // the minimum recommended keepalive - values under with display a warning.
)

var (
	ErrMinimumKeepalive = errors.New("client keepalive is below minimum recommended value and may exhibit connection instability")
)

// ReadFn is the function signature for the function used for reading and processing new packets.
type ReadFn func(*Client, packets.Packet) error

// Clients contains a map of the clients known by the broker.
type Clients struct {
	internal map[string]*Client // clients known by the broker, keyed on client id.
	snap     atomic.Pointer[map[string]*Client]
	sync.RWMutex
}

// NewClients returns an instance of Clients.
func NewClients() *Clients {
	c := &Clients{
		internal: make(map[string]*Client),
	}
	empty := make(map[string]*Client)
	c.snap.Store(&empty)
	return c
}

func (cl *Clients) refreshSnapLocked() {
	m := make(map[string]*Client, len(cl.internal))
	for k, v := range cl.internal {
		m[k] = v
	}
	cl.snap.Store(&m)
}

// Add adds a new client to the clients map, keyed on client id.
func (cl *Clients) Add(val *Client) {
	cl.Lock()
	cl.internal[val.ID] = val
	cl.refreshSnapLocked()
	cl.Unlock()
}

// GetAll returns all the clients.
func (cl *Clients) GetAll() map[string]*Client {
	cl.RLock()
	defer cl.RUnlock()
	m := make(map[string]*Client, len(cl.internal))
	for k, v := range cl.internal {
		m[k] = v
	}
	return m
}

// Get returns the value of a client if it exists.
func (cl *Clients) Get(id string) (*Client, bool) {
	if s := cl.snap.Load(); s != nil {
		val, ok := (*s)[id]
		return val, ok
	}
	cl.RLock()
	defer cl.RUnlock()
	val, ok := cl.internal[id]
	return val, ok
}

// Len returns the length of the clients map.
func (cl *Clients) Len() int {
	if s := cl.snap.Load(); s != nil {
		return len(*s)
	}
	cl.RLock()
	defer cl.RUnlock()
	return len(cl.internal)
}

// Delete removes a client from the internal map.
func (cl *Clients) Delete(id string) {
	cl.Lock()
	delete(cl.internal, id)
	cl.refreshSnapLocked()
	cl.Unlock()
}

// GetByListener returns clients matching a listener id.
func (cl *Clients) GetByListener(id string) []*Client {
	cl.RLock()
	defer cl.RUnlock()
	clients := make([]*Client, 0, len(cl.internal))
	for _, client := range cl.internal {
		if client.Net.Listener == id && !client.Closed() {
			clients = append(clients, client)
		}
	}
	return clients
}

// Client contains information about a client known by the broker.
type Client struct {
	Properties   ClientProperties // client properties
	State        ClientState      // the operational state of the client.
	Net          ClientConnection // network connection state of the client
	ID           string           // the client id.
	ops          *ops             // ops provides a reference to server ops.
	sync.RWMutex                  // mutex
}

// ClientConnection contains the connection transport and metadata for the client.
type ClientConnection struct {
	Conn     net.Conn      // the net.Conn used to establish the connection
	bconn    *bufio.Reader // a buffered net.Conn for reading packets
	outbuf   *bytes.Buffer // a buffer for writing packets
	Remote   string        // the remote address of the client
	Listener string        // listener id of the client
	Inline   bool          // if true, the client is the built-in 'inline' embedded client
}

// ClientProperties contains the properties which define the client behaviour.
type ClientProperties struct {
	Props           packets.Properties
	Will            Will
	Username        []byte
	ProtocolVersion byte
	Clean           bool
}

// Will contains the last will and testament details for a client connection.
type Will struct {
	Payload           []byte                 // -
	User              []packets.UserProperty // -
	TopicName         string                 // -
	Flag              uint32                 // 0,1
	WillDelayInterval uint32                 // -
	Qos               byte                   // -
	Retain            bool                   // -
}

// ClientState tracks the state of the client.
type ClientState struct {
	TopicAliases    TopicAliases         // a map of topic aliases
	stopCause       atomic.Value         // reason for stopping
	Inflight        *Inflight            // a map of in-flight qos messages
	Subscriptions   *Subscriptions       // a map of the subscription filters a client maintains
	disconnected    int64                // the time the client disconnected in unix time, for calculating expiry
	outbound        chan *packets.Packet // queue for pending outbound packets
	endOnce         sync.Once            // only end once
	isTakenOver     atomic.Bool          // used to identify orphaned clients
	packetID        uint32               // the current highest packetID
	open            context.Context      // indicate that the client is open for packet exchange
	cancelOpen      context.CancelFunc   // cancel function for open context
	outboundQty       int32                // number of messages currently in the outbound queue
	writeOverloaded   atomic.Bool          // true while outbound queue is dropping packets
	droppedInOverload atomic.Int64         // packets dropped during current overload window
	lastDeadline      int64                // last unix second and keepalive applied via SetDeadline
	Keepalive         uint16               // the number of seconds the connection can wait
	ServerKeepalive   bool                 // keepalive was set by the server
}

// newClient returns a new instance of Client. This is almost exclusively used by Server
// for creating new clients, but it lives here because it's not dependent.
func newClient(c net.Conn, o *ops) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	cl := &Client{
		State: ClientState{
			Inflight:      NewInflights(),
			Subscriptions: NewSubscriptions(),
			TopicAliases:  NewTopicAliases(o.options.Capabilities.TopicAliasMaximum),
			open:          ctx,
			cancelOpen:    cancel,
			Keepalive:     defaultKeepalive,
			outbound:      make(chan *packets.Packet, o.options.Capabilities.MaximumClientWritesPending),
		},
		Properties: ClientProperties{
			ProtocolVersion: defaultClientProtocolVersion, // default protocol version
		},
		ops: o,
	}

	if c != nil {
		cl.Net = ClientConnection{
			Conn:   c,
			bconn:  bufio.NewReaderSize(c, o.options.ClientNetReadBufferSize),
			Remote: c.RemoteAddr().String(),
		}
	}

	return cl
}

var outboundPacketPool = sync.Pool{
	New: func() any {
		return new(packets.Packet)
	},
}

// WriteLoop ranges over pending outbound messages and writes them to the client connection.
func (cl *Client) WriteLoop() {
	overloadRecoveryThreshold := cap(cl.State.outbound) / 4
	for {
		select {
		case pk := <-cl.State.outbound:
			cl.writeOutboundBatch(pk)
			if cl.State.writeOverloaded.Load() && len(cl.State.outbound) <= overloadRecoveryThreshold {
				if cl.State.writeOverloaded.CompareAndSwap(true, false) {
					dropped := cl.State.droppedInOverload.Swap(0)
					cl.ops.log.Info("client outbound write queue overload cleared", "client", cl.ID, "dropped", dropped)
				}
			}
		case <-cl.State.open.Done():
			return
		}
	}
}

// writeOutboundBatch encodes first and any immediately queued packets into
// cl.Net.outbuf and flushes once when the queue is drained or the buffer fills.
func (cl *Client) writeOutboundBatch(first *packets.Packet) {
	if cl.Closed() || cl.Net.Conn == nil {
		*first = packets.Packet{}
		outboundPacketPool.Put(first)
		atomic.AddInt32(&cl.State.outboundQty, -1)
		return
	}

	hasEncodeHook := cl.ops.hooks.Provides(OnPacketEncode)
	hasSentHook := cl.ops.hooks.Provides(OnPacketSent)
	maxBuf := cl.ops.options.ClientNetWriteBufferSize
	if maxBuf <= 0 {
		maxBuf = 65536
	}

	var (
		totalBytes   int64
		totalPackets int64
		totalPublish int64
		dequeued     int32
		nowUnix      int64
	)

	cl.Lock()
	if cl.Net.outbuf == nil {
		cl.Net.outbuf = bytes.NewBuffer(make([]byte, 0, maxBuf))
	}

	pk := first
	for {
		dequeued++
		if pk.Expiry > 0 {
			if nowUnix == 0 {
				nowUnix = time.Now().Unix()
			}
			expiry := pk.Expiry - nowUnix
			if expiry < 1 {
				expiry = 1
			}
			pk.Properties.MessageExpiryInterval = uint32(expiry)
		}
		pk.ProtocolVersion = cl.Properties.ProtocolVersion
		if pk.Mods.MaxSize == 0 {
			pk.Mods.MaxSize = cl.Properties.Props.MaximumPacketSize
		}
		if cl.Properties.Props.RequestProblemInfoFlag && cl.Properties.Props.RequestProblemInfo == 0x0 {
			pk.Mods.DisallowProblemInfo = true
		}
		if pk.FixedHeader.Type != packets.Connack || cl.Properties.Props.RequestResponseInfo == 0x1 || cl.ops.options.Capabilities.Compatibilities.AlwaysReturnResponseInfo {
			pk.Mods.AllowResponseInfo = true
		}
		if hasEncodeHook {
			*pk = cl.ops.hooks.OnPacketEncode(cl, *pk)
		}

		startLen := cl.Net.outbuf.Len()
		var err error
		if pk.FixedHeader.Type == packets.Publish {
			err = pk.PublishEncode(cl.Net.outbuf)
		} else {
			err = encodeOtherPacket(pk, cl.Net.outbuf)
		}
		if err == nil {
			pktLen := cl.Net.outbuf.Len() - startLen
			if pk.Mods.MaxSize > 0 && uint32(pktLen) > pk.Mods.MaxSize {
				cl.Net.outbuf.Truncate(startLen)
			} else {
				totalBytes += int64(pktLen)
				totalPackets++
				if pk.FixedHeader.Type == packets.Publish {
					totalPublish++
				}
				if hasSentHook {
					raw := cl.Net.outbuf.Bytes()[startLen:]
					cl.ops.hooks.OnPacketSent(cl, *pk, raw)
				}
			}
		}

		*pk = packets.Packet{}
		outboundPacketPool.Put(pk)

		if cl.Net.outbuf.Len() >= maxBuf {
			_ = cl.flushOutbuf()
		}

		// Drain up to 256 packets per lock acquisition if more are waiting
		if dequeued >= 256 {
			break
		}
		select {
		case next := <-cl.State.outbound:
			pk = next
		default:
			pk = nil
		}
		if pk == nil {
			break
		}
	}

	if cl.Net.outbuf.Len() > 0 && (len(cl.State.outbound) == 0 || cl.Net.outbuf.Len() >= maxBuf) {
		_ = cl.flushOutbuf()
	}
	cl.Unlock()

	atomic.AddInt32(&cl.State.outboundQty, -dequeued)
	if totalBytes > 0 {
		atomic.AddInt64(&cl.ops.info.BytesSent, totalBytes)
	}
	if totalPackets > 0 {
		atomic.AddInt64(&cl.ops.info.PacketsSent, totalPackets)
	}
	if totalPublish > 0 {
		atomic.AddInt64(&cl.ops.info.MessagesSent, totalPublish)
	}
}

func encodeOtherPacket(pk *packets.Packet, buf *bytes.Buffer) error {
	switch pk.FixedHeader.Type {
	case packets.Connect:
		return pk.ConnectEncode(buf)
	case packets.Connack:
		return pk.ConnackEncode(buf)
	case packets.Publish:
		return pk.PublishEncode(buf)
	case packets.Puback:
		return pk.PubackEncode(buf)
	case packets.Pubrec:
		return pk.PubrecEncode(buf)
	case packets.Pubrel:
		return pk.PubrelEncode(buf)
	case packets.Pubcomp:
		return pk.PubcompEncode(buf)
	case packets.Subscribe:
		return pk.SubscribeEncode(buf)
	case packets.Suback:
		return pk.SubackEncode(buf)
	case packets.Unsubscribe:
		return pk.UnsubscribeEncode(buf)
	case packets.Unsuback:
		return pk.UnsubackEncode(buf)
	case packets.Pingreq:
		return pk.PingreqEncode(buf)
	case packets.Pingresp:
		return pk.PingrespEncode(buf)
	case packets.Disconnect:
		return pk.DisconnectEncode(buf)
	case packets.Auth:
		return pk.AuthEncode(buf)
	default:
		return fmt.Errorf("%w: %v", packets.ErrNoValidPacketAvailable, pk.FixedHeader.Type)
	}
}

// ParseConnect parses the connect parameters and properties for a client.
func (cl *Client) ParseConnect(lid string, pk packets.Packet) {
	cl.Net.Listener = lid

	cl.Properties.ProtocolVersion = pk.ProtocolVersion
	cl.Properties.Username = pk.Connect.Username
	cl.Properties.Clean = pk.Connect.Clean
	cl.Properties.Props = pk.Properties.Copy(false)

	if cl.Properties.Props.ReceiveMaximum > cl.ops.options.Capabilities.MaximumInflight { // 3.3.4 Non-normative
		cl.Properties.Props.ReceiveMaximum = cl.ops.options.Capabilities.MaximumInflight
	}

	if pk.Connect.Keepalive <= minimumKeepalive {
		cl.ops.log.Warn(
			ErrMinimumKeepalive.Error(),
			"client", cl.ID,
			"keepalive", pk.Connect.Keepalive,
			"recommended", minimumKeepalive,
		)
	}

	cl.State.Keepalive = pk.Connect.Keepalive                                              // [MQTT-3.2.2-22]
	cl.State.Inflight.ResetReceiveQuota(int32(cl.ops.options.Capabilities.ReceiveMaximum)) // server receive max per client
	cl.State.Inflight.ResetSendQuota(int32(cl.Properties.Props.ReceiveMaximum))            // client receive max
	cl.State.TopicAliases.Outbound = NewOutboundTopicAliases(cl.Properties.Props.TopicAliasMaximum)

	cl.ID = pk.Connect.ClientIdentifier
	if cl.ID == "" {
		cl.ID = xid.New().String() // [MQTT-3.1.3-6] [MQTT-3.1.3-7]
		cl.Properties.Props.AssignedClientID = cl.ID
	}

	if pk.Connect.WillFlag {
		cl.Properties.Will = Will{
			Qos:               pk.Connect.WillQos,
			Retain:            pk.Connect.WillRetain,
			Payload:           pk.Connect.WillPayload,
			TopicName:         pk.Connect.WillTopic,
			WillDelayInterval: pk.Connect.WillProperties.WillDelayInterval,
			User:              pk.Connect.WillProperties.User,
		}
		if pk.Properties.SessionExpiryIntervalFlag &&
			pk.Properties.SessionExpiryInterval < pk.Connect.WillProperties.WillDelayInterval {
			cl.Properties.Will.WillDelayInterval = pk.Properties.SessionExpiryInterval
		}
		if pk.Connect.WillFlag {
			cl.Properties.Will.Flag = 1 // atomic for checking
		}
	}
}

// refreshDeadline refreshes the read/write deadline for the net.Conn connection.
func (cl *Client) refreshDeadline(keepalive uint16) {
	if cl.Net.Conn == nil {
		return
	}
	if keepalive == 0 {
		if atomic.SwapInt64(&cl.State.lastDeadline, -1) != -1 {
			_ = cl.Net.Conn.SetDeadline(time.Time{}) // [MQTT-3.1.2-22]
		}
		return
	}
	now := time.Now()
	stamp := (now.Unix() << 16) | int64(keepalive)
	if atomic.LoadInt64(&cl.State.lastDeadline) == stamp {
		return
	}
	atomic.StoreInt64(&cl.State.lastDeadline, stamp)
	expiry := now.Add(time.Duration(keepalive+(keepalive/2)) * time.Second) // [MQTT-3.1.2-22]
	_ = cl.Net.Conn.SetDeadline(expiry)                                     // [MQTT-3.1.2-22]
}

// NextPacketID returns the next available (unused) packet id for the client.
// If no unused packet ids are available, an error is returned and the client
// should be disconnected.
func (cl *Client) NextPacketID() (i uint32, err error) {
	cl.Lock()
	defer cl.Unlock()

	i = atomic.LoadUint32(&cl.State.packetID)
	started := i
	overflowed := false
	for {
		if overflowed && i == started {
			return 0, packets.ErrQuotaExceeded
		}

		if i >= cl.ops.options.Capabilities.maximumPacketID {
			overflowed = true
			i = 0
			continue
		}

		i++

		if _, ok := cl.State.Inflight.Get(uint16(i)); !ok {
			atomic.StoreUint32(&cl.State.packetID, i)
			return i, nil
		}
	}
}

// ResendInflightMessages attempts to resend any pending inflight messages to connected clients.
func (cl *Client) ResendInflightMessages(force bool) error {
	if cl.State.Inflight.Len() == 0 {
		return nil
	}

	for _, tk := range cl.State.Inflight.GetAll(false) {
		if tk.FixedHeader.Type == packets.Publish {
			if !cl.ops.hooks.OnACLCheck(cl, tk.TopicName, false) {
				if ok := cl.State.Inflight.Delete(tk.PacketID); ok {
					cl.ops.hooks.OnQosDropped(cl, tk)
					atomic.AddInt64(&cl.ops.info.Inflight, -1)
				}
				continue
			}
			tk.FixedHeader.Dup = true // [MQTT-3.3.1-1] [MQTT-3.3.1-3]
		}

		cl.ops.hooks.OnQosPublish(cl, tk, tk.Created, 0)
		err := cl.WritePacket(tk)
		if err != nil {
			return err
		}

		if tk.FixedHeader.Type == packets.Puback || tk.FixedHeader.Type == packets.Pubcomp {
			if ok := cl.State.Inflight.Delete(tk.PacketID); ok {
				cl.ops.hooks.OnQosComplete(cl, tk)
				atomic.AddInt64(&cl.ops.info.Inflight, -1)
			}
		}
	}

	return nil
}

// ClearInflights deletes all inflight messages for the client, e.g. for a disconnected user with a clean session.
func (cl *Client) ClearInflights() {
	for _, tk := range cl.State.Inflight.GetAll(false) {
		if ok := cl.State.Inflight.Delete(tk.PacketID); ok {
			cl.ops.hooks.OnQosDropped(cl, tk)
			atomic.AddInt64(&cl.ops.info.Inflight, -1)
		}
	}
}

// ClearExpiredInflights deletes any inflight messages which have expired.
func (cl *Client) ClearExpiredInflights(now, maximumExpiry int64) []uint16 {
	deleted := []uint16{}
	for _, tk := range cl.State.Inflight.GetAll(false) {
		expired := tk.ProtocolVersion == 5 && tk.Expiry > 0 && tk.Expiry < now // [MQTT-3.3.2-5]

		// If the maximum message expiry interval is set (greater than 0), and the message
		// retention period exceeds the maximum expiry, the message will be forcibly removed.
		enforced := maximumExpiry > 0 && now-tk.Created > maximumExpiry

		if expired || enforced {
			if ok := cl.State.Inflight.Delete(tk.PacketID); ok {
				cl.ops.hooks.OnQosDropped(cl, tk)
				atomic.AddInt64(&cl.ops.info.Inflight, -1)
				deleted = append(deleted, tk.PacketID)
			}
		}
	}

	return deleted
}

// Read reads incoming packets from the connected client and transforms them into
// packets to be handled by the packetHandler.
func (cl *Client) Read(packetHandler ReadFn) error {
	var err error
	var fh packets.FixedHeader

	for {
		if cl.Closed() {
			return nil
		}

		cl.refreshDeadline(cl.State.Keepalive)
		fh = packets.FixedHeader{}
		err = cl.ReadFixedHeader(&fh)
		if err != nil {
			return keepaliveErr(err)
		}

		pk, err := cl.ReadPacket(&fh)
		if err != nil {
			return keepaliveErr(err)
		}

		err = packetHandler(cl, pk) // Process inbound packet.
		if err != nil {
			return err
		}
	}
}

// keepaliveErr reports an expired read deadline as a keepalive timeout [MQTT-3.1.2-22].
func keepaliveErr(err error) error {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return packets.ErrKeepAliveTimeout
	}
	return err
}

// Stop instructs the client to shut down all processing goroutines and disconnect.
func (cl *Client) Stop(err error) {
	cl.State.endOnce.Do(func() {

		if cl.Net.Conn != nil {
			_ = cl.Net.Conn.Close() // omit close error
		}

		if err != nil {
			cl.State.stopCause.Store(err)
		}

		if cl.State.cancelOpen != nil {
			cl.State.cancelOpen()
		}

		atomic.StoreInt64(&cl.State.disconnected, time.Now().Unix())
	})
}

// StopCause returns the reason the client connection was stopped, if any.
func (cl *Client) StopCause() error {
	if cl.State.stopCause.Load() == nil {
		return nil
	}
	return cl.State.stopCause.Load().(error)
}

// StopTime returns the the time the client disconnected in unix time, else zero.
func (cl *Client) StopTime() int64 {
	return atomic.LoadInt64(&cl.State.disconnected)
}

// Closed returns true if client connection is closed.
func (cl *Client) Closed() bool {
	return cl.State.open == nil || cl.State.open.Err() != nil
}

func (cl *Client) IsTakenOver() bool {
	return cl.State.isTakenOver.Load()
}

// ReadFixedHeader reads in the values of the next packet's fixed header.
func (cl *Client) ReadFixedHeader(fh *packets.FixedHeader) error {
	if cl.Net.bconn == nil {
		return ErrConnectionClosed
	}

	b, err := cl.Net.bconn.ReadByte()
	if err != nil {
		return err
	}

	err = fh.Decode(b)
	if err != nil {
		return err
	}

	var bu int
	fh.Remaining, bu, err = packets.DecodeLength(cl.Net.bconn)
	if err != nil {
		return err
	}

	if cl.ops.options.Capabilities.MaximumPacketSize > 0 && uint32(1+bu+fh.Remaining) > cl.ops.options.Capabilities.MaximumPacketSize {
		return packets.ErrPacketTooLarge // [MQTT-3.2.2-15]
	}

	atomic.AddInt64(&cl.ops.info.BytesReceived, int64(bu+1))
	return nil
}

// ReadPacket reads the remaining buffer into an MQTT packet.
func (cl *Client) ReadPacket(fh *packets.FixedHeader) (pk packets.Packet, err error) {
	atomic.AddInt64(&cl.ops.info.PacketsReceived, 1)

	pk.ProtocolVersion = cl.Properties.ProtocolVersion // inherit client protocol version for decoding
	pk.FixedHeader = *fh
	p := make([]byte, pk.FixedHeader.Remaining)
	n, err := io.ReadFull(cl.Net.bconn, p)
	if err != nil {
		return pk, err
	}

	atomic.AddInt64(&cl.ops.info.BytesReceived, int64(n))

	// p is freshly allocated for this packet, so decoded fields may safely reference it;
	// no further copy is needed to protect it from the next read.
	px := p
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectDecode(px)
	case packets.Disconnect:
		err = pk.DisconnectDecode(px)
	case packets.Connack:
		err = pk.ConnackDecode(px)
	case packets.Publish:
		err = pk.PublishDecode(px)
		if err == nil {
			atomic.AddInt64(&cl.ops.info.MessagesReceived, 1)
		}
	case packets.Puback:
		err = pk.PubackDecode(px)
	case packets.Pubrec:
		err = pk.PubrecDecode(px)
	case packets.Pubrel:
		err = pk.PubrelDecode(px)
	case packets.Pubcomp:
		err = pk.PubcompDecode(px)
	case packets.Subscribe:
		err = pk.SubscribeDecode(px)
	case packets.Suback:
		err = pk.SubackDecode(px)
	case packets.Unsubscribe:
		err = pk.UnsubscribeDecode(px)
	case packets.Unsuback:
		err = pk.UnsubackDecode(px)
	case packets.Pingreq:
	case packets.Pingresp:
	case packets.Auth:
		err = pk.AuthDecode(px)
	default:
		err = fmt.Errorf("invalid packet type; %v", pk.FixedHeader.Type)
	}

	if err != nil {
		return pk, err
	}

	pk, err = cl.ops.hooks.OnPacketRead(cl, pk)
	return
}

// WritePacket encodes and writes a packet to the client.
func (cl *Client) WritePacket(pk packets.Packet) error {
	if cl.Closed() {
		return ErrConnectionClosed
	}

	if cl.Net.Conn == nil {
		return nil
	}

	if pk.Expiry > 0 {
		expiry := pk.Expiry - time.Now().Unix()
		if expiry < 1 {
			expiry = 1
		}
		pk.Properties.MessageExpiryInterval = uint32(expiry) // [MQTT-3.3.2-6]
	}

	pk.ProtocolVersion = cl.Properties.ProtocolVersion
	if pk.Mods.MaxSize == 0 { // NB we use this statement to embed client packet sizes in tests
		pk.Mods.MaxSize = cl.Properties.Props.MaximumPacketSize
	}

	if cl.Properties.Props.RequestProblemInfoFlag && cl.Properties.Props.RequestProblemInfo == 0x0 {
		pk.Mods.DisallowProblemInfo = true // [MQTT-3.1.2-29] strict, no problem info on any packet if set
	}

	if pk.FixedHeader.Type != packets.Connack || cl.Properties.Props.RequestResponseInfo == 0x1 || cl.ops.options.Capabilities.Compatibilities.AlwaysReturnResponseInfo {
		pk.Mods.AllowResponseInfo = true // [MQTT-3.1.2-28] we need to know which properties we can encode
	}

	pk = cl.ops.hooks.OnPacketEncode(cl, pk)

	var err error
	buf := mempool.GetBuffer()
	defer mempool.PutBuffer(buf)
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(buf)
	case packets.Connack:
		err = pk.ConnackEncode(buf)
	case packets.Publish:
		err = pk.PublishEncode(buf)
	case packets.Puback:
		err = pk.PubackEncode(buf)
	case packets.Pubrec:
		err = pk.PubrecEncode(buf)
	case packets.Pubrel:
		err = pk.PubrelEncode(buf)
	case packets.Pubcomp:
		err = pk.PubcompEncode(buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(buf)
	case packets.Suback:
		err = pk.SubackEncode(buf)
	case packets.Unsubscribe:
		err = pk.UnsubscribeEncode(buf)
	case packets.Unsuback:
		err = pk.UnsubackEncode(buf)
	case packets.Pingreq:
		err = pk.PingreqEncode(buf)
	case packets.Pingresp:
		err = pk.PingrespEncode(buf)
	case packets.Disconnect:
		err = pk.DisconnectEncode(buf)
	case packets.Auth:
		err = pk.AuthEncode(buf)
	default:
		err = fmt.Errorf("%w: %v", packets.ErrNoValidPacketAvailable, pk.FixedHeader.Type)
	}
	if err != nil {
		return err
	}

	if pk.Mods.MaxSize > 0 && uint32(buf.Len()) > pk.Mods.MaxSize {
		return packets.ErrPacketTooLarge // [MQTT-3.1.2-24] [MQTT-3.1.2-25]
	}

	n, err := func() (int64, error) {
		cl.Lock()
		defer cl.Unlock()
		if len(cl.State.outbound) == 0 {
			if cl.Net.outbuf == nil || cl.Net.outbuf.Len() == 0 {
				return buf.WriteTo(cl.Net.Conn)
			}

			// first write to buffer, then flush buffer
			n, _ := cl.Net.outbuf.Write(buf.Bytes()) // will always be successful
			err = cl.flushOutbuf()
			return int64(n), err
		}

		// there are more writes in the queue
		if cl.Net.outbuf == nil || cl.Net.outbuf.Len() == 0 {
			if buf.Len() >= cl.ops.options.ClientNetWriteBufferSize {
				return buf.WriteTo(cl.Net.Conn)
			}
			if cl.Net.outbuf == nil {
				cl.Net.outbuf = bytes.NewBuffer(make([]byte, 0, cl.ops.options.ClientNetWriteBufferSize))
			}
		}

		n, _ := cl.Net.outbuf.Write(buf.Bytes()) // will always be successful
		if cl.Net.outbuf.Len() < cl.ops.options.ClientNetWriteBufferSize {
			return int64(n), nil
		}

		err = cl.flushOutbuf()
		return int64(n), err
	}()
	if err != nil {
		return err
	}

	atomic.AddInt64(&cl.ops.info.BytesSent, n)
	atomic.AddInt64(&cl.ops.info.PacketsSent, 1)
	if pk.FixedHeader.Type == packets.Publish {
		atomic.AddInt64(&cl.ops.info.MessagesSent, 1)
	}

	cl.ops.hooks.OnPacketSent(cl, pk, buf.Bytes())

	return err
}

func (cl *Client) flushOutbuf() (err error) {
	if cl.Net.outbuf == nil || cl.Net.outbuf.Len() == 0 {
		return
	}

	_, err = cl.Net.outbuf.WriteTo(cl.Net.Conn)
	if err == nil {
		cl.Net.outbuf.Reset()
	}
	return
}
