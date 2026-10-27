package session

import (
	"context"
	"io"
	"sync"

	"github.com/oracle/go-oracledb/v26/internal/common"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

const (
	// packetStreamerDataChannelSize allows one DATA packet to wait for TTC.
	packetStreamerDataChannelSize = 1
	// packetStreamerMarkerChannelSize allows one marker to wait for break-reset.
	packetStreamerMarkerChannelSize = 1
	// packetStreamerControlChannelSize buffers control notifications until polled.
	packetStreamerControlChannelSize = 1024
)

// networkPacket is one completely received and decoded TNS packet.
type networkPacket struct {
	// typ is the TNS packet type from the packet header.
	typ int
	// packet is the packet-specific decoded value interpreted by its service.
	packet any
}

// packetReader reads and decodes one packet using a reusable buffer.
type packetReader func(context.Context, []byte) (*networkPacket, error)

// packetWriter sends one fully encoded packet through the active transport.
type packetWriter func(context.Context, []byte) error

// markerPacketDelivery pairs a marker with its one-use acknowledgment. The
// streamer waits for the acknowledgment before reading the next socket packet.
type markerPacketDelivery struct {
	// packet is the decoded marker packet.
	packet *networkPacket
	// acknowledgment closes the handoff after break-reset has handled the marker.
	acknowledgment chan struct{}
}

// packetStreamer owns the only socket reader and serializes socket writes. It
// hands DATA and MARKER packets to one-slot channels and buffers CONTROL packets
// separately until the notification consumer requests them.
type packetStreamer struct {
	// reader decodes one packet from the active transport.
	reader packetReader
	// writer sends packets through the active transport.
	writer packetWriter
	// writeGate serializes socket writes and DATA pause boundaries.
	writeGate chan struct{}
	// bufferSize is the reusable receive buffer size, normally the negotiated SDU.
	bufferSize int
	// ctx is canceled when the transport is replaced or disconnected.
	ctx context.Context
	// cancel stops the socket reader's lifetime context.
	cancel context.CancelFunc
	// done closes when the background reader exits.
	done chan struct{}

	// dataPackets holds at most one packet awaiting the TTC data service.
	dataPackets chan *networkPacket
	// markerPackets holds at most one marker awaiting break-reset handling.
	markerPackets chan *markerPacketDelivery
	// controlPackets buffers control notifications until they are polled.
	controlPackets chan *networkPacket

	// mu protects DATA delivery state, terminalErr, and closed.
	mu sync.Mutex
	// paused drops incoming and outgoing DATA during break-reset.
	paused bool
	// terminalErr is the socket or packet-decoding failure returned to waiters.
	terminalErr error
	// closed reports an intentional shutdown of the packet reader.
	closed bool
}

// newPacketStreamer starts one reader for an established transport. DATA and
// MARKER delivery channels deliberately have capacity one to apply backpressure.
func newPacketStreamer(bufferSize int, reader packetReader, writer packetWriter) *packetStreamer {
	ctx, cancel := context.WithCancel(common.BackgroundContext)
	streamer := &packetStreamer{
		reader:         reader,
		writer:         writer,
		writeGate:      make(chan struct{}, 1),
		bufferSize:     bufferSize,
		ctx:            ctx,
		cancel:         cancel,
		done:           make(chan struct{}),
		dataPackets:    make(chan *networkPacket, packetStreamerDataChannelSize),
		markerPackets:  make(chan *markerPacketDelivery, packetStreamerMarkerChannelSize),
		controlPackets: make(chan *networkPacket, packetStreamerControlChannelSize),
	}
	go streamer.readLoop()
	return streamer
}

// writePacket serializes and writes one packet through the transport. DATA
// pause state applies only to packets received from the socket.
func (ps *packetStreamer) writePacket(ctx context.Context, packet []byte) error {
	if len(packet) < PACKET_HEADER_SIZE {
		return common.NewOracleError(oracleErrors.InvalidNetworkExpectedLength, nil,
			"packet buffer", len(packet), PACKET_HEADER_SIZE)
	}

	select {
	case ps.writeGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-ps.ctx.Done():
		return io.EOF
	}
	defer func() { <-ps.writeGate }()

	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return io.EOF
	}
	if ps.terminalErr != nil {
		err := ps.terminalErr
		ps.mu.Unlock()
		return err
	}
	writer := ps.writer
	ps.mu.Unlock()
	if err := writer(ctx, packet); err != nil {
		return err
	}
	return nil
}

// readLoop is the only goroutine that reads the transport. It dispatches each
// decoded packet by type and waits for a marker decision before reading again.
func (ps *packetStreamer) readLoop() {
	defer close(ps.done)
	buffer := make([]byte, ps.bufferSize)
	for {
		packet, err := ps.reader(ps.ctx, buffer)
		if err != nil {
			if ps.ctx.Err() == nil {
				ps.finish(err)
			}
			return
		}
		if packet == nil {
			ps.finish(common.NewOracleError(oracleErrors.InternalError, nil))
			return
		}

		switch packet.typ {
		case NSPTDA:
			if !ps.deliverDataPacket(packet) {
				return
			}
		case NSPTMK:
			if !ps.deliverMarkerPacket(packet) {
				return
			}
		case NSPTCNL:
			select {
			case ps.controlPackets <- packet:
			case <-ps.ctx.Done():
				return
			}
		default:
			ps.finish(common.NewOracleError(oracleErrors.InvalidNetworkValue, nil,
				"packet type", packet.typ))
			return
		}
	}
}

// deliverDataPacket drops DATA received after handling is paused or hands it
// to the TTC channel. Packets received before the pause remain available.
func (ps *packetStreamer) deliverDataPacket(packet *networkPacket) bool {
	ps.mu.Lock()
	if ps.closed || ps.terminalErr != nil {
		ps.mu.Unlock()
		return false
	}
	if ps.paused {
		ps.mu.Unlock()
		return true
	}
	ps.mu.Unlock()

	select {
	case ps.dataPackets <- packet:
		return true
	case <-ps.ctx.Done():
		return false
	}
}

// deliverMarkerPacket hands the marker to break-reset and waits for the service
// to acknowledge handling it before reading the next socket packet. The
// streamer does not interpret marker contents or change DATA handling here.
func (ps *packetStreamer) deliverMarkerPacket(packet *networkPacket) bool {
	delivery := &markerPacketDelivery{
		packet:         packet,
		acknowledgment: make(chan struct{}),
	}
	select {
	case ps.markerPackets <- delivery:
	case <-ps.ctx.Done():
		return false
	}

	select {
	case <-delivery.acknowledgment:
	case <-ps.ctx.Done():
		return false
	}
	return true
}

// finish records the first terminal reader error so blocked service reads and
// subsequent writes observe the same failure.
func (ps *packetStreamer) finish(err error) {
	ps.mu.Lock()
	if ps.terminalErr == nil && !ps.closed {
		ps.terminalErr = err
	}
	ps.mu.Unlock()
}

// Fail makes err terminal and cancels the reader to release every blocked handoff.
func (ps *packetStreamer) Fail(err error) {
	ps.finish(err)
	ps.cancel()
}

// readDataPacket waits for the next DATA packet. Pausing delivery only prevents
// new DATA handoffs; a packet already in the channel remains available.
func (ps *packetStreamer) readDataPacket(ctx context.Context) (*networkPacket, error) {
	select {
	case packet := <-ps.dataPackets:
		return packet, nil
	case <-ps.done:
		return nil, ps.streamError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// readMarkerPacket waits for a marker delivery from the sole socket reader.
func (ps *packetStreamer) readMarkerPacket(ctx context.Context) (*markerPacketDelivery, error) {
	select {
	case delivery := <-ps.markerPackets:
		return delivery, nil
	case <-ps.done:
		return nil, ps.streamError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// acknowledgeMarker releases the reader after break-reset has handled one marker.
func (ps *packetStreamer) acknowledgeMarker(delivery *markerPacketDelivery) error {
	if delivery == nil {
		return common.NewOracleError(oracleErrors.InternalError, nil)
	}
	select {
	case delivery.acknowledgment <- struct{}{}:
		return nil
	case <-ps.ctx.Done():
		return io.EOF
	}
}

// readControlPacket waits for the next buffered control packet.
func (ps *packetStreamer) readControlPacket(ctx context.Context) (*networkPacket, error) {
	select {
	case packet := <-ps.controlPackets:
		return packet, nil
	case <-ps.done:
		return nil, ps.streamError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// streamError returns the terminal reader error or EOF after an intentional close.
func (ps *packetStreamer) streamError() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.terminalErr != nil {
		return ps.terminalErr
	}
	return io.EOF
}

// pauseDataPacketHandling prevents new inbound DATA handoffs. It leaves any
// packet already in the DATA channel for the data service to consume.
func (ps *packetStreamer) pauseDataPacketHandling() {
	select {
	case ps.writeGate <- struct{}{}:
		defer func() { <-ps.writeGate }()
	case <-ps.ctx.Done():
		return
	}

	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return
	}
	ps.paused = true
	ps.mu.Unlock()
}

// resumeDataPacketHandling reopens DATA delivery after break-reset completes.
func (ps *packetStreamer) resumeDataPacketHandling() {
	ps.mu.Lock()
	if !ps.closed && ps.terminalErr == nil {
		ps.paused = false
	}
	ps.mu.Unlock()
}

// Close cancels the background reader and waits for any active socket write
// before the adapter is disconnected.
func (ps *packetStreamer) Close() error {
	ps.mu.Lock()
	ps.closed = true
	ps.mu.Unlock()
	ps.cancel()
	<-ps.done
	ps.writeGate <- struct{}{}
	<-ps.writeGate
	return nil
}
