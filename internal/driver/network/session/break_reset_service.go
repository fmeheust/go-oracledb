package session

import (
	"context"
	"sync"

	"github.com/oracle/go-oracledb/v26/internal/common"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// breakResetState records whether one break-reset exchange is in progress.
// There can be only one such exchange for a connection because the packet
// streamer pauses DATA until its reset marker is acknowledged.
type breakResetState struct {
	// mu serializes beginning and completing the exchange.
	mu sync.Mutex
	// inProgress is true between the break and reset marker boundaries.
	inProgress bool
}

// begin marks the exchange active. It returns false when a client or server
// break has already started the only permitted exchange.
func (state *breakResetState) begin() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.inProgress {
		return false
	}
	state.inProgress = true
	return true
}

// finish clears the active exchange and returns false for a stray reset marker.
func (state *breakResetState) finish() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.inProgress {
		return false
	}
	state.inProgress = false
	return true
}

// breakResetService owns marker handling, reset requests, and the server-marker
// watcher. It controls the packet streamer and keeps its request state private.
type breakResetService struct {
	// streamer is the shared packet handoff channels and serialized socket writer.
	streamer *packetStreamer
	// attributes contains the negotiated packet format used for marker packets.
	attributes *sessionAtts
	// resetState prevents duplicate client and server reset exchanges.
	resetState *breakResetState
	// markerWatcherDone closes when the connection-lifetime marker consumer exits.
	markerWatcherDone chan struct{}
}

// newBreakResetService creates the runtime marker service for one established
// packet stream. streamer and attributes are mandatory dependencies supplied
// when the network session starts its runtime services.
func newBreakResetService(streamer *packetStreamer, attributes *sessionAtts) *breakResetService {
	service := &breakResetService{
		streamer:          streamer,
		attributes:        attributes,
		resetState:        &breakResetState{},
		markerWatcherDone: make(chan struct{}),
	}
	go func() {
		defer close(service.markerWatcherDone)
		service.watchMarkers()
	}()
	return service
}

// stopMarkerWatcher joins the marker consumer after its streamer is closed.
func (ns *breakResetService) stopMarkerWatcher() {
	<-ns.markerWatcherDone
}

// watchMarkers handles one marker at a time and acknowledges each before the
// streamer reads another socket packet.
func (ns *breakResetService) watchMarkers() {
	for {
		delivery, err := ns.streamer.readMarkerPacket(ns.streamer.ctx)
		if err != nil {
			return
		}
		ns.handleMarkerPacket(delivery.packet)
		if err := ns.streamer.acknowledgeMarker(delivery); err != nil {
			return
		}
	}
}

// handleMarkerPacket interprets marker payloads and controls DATA handling.
// The packet streamer only waits for the separate acknowledgment after this
// method returns; it has no knowledge of marker meanings.
func (ns *breakResetService) handleMarkerPacket(packet *networkPacket) {
	marker, ok := packet.packet.(*markerPacket)
	if !ok {
		ns.streamer.Fail(common.NewOracleError(oracleErrors.InvalidNetworkValue, nil,
			"packet type", packet.typ))
		return
	}
	common.Odl.Finest("marker packet received", "marker-type", marker.markerType, "data", marker.data)

	if marker.markerType == NSPMKTD1 && marker.data == NIQRMARK {
		ns.finishReset(nil)
		return
	}

	if marker.markerType != NSPMKTD0 && marker.markerType != NSPMKTD1 {
		return
	}

	if !ns.beginReset() {
		return
	}
	// Send the reset request on this marker-consumer thread. Once the incoming
	// break marker is acknowledged below, this same loop can receive the reset
	// marker and finish the exchange without a separate reset goroutine.
	if err := ns.sendResetPackets(ns.streamer.ctx, NIQRMARK); err != nil {
		ns.finishReset(err)
		return
	}
}

// beginReset marks a new exchange and pauses future inbound DATA delivery so a
// reset marker that arrives immediately has an owner.
func (ns *breakResetService) beginReset() bool {
	if !ns.resetState.begin() {
		return false
	}
	ns.streamer.pauseDataPacketHandling()
	return true
}

// finishReset opens the DATA path after the reset marker, or terminates the
// stream when the reset exchange failed. A duplicate completion is ignored.
func (ns *breakResetService) finishReset(err error) bool {
	if !ns.resetState.finish() {
		return false
	}
	if err != nil {
		ns.streamer.Fail(err)
		return false
	}
	ns.streamer.resumeDataPacketHandling()
	return true
}

// sendResetPackets writes the supplied break-reset markers in order. The
// marker watcher remains the sole consumer for the server's marker reply.
func (ns *breakResetService) sendResetPackets(ctx context.Context, markers ...byte) error {
	markerPkt := &markerPacket{}
	for _, marker := range markers {
		if err := markerPkt.marshal(nil, ns.attributes, marker); err != nil {
			return err
		}
		if err := ns.streamer.writePacket(ctx, markerPkt.buf); err != nil {
			return err
		}
	}
	common.Odl.Fine("Reset packet sent")
	return nil
}

// CancelOperation starts a client break-reset and returns once its marker
// packets have been written. The marker watcher completes the reset while the
// streamer keeps DATA delivery paused until the server's reset marker arrives.
func (ns *breakResetService) CancelOperation(ctx context.Context) error {
	// Register before sending so a fast server response already has an owner.
	if !ns.beginReset() {
		// Another client or server reset is already in progress; its marker
		// watcher will finish the exchange.
		return nil
	}
	if err := ns.sendResetPackets(ctx, NIQIMARK, NIQRMARK); err != nil {
		ns.finishReset(err)
		return err
	}
	return nil
}
