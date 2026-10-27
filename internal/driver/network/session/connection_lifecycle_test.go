package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	"github.com/oracle/go-oracledb/v26/internal/driver/network/transport"
)

// runtimePacketHarness supplies deterministic in-memory packets to a runtime
// streamer and records complete packets written through it.
type runtimePacketHarness struct {
	inbound chan *networkPacket

	mu       sync.Mutex
	writes   [][]byte
	writeErr error
}

// newRuntimePacketHarness creates one runtime streamer whose reader blocks
// until a test provides a packet or the streamer is closed.
func newRuntimePacketHarness() (*packetStreamer, *runtimePacketHarness) {
	harness := &runtimePacketHarness{inbound: make(chan *networkPacket, 4)}
	streamer := newPacketStreamer(64,
		func(ctx context.Context, _ []byte) (*networkPacket, error) {
			select {
			case packet := <-harness.inbound:
				return packet, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		func(_ context.Context, packet []byte) error {
			harness.mu.Lock()
			defer harness.mu.Unlock()
			if harness.writeErr != nil {
				return harness.writeErr
			}
			harness.writes = append(harness.writes, append([]byte(nil), packet...))
			return nil
		},
	)
	return streamer, harness
}

// enqueue supplies one decoded packet to the streamer's sole reader.
func (h *runtimePacketHarness) enqueue(t *testing.T, packet *networkPacket) {
	t.Helper()
	select {
	case h.inbound <- packet:
	case <-time.After(time.Second):
		t.Fatal("packet streamer did not request the next packet")
	}
}

// writtenPackets returns copies so tests cannot alter the writer's record.
func (h *runtimePacketHarness) writtenPackets() [][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	packets := make([][]byte, len(h.writes))
	for i, packet := range h.writes {
		packets[i] = append([]byte(nil), packet...)
	}
	return packets
}

// waitForRuntimeCondition bounds asynchronous assertions without relying on a
// fixed scheduling delay.
func waitForRuntimeCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

// packetWithData builds a decoded DATA packet with exactly payload as its TTC
// bytes; packet decoding itself is covered by packet tests.
func packetWithData(payload []byte) *networkPacket {
	buffer := make([]byte, NSPDADAT+len(payload))
	copy(buffer[NSPDADAT:], payload)
	return &networkPacket{
		typ: NSPTDA,
		packet: &dataPacket{
			buf:    buffer,
			offset: NSPDADAT,
			len:    len(buffer),
			bufLen: len(buffer),
		},
	}
}

// packetWithMarker builds a decoded MARKER packet for runtime break/reset tests.
func packetWithMarker(markerType, data byte) *networkPacket {
	return &networkPacket{
		typ:    NSPTMK,
		packet: &markerPacket{markerType: markerType, data: data},
	}
}

// streamerPaused reports the protected pause state for tests that verify a
// break-reset boundary.
func streamerPaused(streamer *packetStreamer) bool {
	streamer.mu.Lock()
	defer streamer.mu.Unlock()
	return streamer.paused
}

// breakResetInProgress reports the protected break-reset state for tests.
func breakResetInProgress(service *breakResetService) bool {
	service.resetState.mu.Lock()
	defer service.resetState.mu.Unlock()
	return service.resetState.inProgress
}

// closeBreakResetService stops both cooperating goroutines in the same order
// used by networkSession.Disconnect.
func closeBreakResetService(t *testing.T, service *breakResetService) {
	t.Helper()
	if err := service.streamer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	service.stopMarkerWatcher()
}

// TestPacketStreamerRoutesDataAcrossAPause verifies that DATA already handed
// to TTC remains readable, DATA received while paused is discarded, and DATA
// delivery resumes after the break-reset service reopens it.
func TestPacketStreamerRoutesDataAcrossAPause(t *testing.T) {
	streamer, harness := newRuntimePacketHarness()
	defer func() { _ = streamer.Close() }()

	first := packetWithData([]byte{1})
	harness.enqueue(t, first)
	packet, err := streamer.readDataPacket(context.Background())
	if err != nil {
		t.Fatalf("readDataPacket() error = %v", err)
	}
	if packet != first {
		t.Fatal("readDataPacket() did not return the delivered packet")
	}

	streamer.pauseDataPacketHandling()
	harness.enqueue(t, packetWithData([]byte{2}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := streamer.readDataPacket(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("readDataPacket() error while paused = %v, want deadline exceeded", err)
	}

	streamer.resumeDataPacketHandling()
	third := packetWithData([]byte{3})
	harness.enqueue(t, third)
	packet, err = streamer.readDataPacket(context.Background())
	if err != nil {
		t.Fatalf("readDataPacket() after resume error = %v", err)
	}
	if packet != third {
		t.Fatal("readDataPacket() did not resume with the next DATA packet")
	}
}

// TestPacketStreamerBlocksAfterMarkerUntilAcknowledged verifies that a marker
// delivery stops socket reads until break-reset explicitly acknowledges it.
func TestPacketStreamerBlocksAfterMarkerUntilAcknowledged(t *testing.T) {
	streamer, harness := newRuntimePacketHarness()
	defer func() { _ = streamer.Close() }()

	first := packetWithMarker(NSPMKTD1, NIQBMARK)
	harness.enqueue(t, first)
	delivery, err := streamer.readMarkerPacket(context.Background())
	if err != nil {
		t.Fatalf("readMarkerPacket() error = %v", err)
	}
	if delivery.packet != first {
		t.Fatal("readMarkerPacket() returned an unexpected marker")
	}

	second := packetWithMarker(NSPMKTD1, NIQRMARK)
	harness.enqueue(t, second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := streamer.readMarkerPacket(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reader advanced before marker acknowledgement: %v", err)
	}

	if err := streamer.acknowledgeMarker(delivery); err != nil {
		t.Fatalf("acknowledgeMarker() error = %v", err)
	}
	delivery, err = streamer.readMarkerPacket(context.Background())
	if err != nil {
		t.Fatalf("readMarkerPacket() after acknowledgement error = %v", err)
	}
	if delivery.packet != second {
		t.Fatal("reader did not continue with the queued marker")
	}
	if err := streamer.acknowledgeMarker(delivery); err != nil {
		t.Fatalf("second acknowledgeMarker() error = %v", err)
	}
}

// TestPacketStreamerWritesThroughTheSingleWriter verifies that a valid packet
// reaches the shared writer and that writer failures reach the caller.
func TestPacketStreamerWritesThroughTheSingleWriter(t *testing.T) {
	streamer, harness := newRuntimePacketHarness()
	defer func() { _ = streamer.Close() }()

	packet := make([]byte, PACKET_HEADER_SIZE)
	if err := streamer.writePacket(context.Background(), packet); err != nil {
		t.Fatalf("writePacket() error = %v", err)
	}
	writes := harness.writtenPackets()
	if len(writes) != 1 || len(writes[0]) != PACKET_HEADER_SIZE {
		t.Fatalf("writer received %#v, want one complete packet", writes)
	}

	harness.mu.Lock()
	harness.writeErr = errors.New("write failure")
	harness.mu.Unlock()
	if err := streamer.writePacket(context.Background(), packet); !errors.Is(err, harness.writeErr) {
		t.Fatalf("writePacket() error = %v, want writer error", err)
	}
}

// TestPacketStreamerRoutesControlPackets verifies that CONTROL packets bypass
// DATA and MARKER consumers and remain available to the notification service.
func TestPacketStreamerRoutesControlPackets(t *testing.T) {
	streamer, harness := newRuntimePacketHarness()
	defer func() { _ = streamer.Close() }()

	want := &networkPacket{typ: NSPTCNL, packet: &controlPacket{isNotification: true}}
	harness.enqueue(t, want)
	packet, err := streamer.readControlPacket(context.Background())
	if err != nil {
		t.Fatalf("readControlPacket() error = %v", err)
	}
	if packet != want {
		t.Fatal("readControlPacket() did not return the CONTROL packet")
	}
}

// TestPacketStreamerMakesReaderErrorsTerminal verifies that a reader failure
// is returned consistently to both blocked readers and later writers.
func TestPacketStreamerMakesReaderErrorsTerminal(t *testing.T) {
	want := errors.New("reader failure")
	streamer := newPacketStreamer(64,
		func(context.Context, []byte) (*networkPacket, error) { return nil, want },
		func(context.Context, []byte) error { return nil },
	)
	defer func() { _ = streamer.Close() }()

	select {
	case <-streamer.done:
	case <-time.After(time.Second):
		t.Fatal("reader failure did not stop the streamer")
	}
	if _, err := streamer.readDataPacket(context.Background()); !errors.Is(err, want) {
		t.Fatalf("readDataPacket() error = %v, want reader failure", err)
	}
	if err := streamer.writePacket(context.Background(), make([]byte, PACKET_HEADER_SIZE)); !errors.Is(err, want) {
		t.Fatalf("writePacket() error = %v, want reader failure", err)
	}
}

// TestDataServicePacketizesAndFlushesWrites verifies that the data service
// fills one SDU before writing it and flushes a remaining partial DATA packet.
func TestDataServicePacketizesAndFlushesWrites(t *testing.T) {
	streamer, harness := newRuntimePacketHarness()
	defer func() { _ = streamer.Close() }()
	service := newDataService(streamer, &sessionAtts{sdu: NSPDADAT + 2})

	if err := service.writeBytes(context.Background(), []byte{1, 2, 3}); err != nil {
		t.Fatalf("writeBytes() error = %v", err)
	}
	writes := harness.writtenPackets()
	if len(writes) != 1 || string(writes[0][NSPDADAT:]) != string([]byte{1, 2}) {
		t.Fatalf("full DATA write = %v, want payload [1 2]", writes)
	}
	if err := service.flush(context.Background()); err != nil {
		t.Fatalf("flush() error = %v", err)
	}
	writes = harness.writtenPackets()
	if len(writes) != 2 || string(writes[1][NSPDADAT:]) != string([]byte{3}) {
		t.Fatalf("partial DATA write = %v, want payload [3]", writes)
	}
}

// TestDataServiceReadsFromTheRuntimeStreamer verifies that dataService reads
// TTC bytes from the DATA handoff instead of touching the socket directly.
func TestDataServiceReadsFromTheRuntimeStreamer(t *testing.T) {
	streamer, harness := newRuntimePacketHarness()
	defer func() { _ = streamer.Close() }()
	service := newDataService(streamer, &sessionAtts{sdu: 64})
	harness.enqueue(t, packetWithData([]byte{4, 5, 6}))

	bytes, err := service.readBytes(context.Background(), 3)
	if err != nil {
		t.Fatalf("readBytes() error = %v", err)
	}
	if string(bytes) != string([]byte{4, 5, 6}) {
		t.Fatalf("readBytes() = %v, want [4 5 6]", bytes)
	}
}

// TestBreakResetStatePreventsOverlappingExchanges verifies that the state
// permits one begin/finish pair and rejects duplicate boundaries.
func TestBreakResetStatePreventsOverlappingExchanges(t *testing.T) {
	state := &breakResetState{}
	if !state.begin() || state.begin() {
		t.Fatal("begin() did not permit exactly one active exchange")
	}
	if !state.finish() || state.finish() {
		t.Fatal("finish() did not complete exactly one active exchange")
	}
}

// TestBreakResetServiceCancelsAndResumesData verifies that client cancellation
// sends interrupt and reset markers, then resumes DATA after the reset reply.
func TestBreakResetServiceCancelsAndResumesData(t *testing.T) {
	streamer, harness := newRuntimePacketHarness()
	service := newBreakResetService(streamer, &sessionAtts{sdu: 64})
	defer closeBreakResetService(t, service)

	if err := service.CancelOperation(context.Background()); err != nil {
		t.Fatalf("CancelOperation() error = %v", err)
	}
	writes := harness.writtenPackets()
	if len(writes) != 2 || writes[0][NSPMKDAT] != NIQIMARK || writes[1][NSPMKDAT] != NIQRMARK {
		t.Fatalf("client markers = %v, want interrupt then reset", writes)
	}
	if !streamerPaused(streamer) || !breakResetInProgress(service) {
		t.Fatal("client cancellation did not pause DATA and begin reset")
	}

	harness.enqueue(t, packetWithMarker(NSPMKTD1, NIQRMARK))
	waitForRuntimeCondition(t, "client reset completion", func() bool {
		return !streamerPaused(streamer) && !breakResetInProgress(service)
	})
}

// TestBreakResetServiceRespondsToServerBreak verifies that a server break
// causes one reset marker to be sent and DATA resumes after the server reset.
func TestBreakResetServiceRespondsToServerBreak(t *testing.T) {
	streamer, harness := newRuntimePacketHarness()
	service := newBreakResetService(streamer, &sessionAtts{sdu: 64})
	defer closeBreakResetService(t, service)

	harness.enqueue(t, packetWithMarker(NSPMKTD1, NIQBMARK))
	waitForRuntimeCondition(t, "server reset request", func() bool {
		return len(harness.writtenPackets()) == 1 && streamerPaused(streamer) && breakResetInProgress(service)
	})
	if marker := harness.writtenPackets()[0][NSPMKDAT]; marker != NIQRMARK {
		t.Fatalf("server-break response = %d, want reset marker %d", marker, NIQRMARK)
	}

	harness.enqueue(t, packetWithMarker(NSPMKTD1, NIQRMARK))
	waitForRuntimeCondition(t, "server reset completion", func() bool {
		return !streamerPaused(streamer) && !breakResetInProgress(service)
	})
}

// TestNotificationServiceConsumesControlNotifications verifies that the
// notification service consumes only its CONTROL handoff and clears it.
func TestNotificationServiceConsumesControlNotifications(t *testing.T) {
	streamer, harness := newRuntimePacketHarness()
	defer func() { _ = streamer.Close() }()
	service := &notificationService{streamer: streamer}
	control := &controlPacket{isNotification: true, errno: 12572}
	harness.enqueue(t, &networkPacket{typ: NSPTCNL, packet: control})
	waitForRuntimeCondition(t, "CONTROL packet delivery", func() bool {
		return len(streamer.controlPackets) == 1
	})

	if !service.CheckInbandNotification() {
		t.Fatal("CheckInbandNotification() = false, want true")
	}
	if control.isNotification || control.errno != 0 {
		t.Fatal("CheckInbandNotification() did not clear the consumed control packet")
	}
}

// runtimeTestAdapter keeps a session reader blocked until the runtime streamer
// cancels it, allowing construction tests to use the real transport callbacks.
type runtimeTestAdapter struct {
	mu           sync.Mutex
	disconnected bool
}

func (adapter *runtimeTestAdapter) Connect(context.Context, transport.Address) error { return nil }

func (adapter *runtimeTestAdapter) Send(context.Context, []byte) error { return nil }

func (adapter *runtimeTestAdapter) Receive(ctx context.Context, _ []byte, _ int) (int, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func (adapter *runtimeTestAdapter) Disconnect() error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	adapter.disconnected = true
	return nil
}

// TestNetworkSessionWiresRuntimeServices verifies that ACCEPT creates one
// shared runtime streamer for TTC, break/reset, and CONTROL notifications.
func TestNetworkSessionWiresRuntimeServices(t *testing.T) {
	adapter := &runtimeTestAdapter{}
	attributes := newSessionAtts("test")
	attributes.sdu = 64
	session := newNetworkSession(&networkTransport{sAtts: attributes, ntAdapter: adapter})

	if session.dataService == nil || session.breakReset == nil || session.notifications == nil {
		t.Fatal("newNetworkSession() did not create every runtime service")
	}
	streamer := session.dataService.streamer
	if session.breakReset.streamer != streamer || session.notifications.streamer != streamer {
		t.Fatal("runtime services do not share the packet streamer")
	}
	if err := session.Disconnect(context.Background(), driverCommon.NSFIMM); err != nil {
		t.Fatalf("Disconnect() error = %v", err)
	}
	adapter.mu.Lock()
	disconnected := adapter.disconnected
	adapter.mu.Unlock()
	if !disconnected {
		t.Fatal("Disconnect() did not close the transport adapter")
	}
}
