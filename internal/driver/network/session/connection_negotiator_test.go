package session

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	"github.com/oracle/go-oracledb/v26/internal/driver/network/transport"
)

// TestNegotiationStreamerReadsAndWritesInWireOrder verifies that negotiation
// uses its supplied reader buffer and writer without a runtime goroutine.
func TestNegotiationStreamerReadsAndWritesInWireOrder(t *testing.T) {
	want := &networkPacket{typ: NSPTAC, packet: &acceptPacket{}}
	var receivedBuffer []byte
	var written []byte
	streamer, err := newNegotiationStreamer(32,
		func(_ context.Context, buffer []byte) (*networkPacket, error) {
			receivedBuffer = buffer
			return want, nil
		},
		func(_ context.Context, packet []byte) error {
			written = append([]byte(nil), packet...)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("newNegotiationStreamer() error = %v", err)
	}
	packet, err := streamer.PullNext(context.Background())
	if err != nil || packet != want {
		t.Fatalf("PullNext() = (%v, %v), want (%v, nil)", packet, err, want)
	}
	if len(receivedBuffer) != 32 {
		t.Fatalf("reader buffer length = %d, want 32", len(receivedBuffer))
	}
	if err := streamer.Send(context.Background(), []byte{1, 2, 3}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if string(written) != string([]byte{1, 2, 3}) {
		t.Fatalf("writer received %v, want [1 2 3]", written)
	}
}

// TestNegotiationStreamerRejectsInvalidDependencies verifies that negotiation
// cannot start without a positive buffer size and both transport callbacks.
func TestNegotiationStreamerRejectsInvalidDependencies(t *testing.T) {
	reader := packetReader(func(context.Context, []byte) (*networkPacket, error) { return nil, nil })
	writer := packetWriter(func(context.Context, []byte) error { return nil })
	for _, test := range []struct {
		name       string
		bufferSize int
		reader     packetReader
		writer     packetWriter
	}{
		{name: "zero buffer", bufferSize: 0, reader: reader, writer: writer},
		{name: "nil reader", bufferSize: 1, writer: writer},
		{name: "nil writer", bufferSize: 1, reader: reader},
	} {
		t.Run(test.name, func(t *testing.T) {
			if streamer, err := newNegotiationStreamer(test.bufferSize, test.reader, test.writer); err == nil || streamer != nil {
				t.Fatalf("newNegotiationStreamer() = (%v, %v), want nil streamer and error", streamer, err)
			}
		})
	}
}

// TestNegotiationStreamerPropagatesReadFailures verifies that negotiation
// exposes transport read failures without converting or prefetching packets.
func TestNegotiationStreamerPropagatesReadFailures(t *testing.T) {
	want := errors.New("negotiation read failure")
	streamer, err := newNegotiationStreamer(8,
		func(context.Context, []byte) (*networkPacket, error) { return nil, want },
		func(context.Context, []byte) error { return nil },
	)
	if err != nil {
		t.Fatalf("newNegotiationStreamer() error = %v", err)
	}
	if _, err := streamer.PullNext(context.Background()); !errors.Is(err, want) {
		t.Fatalf("PullNext() error = %v, want reader failure", err)
	}
}

// TestConnectionNegotiatorStartsWithoutRuntimeServices verifies that the
// pre-ACCEPT negotiator owns only negotiation state and transport attributes.
func TestConnectionNegotiatorStartsWithoutRuntimeServices(t *testing.T) {
	attributes := newSessionAtts("test")
	connectData := []byte("(CONNECT_DATA=(SERVICE_NAME=test))")
	negotiator := newConnectionNegotiator(attributes, connectData)

	if negotiator.transport == nil || negotiator.transport.sAtts != attributes {
		t.Fatal("newConnectionNegotiator() did not retain its transport attributes")
	}
	if string(negotiator.cData) != string(connectData) {
		t.Fatal("newConnectionNegotiator() did not retain CONNECT_DATA")
	}
	if negotiator.negotiationStreamer != nil || negotiator.negotiationSendPacket != nil {
		t.Fatal("newConnectionNegotiator() created runtime packet state before connecting")
	}
}

// negotiationTestAdapter supplies a scripted negotiation response and then
// blocks reads until the runtime session closes it.
type negotiationTestAdapter struct {
	mu           sync.Mutex
	incoming     []byte
	offset       int
	sent         [][]byte
	disconnected bool
}

func (adapter *negotiationTestAdapter) Connect(context.Context, transport.Address) error { return nil }

func (adapter *negotiationTestAdapter) Send(_ context.Context, packet []byte) error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	adapter.sent = append(adapter.sent, append([]byte(nil), packet...))
	return nil
}

func (adapter *negotiationTestAdapter) Receive(ctx context.Context, buffer []byte, count int) (int, error) {
	adapter.mu.Lock()
	if adapter.offset < len(adapter.incoming) {
		remaining := len(adapter.incoming) - adapter.offset
		if count > remaining {
			count = remaining
		}
		copy(buffer[:count], adapter.incoming[adapter.offset:adapter.offset+count])
		adapter.offset += count
		adapter.mu.Unlock()
		return count, nil
	}
	adapter.mu.Unlock()
	<-ctx.Done()
	return 0, ctx.Err()
}

func (adapter *negotiationTestAdapter) Disconnect() error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	adapter.disconnected = true
	return nil
}

// acceptedPacketWire builds a complete ACCEPT packet suitable for exercising
// the real negotiation reader and packet decoder.
func acceptedPacketWire() []byte {
	packet := make([]byte, NSPACFL2+4)
	binary.BigEndian.PutUint16(packet[:2], uint16(len(packet)))
	packet[NSPHDTYP] = NSPTAC
	binary.BigEndian.PutUint16(packet[NSPACVSN:], TNS_VERSION_DESIRED)
	binary.BigEndian.PutUint16(packet[NSPACSDU:], NSPMNSDULN)
	binary.BigEndian.PutUint16(packet[NSPACTDU:], NSPMNTDULN)
	binary.BigEndian.PutUint32(packet[NSPACLSD:], NSPMNSDULN)
	binary.BigEndian.PutUint32(packet[NSPACLTD:], NSPMNTDULN)
	return packet
}

// TestConnectionNegotiatorTransfersAcceptedTransport verifies that negotiation
// sends CONNECT, consumes ACCEPT with the negotiation streamer, and only then
// creates the shared runtime services for the accepted socket.
func TestConnectionNegotiatorTransfersAcceptedTransport(t *testing.T) {
	attributes := newSessionAtts("test")
	adapter := &negotiationTestAdapter{incoming: acceptedPacketWire()}
	negotiator := newConnectionNegotiator(attributes, []byte("(CONNECT_DATA=(SERVICE_NAME=test))"))
	negotiator.transport.ntAdapter = adapter

	session, err := negotiator.Connect(context.Background(), transport.Address{})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if !session.connected || session.networkTransport != negotiator.transport {
		t.Fatal("Connect() did not transfer the accepted transport to the runtime session")
	}
	if negotiator.negotiationStreamer != nil {
		t.Fatal("Connect() left the negotiation streamer attached after ACCEPT")
	}
	adapter.mu.Lock()
	sentConnect := len(adapter.sent) == 1 && adapter.sent[0][NSPHDTYP] == NSPTCN
	adapter.mu.Unlock()
	if !sentConnect {
		t.Fatal("Connect() did not send the CONNECT packet through the negotiation streamer")
	}
	if err := session.Disconnect(context.Background(), driverCommon.NSFIMM); err != nil {
		t.Fatalf("Disconnect() error = %v", err)
	}
}
