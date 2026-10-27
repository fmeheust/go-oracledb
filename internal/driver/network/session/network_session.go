package session

import (
	"context"

	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
)

// networkSession is the public network-session facade. Its runtime services
// share one packet streamer, which is the sole reader and writer for the socket.
type networkSession struct {
	// networkTransport owns the accepted socket and negotiated attributes.
	*networkTransport
	// dataService provides the synchronous TTC byte stream for this facade.
	dataService *dataService
	// breakReset consumes MARKER packets for the lifetime of the connection.
	breakReset *breakResetService
	// notifications consumes buffered CONTROL packets on demand.
	notifications *notificationService
}

// newNetworkSession creates the fully initialized runtime session after ACCEPT.
// transport owns the accepted adapter and negotiated session attributes.
func newNetworkSession(transport *networkTransport) *networkSession {
	streamer := newPacketStreamer(transport.sAtts.sdu, transport.readPacketFromTransport, transport.writePacketToTransport)
	dataService := newDataService(streamer, transport.sAtts)
	breakReset := newBreakResetService(streamer, transport.sAtts)
	return &networkSession{
		networkTransport: transport,
		dataService:      dataService,
		breakReset:       breakReset,
		notifications:    &notificationService{streamer: streamer},
	}
}

// ReadByteWithContext reads one TTC byte through the data service.
func (ns *networkSession) ReadByteWithContext(ctx context.Context) (byte, error) {
	return ns.dataService.readByte(ctx)
}

// ReadBytesWithContext reads a TTC byte range, pulling DATA packets as needed.
func (ns *networkSession) ReadBytesWithContext(ctx context.Context, length int32) ([]byte, error) {
	return ns.dataService.readBytes(ctx, length)
}

// WriteBytesWithContext appends bytes to the outgoing TTC stream.
func (ns *networkSession) WriteBytesWithContext(ctx context.Context, bytes []byte) error {
	return ns.dataService.writeBytes(ctx, bytes)
}

// WriteByteWithContext appends one byte to the outgoing TTC packet.
func (ns *networkSession) WriteByteWithContext(ctx context.Context, value byte) error {
	return ns.dataService.writeByte(ctx, value)
}

// Flush sends buffered TTC data through the shared packet streamer.
func (ns *networkSession) Flush(ctx context.Context) error {
	return ns.dataService.flush(ctx)
}

// CancelOperation starts a client break-reset after a connection is accepted.
func (ns *networkSession) CancelOperation(ctx context.Context) error {
	if !ns.connected {
		return nil
	}
	return ns.breakReset.CancelOperation(ctx)
}

// CheckInbandNotification reports a buffered in-band CONTROL notification.
func (ns *networkSession) CheckInbandNotification() bool {
	return ns.notifications.CheckInbandNotification()
}

// Disconnect sends an optional EOF packet, stops the runtime packet services,
// and then closes the transport adapter.
func (ns *networkSession) Disconnect(ctx context.Context, flags int) error {
	var err error
	if ns.connected {
		ns.connected = false
		if flags&driverCommon.NSFIMM == 0 {
			err = ns.dataService.sendEOF(ctx)
		}
	}
	_ = ns.dataService.streamer.Close()
	ns.breakReset.stopMarkerWatcher()
	if ns.ntAdapter == nil {
		return err
	}
	if cleaner, ok := ns.ntAdapter.(interface{ Clear() }); ok {
		cleaner.Clear()
	}
	disconnectErr := ns.ntAdapter.Disconnect()
	ns.ntAdapter = nil
	if err != nil {
		return err
	}
	return disconnectErr
}
