package session

import (
	"context"
	"time"

	"github.com/oracle/go-oracledb/v26/internal/common"
)

// notificationService checks buffered in-band control packets without reading
// from the socket or consuming DATA and MARKER handoffs.
type notificationService struct {
	// streamer provides the buffered CONTROL channel and shared stream lifecycle.
	streamer *packetStreamer
}

// CheckInbandNotification briefly waits for a buffered control packet, then
// consumes it without reading directly from the socket.
func (ns *notificationService) CheckInbandNotification() bool {
	streamer := ns.streamer
	if streamer == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(common.BackgroundContext, 50*time.Microsecond)
	defer cancel()
	packet, err := streamer.readControlPacket(ctx)
	if err != nil {
		return false
	}
	control, ok := packet.packet.(*controlPacket)
	if !ok {
		return false
	}
	if control.isNotification {
		control.Clear()
		return true
	}
	control.Clear()
	return false
}
