package session

import (
	"context"
	"io"

	"github.com/oracle/go-oracledb/v26/internal/common"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// dataService implements the synchronous TTC byte stream over network DATA
// packets. It knows only the packet stream and its own TTC buffers.
type dataService struct {
	// streamer is the shared packet reader and serialized writer for this session.
	streamer *packetStreamer
	// attributes contains the negotiated packet format used by the TTC path.
	attributes *sessionAtts
	// ttcPacketBuffers owns this service's TTC packet state.
	*ttcPacketBuffers
}

// ttcPacketBuffers owns send and receive frames used only by dataService.
type ttcPacketBuffers struct {
	// sndDatapkt buffers outgoing TTC bytes until they are sent as DATA packets.
	sndDatapkt *dataPacket
	// rcvDatapkt is the DATA packet currently exposed to the TTC caller.
	rcvDatapkt *dataPacket
}

// newDataService creates the TTC DATA path for one established packet stream.
// It owns both packet buffers, including the outgoing packet header that must
// be ready before TTC begins writing bytes.
func newDataService(streamer *packetStreamer, attributes *sessionAtts) *dataService {
	sendBuffer := make([]byte, attributes.sdu)
	sendPacket := &dataPacket{}
	if err := sendPacket.marshal(sendBuffer, attributes, 0); err != nil {
		panic(err)
	}
	return &dataService{
		streamer:   streamer,
		attributes: attributes,
		ttcPacketBuffers: &ttcPacketBuffers{
			sndDatapkt: sendPacket,
			rcvDatapkt: &dataPacket{},
		},
	}
}

// flushPendingSendBuffer sends buffered TTC bytes before a packet wait so a
// server that is waiting for request data can produce its response.
func (ns *dataService) flushPendingSendBuffer(ctx context.Context) error {
	if ns.sndDatapkt.offset <= NSPDADAT {
		return nil
	}
	if err := ns.sndDatapkt.Prepare2Send(0, ns.attributes); err != nil {
		return err
	}
	err := ns.streamer.writePacket(ctx, ns.sndDatapkt.buf[:ns.sndDatapkt.offset])
	if err != nil {
		return err
	}
	ns.sndDatapkt.Reset()
	return nil
}

// writeBytes appends TTC bytes to the outgoing DATA packet and writes each
// full SDU-sized packet through the streamer. A partial final packet remains
// buffered until a later write, a read wait, or flush.
func (ns *dataService) writeBytes(ctx context.Context, bytes []byte) error {
	if len(bytes) == 0 {
		return nil
	}
	bytesCopied := 0
	offset := 0
	length := len(bytes)
	if ns.sndDatapkt.offset < ns.sndDatapkt.bufLen {
		bytesCopied = ns.sndDatapkt.FillBuf(bytes, offset, length)
		length -= bytesCopied
		offset += bytesCopied
	}
	for length > 0 {
		err := ns.sndDatapkt.Prepare2Send(0, ns.attributes)
		if err != nil {
			return err
		}
		err = ns.streamer.writePacket(ctx, ns.sndDatapkt.buf)
		ns.sndDatapkt.Reset()
		if err != nil {
			return err
		}
		bytesCopied = ns.sndDatapkt.FillBuf(bytes, offset, length)
		length -= bytesCopied
		offset += bytesCopied
	}
	return nil
}

// prepareReadBuffer ensures that the current TTC data packet has unread bytes.
func (ns *dataService) prepareReadBuffer(ctx context.Context) error {
	if ns.rcvDatapkt.Remaining() > 0 {
		return nil
	}
	return ns.receiveDataPacket(ctx)
}

// receiveDataPacket waits for the next DATA handoff.
func (ns *dataService) receiveDataPacket(ctx context.Context) error {
	// The operation context owns cancellation of TTC work, while the registered
	// cancellation callback sends break-reset. Do not let that same cancellation
	// abort this packet wait before the post-reset response DATA packet arrives.
	readCtx := context.WithoutCancel(ctx)
	if ctx.Err() == nil {
		if err := ns.flushPendingSendBuffer(ctx); err != nil {
			return err
		}
	}
	incoming, err := ns.streamer.readDataPacket(readCtx)
	if err != nil {
		return err
	}
	packet, ok := incoming.packet.(*dataPacket)
	if !ok {
		return common.NewOracleError(oracleErrors.InvalidNetworkValue, nil,
			"packet type", incoming.typ)
	}
	ns.rcvDatapkt = packet
	return nil
}

// readByte reads one TTC byte from the current DATA packet stream.
func (ns *dataService) readByte(ctx context.Context) (byte, error) {
	buffer, err := ns.readNBytes(ctx, 1)
	if err != nil {
		return 0, err
	}
	return buffer[0], nil
}

// readBytes reads length TTC bytes, spanning DATA packets as needed.
func (ns *dataService) readBytes(ctx context.Context, length int32) ([]byte, error) {
	if length < 0 {
		return nil, common.NewOracleError(oracleErrors.InvalidNetworkLength, nil, "buffer", length)
	}
	return ns.readNBytes(ctx, int(length))
}

// readNBytes is the integer-length implementation shared by TTC read helpers.
func (ns *dataService) readNBytes(ctx context.Context, n int) ([]byte, error) {
	err := ns.prepareReadBuffer(ctx)
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, common.NewOracleError(oracleErrors.InvalidNetworkLength, nil, "buffer", n)
	}
	b := make([]byte, n)
	err = ns.readMultiPacket(ctx, b, n)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// readMultiPacket reads bytes across DATA packet boundaries. MARKER packet
// handling is owned by breakResetService, so this service waits only for DATA.
func (ns *dataService) readMultiPacket(ctx context.Context, buf []byte, numBytes int) error {
	bytesRead := 0
	for bytesRead < numBytes {
		if ns.rcvDatapkt.Remaining() == 0 {
			if err := ns.prepareReadBuffer(ctx); err != nil {
				return err
			}
			continue
		}

		bytesToRead := numBytes - bytesRead
		if bytesToRead > ns.rcvDatapkt.Remaining() {
			bytesToRead = ns.rcvDatapkt.Remaining()
		}
		data, err := ns.rcvDatapkt.Read(bytesToRead)
		if err != nil {
			return err
		}

		copy(buf[bytesRead:], data)
		bytesRead += len(data)
	}

	return nil

}

// writeByte appends one byte to the outgoing TTC data packet.
func (ns *dataService) writeByte(ctx context.Context, value byte) error {
	if ns.sndDatapkt.len-ns.sndDatapkt.offset < 1 {
		if err := ns.flush(ctx); err != nil {
			return err
		}
	}
	b := ns.sndDatapkt.buf[ns.sndDatapkt.offset:]
	b[0] = value
	ns.sndDatapkt.offset++
	return nil
}

// sendEOF writes the Oracle Net DATA EOF marker before transport shutdown.
func (ns *dataService) sendEOF(ctx context.Context) error {
	err := ns.sndDatapkt.Prepare2Send(NSPDAFEOF, ns.attributes)
	if err != nil {
		return err
	}
	err = ns.streamer.writePacket(ctx, ns.sndDatapkt.buf[:ns.sndDatapkt.offset])
	if err != nil {
		if err == io.EOF {
			return common.NewOracleError(oracleErrors.ConnectionClosed, err)
		}
		return err
	}
	return nil
}

// flush sends the buffered TTC data packet through the streamer.
func (ns *dataService) flush(ctx context.Context) error {
	if ns.sndDatapkt.offset <= NSPDADAT {
		return nil
	}
	err := ns.sndDatapkt.Prepare2Send(0, ns.attributes)
	if err != nil {
		return err
	}
	err = ns.streamer.writePacket(ctx, ns.sndDatapkt.buf[:ns.sndDatapkt.offset])
	if err != nil {
		if err == io.EOF {
			return common.NewOracleError(oracleErrors.ConnectionClosed, err)
		}
		return err
	}
	// Clear the packet after it is written so a full buffer cannot make the next
	// writeByte index past its end.
	ns.sndDatapkt.Reset()
	return nil
}
