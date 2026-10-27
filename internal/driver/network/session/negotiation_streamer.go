package session

import (
	"context"

	"github.com/oracle/go-oracledb/v26/internal/common"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// negotiationStreamer synchronously reads and writes packets while the
// connection negotiator owns the transport. It has no reader goroutine, so
// packet reads cannot race the later runtime packet streamer for the socket.
type negotiationStreamer struct {
	// buffer is reused for packet reads; decoded packets own their contents.
	buffer []byte
	// reader reads and decodes one complete packet from the active transport.
	reader packetReader
	// writer serializes and writes one complete packet to the active transport.
	writer packetWriter
}

// newNegotiationStreamer creates a single-threaded packet stream for one
// transport connection. A new instance is required after a redirect.
func newNegotiationStreamer(bufferSize int, reader packetReader, writer packetWriter) (*negotiationStreamer, error) {
	if bufferSize <= 0 || reader == nil || writer == nil {
		return nil, common.NewOracleError(oracleErrors.InternalError, nil)
	}
	return &negotiationStreamer{
		buffer: make([]byte, bufferSize),
		reader: reader,
		writer: writer,
	}, nil
}

// PullNext reads exactly one packet in wire order. It does not filter or
// prefetch, so the negotiator can handle every packet type explicitly.
func (streamer *negotiationStreamer) PullNext(ctx context.Context) (*networkPacket, error) {
	packet, err := streamer.reader(ctx, streamer.buffer)
	if err != nil {
		return nil, err
	}
	if packet == nil {
		return nil, common.NewOracleError(oracleErrors.InternalError, nil)
	}
	return packet, nil
}

// Send writes one fully encoded packet through the active transport.
func (streamer *negotiationStreamer) Send(ctx context.Context, packet []byte) error {
	return streamer.writer(ctx, packet)
}
