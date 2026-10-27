package session

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/oracle/go-oracledb/v26/internal/common"
	"github.com/oracle/go-oracledb/v26/internal/driver/network/transport"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// networkTransport owns the active adapter and packet streamer. Every higher-
// level service transfers packets through the streamer rather than accessing
// the socket directly.
type networkTransport struct {
	// sAtts contains negotiated network and packet-format attributes.
	sAtts *sessionAtts
	// ntAdapter is the active TCP or TCPS socket adapter.
	ntAdapter transport.NTAdapter
	// connected records whether Oracle Net accepted this transport.
	connected bool
}

// GetRemoteAddress returns the connected remote network address when it is
// available, or an empty string otherwise.
func (ns *networkTransport) GetRemoteAddress() string {
	remoteAddr := ns.getRemoteTCPAddr()
	if remoteAddr == nil {
		return ""
	}
	return remoteAddr.IP.String()
}

// GetRemotePort returns the connected remote network port when it is
// available, or 0 otherwise.
func (ns *networkTransport) GetRemotePort() int {
	remoteAddr := ns.getRemoteTCPAddr()
	if remoteAddr == nil {
		return 0
	}
	return remoteAddr.Port
}

// getRemoteTCPAddr extracts a TCP address from adapters that expose RemoteAddr.
func (ns *networkTransport) getRemoteTCPAddr() *net.TCPAddr {
	type remoteAddrProvider interface {
		RemoteAddr() net.Addr
	}
	adapter, ok := ns.ntAdapter.(remoteAddrProvider)
	if !ok {
		return nil
	}
	return remoteTCPAddrFromConn(adapter.RemoteAddr())
}

// remoteTCPAddrFromConn returns the remote address only when it is TCP.
func remoteTCPAddrFromConn(remoteAddr net.Addr) *net.TCPAddr {
	if remoteAddr == nil {
		return nil
	}
	tcpAddr, ok := remoteAddr.(*net.TCPAddr)
	if !ok {
		return nil
	}
	return tcpAddr
}

// isDownHostError identifies transport failures that indicate a host or its
// route is unavailable. A refusal is excluded because it proves the host
// responded, even when no listener is available there.
func isDownHostError(ctx context.Context, err error) bool {
	// Caller deadlines indicate that the operation gave up, not that the host
	// should be marked unavailable.
	if errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return false
	}
	if errors.Is(err, syscall.EHOSTDOWN) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) {
		return true
	}

	var timeoutCause common.CtxTimeoutCauseError
	if errors.As(err, &timeoutCause) {
		return true
	}
	var sqlErr oracleErrors.SQLError
	if errors.As(err, &sqlErr) && sqlErr.ErrorCode() == string(oracleErrors.CtxTimeout) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// readPacketFromTransport reads one complete packet from the active socket and
// decodes it. The packet streamer supplies a private scratch buffer; decoded
// packet values receive owned bytes before this function returns.
func (ns *networkTransport) readPacketFromTransport(ctx context.Context, buffer []byte) (*networkPacket, error) {
	n, err := ns.ntAdapter.Receive(ctx, buffer, PACKET_HEADER_SIZE)
	if err != nil {
		return nil, err
	}
	if n != PACKET_HEADER_SIZE {
		return nil, common.NewOracleError(oracleErrors.InvalidNetworkExpectedLength, nil,
			"packet header", n, PACKET_HEADER_SIZE)
	}

	var packetLen int
	if ns.sAtts.largeSDU {
		packetLen = int(binary.BigEndian.Uint32(buffer[0:4]))
	} else {
		packetLen = int(binary.BigEndian.Uint16(buffer[0:2]))
	}
	if packetLen < PACKET_HEADER_SIZE || packetLen > len(buffer) {
		return nil, common.NewOracleError(oracleErrors.InvalidNetworkLength, nil, "packet", packetLen)
	}
	bodyLen := packetLen - PACKET_HEADER_SIZE
	if bodyLen > 0 {
		n, err = ns.ntAdapter.Receive(ctx, buffer[PACKET_HEADER_SIZE:packetLen], bodyLen)
		if err != nil {
			return nil, err
		}
		if n != bodyLen {
			return nil, common.NewOracleError(oracleErrors.InvalidNetworkExpectedLength, nil,
				"packet body", n, bodyLen)
		}
	}

	wire := append([]byte(nil), buffer[:packetLen]...)
	common.Odl.PacketDump(wire)
	hdr := &header{}
	if err := hdr.unmarshal(wire, ns.sAtts, nil); err != nil {
		return nil, err
	}
	packet, err := ns.processOwnedPacket(wire, hdr)
	if err != nil {
		return nil, err
	}
	return &networkPacket{typ: hdr.typ, packet: packet}, nil
}

// processOwnedPacket decodes an owned copy of the packet. The background reader
// cannot retain its reusable scratch buffer in any decoded packet value.
func (ns *networkTransport) processOwnedPacket(buf []byte, hdr *header) (any, error) {
	var packet packetUnmarshaller
	switch hdr.typ {
	case NSPTAC:
		packet = &acceptPacket{}
	case NSPTRF:
		packet = &refusePacket{}
	case NSPTRD:
		packet = &redirectPacket{}
	case NSPTRS:
		packet = &resendPacket{}
	case NSPTMK:
		packet = &markerPacket{}
	case NSPTCNL:
		packet = &controlPacket{}
	// NSPTDA is an Oracle Net DATA packet; its payload begins at NSPDADAT.
	case NSPTDA:
		if int(hdr.packetLength) < NSPDADAT {
			return nil, common.NewOracleError(oracleErrors.InvalidNetworkContextExpectedLength, nil, "packet", "NSPTDA", hdr.packetLength, NSPDADAT)
		}
		flags := binary.BigEndian.Uint16(buf[NSPDAFLG:])
		if ns.sAtts.networkCompressionEnabled && flags&NSPDAFCMP != 0 {
			// NSPDAFCMP applies only to the payload; keep the wire header intact.
			header := append([]byte(nil), buf[:NSPDADAT]...)
			payload := buf[NSPDADAT:]
			var r io.ReadCloser
			var err error
			if ns.sAtts.firstRecvCompressedPacket {
				// Oracle Net uses zlib Z_SYNC_FLUSH framing: the first packet has a
				// zlib wrapper and later packets use raw DEFLATE.
				r, err = zlib.NewReader(bytes.NewReader(payload))
				ns.sAtts.firstRecvCompressedPacket = false
			} else {
				r = flate.NewReader(bytes.NewReader(payload))
			}
			if err != nil {
				common.Odl.Error("failed to initialize network decompression", "algorithm", "zlib", "error", err, "payload-length", len(payload))
				return nil, common.NewOracleError(oracleErrors.NetworkDecompressionFailed, err, "zlib")
			}
			decompressed, err := io.ReadAll(r)
			closeErr := r.Close()
			syncFlush := bytes.HasSuffix(payload, []byte{0, 0, 0xff, 0xff})
			// Oracle Net packets end a continuing zlib/DEFLATE stream with a
			// SYNC_FLUSH marker, not a final stream marker. Go reports
			// io.ErrUnexpectedEOF for that valid packet boundary. Any other
			// unexpected EOF is a truncated packet.
			if err != nil && (!errors.Is(err, io.ErrUnexpectedEOF) || !syncFlush) {
				common.Odl.Error("failed to decompress network packet", "algorithm", "zlib", "error", err, "payload-length", len(payload), "sync-flush", syncFlush)
				return nil, common.NewOracleError(oracleErrors.NetworkDecompressionFailed, err, "zlib")
			}
			if closeErr != nil && (!errors.Is(closeErr, io.ErrUnexpectedEOF) || !syncFlush) {
				common.Odl.Error("failed to close network decompressor", "algorithm", "zlib", "error", closeErr, "payload-length", len(payload), "sync-flush", syncFlush)
				return nil, common.NewOracleError(oracleErrors.NetworkDecompressionFailed, closeErr, "zlib")
			}
			buf = append(header, decompressed...)
			// The payload size changed, so update the packet length and remove its compression flag
			// before the normal data-packet unmarshal path reads the packet.
			if ns.sAtts.largeSDU {
				binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)))
			} else {
				binary.BigEndian.PutUint16(buf[:2], uint16(len(buf)))
			}
			binary.BigEndian.PutUint16(buf[NSPDAFLG:], flags&^NSPDAFCMP)
			hdr.unmarshal(buf, ns.sAtts, nil)
		}
		packet = &dataPacket{}
	default:
		return nil, common.NewOracleError(oracleErrors.InvalidNetworkValue, nil, "packet type", hdr.typ)
	}
	err := packet.unmarshal(buf, ns.sAtts, hdr)
	if err != nil {
		return nil, err
	}
	return packet, nil
}

// validateOutgoingPacket checks the common packet header and any DATA header
// fields the compression path will inspect before starting or using a stream.
func (ns *networkTransport) validateOutgoingPacket(buf []byte) error {
	if len(buf) < PACKET_HEADER_SIZE {
		return common.NewOracleError(oracleErrors.InvalidNetworkExpectedLength, nil, "packet buffer", len(buf), PACKET_HEADER_SIZE)
	}
	if ns.sAtts != nil && ns.sAtts.networkCompressionEnabled &&
		len(buf) > ns.sAtts.networkCompressionThreshold && buf[4] == NSPTDA && len(buf) < NSPDADAT {
		return common.NewOracleError(oracleErrors.InvalidNetworkContextExpectedLength, nil, "packet", "NSPTDA", len(buf), NSPDADAT)
	}
	return nil
}

// writePacketToTransport compresses eligible DATA packets and writes them to
// the adapter. packetStreamer calls this while holding its serialized write
// gate, so compression state advances in the same order as socket writes.
func (ns *networkTransport) writePacketToTransport(ctx context.Context, buf []byte) error {
	if err := ns.validateOutgoingPacket(buf); err != nil {
		return err
	}
	if ns.sAtts.networkCompressionEnabled && len(buf) > ns.sAtts.networkCompressionThreshold && buf[4] == NSPTDA {
		// Only data-packet payloads above the negotiated threshold may be compressed.
		header := append([]byte(nil), buf[:NSPDADAT]...)
		payload := buf[NSPDADAT:]
		var (
			compressed bytes.Buffer
			err        error
		)
		if ns.sAtts.firstSendCompressedPacket {
			// Start the stream with zlib framing; later packets use raw DEFLATE.
			zw, zErr := zlib.NewWriterLevel(&compressed, zlib.DefaultCompression)
			if zErr != nil {
				common.Odl.Error("failed to initialize network compression", "algorithm", "zlib", "error", zErr, "payload-length", len(payload))
				return zErr
			}
			if _, err = zw.Write(payload); err == nil {
				err = zw.Flush()
			}
		} else {
			fw, fErr := flate.NewWriter(&compressed, flate.DefaultCompression)
			if fErr != nil {
				common.Odl.Error("failed to initialize network compression", "algorithm", "deflate", "error", fErr, "payload-length", len(payload))
				return fErr
			}
			if _, err = fw.Write(payload); err == nil {
				err = fw.Flush()
			}
		}

		if err != nil {
			common.Odl.Error("failed to compress network packet", "algorithm", "zlib", "error", err, "payload-length", len(payload))
			return common.NewOracleError(oracleErrors.NetworkCompressionFailed, err, "zlib")
		}
		compressedBytes := compressed.Bytes()
		if len(compressedBytes) < len(payload) {
			// Use compression only if it makes the payload smaller.
			if ns.sAtts.firstSendCompressedPacket {
				ns.sAtts.firstSendCompressedPacket = false
			}
			buf = append(header, compressedBytes...)
			// Mark the data flags as compressed, then publish the new length.
			flags := binary.BigEndian.Uint16(buf[NSPDAFLG:])
			binary.BigEndian.PutUint16(buf[NSPDAFLG:], flags|NSPDAFCMP)
			if ns.sAtts.largeSDU {
				binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)))
			} else {
				binary.BigEndian.PutUint16(buf[:2], uint16(len(buf)))
			}
		}
	}
	return ns.ntAdapter.Send(ctx, buf)
}
