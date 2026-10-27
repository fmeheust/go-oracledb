package session

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	"github.com/oracle/go-oracledb/v26/internal/driver/network/naming"
	"github.com/oracle/go-oracledb/v26/internal/driver/network/transport"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

const (
	maxRedirectCount = 4
	maxResendCount   = 4
)

// connectionNegotiator owns pre-ACCEPT transport state and activates the
// runtime networkSession services after the server accepts the connection.
type connectionNegotiator struct {
	// cData is the serialized CONNECT_DATA descriptor used during negotiation.
	cData []byte
	// redirectCount and resendCount bound negotiation retries for this attempt.
	redirectCount int
	resendCount   int
	// networkTransport provides connection negotiation with socket ownership.
	transport *networkTransport
	// negotiationSendPacket frames overflow CONNECT_DATA independently from TTC.
	negotiationSendPacket *dataPacket
	// negotiationStreamer is created by transportConnect before negotiation
	// starts; redirects replace it, and ACCEPT transfers socket ownership to the
	// runtime packet streamer and clears it.
	negotiationStreamer *negotiationStreamer
}

// newConnectionNegotiator creates a negotiator for one connection attempt.
func newConnectionNegotiator(attributes *sessionAtts, connectData []byte) *connectionNegotiator {
	return &connectionNegotiator{
		cData:     connectData,
		transport: &networkTransport{sAtts: attributes},
	}
}

// Connect negotiates a transport and returns the fully wired network session.
func (negotiator *connectionNegotiator) Connect(ctx context.Context, address transport.Address) (*networkSession, error) {
	if err := negotiator.connect(ctx, address); err != nil {
		// Some early setup failures occur before the packet path can be started.
		// Close any transport that remains attached to the negotiation shell.
		if negotiator.transport.ntAdapter != nil {
			if cleaner, ok := negotiator.transport.ntAdapter.(interface{ Clear() }); ok {
				cleaner.Clear()
			}
			disconnectErr := negotiator.transport.ntAdapter.Disconnect()
			negotiator.transport.ntAdapter = nil
			if disconnectErr != nil {
				return nil, disconnectErr
			}
		}
		return nil, err
	}
	return newNetworkSession(negotiator.transport), nil
}

// cidSanitizer prevents OS-provided CID values from changing the naming structure.
var cidSanitizer = strings.NewReplacer("(", "_", ")", "_", "=", "_")

// sanitizeCIDValue returns a value that is safe to serialize in a naming node.
func sanitizeCIDValue(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "unknown"
	}
	return cidSanitizer.Replace(v)
}

// buildCIDNode creates the Oracle Net client identifier nested under CONNECT_DATA.
func buildCIDNode() naming.Node {
	return naming.Node{
		Name: "CID",
		Children: []naming.Node{
			{Name: "PROGRAM", Value: sanitizeCIDValue(common.ProgramName)},
			{Name: "HOST", Value: sanitizeCIDValue(common.HostName)},
			{Name: "USER", Value: sanitizeCIDValue(common.UserName)},
		},
	}
}

// cachedCIDNode is immutable process metadata reused by connection attempts.
var cachedCIDNode = buildCIDNode()

// transportConnect establishes the transport-level connection.
func (negotiator *connectionNegotiator) transportConnect(ctx context.Context, address transport.Address) error {
	if address.Protocol == driverCommon.ProtocolTCP && (address.HTTPSProxy != "" || negotiator.transport.sAtts.nt.HttpsProxy != "") {
		return common.NewOracleError(oracleErrors.HTTPSProxyRequiresTCPS, nil)
	}
	if negotiator.transport.ntAdapter == nil {
		switch address.Protocol {
		case driverCommon.ProtocolTCP:
			negotiator.transport.ntAdapter = transport.NewNTTCP(negotiator.transport.sAtts.nt, TCP_DEFAULT_PORT)
		case driverCommon.ProtocolTCPS:
			negotiator.transport.ntAdapter = transport.NewNTTCPS(negotiator.transport.sAtts.nt)
		}
	}
	err := negotiator.transport.ntAdapter.Connect(ctx, address)
	if err != nil {
		// Only transport connection failures can mean that an endpoint is down.
		// Oracle Net, TLS, and authentication failures happen later and do not
		// reach this point. When a proxy is configured, the failure may belong
		// to the proxy rather than the database endpoint, so do not cache the
		// database address as down.
		proxyConfigured := address.HTTPSProxy != "" || negotiator.transport.sAtts.nt.HttpsProxy != ""
		if !proxyConfigured && isDownHostError(ctx, err) {
			key := address.ResolvedIP
			if key == "" {
				key = address.Host
			}
			naming.MarkDownHost(key)
		}
		return err
	}
	// The negotiation stream is required for every packet exchange. Create it
	// immediately after the transport connects, before any negotiation packet
	// buffers are prepared or sent. Redirects pass through this same setup.
	streamer, err := newNegotiationStreamer(negotiator.transport.sAtts.sdu, negotiator.transport.readPacketFromTransport, negotiator.transport.writePacketToTransport)
	if err != nil {
		return err
	}
	negotiator.negotiationStreamer = streamer
	// Allocate a negotiation-owned packet buffer for a possible overflow
	// CONNECT_DATA continuation. TTC buffers do not exist until ACCEPT.
	sendBuffer := make([]byte, negotiator.transport.sAtts.sdu)
	negotiator.negotiationSendPacket = &dataPacket{}
	err = negotiator.negotiationSendPacket.marshal(sendBuffer, negotiator.transport.sAtts, 0)
	if err != nil {
		return err
	}
	return nil
}

// handleAccept validates the server capabilities and finalizes session setup.
func (negotiator *connectionNegotiator) handleAccept(ctx context.Context, p *acceptPacket) error {
	if negotiator.transport.sAtts.version < TNS_VERSION_MINIMUM {
		return common.NewOracleError(oracleErrors.InvalidNetworkContextExpectedValue, nil, "TNS version", "NSPTAC", negotiator.transport.sAtts.version, TNS_VERSION_MINIMUM)
	}

	if negotiator.transport.sAtts.version >= TNS_VERSION_MIN_DATA_FLAGS {
		// Validate the capability field even though this session does not retain
		// the individual ACCEPT flags.
		if len(p.buf) < NSPACFL2+4 { // we gonna read an Uint32
			msg := fmt.Sprintf("Unexpected buffer length (%d) in accept packet", len(p.buf))
			common.Odl.Warn(msg)
			return common.NewOracleError(oracleErrors.InvalidNetworkContextExpectedLength, nil, "packet", "NSPTAC", len(p.buf), NSPACFL2+4)
		}
	}

	tlsadapter, ok := negotiator.transport.ntAdapter.(interface {
		VerifyPostAcceptDNMatch() error
		Clear()
	})
	if ok {
		if err := tlsadapter.VerifyPostAcceptDNMatch(); err != nil {
			return err
		}
		tlsadapter.Clear()
	}
	negotiator.transport.connected = true
	// The server sets NSINAREQUIRED in an ACCEPT flag when Advanced Networking is required.
	if (p.flag0|p.flag1)&NSINAREQUIRED != 0 {
		return common.NewOracleError(oracleErrors.UnsupportedFeature, nil,
			"Native Network Encryption and Data Integrity")
	}
	return nil
}

// refuseArgs collects the placeholder values needed to format the
// localized message for a specific ORA code.
func (negotiator *connectionNegotiator) refuseArgs(errCode string, address transport.Address) ([]any, error) {
	// Parse ns.cData to extract service_name, host, port
	cDataStr := string(negotiator.cData)
	cDataNode, err := naming.Parse(cDataStr)
	if err != nil {
		return nil, err
	}
	switch errCode {
	case "12514":
		serviceName, err := cDataNode.GetValue("DESCRIPTION/CONNECT_DATA/SERVICE_NAME")
		if err != nil {
			return nil, err
		}
		return []any{serviceName, address.Host, address.Port, negotiator.transport.sAtts.nt.Connectionid}, nil
	case "12520":
		serverType, err := cDataNode.GetValue("DESCRIPTION/CONNECT_DATA/SERVER")
		if err != nil {
			return nil, err
		}
		serviceName, err := cDataNode.GetValue("DESCRIPTION/CONNECT_DATA/SERVICE_NAME")
		if err != nil {
			return nil, err
		}
		return []any{serverType, serviceName, address.Host, address.Port, negotiator.transport.sAtts.nt.Connectionid}, nil
	case "12521":
		instanceName, err := cDataNode.GetValue("DESCRIPTION/CONNECT_DATA/INSTANCE_NAME")
		if err != nil {
			return nil, err
		}
		serviceName, err := cDataNode.GetValue("DESCRIPTION/CONNECT_DATA/SERVICE_NAME")
		if err != nil {
			return nil, err
		}
		return []any{instanceName, serviceName, address.Host, address.Port, negotiator.transport.sAtts.nt.Connectionid}, nil
	case "12505":
		sid, err := cDataNode.GetValue("DESCRIPTION/CONNECT_DATA/SID")
		if err != nil {
			return nil, err
		}
		return []any{sid, address.Host, address.Port, negotiator.transport.sAtts.nt.Connectionid}, nil
	default:
		return nil, nil
	}
}

// handleRefuse resolves optional overflow data and maps the server refusal.
func (negotiator *connectionNegotiator) handleRefuse(ctx context.Context, p *refusePacket, address transport.Address) error {
	if p.overflow {
		overflowPacket, err := negotiator.negotiationStreamer.PullNext(ctx)
		if err != nil {
			common.Odl.Error("An error occurred while receiving packet", "error", err)
			return err
		}
		if overflowPacket.typ != NSPTDA {
			return common.NewOracleError(oracleErrors.UnexpectedConnectResponse, nil)
		}
		data, ok := overflowPacket.packet.(*dataPacket)
		if !ok {
			return common.NewOracleError(oracleErrors.UnexpectedConnectResponse, nil)
		}
		p.dataBuf = string(data.buf[data.offset:data.len])
	}
	refuseNode, err := naming.Parse(p.dataBuf)
	if err != nil {
		return common.NewOracleError(oracleErrors.RefuseDataParseFailed, err)
	}
	errCode, err := refuseNode.GetValue("DESCRIPTION/ERR")
	if err != nil {
		return err
	}
	mappedCode, ok := oracleErrors.OracleRefuseErrorCodes[errCode]
	if !ok {
		return common.NewOracleError(oracleErrors.ConnectionRefusedDetail, nil, errCode, p.userReason, p.systemReason)
	}

	args, err := negotiator.refuseArgs(errCode, address)
	if err != nil {
		return err
	}
	return common.NewOracleError(mappedCode, nil, args...)
}

// handleRedirect reads optional overflow data and reconnects through the redirect target.
func (negotiator *connectionNegotiator) handleRedirect(ctx context.Context, p *redirectPacket, address transport.Address) error {
	negotiator.redirectCount++
	if negotiator.redirectCount > maxRedirectCount {
		return common.NewOracleError(oracleErrors.NetworkRetryLimitExceeded, nil, "redirects", maxRedirectCount)
	}
	if p.overflow {
		overflowPacket, err := negotiator.negotiationStreamer.PullNext(ctx)
		if err != nil {
			return err
		}
		if overflowPacket.typ != NSPTDA {
			return common.NewOracleError(oracleErrors.UnexpectedConnectResponse, nil)
		}
		data, ok := overflowPacket.packet.(*dataPacket)
		if !ok {
			return common.NewOracleError(oracleErrors.UnexpectedConnectResponse, nil)
		}
		p.dataBuf = data.buf[data.offset:data.len]
	}
	var addrStr string
	var redirectConnectData []byte
	if (p.hdr.flags & byte(NSPFRDS)) != 0 {
		parts := bytes.Split(p.dataBuf, []byte{0})
		if len(parts) > 0 {
			addrStr = string(parts[0])
		}
		if len(parts) > 1 {
			redirectConnectData = parts[1]
		}
	} else {
		addrStr = string(p.dataBuf)
		redirectConnectData = negotiator.cData
	}
	redirAddressNode, err := naming.Parse(addrStr)
	if err != nil {
		return err
	}
	oldhostname := address.Hostname
	redirCtx, err := naming.ExtractConnectionContext(redirAddressNode)
	if err != nil {
		return err
	}
	iter := naming.NewConnectionIterator(ctx, redirAddressNode, redirCtx)

	if iter.HasNext() {
		redirOption := iter.Next()
		redirOption.Address.OriginHost = oldhostname
		hostToBeUsed := redirOption.Address.Host
		if redirOption.Address.ResolvedIP != "" {
			hostToBeUsed = redirOption.Address.ResolvedIP
		}
		newAddress := transport.Address{
			Address: naming.Address{
				Host:           hostToBeUsed,
				Port:           redirOption.Address.Port,
				Protocol:       redirOption.Address.Protocol,
				HTTPSProxy:     address.HTTPSProxy,
				HTTPSProxyPort: address.HTTPSProxyPort,
				OriginHost:     oldhostname,
				ResolvedIP:     redirOption.Address.ResolvedIP,
			},
			Hostname:       redirOption.Address.Host,
			HTTPSProxy:     address.HTTPSProxy,
			HTTPSProxyPort: address.HTTPSProxyPort,
		}
		// Negotiation reads synchronously, so no reader goroutine remains to
		// drain packets from the old transport after this point.
		negotiator.negotiationStreamer = nil
		negotiator.transport.ntAdapter.Disconnect()
		negotiator.transport.connected = false
		err = negotiator.transportConnect(ctx, newAddress)
		if err != nil {
			return err
		}
		connectPkt := &connectPacket{}
		connectPkt.marshal(redirectConnectData, negotiator.transport.sAtts, NSPFRDR)
		err = negotiator.sendConnect(ctx, connectPkt)
		if err != nil {
			return err
		}
		return nil // continue the loop
	}
	return common.NewOracleError(oracleErrors.RedirectAddressMissing, nil)
}

// handleResend processes a server resend request and resends the connect packet.
func (negotiator *connectionNegotiator) handleResend(ctx context.Context, p *resendPacket, connectPkt *connectPacket) error {
	if p.hdr.flags&NSPFSRN != 0 {
		tlsAdapter, ok := negotiator.transport.ntAdapter.(interface{ TLSReneg() })
		/*
			Oracle uses that flag on resend packets (NSPTRS) to tell the client
			"please renegotiate the TLS session" essentially a server-side TLS renegotiation request.
			In normal operation it should only come back when you're connected over TCPS, because there's
			nothing to renegotiate on plain TCP. However, since the packet header is remote-controlled,
			a hostile listener could flip the bit even on a TCP session, which is why we now treat it as
			an error when there's no TLS-capable adapter behind the session.
		*/
		if !ok {
			return common.NewOracleError(oracleErrors.TLSRenegotiationUnsupported, nil)
		}
		tlsAdapter.TLSReneg()
	}
	err := negotiator.sendConnect(ctx, connectPkt)
	if err != nil {
		negotiator.transport.ntAdapter.Disconnect()
		return err
	}
	return nil // continue the loop
}

// connect negotiates Oracle Net synchronously, then transfers the socket to
// the runtime packet streamer once the server accepts the connection.
func (negotiator *connectionNegotiator) connect(ctx context.Context, address transport.Address) error {
	var err error
	err = negotiator.transportConnect(ctx, address)
	if err != nil {
		return err
	}
	connectPkt := &connectPacket{}
	connectPkt.marshal(negotiator.cData, negotiator.transport.sAtts, NO_HEADER_FLAGS)
	err = negotiator.sendConnect(ctx, connectPkt)
	if err != nil {
		return err
	}
	for {
		// An overflow CONNECT descriptor may leave its continuation buffered
		// while the server waits for it, so flush before waiting for its response.
		if err := negotiator.flushNegotiationSendBuffer(ctx); err != nil {
			return err
		}
		incoming, err := negotiator.negotiationStreamer.PullNext(ctx)
		if err != nil {
			return err
		}
		pkt := incoming.packet
		switch p := pkt.(type) {
		case *acceptPacket:
			err = negotiator.handleAccept(ctx, p)
			if err != nil {
				return err
			}
			// The background reader now exclusively owns transport reads.
			negotiator.negotiationStreamer = nil
			return nil
		case *refusePacket:
			err = negotiator.handleRefuse(ctx, p, address)
			return err
		case *redirectPacket:
			err = negotiator.handleRedirect(ctx, p, address)
			if err != nil {
				return err
			}
		case *resendPacket:
			negotiator.resendCount++
			if negotiator.resendCount > maxResendCount {
				return common.NewOracleError(oracleErrors.NetworkRetryLimitExceeded, nil, "resends", maxResendCount)
			}
			err = negotiator.handleResend(ctx, p, connectPkt)
			if err != nil {
				return err
			}
		default:
			return common.NewOracleError(oracleErrors.UnexpectedConnectResponse, nil)
		}
	}
}

// ConnectToOptionWithConnectionID opens a session and adds connectionID to its descriptor.
func ConnectToOptionWithConnectionID(ctx context.Context, option *naming.ConnectionOption, connectionID string) (driverCommon.NetworkSession, error) {
	addressOption := option.Address
	description := option.Description

	portToBeUsed := addressOption.Port
	if addressOption.Port == 0 {
		common.Odl.Fine("no port specified, fall-back to default", "port", TCP_DEFAULT_PORT)
		portToBeUsed = TCP_DEFAULT_PORT
	}

	attributes := newSessionAtts(connectionID)
	if description != nil {
		attributes.setFrom(description)
	}
	err := attributes.prepare(addressOption.Protocol)
	if err != nil {
		return nil, err
	}
	connectStr := option.ConnectString
	root, err := naming.Parse(connectStr)
	if err != nil {
		return nil, err
	}
	connectData, err := root.GetNode("DESCRIPTION/CONNECT_DATA")
	if err != nil {
		root.Children = append(root.Children, naming.Node{Name: "CONNECT_DATA"})
		connectData = &root.Children[len(root.Children)-1]
	}
	connectData.Children = append(connectData.Children, cachedCIDNode)
	connIDNode := naming.Node{Name: "CONNECTION_ID", Value: attributes.nt.Connectionid}
	connectData.Children = append(connectData.Children, connIDNode)
	newConnectStr := root.ToString()

	hostToBeUsed := addressOption.Host
	if addressOption.ResolvedIP != "" {
		hostToBeUsed = addressOption.ResolvedIP
	}
	address := transport.Address{
		Address: naming.Address{
			Host:           hostToBeUsed,
			Port:           portToBeUsed,
			Protocol:       addressOption.Protocol,
			ResolvedIP:     addressOption.ResolvedIP,
			HTTPSProxy:     addressOption.HTTPSProxy,
			HTTPSProxyPort: addressOption.HTTPSProxyPort,
		},
		Hostname:       addressOption.Host,
		HTTPSProxy:     addressOption.HTTPSProxy,
		HTTPSProxyPort: addressOption.HTTPSProxyPort,
	}

	negotiator := newConnectionNegotiator(attributes, []byte(newConnectStr))
	return negotiator.Connect(ctx, address)
}

// sendConnect sends the NSPTCN packet and any overflow connection data.
func (negotiator *connectionNegotiator) sendConnect(ctx context.Context, connectPkt *connectPacket) error {
	if err := negotiator.negotiationStreamer.Send(ctx, connectPkt.buf); err != nil {
		return err
	}
	if connectPkt.overflow {
		return negotiator.sendConnectData(ctx, connectPkt.connectData, connectPkt.connectDataLen)
	}
	return nil
}

// sendConnectData packetizes CONNECT_DATA that did not fit in the CONNECT
// packet. The final partial packet stays buffered until the response loop
// flushes it, matching the server's expected continuation sequence.
func (negotiator *connectionNegotiator) sendConnectData(ctx context.Context, data []byte, length int) error {
	if length <= 0 {
		return nil
	}
	packet := negotiator.negotiationSendPacket
	if length > len(data) || packet == nil {
		return common.NewOracleError(oracleErrors.InternalError, nil)
	}
	offset := 0
	for offset < length {
		copied := packet.FillBuf(data, offset, length-offset)
		offset += copied
		if offset == length {
			break
		}
		if copied == 0 && packet.offset < packet.bufLen {
			return common.NewOracleError(oracleErrors.InternalError, nil)
		}
		if err := packet.Prepare2Send(0, negotiator.transport.sAtts); err != nil {
			return err
		}
		if err := negotiator.negotiationStreamer.Send(ctx, packet.buf[:packet.offset]); err != nil {
			return err
		}
		packet.Reset()
	}
	return nil
}

// flushNegotiationSendBuffer sends a partial CONNECT_DATA continuation before
// waiting for the server's response. This buffer belongs to negotiation and
// is independent of the runtime data service's packet streamer.
func (negotiator *connectionNegotiator) flushNegotiationSendBuffer(ctx context.Context) error {
	packet := negotiator.negotiationSendPacket
	if packet == nil || packet.offset <= NSPDADAT {
		return nil
	}
	if err := packet.Prepare2Send(0, negotiator.transport.sAtts); err != nil {
		return err
	}
	if err := negotiator.negotiationStreamer.Send(ctx, packet.buf[:packet.offset]); err != nil {
		return err
	}
	packet.Reset()
	return nil
}
