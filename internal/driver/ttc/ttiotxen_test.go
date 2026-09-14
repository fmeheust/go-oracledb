/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package ttc

import (
	"bytes"
	"context"
	"strings"
	"testing"

	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	extensions "github.com/oracle/go-oracledb/v26/oracle/extensions"
)

func newOTxEnEngine(capacity int) (*ArrayBasedDataBuffer, *MarshalEngine) {
	buf := NewArrayDataBuffer(capacity)
	engine := NewMarshalEngine(buf, driverCommon.BIG_ENDIAN, [5]byte{Native, Universal, Universal, Universal, Universal})
	return buf, engine
}

func TestOTxEnFactoryRegistration(t *testing.T) {
	t.Parallel()

	functionRegistry := NewRegistry[functionRegistryKey]()
	if err := functionRegistry.Register(functionRegistryKey{messageType: TTIFUN, functionType: oTxEn}, 18, newOTxEn18); err != nil {
		t.Fatalf("register TTC 18+ OTXEN failed: %v", err)
	}
	if err := functionRegistry.Register(functionRegistryKey{messageType: TTIFUN, functionType: oTxEn}, MinTTCProtocolVersion, newOTxEn); err != nil {
		t.Fatalf("register legacy OTXEN failed: %v", err)
	}

	factory := &SimpleFactory{
		ttcVersion:   18,
		msgregistry:  NewRegistry[driverCommon.MessageType](),
		funcregistry: functionRegistry,
	}
	msg, err := factory.GetMessageForFunction(TTIFUN, oTxEn)
	if err != nil {
		t.Fatalf("GetMessageForFunction(TTIFUN, oTxEn) failed: %v", err)
	}
	if msg.GetMsgCode() != TTIFUN {
		t.Fatalf("message code = %v, want %v", msg.GetMsgCode(), TTIFUN)
	}
	if got := msg.(driverCommon.Function).GetFuncCode(); got != oTxEn {
		t.Fatalf("function code = %v, want %v", got, oTxEn)
	}
}

func TestOTxEnMarshalTo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	xid := driverCommon.B1Array{0x11, 0x22, 0x33, 0x44}
	tx := &sessionlessTransaction{
		globalTransactionID: extensions.GlobalTransactionId("g1"),
		xid:                 xid,
		gtridLength:         2,
		bqualLength:         2,
		timeout:             30,
	}

	msg := newOTxEn18().(*tTIOtxen)
	msg.confugureForCommit(tx)

	buf, engine := newOTxEnEngine(256)
	if err := msg.MarshalTo(ctx, engine); err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}
	got := buf.bytes[:buf.currentWritePosition]
	if got[0] != byte(oTxEn) {
		t.Fatalf("function code = %d, want %d", got[0], oTxEn)
	}

	idx := 3 // TTC 18+ function, sequence, and token fields.
	assertUniversal := func(name string, want int) {
		t.Helper()
		value, size, ok := decodeUniversalAt(got, idx)
		if !ok {
			t.Fatalf("could not decode %s at offset %d", name, idx)
		}
		if value != want {
			t.Fatalf("%s = %d, want %d", name, value, want)
		}
		idx += size
	}
	assertPointer := func(name string, want byte) {
		t.Helper()
		if idx >= len(got) {
			t.Fatalf("%s is missing, want %#x", name, want)
		}
		if got[idx] != want {
			t.Fatalf("%s = %#x, want %#x", name, got[idx], want)
		}
		idx++
	}

	assertUniversal("operation", int(otxenCommit))
	assertPointer("transaction context pointer", 0)
	assertUniversal("transaction context length", 0)
	assertUniversal("format ID", int(k2gSessionless))
	assertUniversal("GTRID length", 2)
	assertUniversal("BQUAL length", 2)
	assertPointer("XID pointer", 1)
	assertUniversal("XID length", len(xid))
	assertUniversal("timeout", 30)
	assertUniversal("in state", int(k2cmdCommit))
	assertPointer("out-state pointer", 1)
	assertUniversal("transaction state change flags", 0)

	if !bytes.Equal(got[idx:idx+len(xid)], xid) {
		t.Fatalf("XID bytes = % X, want % X", got[idx:idx+len(xid)], xid)
	}
	idx += len(xid)
	if idx != len(got) {
		t.Fatalf("unexpected trailing OTXEN bytes: % X", got[idx:])
	}
}

func TestOTxEnMarshalToEmptyVariableData(t *testing.T) {
	t.Parallel()

	msg := newOTxEn().(*tTIOtxen)
	msg.confugureForAbort(nil)
	buf, engine := newOTxEnEngine(128)
	if err := msg.MarshalTo(context.Background(), engine); err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}
	got := buf.bytes[:buf.currentWritePosition]

	idx := 2 // legacy header contains the function code and sequence number.
	_, size, ok := decodeUniversalAt(got, idx)
	if !ok {
		t.Fatal("could not decode operation")
	}
	idx += size
	if got[idx] != 0 {
		t.Fatalf("transaction context pointer = %#x, want null", got[idx])
	}
	idx++
	idx += universalSizeAt(t, got, idx, "transaction context length")
	idx += universalSizeAt(t, got, idx, "format ID")
	idx += universalSizeAt(t, got, idx, "GTRID length")
	idx += universalSizeAt(t, got, idx, "BQUAL length")
	if got[idx] != 0 {
		t.Fatalf("XID pointer = %#x, want null", got[idx])
	}
	idx++
	idx += universalSizeAt(t, got, idx, "XID length")
	idx += universalSizeAt(t, got, idx, "timeout")
	idx += universalSizeAt(t, got, idx, "in state")
	if got[idx] != 1 {
		t.Fatalf("out-state pointer = %#x, want non-null", got[idx])
	}
	idx++
	idx += universalSizeAt(t, got, idx, "transaction state change flags")
}

func TestOTxEnConfigureOperations(t *testing.T) {
	t.Parallel()

	tx := &sessionlessTransaction{
		globalTransactionID: extensions.GlobalTransactionId("g1"),
		xid:                 driverCommon.B1Array{0x11, 0x22, 0x33, 0x44},
		gtridLength:         2,
		bqualLength:         2,
		timeout:             30,
	}
	tests := []struct {
		name      string
		configure func(*tTIOtxen, oracleTx)
		operation driverCommon.SB4
		inState   driverCommon.UB4
	}{
		{name: "commit", configure: func(msg *tTIOtxen, tx oracleTx) { msg.confugureForCommit(tx) }, operation: driverCommon.SB4(otxenCommit), inState: k2cmdCommit},
		{name: "abort", configure: func(msg *tTIOtxen, tx oracleTx) { msg.confugureForAbort(tx) }, operation: driverCommon.SB4(otxenAbort), inState: k2cmdAbort},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msg := newOTxEn().(*tTIOtxen)
			test.configure(msg, tx)

			if msg.operation != test.operation {
				t.Fatalf("operation = %d, want %d", msg.operation, test.operation)
			}
			if msg.inState != test.inState {
				t.Fatalf("in-state = %d, want %d", msg.inState, test.inState)
			}
			if msg.flags != 0 {
				t.Fatalf("flags = %#x, want 0", msg.flags)
			}
			if msg.formatID != k2gSessionless {
				t.Fatalf("format ID = %#x, want %#x", msg.formatID, k2gSessionless)
			}
			if !bytes.Equal(msg.xid, tx.xid) {
				t.Fatalf("XID = % X, want % X", msg.xid, tx.xid)
			}
			if msg.timeout != driverCommon.UB2(tx.timeout) {
				t.Fatalf("timeout = %d, want %d", msg.timeout, tx.timeout)
			}
		})
	}
}

func TestOTxEnRPAFactoryRegistration(t *testing.T) {
	t.Parallel()

	functionRegistry := NewRegistry[functionRegistryKey]()
	if err := functionRegistry.Register(functionRegistryKey{messageType: TTIRPA, functionType: oTxEn}, MinTTCProtocolVersion, newOTxEnRPA); err != nil {
		t.Fatalf("register OTXEN RPA failed: %v", err)
	}
	factory := &SimpleFactory{
		ttcVersion:   18,
		msgregistry:  NewRegistry[driverCommon.MessageType](),
		funcregistry: functionRegistry,
	}

	msg, err := factory.GetMessageForFunction(TTIRPA, oTxEn)
	if err != nil {
		t.Fatalf("GetMessageForFunction(TTIRPA, oTxEn) failed: %v", err)
	}
	if _, ok := msg.(*ttiOTxEnRPA); !ok {
		t.Fatalf("message type = %T, want *ttiOTxEnRPA", msg)
	}
}

func TestOTxEnRPAUnMarshalFrom(t *testing.T) {
	t.Parallel()

	buf := NewTestDataBuffer()
	engine := NewMarshalEngine(buf, driverCommon.BIG_ENDIAN, [5]byte{Native, Universal, Universal, Universal, Universal})
	if err := engine.MarshalUB4(context.Background(), 0x12345678); err != nil {
		t.Fatalf("MarshalUB4 failed: %v", err)
	}

	msg := newOTxEnRPA().(*ttiOTxEnRPA)
	if err := msg.UnMarshalFrom(context.Background(), engine); err != nil {
		t.Fatalf("UnMarshalFrom failed: %v", err)
	}
	if got := msg.GetOutState(); got != 0x12345678 {
		t.Fatalf("GetOutState = %#x, want %#x", got, 0x12345678)
	}
}

func TestOTxEnRPAUnMarshalFromFailure(t *testing.T) {
	t.Parallel()

	msg := newOTxEnRPA().(*ttiOTxEnRPA)
	err := msg.UnMarshalFrom(context.Background(), createMarshaller([]byte{0x01}, failOnReadByte, 1))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "simulated read error") {
		t.Fatalf("expected simulated read error, got %v", err)
	}
}

func universalSizeAt(t *testing.T, payload []byte, index int, name string) int {
	t.Helper()
	_, size, ok := decodeUniversalAt(payload, index)
	if !ok {
		t.Fatalf("could not decode %s at offset %d", name, index)
	}
	return size
}
