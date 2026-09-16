/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package ttc

import (
	"bytes"
	"context"
	"testing"

	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	extensions "github.com/oracle/go-oracledb/v26/oracle/extensions"
)

func sessionlessSyncEventData(t *testing.T, globalTransactionID string, mode byte, reason sessionlessTxSyncReason) eventData {
	t.Helper()
	raw := driverCommon.B1Array(append([]byte(globalTransactionID), mode|byte(reason), 2))
	sync, err := newSessionlessGlobalTransactionIDSync(raw)
	if err != nil {
		t.Fatalf("newSessionlessGlobalTransactionIDSync failed: %v", err)
	}
	properties := driverCommon.NewProperties[string]()
	properties.SetProperty(sessionlessGlobalTransactionIDProperty, sync)
	return propertiesEventData{properties: properties}
}

// TestSessionlessSyncUsesCurrentEventDelta verifies that a property update
// without SESSIONLESS_GTRID is ignored even when the session context contains
// a previous SESSIONLESS_GTRID value.
func TestSessionlessSyncUsesCurrentEventDelta(t *testing.T) {
	t.Parallel()

	conn, _ := newSessionlessTransactionTestConnection()
	staleSync := sessionlessSyncEventData(t, "stale-id", sessionlessGlobalTransactionIDSyncSet, sessionlessGlobalTransactionIDSyncServer)
	staleProperties := staleSync.(propertiesEventData).properties
	conn.sessCtx.GetSessionProperties().PutAll(staleProperties)

	current := newSessionlessTransaction(context.Background(), conn, extensions.GlobalTransactionID("current-id"), 300)
	conn.shelf.registerTransaction(current)
	conn.handleSessionPropertyChange(propertiesEventData{properties: driverCommon.NewProperties[string]()})
	conn.handleSessionPropertyChange(nil)

	if conn.shelf.getTransaction() != current {
		t.Fatal("a stale or missing event payload changed the current transaction")
	}
}

// TestSessionlessServerSyncDoesNotReplaceOrUnregisterUnrelatedTransaction
// verifies that replayed server notifications cannot corrupt local state.
func TestSessionlessServerSyncDoesNotReplaceOrUnregisterUnrelatedTransaction(t *testing.T) {
	t.Parallel()

	conn, _ := newSessionlessTransactionTestConnection()
	current := newSessionlessTransaction(context.Background(), conn, extensions.GlobalTransactionID("current-id"), 300)
	conn.shelf.registerTransaction(current)

	conn.handleSessionPropertyChange(sessionlessSyncEventData(t, "other-id", sessionlessGlobalTransactionIDSyncSet, sessionlessGlobalTransactionIDSyncServer))
	conn.handleSessionPropertyChange(sessionlessSyncEventData(t, "", sessionlessGlobalTransactionIDSyncUnset, sessionlessGlobalTransactionIDSyncServer))

	if conn.shelf.getTransaction() != current {
		t.Fatal("a conflicting server notification changed the current transaction")
	}
}

// TestSessionlessServerSyncLifecycle verifies that an implicit transaction is
// registered only for a server start and removed only by its matching end.
func TestSessionlessServerSyncLifecycle(t *testing.T) {
	t.Parallel()

	conn, _ := newSessionlessTransactionTestConnection()
	conn.handleSessionPropertyChange(sessionlessSyncEventData(t, "server-id", sessionlessGlobalTransactionIDSyncSet, sessionlessGlobalTransactionIDSyncServer))

	implicit, ok := conn.shelf.getTransaction().(*sessionlessTransaction)
	if !ok {
		t.Fatalf("current transaction = %T, want *sessionlessTransaction", conn.shelf.getTransaction())
	}
	if !implicit.isStartedOnServer {
		t.Fatal("implicit transaction was not marked as started on the server")
	}

	conn.handleSessionPropertyChange(sessionlessSyncEventData(t, "server-id", sessionlessGlobalTransactionIDSyncUnset, sessionlessGlobalTransactionIDSyncServer))
	if conn.shelf.getTransaction() != nil {
		t.Fatal("matching server end notification did not unregister the implicit transaction")
	}
}

// TestSessionlessTransactionServerIDMismatchRebuildsXID verifies that a
// server-canonical GTRID is reflected in the XID sent by later operations.
func TestSessionlessTransactionServerIDMismatchRebuildsXID(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		setState func(*sessionlessTransaction, extensions.GlobalTransactionID)
	}{
		{name: "started", setState: (*sessionlessTransaction).setStartedOnServer},
		{name: "ended", setState: (*sessionlessTransaction).setEndedOnServer},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := newSessionlessTransactionTestConnection()
			tx := newSessionlessTransaction(context.Background(), conn, extensions.GlobalTransactionID("client-id"), 300)
			test.setState(tx, extensions.GlobalTransactionID("server-id"))

			if !bytes.Equal(tx.globalTransactionID, extensions.GlobalTransactionID("server-id")) {
				t.Fatalf("global transaction ID = %q, want server-id", tx.globalTransactionID)
			}
			if tx.globalTransactionIDLength != driverCommon.UB4(len("server-id")) {
				t.Fatalf("global transaction ID length = %d, want %d", tx.globalTransactionIDLength, len("server-id"))
			}
			if !bytes.Equal(tx.xid[:len("server-id")], []byte("server-id")) {
				t.Fatalf("XID prefix = %q, want server-id", tx.xid[:len("server-id")])
			}
		})
	}
}

// TestNewSessionlessGlobalTransactionIDSyncRejectsInvalidPayloads verifies
// that malformed server-supplied transaction identities are not accepted.
func TestNewSessionlessGlobalTransactionIDSyncRejectsInvalidPayloads(t *testing.T) {
	t.Parallel()

	for _, raw := range []driverCommon.B1Array{
		{},
		{sessionlessGlobalTransactionIDSyncSet, 2},
		driverCommon.B1Array(append(make([]byte, maxSessionlessGlobalTransactionIDSize+1), sessionlessGlobalTransactionIDSyncSet, 2)),
	} {
		if _, err := newSessionlessGlobalTransactionIDSync(raw); err == nil {
			t.Fatalf("newSessionlessGlobalTransactionIDSync accepted payload of length %d", len(raw))
		}
	}
}
