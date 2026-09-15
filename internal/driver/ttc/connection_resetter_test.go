/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below), to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the
** Software and the Larger Work(s), and to sublicense the foregoing rights on
** either these or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or
** substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package ttc

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/oracle/go-oracledb/v26/internal/driver/common"
)

func TestConnectionResetter_Reset(t *testing.T) {
	t.Parallel()
	mockFac := &mockFactory{
		returnMsg: NewOall18(),
	}
	mockStr := &mockStreamer{
		pullMsg: &mockOer{err: nil},
	}

	mockNs := &mockNetworkSession{
		disconnectCalls: 0,
		disconnectErr:   nil,
		sleepDuration:   0,
		cancelErr:       nil,
	}

	shelf := newShelf[common.MessageType]()
	shelf.RegisterMessageFactory(mockFac)
	shelf.RegisterMessageStreamer(mockStr)

	sessionCtx := common.NewSessionContext()

	connection := newTestConnection(shelf, sessionCtx, mockNs)

	connection.PrepareContext(context.Background(), "SELECT * FROM DUAL")

	if len(connection.shelf.GetStatements(false)) != 1 {
		t.Errorf("list of statements should not be empty, got [%v]", connection.shelf.GetStatements(false))
	}

	sessionCtx.GetSessionProperties().SetProperty("pasta", "carbonara")

	connection.ResetSession(context.Background())
	if len(connection.shelf.GetStatements(false)) != 0 {
		t.Errorf("list of statements should be empty, got [%v]", connection.shelf.GetStatements(false))
	}

	if v := sessionCtx.GetSessionProperties().GetProperty("pasta"); v != nil {
		t.Errorf("session porperties should have been resetted")
	}

}

// TestConnectionResetter_RollsBackActiveTransaction verifies that ResetSession
// rolls back a transaction reported as active by the server before flushing.
func TestConnectionResetter_RollsBackActiveTransaction(t *testing.T) {
	t.Parallel()

	streamer := &mockStreamer{pullMsg: &mockOer{}}
	connection := newTransactionTestConnection(streamer)
	connection.sessCtx = common.NewSessionContext()
	connection._isInTransaction = true

	if err := connection.ResetSession(context.Background()); err != nil {
		t.Fatalf("ResetSession returned error: %v", err)
	}
	assertTransactionFunction(t, streamer, common.SB4(otxenAbort), k2cmdAbort)
	if connection._isInTransaction {
		t.Fatal("ResetSession should clear the active transaction state")
	}
	if connection.shelf.isInTransaction() {
		t.Fatal("ResetSession should unregister the rolled back transaction")
	}
}

// TestConnectionResetter_RollbackFailureInvalidatesConnection verifies that a
// failed rollback prevents a connection from being returned to the pool.
func TestConnectionResetter_RollbackFailureInvalidatesConnection(t *testing.T) {
	t.Parallel()

	streamer := &mockStreamer{pullMsg: &mockOer{err: errors.New("rollback failed")}}
	connection := newTransactionTestConnection(streamer)
	connection.sessCtx = common.NewSessionContext()
	connection._isInTransaction = true

	if err := connection.ResetSession(context.Background()); err != driver.ErrBadConn {
		t.Fatalf("ResetSession error = %v, want %v", err, driver.ErrBadConn)
	}
	if connection._isValid {
		t.Fatal("ResetSession rollback failure should invalidate the connection")
	}
}
