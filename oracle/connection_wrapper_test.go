/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** and patent rights owned by each licensor hereunder covering either (i) the
** unmodified Software as contributed to or provided by such licensor, or (ii)
** the Larger Works (as defined below), to deal in both (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors), without restriction, including without
** limitation the rights to copy, create derivative works of, display, perform,
** and distribute the Software and the Larger Work(s), and to sublicense the
** foregoing rights on either these or other terms.
**
** This license is subject to the condition that the above copyright notice and
** either this complete permission notice or at a minimum a reference to the UPL
** must be included in all copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package oracle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/oracle/go-oracledb/v26/internal/common"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// sessionlessTransactionTestConnector exposes a test driver connection to
// database/sql without registering a real driver name.
type sessionlessTransactionTestConnector struct {
	// connection is the fake connection returned to database/sql.
	connection driver.Conn
}

// Connect returns the configured fake connection.
func (c sessionlessTransactionTestConnector) Connect(context.Context) (driver.Conn, error) {
	return c.connection, nil
}

// Driver returns the test driver descriptor.
func (c sessionlessTransactionTestConnector) Driver() driver.Driver {
	return sessionlessTransactionTestDriver{}
}

// sessionlessTransactionTestDriver is the driver descriptor used by wrapper tests.
type sessionlessTransactionTestDriver struct{}

// Open is unsupported because tests inject an already-created connection.
func (sessionlessTransactionTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("sessionless test driver does not use Open")
}

// sessionlessTransactionTestConn records wrapper calls and implements the
// driver connection methods needed by database/sql.
type sessionlessTransactionTestConn struct {
	// startTransaction is returned by the prepared-start operation.
	startTransaction common.SessionlessTransaction
	// startErr is returned by the prepared-start operation.
	startErr error
	// startCtx records the context supplied to the prepared-start operation.
	startCtx context.Context
	// startTimeout records the timeout supplied to the prepared-start operation.
	startTimeout uint16
	// startPrepared records whether a start was prepared.
	startPrepared bool
	// startCleared records whether a failed start was cleared.
	startCleared bool

	// resumeTransaction is returned by the prepared-resume operation.
	resumeTransaction common.SessionlessTransaction
	// resumeErr is returned by the prepared-resume operation.
	resumeErr error
	// resumeCtx records the context supplied to the prepared-resume operation.
	resumeCtx context.Context
	// resumeID records the identifier supplied to the prepared-resume operation.
	resumeID []byte
	// resumeTimeout records the timeout supplied to the prepared-resume operation.
	resumeTimeout uint16
	// resumePrepared records whether a resume was prepared.
	resumePrepared bool
	// resumeCleared records whether the prepared resume was cleared.
	resumeCleared bool
	// resumeClearErr records the BeginTx result passed to the clear operation.
	resumeClearErr error

	// driverTx is returned by the fake BeginTx implementation.
	driverTx driver.Tx
	// driverTxErr is returned by the fake BeginTx implementation when set.
	driverTxErr error
	// driverTxCtx records the context passed to BeginTx.
	driverTxCtx context.Context
	// driverTxOpts records the options passed to BeginTx.
	driverTxOpts driver.TxOptions
	// driverTxCalls counts calls made to BeginTx.
	driverTxCalls int
}

// PrepareForSessionlessStartOperation records a prepared sessionless start.
func (c *sessionlessTransactionTestConn) PrepareForSessionlessStartOperation(
	ctx context.Context, timeout uint16,
) (common.SessionlessTransaction, error) {
	c.startPrepared = true
	c.startCtx = ctx
	c.startTimeout = timeout
	return c.startTransaction, c.startErr
}

// ClearPrepareForSessionlessStartOperation records cleanup of a prepared start.
func (c *sessionlessTransactionTestConn) ClearPrepareForSessionlessStartOperation() {
	c.startCleared = true
}

// PrepareForSessionlessResumeOperation records a prepared sessionless resume.
func (c *sessionlessTransactionTestConn) PrepareForSessionlessResumeOperation(
	ctx context.Context, globalTransactionID []byte, timeout uint16,
) (common.SessionlessTransaction, error) {
	c.resumePrepared = true
	c.resumeCtx = ctx
	c.resumeID = append([]byte(nil), globalTransactionID...)
	c.resumeTimeout = timeout
	return c.resumeTransaction, c.resumeErr
}

// ClearPrepareForSessionlessResumeOperation records cleanup of a prepared resume.
func (c *sessionlessTransactionTestConn) ClearPrepareForSessionlessResumeOperation(
	transaction common.SessionlessTransaction, err error,
) {
	c.resumeCleared = true
	c.resumeClearErr = err
}

// Prepare returns a statement that accepts arbitrary arguments.
func (*sessionlessTransactionTestConn) Prepare(string) (driver.Stmt, error) {
	return &sessionlessTransactionTestStmt{}, nil
}

// Close closes the fake connection.
func (*sessionlessTransactionTestConn) Close() error { return nil }

// Begin reports that the legacy transaction API is unused by these tests.
func (*sessionlessTransactionTestConn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

// BeginTx records the database/sql transaction request and returns its result.
func (c *sessionlessTransactionTestConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.driverTxCalls++
	c.driverTxCtx = ctx
	c.driverTxOpts = opts
	if c.driverTxErr != nil {
		return nil, c.driverTxErr
	}
	return c.driverTx, nil
}

// sessionlessTransactionTestPlainConn is a connection that cannot be wrapped.
type sessionlessTransactionTestPlainConn struct{}

// Prepare reports that statements are unsupported by the plain connection.
func (*sessionlessTransactionTestPlainConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

// Close closes the plain fake connection.
func (*sessionlessTransactionTestPlainConn) Close() error { return nil }

// Begin reports that the legacy transaction API is unsupported.
func (*sessionlessTransactionTestPlainConn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

// sessionlessTransactionTestTx records transaction lifecycle calls.
type sessionlessTransactionTestTx struct {
	// commitCalls counts Commit calls.
	commitCalls int
	// rollbackCalls counts Rollback calls.
	rollbackCalls int
	// PrepareForOperationCalls counts suspension markers.
	PrepareForOperationCalls int
	// clearSuspendCalls counts suspension marker cleanup.
	clearSuspendCalls int
}

// Commit records a commit call.
func (tx *sessionlessTransactionTestTx) Commit() error {
	tx.commitCalls++
	return nil
}

// Rollback records a rollback call.
func (tx *sessionlessTransactionTestTx) Rollback() error {
	tx.rollbackCalls++
	return nil
}

// GlobalTransactionID returns the identifier used by wrapper tests.
func (*sessionlessTransactionTestTx) GlobalTransactionID() []byte {
	return []byte("test-global-transaction-id")
}

// sessionlessTransactionTestStmt is a statement used by transaction wrapper tests.
type sessionlessTransactionTestStmt struct{}

// Close closes the fake statement.
func (*sessionlessTransactionTestStmt) Close() error { return nil }

// NumInput reports that the fake statement accepts any number of arguments.
func (*sessionlessTransactionTestStmt) NumInput() int { return -1 }

// Exec returns one affected row.
func (*sessionlessTransactionTestStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

// Query reports that row queries are unsupported by the fake statement.
func (*sessionlessTransactionTestStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("query not implemented by sessionless test statement")
}

// openSessionlessTransactionTestConnection opens an injected driver connection.
func openSessionlessTransactionTestConnection(t *testing.T, conn driver.Conn) *sql.Conn {
	t.Helper()
	db := sql.OpenDB(sessionlessTransactionTestConnector{connection: conn})
	t.Cleanup(func() { _ = db.Close() })

	sqlConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("get test connection: %v", err)
	}
	t.Cleanup(func() { _ = sqlConn.Close() })
	return sqlConn
}

// openSessionlessTransactionTestConnectionWrapper opens and wraps an injected connection.
func openSessionlessTransactionTestConnectionWrapper(t *testing.T, conn driver.Conn) *connectionWrapper {
	t.Helper()
	sqlConn := openSessionlessTransactionTestConnection(t, conn)
	wrapper, err := NewConnectionWrapper(sqlConn)
	if err != nil {
		t.Fatalf("wrap test connection: %v", err)
	}
	return wrapper
}

// newSessionlessTransactionTestConnection builds a fake connection for wrapper tests.
func newSessionlessTransactionTestConnection(start, resume common.SessionlessTransaction) *sessionlessTransactionTestConn {
	driverTx := &sessionlessTransactionTestTx{}
	return &sessionlessTransactionTestConn{
		startTransaction:  start,
		resumeTransaction: resume,
		driverTx:          driverTx,
	}
}

// TestBeginSessionlessTxDelegates verifies that the wrapper prepares the
// sessionless transaction and passes the options to database/sql.
// func TestBeginSessionlessTxDelegates(t *testing.T) {
// 	t.Parallel()

// 	ctx := context.WithValue(context.Background(), struct{}{}, "begin")
// 	wantTx := &sessionlessTransactionTestTx{}
// 	driverConn := newSessionlessTransactionTestConnection(wantTx, nil)
// 	driverConn.driverTx = wantTx
// 	connectionWrapper := openSessionlessTransactionTestConnectionWrapper(t, driverConn)
// 	opts := &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true}

// 	gotTx, err := connectionWrapper.BeginSessionlessTx(ctx, opts, 123)
// 	if err != nil {
// 		t.Fatalf("BeginSessionlessTx returned error: %v", err)
// 	}
// 	if gotTx.underlyingTx != wantTx {
// 		t.Fatal("BeginSessionlessTx did not retain the prepared sessionless transaction")
// 	}
// 	if got, want := string(gotTx.GlobalTransactionID()), "test-global-transaction-id"; got != want {
// 		t.Fatalf("GlobalTransactionID() = %q, want %q", got, want)
// 	}
// 	if !driverConn.startPrepared || driverConn.startCtx != ctx || driverConn.startTimeout != 123 {
// 		t.Fatalf("start preparation = (called %v, ctx %v, timeout %d), want (true, supplied context, 123)",
// 			driverConn.startPrepared, driverConn.startCtx, driverConn.startTimeout)
// 	}
// 	if driverConn.driverTxCalls != 1 || driverConn.driverTxCtx != ctx {
// 		t.Fatalf("database/sql BeginTx calls/context = (%d, %v), want (1, supplied context)",
// 			driverConn.driverTxCalls, driverConn.driverTxCtx)
// 	}
// 	if got, want := driverConn.driverTxOpts, (driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelSerializable), ReadOnly: true}); got != want {
// 		t.Fatalf("driver transaction options = %+v, want %+v", got, want)
// 	}
// 	if driverConn.startCleared {
// 		t.Fatal("successful start cleared its prepared transaction")
// 	}
// 	if err := gotTx.Rollback(); err != nil {
// 		t.Fatalf("rollback test transaction: %v", err)
// 	}
// }

// TestSessionlessTransactionStmtMethods verifies that prepared statements can
// be associated with a sessionless transaction using both standard Tx methods.
// func TestSessionlessTransactionStmtMethods(t *testing.T) {
// 	t.Parallel()

// 	underlyingTx := &sessionlessTransactionTestTx{}
// 	driverConn := newSessionlessTransactionTestConnection(underlyingTx, nil)
// 	sqlConn := openSessionlessTransactionTestConnection(t, driverConn)
// 	connectionWrapper, err := NewConnectionWrapper(sqlConn)
// 	if err != nil {
// 		t.Fatalf("NewConnectionWrapper returned error: %v", err)
// 	}
// 	preparedStmt, err := sqlConn.PrepareContext(context.Background(), "SELECT 1")
// 	if err != nil {
// 		t.Fatalf("sql.Conn.Prepare returned error: %v", err)
// 	}
// 	defer preparedStmt.Close()

// 	tx, err := connectionWrapper.BeginSessionlessTx(context.Background(), &sql.TxOptions{}, 300)
// 	if err != nil {
// 		t.Fatalf("BeginSessionlessTx returned error: %v", err)
// 	}
// 	defer tx.Rollback()

// 	stmt := tx.Stmt(preparedStmt)
// 	if _, err := stmt.Exec(); err != nil {
// 		t.Fatalf("Stmt().Exec returned error: %v", err)
// 	}
// 	if err := stmt.Close(); err != nil {
// 		t.Fatalf("transaction Stmt.Close returned error: %v", err)
// 	}

// 	ctx := context.WithValue(context.Background(), struct{}{}, "stmt context")
// 	contextStmt := tx.StmtContext(ctx, preparedStmt)
// 	if _, err := contextStmt.ExecContext(ctx); err != nil {
// 		t.Fatalf("StmtContext().ExecContext returned error: %v", err)
// 	}
// 	if err := contextStmt.Close(); err != nil {
// 		t.Fatalf("transaction StmtContext.Close returned error: %v", err)
// 	}
// }

// // TestBeginSessionlessTxPropagatesError verifies that a preparation error is
// // returned before database/sql begins a transaction.
// func TestBeginSessionlessTxPropagatesError(t *testing.T) {
// 	t.Parallel()

// 	wantErr := errors.New("prepare sessionless transaction failed")
// 	driverConn := newSessionlessTransactionTestConnection(nil, nil)
// 	driverConn.startErr = wantErr
// 	connectionWrapper := openSessionlessTransactionTestConnectionWrapper(t, driverConn)

// 	gotTx, err := connectionWrapper.BeginSessionlessTx(context.Background(), &sql.TxOptions{}, 300)
// 	if gotTx != nil {
// 		t.Fatalf("BeginSessionlessTx returned transaction %p on error", gotTx)
// 	}
// 	if !errors.Is(err, wantErr) {
// 		t.Fatalf("BeginSessionlessTx error = %v, want %v", err, wantErr)
// 	}
// 	if driverConn.driverTxCalls != 0 {
// 		t.Fatalf("database/sql BeginTx calls = %d, want 0 after preparation failure", driverConn.driverTxCalls)
// 	}
// }

// TestResumeSessionlessTxDelegates verifies that the wrapper prepares the
// requested global transaction ID and starts a database/sql transaction.
// func TestResumeSessionlessTxDelegates(t *testing.T) {
// 	t.Parallel()

// 	ctx := context.WithValue(context.Background(), struct{}{}, "resume")
// 	wantTx := &sessionlessTransactionTestTx{}
// 	driverConn := newSessionlessTransactionTestConnection(nil, wantTx)
// 	driverConn.driverTx = wantTx
// 	connectionWrapper := openSessionlessTransactionTestConnectionWrapper(t, driverConn)
// 	globalTransactionID := GlobalTransactionID("resume-global-transaction-id")

// 	gotTx, err := connectionWrapper.ResumeSessionlessTx(ctx, globalTransactionID, 123)
// 	if err != nil {
// 		t.Fatalf("ResumeSessionlessTx returned error: %v", err)
// 	}
// 	if gotTx.underlyingTx != wantTx {
// 		t.Fatal("ResumeSessionlessTx did not retain the prepared sessionless transaction")
// 	}
// 	if !driverConn.resumePrepared || driverConn.resumeCtx != ctx ||
// 		string(driverConn.resumeID) != string(globalTransactionID) || driverConn.resumeTimeout != 123 {
// 		t.Fatalf("resume preparation did not receive the requested context, ID, and timeout")
// 	}
// 	if driverConn.driverTxCalls != 1 || driverConn.driverTxCtx != ctx {
// 		t.Fatalf("database/sql BeginTx calls/context = (%d, %v), want (1, supplied context)",
// 			driverConn.driverTxCalls, driverConn.driverTxCtx)
// 	}
// 	if got, want := driverConn.driverTxOpts, (driver.TxOptions{}); got != want {
// 		t.Fatalf("resume driver transaction options = %+v, want %+v", got, want)
// 	}
// 	if !driverConn.resumeCleared || driverConn.resumeClearErr != nil {
// 		t.Fatalf("resume preparation cleanup = (called %v, error %v), want (true, nil)",
// 			driverConn.resumeCleared, driverConn.resumeClearErr)
// 	}
// 	if err := gotTx.Rollback(); err != nil {
// 		t.Fatalf("rollback test transaction: %v", err)
// 	}
// }

// TestResumeSessionlessTxPropagatesError verifies that a preparation error is
// returned before database/sql begins a transaction.
func TestResumeSessionlessTxPropagatesError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("prepare sessionless resume failed")
	driverConn := newSessionlessTransactionTestConnection(nil, nil)
	driverConn.resumeErr = wantErr
	connectionWrapper := openSessionlessTransactionTestConnectionWrapper(t, driverConn)

	gotTx, err := connectionWrapper.ResumeSessionlessTx(
		context.Background(), GlobalTransactionID("resume-global-transaction-id"), 300)
	if gotTx != nil {
		t.Fatalf("ResumeSessionlessTx returned transaction %p on error", gotTx)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("ResumeSessionlessTx error = %v, want %v", err, wantErr)
	}
	if driverConn.driverTxCalls != 0 {
		t.Fatalf("database/sql BeginTx calls = %d, want 0 after preparation failure", driverConn.driverTxCalls)
	}
}

// TestSessionlessTransactionBeginTxFailureClearsPreparation checks that a
// failed database/sql BeginTx does not leave the prepared transaction attached.
// func TestSessionlessTransactionBeginTxFailureClearsPreparation(t *testing.T) {
// 	t.Parallel()

// 	for _, operation := range []string{"begin", "resume"} {
// 		t.Run(operation, func(t *testing.T) {
// 			wantErr := errors.New("database/sql BeginTx failed")
// 			underlyingTx := &sessionlessTransactionTestTx{}
// 			driverConn := newSessionlessTransactionTestConnection(underlyingTx, underlyingTx)
// 			driverConn.driverTxErr = wantErr
// 			connectionWrapper := openSessionlessTransactionTestConnectionWrapper(t, driverConn)

// 			var err error
// 			if operation == "begin" {
// 				_, err = connectionWrapper.BeginSessionlessTx(context.Background(), &sql.TxOptions{}, 300)
// 				if !driverConn.startCleared {
// 					t.Fatal("failed BeginTx did not clear the prepared start")
// 				}
// 			} else {
// 				_, err = connectionWrapper.ResumeSessionlessTx(
// 					context.Background(), GlobalTransactionID("resume-id"), 300)
// 				if !driverConn.resumeCleared || !errors.Is(driverConn.resumeClearErr, wantErr) {
// 					t.Fatalf("failed resume cleanup = (called %v, error %v), want (true, %v)",
// 						driverConn.resumeCleared, driverConn.resumeClearErr, wantErr)
// 				}
// 			}
// 			if !errors.Is(err, wantErr) {
// 				t.Fatalf("wrapper error = %v, want %v", err, wantErr)
// 			}
// 		})
// 	}
// }

// TestSessionlessTransactionEnds verifies that commit, rollback, and suspend
// make the database/sql transaction handle unusable.
// func TestSessionlessTransactionEnds(t *testing.T) {
// 	t.Parallel()

// 	for _, test := range []struct {
// 		name string
// 		end  func(*sessionlessTx) error
// 	}{
// 		{name: "commit", end: (*sessionlessTx).Commit},
// 		{name: "rollback", end: (*sessionlessTx).Rollback},
// 		{name: "suspend", end: (*sessionlessTx).Suspend},
// 	} {
// 		t.Run(test.name, func(t *testing.T) {
// 			underlyingTx := &sessionlessTransactionTestTx{}
// 			driverConn := newSessionlessTransactionTestConnection(underlyingTx, nil)
// 			connectionWrapper := openSessionlessTransactionTestConnectionWrapper(t, driverConn)
// 			tx, err := connectionWrapper.BeginSessionlessTx(context.Background(), &sql.TxOptions{}, 300)
// 			if err != nil {
// 				t.Fatalf("BeginSessionlessTx returned error: %v", err)
// 			}

// 			if err := test.end(tx); err != nil {
// 				t.Fatalf("%s returned error: %v", test.name, err)
// 			}
// 			if test.name == "suspend" {
// 				if underlyingTx.PrepareForOperationCalls != 1 || underlyingTx.clearSuspendCalls != 1 {
// 					t.Errorf("suspend preparation calls = (%d, %d), want (1, 1)",
// 						underlyingTx.PrepareForOperationCalls, underlyingTx.clearSuspendCalls)
// 				}
// 			}
// 			if _, err := tx.Exec("SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
// 				t.Errorf("Exec after %s error = %v, want %v", test.name, err, sql.ErrTxDone)
// 			}
// 			if _, err := tx.ExecContext(context.Background(), "SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
// 				t.Errorf("ExecContext after %s error = %v, want %v", test.name, err, sql.ErrTxDone)
// 			}
// 			if _, err := tx.Prepare("SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
// 				t.Errorf("Prepare after %s error = %v, want %v", test.name, err, sql.ErrTxDone)
// 			}
// 			if _, err := tx.PrepareContext(context.Background(), "SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
// 				t.Errorf("PrepareContext after %s error = %v, want %v", test.name, err, sql.ErrTxDone)
// 			}
// 			if _, err := tx.Query("SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
// 				t.Errorf("Query after %s error = %v, want %v", test.name, err, sql.ErrTxDone)
// 			}
// 			if _, err := tx.QueryContext(context.Background(), "SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
// 				t.Errorf("QueryContext after %s error = %v, want %v", test.name, err, sql.ErrTxDone)
// 			}
// 			if err := tx.QueryRow("SELECT 1").Scan(new(int)); !errors.Is(err, sql.ErrTxDone) {
// 				t.Errorf("QueryRow after %s error = %v, want %v", test.name, err, sql.ErrTxDone)
// 			}
// 			if err := tx.QueryRowContext(context.Background(), "SELECT 1").Scan(new(int)); !errors.Is(err, sql.ErrTxDone) {
// 				t.Errorf("QueryRowContext after %s error = %v, want %v", test.name, err, sql.ErrTxDone)
// 			}
// 			if err := tx.Commit(); !errors.Is(err, sql.ErrTxDone) {
// 				t.Errorf("second Commit after %s error = %v, want %v", test.name, err, sql.ErrTxDone)
// 			}
// 			if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
// 				t.Errorf("second Rollback after %s error = %v, want %v", test.name, err, sql.ErrTxDone)
// 			}
// 		})
// 	}
// }

// TestSessionlessTransactionWrappersRejectUnsupportedConnection verifies that
// the public wrapper rejects connections without sessionless support.
func TestSessionlessTransactionWrappersRejectUnsupportedConnection(t *testing.T) {
	t.Parallel()

	sqlConn := openSessionlessTransactionTestConnection(t, &sessionlessTransactionTestPlainConn{})
	if _, err := NewConnectionWrapper(sqlConn); err == nil {
		t.Fatal("NewConnectionWrapper unexpectedly accepted an unsupported connection")
	} else {
		sqlError, ok := err.(oracleErrors.SQLError)
		if !ok {
			t.Fatalf("unexpected wrapper error type %T: %v", err, err)
		}
		if sqlError.ErrorCode() != string(oracleErrors.UnsupportedFeature) {
			t.Fatalf("unexpected wrapper error code: %s", sqlError.ErrorCode())
		}
	}
}

// TestGlobalTransactionIDString verifies that the public transaction ID uses
// hexadecimal encoding for its string representation.
func TestGlobalTransactionIDString(t *testing.T) {
	t.Parallel()

	if got, want := (GlobalTransactionID{0x01, 0xab, 0xff}).String(), "01abff"; got != want {
		t.Fatalf("GlobalTransactionID.String() = %q, want %q", got, want)
	}
}
