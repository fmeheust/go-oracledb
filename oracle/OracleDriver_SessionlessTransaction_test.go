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

package oracle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"slices"
	"testing"
	"time"

	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
	"github.com/oracle/go-oracledb/v26/oracle/extensions"
)

func countRows(ctx context.Context, db *sql.DB, table string) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
	return count, err
}

func openSessionlessTestDB(t *testing.T) (context.Context, *sql.DB) {
	t.Helper()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(6)
	return context.Background(), db
}

func requireSessionlessSQLError(t *testing.T, err error, want oracleErrors.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", want)
	}
	sqlError, ok := err.(oracleErrors.SQLError)
	if !ok {
		t.Fatalf("expected SQLError %s, got %T: %v", want, err, err)
	}
	if sqlError.ErrorCode() != string(want) {
		t.Fatalf("expected error %s, got %s", want, sqlError.ErrorCode())
	}
}

// TestSessionlessTransactionCommit verifies the public sessionless transaction
// API can start, suspend, resume on another connection, and commit while
// uncommitted changes remain isolated from other connections.
func TestSessionlessTransactionCommit(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	ctx := context.Background()
	table := createObjectName("sessionless_tx_commit")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn.Close()

	var globalTransactionID extensions.GlobalTransactionId
	tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{
		Isolation: sql.LevelReadCommitted, ReadOnly: false}, 300)
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}
	globalTransactionID = tx.GlobalTransactionID()
	if globalTransactionID == nil {
		t.Fatal("empty global transaction ID")
	}

	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-start')"); err != nil {
		t.Fatalf("an unexpected error occurred while executing statement: %v", err)
	}

	err = tx.Suspend()
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}

	var count int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after suspend = %d, want 0", count)
	}

	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count before resume/commit failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count before resume/commit = %d, want 0", count)
	}

	resumeConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer resumeConn.Close()
	tx2, err := ResumeSessionlessTransaction(ctx, resumeConn, globalTransactionID)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if err := resumeConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count after resume failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("count after commit = %d, want 1", count)
	}
	if _, err := resumeConn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-resume')"); err != nil {
		t.Fatalf("an unexpected error occurred while executing statement: %v", err)
	}

	// before commit in other connection, count should still be 0
	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count after commit failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after commit = %d, want 0", count)
	}

	err = tx2.Commit()
	if err != nil {
		t.Fatalf("resume/commit failed: %v", err)
	}

	if err := resumeConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count after commit failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("count after commit = %d, want 2", count)
	}

	// after commit, count should now be 2
	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count after commit failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("count after commit = %d, want 2", count)
	}
}

// TestSessionlessTransactionRollback verifies the public sessionless
// transaction API can start, suspend, resume on another connection, and roll
// back so that none of its changes become visible to other connections.
func TestSessionlessTransactionRollback(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	ctx := context.Background()
	table := createObjectName("sessionless_tx_rollback")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn.Close()

	var globalTransactionID extensions.GlobalTransactionId

	tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{
		Isolation: sql.LevelReadCommitted, ReadOnly: false}, 300)
	if err != nil {
		t.Fatalf("failed to get start sessionless transaction: %v", err)
	}

	globalTransactionID = tx.GlobalTransactionID()
	if globalTransactionID == nil {
		t.Fatal("global transaction ID is empty")
	}

	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-rollback')"); err != nil {
		t.Fatalf("an unexpected error occurred while inserting: %v", err)
	}

	err = tx.Suspend()
	if err != nil {
		t.Fatalf("suspend failed: %v", err)
	}

	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after suspend = %d, want 0", count)
	}

	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count before resume/rollback failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count before resume/rollback = %d, want 0", count)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn2.Close()

	tx2, err := ResumeSessionlessTransaction(ctx, conn2, globalTransactionID)
	if err != nil {
		t.Fatalf("failed to resume sessionless transaction with global transaction ID %s: %v", globalTransactionID, err)
	}

	if err := conn2.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("count after resume = %d, want 1", count)
	}
	if _, err := conn2.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-rollback-resume')"); err != nil {
		t.Fatalf("an unexpected error occurred while executing statement: %v", err)
	}

	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count before rollback failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count before rollback = %d, want 0", count)
	}

	err = tx2.Rollback()
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	if err := conn2.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count after rollback failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after rollback = %d, want 0", count)
	}

	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count after rollback failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after rollback = %d, want 0", count)
	}
}

// TestSessionlessTransactionStartTwice verifies starting a second sessionless
// transaction on a connection that already has one returns
// AlreadyInTransaction.
func TestSessionlessTransactionStartTwice(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	ctx := context.Background()
	table := createObjectName("sessionless_tx_commit")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn.Close()

	var globalTransactionID extensions.GlobalTransactionId
	tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{
		Isolation: sql.LevelReadCommitted, ReadOnly: false}, 300)
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}
	globalTransactionID = tx.GlobalTransactionID()
	if globalTransactionID == nil {
		t.Fatal("empty global transaction ID")
	}

	_, err = BeginSessionlessTx(ctx, conn, sql.TxOptions{
		Isolation: sql.LevelReadCommitted, ReadOnly: false}, 300)
	if err == nil {
		tx.Rollback()
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}
	if sqlError, ok := err.(oracleErrors.SQLError); ok {
		if sqlError.ErrorCode() != string(oracleErrors.AlreadyInTransaction) {
			t.Fatalf("Expected error to be %s, but was %s", oracleErrors.AlreadyInTransaction, sqlError.ErrorCode())
		}
	} else {
		t.Fatalf("Expected SQLError but got %v", err)
	}
	tx.Rollback()

}

// TestSessionlessTransactionSuspendTwice verifies suspending an already
// suspended sessionless transaction is treated as a no-op and the transaction
// can still be resumed and ended.
func TestSessionlessTransactionSuspendTwice(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	ctx := context.Background()
	table := createObjectName("sessionless_tx_commit")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn.Close()

	var globalTransactionID extensions.GlobalTransactionId
	tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{
		Isolation: sql.LevelReadCommitted, ReadOnly: false}, 300)
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}
	globalTransactionID = tx.GlobalTransactionID()
	if globalTransactionID == nil {
		t.Fatal("empty global transaction ID")
	}

	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-start')"); err != nil {
		t.Fatalf("an unexpected error occurred while executing statement: %v", err)
	}

	err = tx.Suspend()
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}

	err = tx.Suspend()
	if err != nil {
		t.Fatalf("second suspend should be a no-op, no error should be returned: %v", err)
	}

	resumeConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer resumeConn.Close()
	tx2, err := ResumeSessionlessTransaction(ctx, resumeConn, globalTransactionID)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}

	err = tx2.Rollback()
	if err != nil {
		t.Fatalf("resume/commit failed: %v", err)
	}
}

// TestSessionlessTransactionResumeTwice verifies resuming a sessionless
// transaction twice on the same connection returns AlreadyInTransaction.
func TestSessionlessTransactionResumeTwice(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	ctx := context.Background()
	table := createObjectName("sessionless_tx_commit")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn.Close()

	var globalTransactionID extensions.GlobalTransactionId
	tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{
		Isolation: sql.LevelReadCommitted, ReadOnly: false}, 300)
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}
	globalTransactionID = tx.GlobalTransactionID()
	if globalTransactionID == nil {
		t.Fatal("empty global transaction ID")
	}

	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-start')"); err != nil {
		t.Fatalf("an unexpected error occurred while executing statement: %v", err)
	}

	err = tx.Suspend()
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}

	resumeConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer resumeConn.Close()
	tx2, err := ResumeSessionlessTransaction(ctx, resumeConn, globalTransactionID)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}

	_, err = ResumeSessionlessTransaction(ctx, resumeConn, globalTransactionID)
	if err == nil {
		tx2.Rollback()
		t.Fatalf("resume failed: %v", err)
	}
	if sqlError, ok := err.(oracleErrors.SQLError); ok {
		if sqlError.ErrorCode() != string(oracleErrors.AlreadyInTransaction) {
			t.Fatalf("Expected error to be %s, but was %s", oracleErrors.AlreadyInTransaction, sqlError.ErrorCode())
		}
	} else {
		t.Fatalf("Expected SQLError but got %v", err)
	}
	tx2.Rollback()

}

// TestSessionlessTransactionResumeTwiceDifferentConnection verifies a
// sessionless transaction cannot be resumed concurrently on a second
// connection while it is active on the first resumed connection.
func TestSessionlessTransactionResumeTwiceDifferentConnection(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	ctx := context.Background()
	table := createObjectName("sessionless_tx_commit")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn.Close()

	var globalTransactionID extensions.GlobalTransactionId
	tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{
		Isolation: sql.LevelReadCommitted, ReadOnly: false}, 300)
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}
	globalTransactionID = tx.GlobalTransactionID()
	if globalTransactionID == nil {
		t.Fatal("empty global transaction ID")
	}

	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-start')"); err != nil {
		t.Fatalf("an unexpected error occurred while executing statement: %v", err)
	}

	err = tx.Suspend()
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}

	resumeConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer resumeConn.Close()
	tx2, err := ResumeSessionlessTransaction(ctx, resumeConn, globalTransactionID)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	err = resumeConn.PingContext(ctx)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}

	resumeConn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer resumeConn2.Close()
	_, err = ResumeSessionlessTransaction(ctx, resumeConn2, globalTransactionID)
	if err != nil {
		tx2.Rollback()
		t.Fatalf("resume request failed before round trip: %v", err)
	}
	err = resumeConn2.PingContext(ctx)
	requireSessionlessSQLError(t, err, "ORA-25351")
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("rollback after failed concurrent resume: %v", err)
	}

}

// TestSessionlessTransactionCommitTwice verifies committing a sessionless
// transaction twice returns NotInTransaction on the second commit.
func TestSessionlessTransactionCommitTwice(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	ctx := context.Background()
	table := createObjectName("sessionless_tx_commit")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn.Close()

	var globalTransactionID extensions.GlobalTransactionId
	tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{
		Isolation: sql.LevelReadCommitted, ReadOnly: false}, 300)
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}
	globalTransactionID = tx.GlobalTransactionID()
	if globalTransactionID == nil {
		t.Fatal("empty global transaction ID")
	}

	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-start')"); err != nil {
		t.Fatalf("an unexpected error occurred while executing statement: %v", err)
	}

	err = tx.Suspend()
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}

	var count int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after suspend = %d, want 0", count)
	}

	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count before resume/commit failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count before resume/commit = %d, want 0", count)
	}

	resumeConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer resumeConn.Close()
	tx2, err := ResumeSessionlessTransaction(ctx, resumeConn, globalTransactionID)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if err := resumeConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count after resume failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("count after commit = %d, want 1", count)
	}
	if _, err := resumeConn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-resume')"); err != nil {
		t.Fatalf("an unexpected error occurred while executing statement: %v", err)
	}

	// before commit in other connection, count should still be 0
	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count after commit failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after commit = %d, want 0", count)
	}

	err = tx2.Commit()
	if err != nil {
		t.Fatalf("resume/commit failed: %v", err)
	}

	err = tx2.Commit()
	if err == nil {
		tx2.Rollback()
		t.Fatalf("resume failed: %v", err)
	}
	if sqlError, ok := err.(oracleErrors.SQLError); ok {
		if sqlError.ErrorCode() != string(oracleErrors.NotInTransaction) {
			t.Fatalf("Expected error to be %s, but was %s", oracleErrors.NotInTransaction, sqlError.ErrorCode())
		}
	} else {
		t.Fatalf("Expected SQLError but got %v", err)
	}

}

// TestSessionlessTransactionCommitPLSQL verifies the server-side PL/SQL
// sessionless transaction lifecycle can start and suspend through one SQL
// transaction, resume through PL/SQL on another connection, and commit.
func TestSessionlessTransactionCommitPLSQL(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(3)

	ctx := context.Background()
	table := createObjectName("sessionless_tx_plsql_commit")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn.Close()

	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false})
	if err != nil {
		t.Fatalf("begin transaction failed: %v", err)
	}
	defer tx.Rollback()

	var globalTransactionID string
	if _, err := tx.ExecContext(ctx, `
		BEGIN
			:global_transaction_id := DBMS_TRANSACTION.START_TRANSACTION(
				xid              => NULL,
				transaction_type => DBMS_TRANSACTION.TRANSACTION_TYPE_SESSIONLESS,
				timeout          => 300,
				flag             => DBMS_TRANSACTION.TRANSACTION_NEW);
		END;`, sql.Named("global_transaction_id", sql.Out{Dest: &globalTransactionID})); err != nil {
		t.Fatalf("start and suspend sessionless transaction through PL/SQL failed: %v", err)
	}
	if globalTransactionID == "" {
		t.Fatal("empty global transaction ID")
	}

	_, err = tx.ExecContext(context.Background(), "INSERT INTO "+table+" (str_value) VALUES ('sessionless-plsql-commit')")
	if err != nil {
		t.Fatalf("unexpected error while inserting, %v", err)
	}
	_, err = tx.ExecContext(context.Background(), "BEGIN DBMS_TRANSACTION.SUSPEND_TRANSACTION; END;")
	if err != nil {
		t.Fatalf("unexpected error while suspending, %v", err)
	}
	_ = tx.Rollback()
	if err := conn.PingContext(context.Background()); err != nil {
		t.Fatalf("ping after suspend failed: %v", err)
	}

	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after suspend = %d, want 0", count)
	}

	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count before resume/commit failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count before resume/commit = %d, want 0", count)
	}

	resumeConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get resume connection: %v", err)
	}
	defer resumeConn.Close()

	resumeTx, err := resumeConn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false})
	if err != nil {
		t.Fatalf("begin resume transaction failed: %v", err)
	}
	defer resumeTx.Rollback()
	var resumedGlobalTransactionID string
	if _, err := resumeTx.ExecContext(ctx, `
		BEGIN
			:resumed_global_transaction_id := DBMS_TRANSACTION.START_TRANSACTION(
				xid              => HEXTORAW(:global_transaction_id),
				transaction_type => DBMS_TRANSACTION.TRANSACTION_TYPE_SESSIONLESS,
				timeout          => 300,
				flag             => DBMS_TRANSACTION.TRANSACTION_RESUME);
		END;`,
		sql.Named("global_transaction_id", globalTransactionID),
		sql.Named("resumed_global_transaction_id", sql.Out{Dest: &resumedGlobalTransactionID}),
	); err != nil {
		t.Fatalf("resume sessionless transaction through PL/SQL failed: %v", err)
	}
	if resumedGlobalTransactionID != globalTransactionID {
		t.Fatalf("resumed global transaction ID = %q, want %q", resumedGlobalTransactionID, globalTransactionID)
	}

	if err := resumeConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("count after resume = %d, want 1", count)
	}
	if _, err := resumeConn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-plsql-resume')"); err != nil {
		t.Fatalf("an unexpected error occurred while executing statement: %v", err)
	}

	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count after commit failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after commit = %d, want 0", count)
	}

	if err := resumeTx.Commit(); err != nil {
		t.Fatalf("resume/commit failed: %v", err)
	}

	if err := resumeConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count after commit failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("count after commit = %d, want 2", count)
	}

	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count after commit failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("count after commit = %d, want 2", count)
	}
}

// TestSessionlessTransactionCommitPLSQLRunQueryBeforeSessionless verifies
// starting a sessionless transaction through PL/SQL fails with ORA-24776 when
// the current SQL transaction has already executed a query or DML statement.
func TestSessionlessTransactionCommitPLSQLRunQueryBeforeSessionless(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(3)

	ctx := context.Background()
	table := createObjectName("sessionless_tx_plsql_run_query_commit")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn.Close()

	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false})
	if err != nil {
		t.Fatalf("begin transaction failed: %v", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(context.Background(), "INSERT INTO "+table+" (str_value) VALUES ('sessionless-plsql-commit')")
	if err != nil {
		t.Fatalf("unexpected error while inserting, %v", err)
	}

	var globalTransactionID string
	if _, err = tx.ExecContext(ctx, `
		BEGIN
			:global_transaction_id := DBMS_TRANSACTION.START_TRANSACTION(
				xid              => NULL,
				transaction_type => DBMS_TRANSACTION.TRANSACTION_TYPE_SESSIONLESS,
				timeout          => 300,
				flag             => DBMS_TRANSACTION.TRANSACTION_NEW);
		END;`, sql.Named("global_transaction_id", sql.Out{Dest: &globalTransactionID})); err == nil {
		t.Fatalf("Expected exception")
	}

	if sqlError, ok := err.(oracleErrors.SQLError); ok {
		if sqlError.ErrorCode() != "ORA-24776" {
			t.Fatalf("Expected error code to be %s but was %s", "ORA-24776", sqlError.ErrorCode())
		}
		t.Logf("Got expected sqlError %v", err)
	} else {
		t.Fatalf("Error should be a sql error")
	}

}

// TestSessionlessTransactionCommitPLSQLConn verifies the server-side PL/SQL
// sessionless transaction calls work directly on a dedicated auto-commit
// connection and that suspend does not hide the already committed change.
func TestSessionlessTransactionCommitPLSQLConn(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(3)

	ctx := context.Background()
	table := createObjectName("sessionless_tx_plsql_conn")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get dedicated connection: %v", err)
	}
	defer conn.Close()

	var globalTransactionID string
	if _, err := conn.ExecContext(ctx, `
		BEGIN
			:global_transaction_id := DBMS_TRANSACTION.START_TRANSACTION(
				xid              => NULL,
				transaction_type => DBMS_TRANSACTION.TRANSACTION_TYPE_SESSIONLESS,
				timeout          => 300,
				flag             => DBMS_TRANSACTION.TRANSACTION_NEW);
		END;`, sql.Named("global_transaction_id", sql.Out{Dest: &globalTransactionID})); err != nil {
		t.Fatalf("start sessionless transaction through PL/SQL failed: %v", err)
	}
	if globalTransactionID == "" {
		t.Fatal("empty global transaction ID")
	}

	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) VALUES ('sessionless-plsql-conn')"); err != nil {
		t.Fatalf("unexpected error while inserting: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN DBMS_TRANSACTION.SUSPEND_TRANSACTION; END;"); err != nil {
		t.Fatalf("unexpected error while suspending: %v", err)
	}

	// since a connection is always on auto-commit mode, the count should be 1, the
	// sessionless transaction was created and committed in the same call and the
	// suspend was a noop
	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("count after suspend = %d, want 1", count)
	}
}

// TestSessionlessTransactionBeginOptions verifies that the public API accepts
// every supported isolation/read-only combination and that each transaction
// can be ended cleanly.
func TestSessionlessTransactionBeginOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		opts sql.TxOptions
	}{
		{name: "default read-write", opts: sql.TxOptions{}},
		{name: "read-committed read-write", opts: sql.TxOptions{Isolation: sql.LevelReadCommitted}},
		{name: "serializable read-write", opts: sql.TxOptions{Isolation: sql.LevelSerializable}},
		{name: "default read-only", opts: sql.TxOptions{ReadOnly: true}},
		{name: "read-committed read-only", opts: sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: true}},
		{name: "serializable read-only", opts: sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, db := openSessionlessTestDB(t)
			defer db.Close()

			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("get connection: %v", err)
			}
			defer conn.Close()

			table := createObjectName("sessionless_tx_commit")
			if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
				t.Fatalf("create table failed: %v", err)
			}
			defer dropTable(ctx, db, table)

			tx, err := BeginSessionlessTx(ctx, conn, test.opts, 300)
			if err != nil {
				t.Fatalf("begin sessionless transaction: %v", err)
			}
			if tx.GlobalTransactionID() == nil {
				t.Fatal("begin returned an empty global transaction ID")
			}

			_, err = conn.ExecContext(ctx, "INSERT INTO "+table+" (str_value) values ('sessionless-start')")
			if test.opts.ReadOnly && err == nil {
				t.Fatalf("Should not be able to insert on read-only TXN")
			}
			if !test.opts.ReadOnly && err != nil {
				t.Fatalf("insert failed: %v", err)
			}

			var flag int
			conn.QueryRowContext(ctx, "select bitand(flag, power(2, 28)) from v$transaction").Scan(&flag)
			if test.opts.Isolation == sql.LevelSerializable && !test.opts.ReadOnly {
				if flag == 0 {
					t.Fatalf("Should be serializable")
				}
			} else {
				if flag != 0 {
					t.Fatalf("Should not be serializable")
				}
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("rollback sessionless transaction: %v", err)
			}

		})
	}
}

// TestSessionlessTransactionResumeValidation verifies that invalid global transaction IDs are
// rejected locally and that a well-formed but unknown global transaction ID is rejected by the
// server.
func TestSessionlessTransactionResumeValidation(t *testing.T) {
	t.Parallel()
	ctx, db := openSessionlessTestDB(t)
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get connection: %v", err)
	}
	defer conn.Close()

	for _, test := range []struct {
		name                string
		globalTransactionID []byte
	}{
		{name: "empty", globalTransactionID: nil},
		{name: "too long", globalTransactionID: slices.Repeat([]byte{70}, 65)},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ResumeSessionlessTransaction(ctx, conn, test.globalTransactionID)
			requireSessionlessSQLError(t, err, oracleErrors.InvalidGlobalTransactionIDValue)
		})
	}

	tx, err := ResumeSessionlessTransaction(ctx, conn, extensions.GlobalTransactionId("sessionless-global-transaction-id-that-does-not-exist"))
	err = conn.PingContext(ctx)
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("resume with an unknown global transaction ID unexpectedly succeeded")
	}
	requireSessionlessSQLError(t, err, "ORA-26218")
}

// TestSessionlessTransactionSQLCommitOrRollbackThenSuspend verifies that a
// transaction ended by executing SQL COMMIT or ROLLBACK is already inactive
// when the API Suspend method is called afterward.
func TestSessionlessTransactionSQLCommitOrRollbackThenSuspend(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		endStatement string
		wantRows     int
	}{
		{name: "commit", endStatement: "COMMIT", wantRows: 2},
		{name: "rollback", endStatement: "ROLLBACK", wantRows: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, db := openSessionlessTestDB(t)
			defer db.Close()

			table := createObjectName("sessionless_tx_sql_end")
			if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
				t.Fatalf("create table failed: %v", err)
			}
			defer dropTable(ctx, db, table)

			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("get connection: %v", err)
			}
			defer conn.Close()

			tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{Isolation: sql.LevelReadCommitted}, 300)
			if err != nil {
				t.Fatalf("begin sessionless transaction: %v", err)
			}
			for _, value := range []string{"sql-end-one", "sql-end-two"} {
				if _, err := conn.ExecContext(ctx,
					"INSERT INTO "+table+" (str_value) VALUES ('"+value+"')"); err != nil {
					t.Fatalf("insert %q: %v", value, err)
				}
			}

			if _, err := conn.ExecContext(ctx, test.endStatement); err != nil {
				t.Fatalf("execute %s: %v", test.endStatement, err)
			}
			if err := tx.Suspend(); err != nil {
				t.Fatalf("suspend after SQL %s: %v", test.endStatement, err)
			}

			count, err := countRows(ctx, db, table)
			if err != nil {
				t.Fatalf("count after SQL %s: %v", test.endStatement, err)
			}
			if count != test.wantRows {
				t.Fatalf("count after SQL %s = %d, want %d", test.endStatement, count, test.wantRows)
			}
		})
	}
}

// TestSessionlessTransactionAPIOperationThenSuspend verifies that API Commit
// and Rollback both make a later API Suspend call a no-op and preserve the
// corresponding commit/rollback result.
func TestSessionlessTransactionAPIOperationThenSuspend(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		commit   bool
		wantRows int
	}{
		{name: "commit", commit: true, wantRows: 1},
		{name: "rollback", commit: false, wantRows: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, db := openSessionlessTestDB(t)
			defer db.Close()

			table := createObjectName("sessionless_tx_api_end")
			if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
				t.Fatalf("create table failed: %v", err)
			}
			defer dropTable(ctx, db, table)

			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("get connection: %v", err)
			}
			defer conn.Close()

			tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{Isolation: sql.LevelReadCommitted}, 300)
			if err != nil {
				t.Fatalf("begin sessionless transaction: %v", err)
			}
			if _, err := conn.ExecContext(ctx,
				"INSERT INTO "+table+" (str_value) VALUES ('api-end')"); err != nil {
				t.Fatalf("insert: %v", err)
			}

			if test.commit {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
			}
			if err != nil {
				t.Fatalf("end sessionless transaction: %v", err)
			}
			if err := tx.Suspend(); err != nil {
				t.Fatalf("suspend after API end: %v", err)
			}

			count, err := countRows(ctx, db, table)
			if err != nil {
				t.Fatalf("count after API end: %v", err)
			}
			if count != test.wantRows {
				t.Fatalf("count after API end = %d, want %d", count, test.wantRows)
			}
		})
	}
}

// TestSessionlessTransactionEndTwice verifies that every second end
// operation—commit twice, rollback twice, or the opposite operation—returns
// NotInTransaction.
func TestSessionlessTransactionEndTwice(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		firstCommit  bool
		secondCommit bool
	}{
		{name: "commit twice", firstCommit: true, secondCommit: true},
		{name: "rollback twice", firstCommit: false, secondCommit: false},
		{name: "commit then rollback", firstCommit: true, secondCommit: false},
		{name: "rollback then commit", firstCommit: false, secondCommit: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, db := openSessionlessTestDB(t)
			defer db.Close()

			table := createObjectName("sessionless_tx_end_twice")
			if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
				t.Fatalf("create table failed: %v", err)
			}
			defer dropTable(ctx, db, table)

			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("get connection: %v", err)
			}
			defer conn.Close()

			tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{Isolation: sql.LevelReadCommitted}, 300)
			if err != nil {
				t.Fatalf("begin sessionless transaction: %v", err)
			}
			if _, err := conn.ExecContext(ctx,
				"INSERT INTO "+table+" (str_value) VALUES ('end-twice')"); err != nil {
				t.Fatalf("insert: %v", err)
			}

			end := func(commit bool) error {
				if commit {
					return tx.Commit()
				}
				return tx.Rollback()
			}
			if err := end(test.firstCommit); err != nil {
				t.Fatalf("first end operation: %v", err)
			}
			requireSessionlessSQLError(t, end(test.secondCommit), oracleErrors.NotInTransaction)
			if err := tx.Suspend(); err != nil {
				t.Fatalf("suspend after repeated end operation: %v", err)
			}
		})
	}
}

// TestSessionlessTransactionResumeSameConnection verifies that a suspended
// transaction can be resumed on the same *sql.Conn and retains its global
// transaction ID.
func TestSessionlessTransactionResumeSameConnection(t *testing.T) {
	t.Parallel()
	ctx, db := openSessionlessTestDB(t)
	defer db.Close()

	table := createObjectName("sessionless_tx_same_conn")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get connection: %v", err)
	}
	defer conn.Close()

	tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{Isolation: sql.LevelReadCommitted}, 300)
	if err != nil {
		t.Fatalf("begin sessionless transaction: %v", err)
	}
	globalTransactionID := tx.GlobalTransactionID()
	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) VALUES ('same-connection-before')"); err != nil {
		t.Fatalf("insert before suspend: %v", err)
	}
	if err := tx.Suspend(); err != nil {
		t.Fatalf("suspend: %v", err)
	}

	resumedTx, err := ResumeSessionlessTransaction(ctx, conn, globalTransactionID)
	if err != nil {
		t.Fatalf("resume on same connection: %v", err)
	}
	if !slices.Equal(resumedTx.GlobalTransactionID(), globalTransactionID) {
		t.Fatalf("resumed global transaction ID = %q, want %q", resumedTx.GlobalTransactionID(), globalTransactionID)
	}
	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) VALUES ('same-connection-after')"); err != nil {
		t.Fatalf("insert after resume: %v", err)
	}
	if err := resumedTx.Commit(); err != nil {
		t.Fatalf("commit after same-connection resume: %v", err)
	}

	count, err := countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count after commit: %v", err)
	}
	if count != 2 {
		t.Fatalf("count after same-connection resume = %d, want 2", count)
	}
}

// TestSessionlessTransactionResumeAfterConnectionClose verifies that closing
// the connection that suspended a transaction does not prevent resuming it on
// a newly acquired connection.
func TestSessionlessTransactionResumeAfterConnectionClose(t *testing.T) {
	t.Parallel()
	ctx, db := openSessionlessTestDB(t)
	defer db.Close()

	table := createObjectName("sessionless_tx_closed_conn")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get connection: %v", err)
	}
	tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{Isolation: sql.LevelReadCommitted}, 300)
	if err != nil {
		conn.Close()
		t.Fatalf("begin sessionless transaction: %v", err)
	}
	globalTransactionID := tx.GlobalTransactionID()
	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) VALUES ('closed-connection')"); err != nil {
		conn.Close()
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Suspend(); err != nil {
		conn.Close()
		t.Fatalf("suspend: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close original connection: %v", err)
	}

	resumeConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get resume connection: %v", err)
	}
	defer resumeConn.Close()
	resumedTx, err := ResumeSessionlessTransaction(ctx, resumeConn, globalTransactionID)
	if err != nil {
		t.Fatalf("resume after closing original connection: %v", err)
	}
	if err := resumedTx.Rollback(); err != nil {
		t.Fatalf("rollback after resume: %v", err)
	}

	count, err := countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count after rollback: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after rollback = %d, want 0", count)
	}
}

// TestSessionlessTransactionRegularTransactionConflict verifies that the API
// refuses to start a sessionless transaction while a regular SQL transaction
// is active on the same connection.
func TestSessionlessTransactionRegularTransactionConflict(t *testing.T) {
	t.Parallel()
	ctx, db := openSessionlessTestDB(t)
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get connection: %v", err)
	}
	defer conn.Close()

	regularTx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin regular transaction: %v", err)
	}
	defer regularTx.Rollback()

	_, err = BeginSessionlessTx(ctx, conn, sql.TxOptions{Isolation: sql.LevelReadCommitted}, 300)
	requireSessionlessSQLError(t, err, oracleErrors.AlreadyInTransaction)
}

// TestSessionlessTransactionGlobalTransactionIDUniqueness verifies that independent API
// starts receive non-empty, distinct global transaction identifiers.
func TestSessionlessTransactionGlobalTransactionIDUniqueness(t *testing.T) {
	t.Parallel()
	ctx, db := openSessionlessTestDB(t)
	defer db.Close()

	conn1, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get first connection: %v", err)
	}
	defer conn1.Close()
	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get second connection: %v", err)
	}
	defer conn2.Close()

	tx1, err := BeginSessionlessTx(ctx, conn1, sql.TxOptions{}, 300)
	if err != nil {
		t.Fatalf("begin first sessionless transaction: %v", err)
	}
	tx2, err := BeginSessionlessTx(ctx, conn2, sql.TxOptions{}, 300)
	if err != nil {
		_ = tx1.Rollback()
		t.Fatalf("begin second sessionless transaction: %v", err)
	}
	globalTransactionID1 := tx1.GlobalTransactionID()
	globalTransactionID2 := tx2.GlobalTransactionID()
	if globalTransactionID1 == nil || globalTransactionID2 == nil {
		t.Fatalf("global transaction IDs must be non-empty: %q, %q", globalTransactionID1, globalTransactionID2)
	}
	if slices.Equal(globalTransactionID1, globalTransactionID2) {
		t.Fatalf("independent transactions reused global transaction ID %q", globalTransactionID1)
	}
	if err := tx1.Rollback(); err != nil {
		t.Fatalf("rollback first transaction: %v", err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("rollback second transaction: %v", err)
	}
}

// TestSessionlessTransactionTimeout verifies that a suspended transaction
// cannot be resumed after its server-side timeout has elapsed.
func TestSessionlessTransactionTimeout(t *testing.T) {
	t.Parallel()
	ctx, db := openSessionlessTestDB(t)
	defer db.Close()

	table := createObjectName("sessionless_tx_timeout")
	if err := createTable(ctx, db, table, map[string]string{"str_value": "VARCHAR(50)"}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	defer dropTable(ctx, db, table)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get connection: %v", err)
	}
	defer conn.Close()

	tx, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{Isolation: sql.LevelReadCommitted}, 1)
	if err != nil {
		t.Fatalf("begin sessionless transaction: %v", err)
	}
	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) VALUES ('timeout')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	globalTransactionID := tx.GlobalTransactionID()
	if err := tx.Suspend(); err != nil {
		t.Fatalf("suspend: %v", err)
	}

	time.Sleep(5 * time.Second)
	resumeConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get resume connection: %v", err)
	}
	defer resumeConn.Close()
	resumedTx, err := ResumeSessionlessTransaction(ctx, resumeConn, globalTransactionID)
	err = resumeConn.PingContext(ctx)
	if err == nil {
		_ = resumedTx.Rollback()
		t.Fatal("resume after timeout unexpectedly succeeded")
	}
	requireSessionlessSQLError(t, err, "ORA-26218")
}

// TestSessionlessTransactionUnsupportedConnection verifies that the public API
// returns its documented unsupported-connection error for a non-Oracle driver.
func TestSessionlessTransactionUnsupportedConnection(t *testing.T) {
	const driverName = "oracle-sessionless-unsupported"
	sql.Register(driverName, unsupportedSessionlessDriver{})

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open unsupported driver: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get connection: %v", err)
	}
	defer conn.Close()

	if _, err := BeginSessionlessTx(ctx, conn, sql.TxOptions{}, 300); err == nil {
		t.Fatal("begin unexpectedly succeeded on an unsupported connection")
	} else if err.Error() != "the connection does not support sessionless transactions" {
		t.Fatalf("unexpected begin error: %v", err)
	}
	if _, err := ResumeSessionlessTransaction(ctx, conn, extensions.GlobalTransactionId("global-transaction-id")); err == nil {
		t.Fatal("resume unexpectedly succeeded on an unsupported connection")
	} else if err.Error() != "the connection does not support sessionless transactions" {
		t.Fatalf("unexpected resume error: %v", err)
	}
}

type unsupportedSessionlessDriver struct{}

func (unsupportedSessionlessDriver) Open(string) (driver.Conn, error) {
	return unsupportedSessionlessConn{}, nil
}

type unsupportedSessionlessConn struct{}

func (unsupportedSessionlessConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unsupported test connection")
}

func (unsupportedSessionlessConn) Close() error {
	return nil
}

func (unsupportedSessionlessConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("unsupported test connection")
}
