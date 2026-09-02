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
	sqldriver "database/sql/driver"
	"testing"
)

func countRows(ctx context.Context, db *sql.DB, table string) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
	return count, err
}

// TestSessionlessTransactionCommit verifies that a sessionless transaction can
// be started, suspended, resumed, and committed while preserving isolation
// across connections.
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
	db.SetMaxOpenConns(2)

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

	var gtrid string
	tx, err := StartSessionlessTransaction(ctx, conn, sqldriver.TxOptions{
		Isolation: sqldriver.IsolationLevel(sql.LevelDefault),
	}, 300)
	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}
	gtrid = tx.GlobalTransactionID()
	if gtrid == "" {
		t.Fatal("empty gtrid")
	}

	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-commit')"); err != nil {
		t.Fatalf("an unexpected error occured while executing statemet: %v", err)
	}

	err = tx.Suspend()

	if err != nil {
		t.Fatalf("begin/insert/suspend failed: %v", err)
	}

	count, err := countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count before resume/commit failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count before resume/commit = %d, want 0", count)
	}

	tx, err = ResumeSessionlessTransaction(ctx, conn, gtrid)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("resume/commit failed: %v", err)
	}

	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count after commit failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("count after commit = %d, want 1", count)
	}
}

// TestSessionlessTransactionRollback verifies that a sessionless transaction can
// be started, suspended, resumed, and rolled back without making changes
// visible to other connections.
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

	var gtrid string

	tx, err := StartSessionlessTransaction(ctx, conn, sqldriver.TxOptions{
		Isolation: sqldriver.IsolationLevel(sql.LevelReadCommitted),
	}, 300)
	if err != nil {
		t.Fatalf("failed to get start sessionless transaction: %v", err)
	}
	gtrid = tx.GlobalTransactionID()
	if gtrid == "" {
		t.Fatal("gtrid is empty")
	}

	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+table+" (str_value) values ('sessionless-rollback')"); err != nil {
		t.Fatalf("an unexpected error occured while inserting: %v", err)
	}

	err = tx.Suspend()
	if err != nil {
		t.Fatalf("suspend failed: %v", err)
	}

	count, err := countRows(ctx, db, table)
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

	tx2, err := ResumeSessionlessTransaction(ctx, conn2, gtrid)
	if err != nil {
		t.Fatalf("failed to resume sessionless transaction with transactionId %s: %v", gtrid, err)
	}

	err = tx2.Rollback()
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	count, err = countRows(ctx, db, table)
	if err != nil {
		t.Fatalf("count after rollback failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after rollback = %d, want 0", count)
	}
}
