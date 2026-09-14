package oracle

import (
	"context"
	"database/sql"
	"errors"

	"github.com/oracle/go-oracledb/v26/oracle/extensions"
)

// BeginSessionlessTx starts a sessionless transaction on connection.
//
// Parameters:
//   - ctx: Context used for the transaction start operation.
//   - connection: SQL connection on which to start the transaction.
//   - opts: Standard transaction options.
//   - timeout: Sessionless transaction timeout in seconds.
//
// Returns:
//   - extensions.SessionlessTx: Started sessionless transaction.
//   - error: Error if the connection does not support sessionless transactions
//     or the transaction cannot be started.
func BeginSessionlessTx(ctx context.Context, connection *sql.Conn, opts sql.TxOptions, timeout uint16) (extensions.SessionlessTx, error) {
	var publicSessionlessTransaction extensions.SessionlessTx
	err := connection.Raw(func(c any) error {
		var err error
		sessionlessTxStarter, ok := c.(extensions.ConnSessionlessTx)
		if !ok {
			return errors.New("the connection does not support sessionless transactions")
		}
		publicSessionlessTransaction, err = sessionlessTxStarter.BeginSessionlessTx(ctx, opts, timeout)
		if err != nil {
			return err
		}
		return nil
	})
	return publicSessionlessTransaction, err
}

// ResumeSessionlessTransaction resumes a sessionless transaction on connection.
//
// Parameters:
//   - ctx: Context used for the transaction resume operation.
//   - connection: SQL connection on which to resume the transaction.
//   - globalTransactionID: Identifier of the sessionless transaction to resume.
//
// Returns:
//   - extensions.SessionlessTx: Resumed sessionless transaction.
//   - error: Error if the connection does not support sessionless transactions
//     or the transaction cannot be resumed.
func ResumeSessionlessTransaction(ctx context.Context, connection *sql.Conn, globalTransactionID extensions.GlobalTransactionId) (extensions.SessionlessTx, error) {
	var publicSessionlessTransaction extensions.SessionlessTx
	err := connection.Raw(func(c any) error {
		var err error
		sessionlessTxStarter, ok := c.(extensions.ConnSessionlessTx)
		if !ok {
			return errors.New("the connection does not support sessionless transactions")
		}
		publicSessionlessTransaction, err = sessionlessTxStarter.ResumeSessionlessTx(ctx, globalTransactionID)
		if err != nil {
			return err
		}
		return nil
	})
	return publicSessionlessTransaction, err
}
