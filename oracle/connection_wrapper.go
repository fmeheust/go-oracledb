package oracle

import (
	"context"
	"database/sql"
	"errors"

	"github.com/oracle/go-oracledb/v26/oracle/extensions"
)

type connectionWrapper struct {
	connection *sql.Conn
}

func NewConnectionWrapper(connection *sql.Conn) (*connectionWrapper, error) {
	err := connection.Raw(func(c any) error {
		// Include here all functions/interfaces we want a connection to implement in
		// order to be wrapped by this wrapper
		type canBeWrapped interface {
			extensions.ConnSessionlessTx
		}
		_, ok := c.(canBeWrapped)
		if !ok {
			return errors.New("unsupported connection type")
		}
		return nil
	})
	return &connectionWrapper{connection: connection}, err
}

// BeginSessionlessTx starts a sessionless transaction on the wrapped connection.
//
// Parameters:
//   - ctx: Context used for the transaction start operation.
//   - opts: Standard transaction options.
//   - timeout: Sessionless transaction timeout in seconds.
//
// Returns:
//   - extensions.SessionlessTx: Started sessionless transaction.
//   - error: Error if the transaction cannot be started.
func (wrapper *connectionWrapper) BeginSessionlessTx(ctx context.Context, opts sql.TxOptions, timeout uint16) (extensions.SessionlessTx, error) {
	var publicSessionlessTransaction extensions.SessionlessTx
	err := wrapper.connection.Raw(func(c any) error {
		var err error
		publicSessionlessTransaction, err = c.(extensions.ConnSessionlessTx).BeginSessionlessTx(ctx, opts, timeout)
		return err
	})
	return publicSessionlessTransaction, err
}

// ResumeSessionlessTx resumes a sessionless transaction on connection.
//
// Parameters:
//   - ctx: Context used for the transaction resume operation.
//   - globalTransactionID: Identifier of the sessionless transaction to resume.
//
// Returns:
//   - extensions.SessionlessTx: Resumed sessionless transaction.
//   - error: Error if the transaction cannot be resumed.
func (wrapper *connectionWrapper) ResumeSessionlessTx(ctx context.Context, globalTransactionID extensions.GlobalTransactionID) (extensions.SessionlessTx, error) {
	var publicSessionlessTransaction extensions.SessionlessTx
	err := wrapper.connection.Raw(func(c any) error {
		var err error
		publicSessionlessTransaction, err = c.(extensions.ConnSessionlessTx).ResumeSessionlessTx(ctx, globalTransactionID)
		return err
	})
	return publicSessionlessTransaction, err
}
