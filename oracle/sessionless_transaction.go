package oracle

import (
	"context"
	"database/sql"
	"errors"

	"github.com/oracle/go-oracledb/v26/oracle/extensions"
)

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
