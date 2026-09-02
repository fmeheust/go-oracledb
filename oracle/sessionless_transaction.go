package oracle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/oracle/go-oracledb/v26/internal/common"
)

type SessionlessTransaction interface {
	Suspend() error
	Commit() error
	Rollback() error
	GlobalTransactionID() string
}

func StartSessionlessTransaction(ctx context.Context, connection *sql.Conn, opts driver.TxOptions, timeout uint16) (SessionlessTransaction, error) {
	var publicSessionlessTransaction SessionlessTransaction
	err := connection.Raw(func(c any) error {
		sessionlessTxStarter, ok := c.(common.ConnBeginSessionlessTx)
		if !ok {
			return errors.New("the connection does not support sessionless transactions")
		}
		sessionlessTx, internalErr := sessionlessTxStarter.BeginSessionlessTx(ctx, opts, timeout)
		if internalErr != nil {
			return internalErr
		}
		if publicSessionlessTransaction, ok = sessionlessTx.(SessionlessTransaction); !ok {
			return errors.New("invalid transaction returned")
		}
		return nil
	})
	return publicSessionlessTransaction, err
}

func ResumeSessionlessTransaction(ctx context.Context, connection *sql.Conn, globalTransactionID string) (SessionlessTransaction, error) {
	var publicSessionlessTransaction SessionlessTransaction
	err := connection.Raw(func(c any) error {
		sessionlessTxStarter, ok := c.(common.ConnBeginSessionlessTx)
		if !ok {
			return errors.New("the connection does not support sessionless transactions")
		}
		sessionlessTx, internalErr := sessionlessTxStarter.ResumeSessionlessTx(ctx, globalTransactionID)
		if internalErr != nil {
			return internalErr
		}
		if publicSessionlessTransaction, ok = sessionlessTx.(SessionlessTransaction); !ok {
			return errors.New("invalid transaction returned")
		}
		return nil
	})
	return publicSessionlessTransaction, err
}
