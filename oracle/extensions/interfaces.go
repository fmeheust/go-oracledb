package extensions

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
)

// ConnBeginSessionlessTx is implemented by connections that support Oracle
// sessionless transaction lifecycle operations in addition to standard BeginTx.
type ConnSessionlessTx interface {
	Close() error
	// BeginSessionlessTx starts a new sessionless transaction using the provided
	// standard transaction options.
	BeginSessionlessTx(ctx context.Context, opts sql.TxOptions, timeout uint16) (SessionlessTx, error)
	// ResumeSessionlessTx resumes the sessionless transaction identified by gtrid
	// using the provided standard transaction options.
	ResumeSessionlessTx(ctx context.Context, gtrid GlobalTransactionId) (SessionlessTx, error)
}

type GlobalTransactionId []byte

func (value GlobalTransactionId) String() string {
	return base64.StdEncoding.EncodeToString([]byte(value))
}

type SessionlessTx interface {
	driver.Tx
	Suspend() error
	GlobalTransactionID() GlobalTransactionId
}
