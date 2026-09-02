package common

import (
	"context"
	"database/sql/driver"
)

// ConnBeginSessionlessTx is implemented by connections that support Oracle
// sessionless transaction lifecycle operations in addition to standard BeginTx.
type ConnBeginSessionlessTx interface {
	// BeginSessionlessTx starts a new sessionless transaction using the provided
	// standard transaction options.
	BeginSessionlessTx(ctx context.Context, opts driver.TxOptions, timeout uint16) (OracleTx, error)
	// ResumeSessionlessTx resumes the sessionless transaction identified by gtrid
	// using the provided standard transaction options.
	ResumeSessionlessTx(ctx context.Context, gtrid string) (OracleTx, error)
}

// SessionlessTx extends driver.Tx with Oracle-specific suspend and transaction
// identity operations for sessionless transactions.
type OracleTx interface {
	driver.Tx
	// Suspend detaches the current sessionless transaction from the connection.
	Suspend() error
	// GlobalTransactionID returns the identifier associated with this
	// sessionless transaction.
	GlobalTransactionID() string
	// IsSessionlessTx reports whether this transaction has an associated GTRID.
	IsSessionlessTx() bool
}
