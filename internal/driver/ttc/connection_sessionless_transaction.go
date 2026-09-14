package ttc

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"io"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
	extensions "github.com/oracle/go-oracledb/v26/oracle/extensions"
)

const maxSessionlessGTRIDSize = 64
const maxSessionlessBQUALSize = 64

type sessionlessTransaction struct {
	transaction

	globalTransactionID extensions.GlobalTransactionId // globalTransactionID is the identifier of the sessionless transaction
	timeout             uint16                         // transaction timeout in seconds
	startedOnServer     bool                           // startedOnServer indicates that the transaction has been started on the server
	endedOnServer       bool                           // endedOnServer indicates that the transaction has been started on the server
	xid                 driverCommon.B1Array           // calculate XID using globalTransactionID and instance name
	bqualLength         driverCommon.UB4
	gtridLength         driverCommon.UB4
}

func newSessionlesTransaction(conn *connection, ctx context.Context, globalTransactionID extensions.GlobalTransactionId, timeout uint16) *sessionlessTransaction {
	tx := &transaction{
		_underlyingConnection: conn,
		_transactionContext:   ctx,
	}
	return upgradeFromTransaction(tx, globalTransactionID, timeout)
}

func upgradeFromTransaction(tx *transaction, globalTransactionID extensions.GlobalTransactionId, timeout uint16) *sessionlessTransaction {
	sessionlessTx := &sessionlessTransaction{
		transaction:         *tx,
		timeout:             timeout,
		globalTransactionID: globalTransactionID,
	}
	sessionlessTx.buildSessionlessXID()
	sessionlessTx.underlyingConnection().shelf.getEventService().register(sessionlessTx, sessionlessTranzactionStartClient)
	sessionlessTx.underlyingConnection().shelf.getEventService().register(sessionlessTx, sessionlessTranzactionEndClient)
	return sessionlessTx
}

// generateGlobalTransactionId creates a JDBC-compatible default GTRID using 16
// random bytes encoded with UUID version and variant bits. The returned string
// stores the raw bytes directly so it can be passed unchanged to TTC payloads.
func generateGlobalTransactionId() (extensions.GlobalTransactionId, error) {
	var gtrid [16]byte
	if _, err := io.ReadFull(rand.Reader, gtrid[:]); err != nil {
		return nil, err
	}

	// Match UUID.randomUUID() layout used by the JDBC thin driver.
	gtrid[6] = (gtrid[6] & 0x0F) | 0x40
	gtrid[8] = (gtrid[8] & 0x3F) | 0x80

	return gtrid[:], nil
}

func validateSessionlessGTRID(gtrid extensions.GlobalTransactionId) error {
	size := len(gtrid)
	if size == 0 {
		return common.NewOracleError(oracleErrors.InvalidGTRIDValue, nil)
	}
	if size > maxSessionlessGTRIDSize {
		return common.NewOracleError(oracleErrors.InvalidGTRIDValue, nil)
	}
	return nil
}

func (t *sessionlessTransaction) buildSessionlessXID() {
	gtridBytes := []byte(t.globalTransactionID)
	gtridLength := len(gtridBytes)
	if gtridLength > maxSessionlessGTRIDSize {
		gtridLength = maxSessionlessGTRIDSize
	}

	var bqualBytes []byte
	c := t.underlyingConnection()
	if c != nil && c.sessCtx != nil {
		if instance, err := c.sessCtx.GetSessionProperties().GetTrimmedString(instanceName); err == nil {
			bqualBytes = []byte(instance)
		}
	}
	bqualLength := len(bqualBytes)
	if bqualLength > maxSessionlessBQUALSize {
		bqualLength = maxSessionlessBQUALSize
	}

	xid := make(driverCommon.B1Array, maxSessionlessGTRIDSize+maxSessionlessBQUALSize)
	copy(xid, gtridBytes[:gtridLength])
	copy(xid[gtridLength:], bqualBytes[:bqualLength])

	t.xid = xid
	t.bqualLength = driverCommon.UB4(bqualLength)
	t.gtridLength = driverCommon.UB4(gtridLength)
}

// BeginSessionlessTx starts a sessionless transaction using the provided
// standard transaction options and returns a transaction object that exposes
// sessionless lifecycle operations.
func (c *connection) BeginSessionlessTx(ctx context.Context, opts sql.TxOptions, timeout uint16) (extensions.SessionlessTx, error) {
	common.Odl.Debug("Starting sessionless transaction")

	// check that there is no active transaction in the connection
	if c.shelf.isInTransaction() {
		return nil, c.shelf.LocalizeError(common.NewOracleError(oracleErrors.AlreadyInTransaction, nil, nil))
	}

	// check that the server supports sessionless transactions
	capabilities := c.shelf.GetCapabilities()
	if cap, ok := capabilities[kpccapCtbTtc5SessionlessTxn]; !ok || !cap.IsSet {
		return nil, c.shelf.LocalizeError(common.NewOracleError(oracleErrors.UnsupportedFeature, nil, "Sessionless Transactions"))
	}

	driverOpts := driver.TxOptions{
		Isolation: driver.IsolationLevel(opts.Isolation),
		ReadOnly:  opts.ReadOnly,
	}

	// check that the isolation level is supported
	if !isSupportedIsolationLevel(driverOpts) {
		return nil, c.shelf.LocalizeError(common.NewOracleError(oracleErrors.IsolationLevelNotSupported, nil, nil))
	}

	// generate a global transaction id
	globalTransactionId, err := generateGlobalTransactionId()
	if err != nil {
		return nil, c.shelf.LocalizeError(err)
	}

	// create and register the sessionless transaction object
	tx := newSessionlesTransaction(c, ctx, globalTransactionId, timeout)
	c.shelf.registerTransaction(tx)
	if err := c.beginTransaction(ctx, tx, driverOpts); err != nil {
		// unregister as current transaction
		c.shelf.unregisterTransaction()
		return nil, c.shelf.LocalizeError(common.NewOracleError(oracleErrors.StartResumeTransactionFailure, err, nil))
	}

	// return the sessionless transaction
	return tx, nil
}

// ResumeSessionlessTx resumes the sessionless transaction identified by gtrid
// using the provided standard transaction options and returns a transaction
// object that exposes sessionless lifecycle operations.
func (c *connection) ResumeSessionlessTx(ctx context.Context, globalTransactionId extensions.GlobalTransactionId) (extensions.SessionlessTx, error) {

	// check that the global trasanction id is valid
	if err := validateSessionlessGTRID(globalTransactionId); err != nil {
		return nil, c.shelf.LocalizeError(err)
	}

	// check that there is no active transaction in the connection
	if c.shelf.isInTransaction() {
		return nil, c.shelf.LocalizeError(common.NewOracleError(oracleErrors.AlreadyInTransaction, nil, nil))
	}

	// check that the server supports sessionless transactions
	capabilities := c.shelf.GetCapabilities()
	if cap, ok := capabilities[kpccapCtbTtc5SessionlessTxn]; !ok || !cap.IsSet {
		return nil, c.shelf.LocalizeError(common.NewOracleError(oracleErrors.UnsupportedFeature, nil, "Sessionless Transactions"))
	}

	// create and register the sessionless transaction object
	tx := newSessionlesTransaction(c, ctx, globalTransactionId, 0)
	c.shelf.registerTransaction(tx)
	if err := c.resumeSessionlessTx(ctx, tx, 0); err != nil {
		// unregister as current transaction
		c.shelf.unregisterTransaction()
		return nil, c.shelf.LocalizeError(err)
	}

	return tx, nil
}

// Suspend detaches the current sessionless transaction from the connection, if
// no transaction is active suspend is a no-op.
func (t *sessionlessTransaction) Suspend() error {

	// check that there is no active transaction in the connection
	if !t.underlyingConnection().shelf.isInTransaction() {
		return nil
	}

	if err := t._underlyingConnection.detachTransaction(t._transactionContext); err != nil {
		t._underlyingConnection.shelf.unregisterTransaction()
		return t._underlyingConnection.shelf.LocalizeError(
			common.NewOracleError(oracleErrors.ErrorInTransaction, err, "Suspend"),
		)
	}
	t._underlyingConnection.shelf.unregisterTransaction()
	return nil
}

func (c *connection) resumeSessionlessTx(ctx context.Context, tx *sessionlessTransaction, timeout uint16) error {
	stmr, ok := c.shelf.GetMessageStreamer().(MessageStreamerInterface)
	if !ok {
		common.Odl.Warn("Sessionless transactions require a message streamer with callback support")
		return common.NewOracleError(oracleErrors.InternalError, nil)
	}

	msg, err := c.shelf.GetMessageFactory().GetMessageForFunction(TTIPFN, oTxSe)
	if err != nil {
		common.Odl.Warn("Error creating OTXSE message", "error", err)
		return common.NewOracleError(oracleErrors.InternalError, err)
	}

	otxse, ok := msg.(*tTIOtxse)
	if !ok {
		common.Odl.Warn("Unexpected message type for OTXSE", "message", msg)
		return common.NewOracleError(oracleErrors.InternalError, nil)
	}

	otxse.confugureForResume(tx)

	err = stmr.Push(ctx, msg)
	if err != nil {
		common.Odl.Warn("Error pushing OTXSE message", "error", err)
		return common.NewOracleError(oracleErrors.StartResumeTransactionFailure, err)
	}

	return nil
}

// runSessionlessTransaction executes an OTXSE request and waits for the
// terminal TTIOER or TTISTA response.
func (c *connection) detachTransaction(ctx context.Context) error {
	common.Odl.Debug("Running sessionless transaction detach")

	stmr, ok := c.shelf.GetMessageStreamer().(MessageStreamerInterface)
	if !ok {
		common.Odl.Warn("Sessionless transactions require a message streamer with callback support")
		return common.NewOracleError(oracleErrors.InternalError, nil)
	}

	msg, err := c.shelf.GetMessageFactory().GetMessageForFunction(TTIFUN, oTxSe)
	if err != nil {
		common.Odl.Warn("Error creating OTXSE message", "error", err)
		return common.NewOracleError(oracleErrors.InternalError, err)
	}

	otxse, ok := msg.(*tTIOtxse)
	if !ok {
		common.Odl.Warn("Unexpected message type for OTXSE", "message", msg)
		return common.NewOracleError(oracleErrors.InternalError, nil)
	}

	otxse.confugureForSuspend()

	err = c.shelf.GetMessageStreamer().Push(ctx, msg)
	if err != nil {
		common.Odl.Warn("Error pushing OTXSE message", "error", err)
		return common.NewOracleError(oracleErrors.StreamerWriteError, err)
	}
	err = stmr.Flush(ctx)
	if err != nil {
		common.Odl.Warn("Error flushing OTXSE message", "error", err)
		return common.NewOracleError(oracleErrors.StreamerWriteError, err)
	}

	stmr.RegisterPreUnmarshallCallback(TTIRPA, func(*messageHeader) (driverCommon.Message[driverCommon.MessageType], error) {
		return c.shelf.GetMessageFactory().GetMessageForFunction(TTIRPA, oTxSe)
	})
	defer stmr.UnRegisterPreUnmarshallCallback(TTIRPA)

	for {
		retMsg, err := stmr.Pull(ctx, TTIRPA, TTIOER, TTISTA)
		if err != nil {
			common.Odl.Warn("Error pulling OTXSE response", "error", err)
			return common.NewOracleError(oracleErrors.StreamerReadError, err)
		}
		switch retMsg.GetMsgCode() {
		case TTIRPA:
			// OTXSE returns context/application return values in TTIRPA, but the
			// transaction operation completes only when terminal status follows.
			continue
		case TTIOER:
			err = retMsg.(tTIOerIface).getError()
			if err != nil {
				return err
			}
			return nil
		case TTISTA:
			return nil
		}
	}
}

// GlobalTransactionID returns the identifier associated with this sessionless
// transaction.
func (t *sessionlessTransaction) GlobalTransactionID() extensions.GlobalTransactionId {
	// check that there is no active transaction in the connection
	if !t.underlyingConnection().shelf.isInTransaction() {
		return nil
	}
	return t.globalTransactionID
}

func (t *sessionlessTransaction) notify(event eventType) {
	newValue := t._underlyingConnection.sessCtx.GetSessionProperties().GetProperty(sessionlessGTRIDProperty)
	sync, ok := newValue.(SessionlessGTRIDSync)
	if !ok {
		return
	}

	switch event {
	case sessionlessTranzactionStartClient:
		if !bytes.Equal(sync.gtrid, t.globalTransactionID) {
			common.Odl.Debug("Global transaction id mismatch", "server GTRID", sync.gtrid, "client GTRID", t.globalTransactionID)
			t.globalTransactionID = sync.gtrid
		}
		common.Odl.Debug("Transaction has started by client received by server")
		t.startedOnServer = true
	case sessionlessTranzactionEndClient:
		if !bytes.Equal(sync.gtrid, t.globalTransactionID) {
			common.Odl.Debug("Global transaction id mismatch", "server GTRID", sync.gtrid, "client GTRID", t.globalTransactionID)
			t.globalTransactionID = sync.gtrid
		}
		common.Odl.Debug("Transaction has ended by client received by server")
		t.endedOnServer = true
	}
}
