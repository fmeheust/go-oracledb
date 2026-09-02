package ttc

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"io"
	"math"
	"time"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

const maxSessionlessGTRIDSize = 64
const maxSessionlessBQUALSize = 64

// generateSessionlessGTRID creates a JDBC-compatible default GTRID using 16
// random bytes encoded with UUID version and variant bits. The returned string
// stores the raw bytes directly so it can be passed unchanged to TTC payloads.
func generateSessionlessGTRID() (string, error) {
	var gtrid [16]byte
	if _, err := io.ReadFull(rand.Reader, gtrid[:]); err != nil {
		return "", err
	}

	// Match UUID.randomUUID() layout used by the JDBC thin driver.
	gtrid[6] = (gtrid[6] & 0x0F) | 0x40
	gtrid[8] = (gtrid[8] & 0x3F) | 0x80

	return string(gtrid[:]), nil
}

func validateSessionlessGTRID(gtrid string) error {
	size := len([]byte(gtrid))
	if size == 0 {
		return common.NewOracleError(oracleErrors.InvalidGTRIDValue, nil)
	}
	if size > maxSessionlessGTRIDSize {
		return common.NewOracleError(oracleErrors.InvalidGTRIDValue, nil)
	}
	return nil
}

func sessionlessTxTimeoutFromContext(ctx context.Context) driverCommon.UB2 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0
	}

	seconds := math.Ceil(remaining.Seconds())
	if seconds > float64(math.MaxUint16) {
		return math.MaxUint16
	}

	return driverCommon.UB2(seconds)
}

func buildSessionlessXID(c *connection, gtrid string) (driverCommon.B1Array, driverCommon.UB4, driverCommon.UB4) {
	gtridBytes := []byte(gtrid)
	gtridLength := len(gtridBytes)
	if gtridLength > maxSessionlessGTRIDSize {
		gtridLength = maxSessionlessGTRIDSize
	}

	var bqualBytes []byte
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

	return xid, driverCommon.UB4(gtridLength), driverCommon.UB4(bqualLength)
}

// runSessionlessTransaction executes an OTXSE request and waits for the
// terminal TTIOER or TTISTA response.
func (c *connection) runSessionlessTransaction(ctx context.Context, operation driverCommon.SB4, flags driverCommon.UB4, gtrid string, timeout driverCommon.UB2) error {
	common.Odl.Debug("Running sessionless transaction operation", "operation", operation)

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

	otxse.setOperation(operation)
	otxse.setTimeout(timeout)
	otxse.setFlags(flags)
	otxse.setFormatID(k2gSessionless)
	otxse.setApplicationValue(0)
	if gtrid != "" {
		xid, gtridLength, bqualLength := buildSessionlessXID(c, gtrid)
		otxse.setXID(xid, gtridLength, bqualLength)
	}

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

// BeginSessionlessTx starts a sessionless transaction using the provided
// standard transaction options and returns a transaction object that exposes
// sessionless lifecycle operations.
func (c *connection) BeginSessionlessTx(ctx context.Context, opts driver.TxOptions, timeout uint16) (common.OracleTx, error) {
	common.Odl.Debug("Starting transaction")

	tx, err := c.beginTx(ctx, opts)
	if err != nil {
		c.shelf.unregisterTransaction()
		return nil, err
	}

	gtrid, err := generateSessionlessGTRID()
	if err != nil {
		c.shelf.unregisterTransaction()
		return nil, c.shelf.LocalizeError(common.NewOracleError(oracleErrors.InternalError, err))
	}
	tx.GTRID = gtrid

	flags := otxseTransSessionless | otxseTransNew
	err = c.runSessionlessTransaction(ctx, otxseStart, flags, tx.GTRID, driverCommon.UB2(timeout))
	if err != nil {
		c.shelf.unregisterTransaction()
		return nil, c.shelf.LocalizeError(common.NewOracleError(oracleErrors.ConfigureTransactionError, err, nil))
	}

	return tx, nil
}

// ResumeSessionlessTx resumes the sessionless transaction identified by gtrid
// using the provided standard transaction options and returns a transaction
// object that exposes sessionless lifecycle operations.
func (c *connection) ResumeSessionlessTx(ctx context.Context, gtrid string) (common.OracleTx, error) {
	if err := validateSessionlessGTRID(gtrid); err != nil {
		return nil, c.shelf.LocalizeError(err)
	}

	if c.shelf.isInTransaction() {
		return nil, c.shelf.LocalizeError(common.NewOracleError(oracleErrors.AlreadyInTransaction, nil, nil))
	}

	tx := newTransaction(c, ctx)
	tx.GTRID = gtrid
	c.shelf.registerTransaction(tx)

	err := c.runSessionlessTransaction(ctx, otxseStart, otxseTransSessionless|otxseTransResume, gtrid, sessionlessTxTimeoutFromContext(ctx))
	if err != nil {
		c.shelf.unregisterTransaction()
		return nil, c.shelf.LocalizeError(common.NewOracleError(oracleErrors.ConfigureTransactionError, err, nil))
	}

	return tx, nil
}

// Suspend detaches the current sessionless transaction from the connection.
func (t *transaction) Suspend() error {
	if !t.IsSessionlessTx() {
		return t._underlyingConnection.shelf.LocalizeError(
			common.NewOracleError(oracleErrors.NotInTransaction, nil, nil),
		)
	}
	err := t._underlyingConnection.runSessionlessTransaction(
		common.BackgroundContext,
		otxseDetach,
		otxseTransSessionless,
		"",
		0,
	)
	if err != nil {
		return t._underlyingConnection.shelf.LocalizeError(
			common.NewOracleError(oracleErrors.ErrorInTransaction, err, "Suspend"),
		)
	}
	t.GTRID = ""
	t._underlyingConnection.shelf.unregisterTransaction()
	return nil
}

// GlobalTransactionID returns the identifier associated with this sessionless
// transaction.
func (t *transaction) GlobalTransactionID() string {
	return t.GTRID
}
