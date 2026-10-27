package session

import (
	"testing"

	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// expectOracleErrorCode verifies that an operation returned the expected
// driver error code without requiring each packet test to repeat the assertion.
func expectOracleErrorCode(t *testing.T, err error, want oracleErrors.ErrorCode) {
	t.Helper()
	sqlErr, ok := err.(oracleErrors.SQLError)
	if !ok {
		t.Fatalf("expected Oracle error, got %T (%v)", err, err)
	}
	if sqlErr.ErrorCode() != string(want) {
		t.Errorf("error code = %s, want %s", sqlErr.ErrorCode(), want)
	}
}
