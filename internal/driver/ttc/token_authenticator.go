/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package ttc

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"strings"
	"time"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
	oracleProviders "github.com/oracle/go-oracledb/v26/oracle/providers"
)

type tokenAuthenticator struct {
	shelf          ttiShelf[driverCommon.MessageType]
	sessionContext *driverCommon.SessionContext
	tokenProvider  oracleProviders.TokenAuthenticationProvider
	connectString  string
}

func NewTokenAuthenticator(providerRegistry []oracleProviders.Provider, connectString string) *tokenAuthenticator {
	return &tokenAuthenticator{
		tokenProvider: findFirstTokenAuthenticatorProvider(providerRegistry),
		connectString: connectString,
	}
}

func findFirstTokenAuthenticatorProvider(providerRegistry []oracleProviders.Provider) oracleProviders.TokenAuthenticationProvider {
	for _, provider := range providerRegistry {
		if tokenProvider, ok := provider.(oracleProviders.TokenAuthenticationProvider); ok {
			return tokenProvider
		}
	}
	return nil
}

func (ta *tokenAuthenticator) SetShelf(shelf *ttiShelf[driverCommon.MessageType]) {
	ta.shelf = *shelf
}

func (ta *tokenAuthenticator) SetSessionContext(sessCtx *driverCommon.SessionContext) {
	ta.sessionContext = sessCtx
}

func (ta *tokenAuthenticator) Authenticate(ctx context.Context) error {
	common.Odl.Debug("Start TOKEN authentication")
	if ta.tokenProvider == nil {
		return common.NewOracleError(oracleErrors.NoAuthenticatorError, nil)
	}
	if ta.sessionContext == nil {
		return common.NewOracleError(oracleErrors.InternalError, nil)
	}

	// Validate token
	token, err := ta.tokenProvider.Token(ctx)
	if err != nil {
		return common.NewOracleError(oracleErrors.AuthenticatorError, err, nil)
	}
	if err := validateJWTExpiration(token); err != nil {
		return common.NewOracleError(oracleErrors.AuthenticatorError, err, nil)
	}

	// Generate header and sign it if expected
	tokenHeader, err := ta.generateTokenHeader()
	if err != nil {
		return common.NewOracleError(oracleErrors.AuthenticatorError, err, nil)
	}
	signature, err := ta.signHeader(ctx, tokenHeader)
	if err != nil {
		return common.NewOracleError(oracleErrors.AuthenticatorError, err, nil)
	}

	shelf := ta.shelf
	streamer := shelf.GetMessageStreamer().(MessageStreamerInterface)

	msg, err := shelf.GetMessageFactory().(Factory).GetMessageForFunction(TTIFUN, oauth)
	if err != nil {
		return common.NewOracleError(oracleErrors.AuthenticatorError, err, nil)
	}

	// build oauth message
	oauthMsg := msg.(*oAuth)
	oauthMsg.setConnectString(ta.connectString)
	oauthMsg.setLogonMode(logonMode(ta.tokenProvider))
	oauthMsg.prepareForTokenOAUTH(driverCommon.B1Array{})
	oauthMsg.setTokenKeyValsForOAUTH(token, tokenHeader, signature)

	// send oauth message
	if err := streamer.Push(ctx, msg); err != nil {
		return common.NewOracleError(oracleErrors.AuthenticatorError, err, nil)
	}
	if err := streamer.Flush(ctx); err != nil {
		return common.NewOracleError(oracleErrors.AuthenticatorError, err, nil)
	}

	// prepare to receive response
	oauthRPACallBack := func(t *messageHeader) (driverCommon.Message[driverCommon.MessageType], error) {
		return shelf.GetMessageFactory().(Factory).GetMessageForFunction(TTIRPA, oauth)
	}
	streamer.RegisterPreUnmarshallCallback(TTIRPA, oauthRPACallBack)
	defer streamer.UnRegisterPreUnmarshallCallback(TTIRPA)

	// fetch response
	var oauthrpa *OAuthRPA
	for {
		msg, err := streamer.Pull(ctx, TTIRPA, TTIOER, TTIWRN)
		if err != nil {
			return common.NewOracleError(oracleErrors.AuthenticatorError, err, nil)
		}
		switch msg.GetMsgCode() {
		case TTIRPA:
			oauthrpa = msg.(*OAuthRPA)
		case TTIOER:
			ttioer := msg.(tTIOerIface)
			if err := ttioer.getError(); err != nil {
				return err
			}
			if oauthrpa == nil {
				return common.NewOracleError(oracleErrors.InternalError, nil, nil)
			}
			ta.sessionContext.UpdateSessionProperties(oauthrpa.connectionValues)
			return nil
		case TTIWRN:
			logAuthenticationWarning(msg.(*tTIwrn))
		default:
			return common.NewOracleError(oracleErrors.InternalError, nil, nil)
		}
	}
}

func expectsHeader(tokenProvider oracleProviders.TokenAuthenticationProvider) bool {
	_, ok := tokenProvider.(oracleProviders.OCITokenAuthenticationProvider)
	return ok
}

func logonMode(tokenProvider oracleProviders.TokenAuthenticationProvider) int64 {
	if expectsHeader(tokenProvider) {
		return common.KpzLogon.Value() | common.KpzLogonToken.Value()
	} else {
		return common.KpzLogon.Value()
	}
}

func (ta *tokenAuthenticator) generateTokenHeader() (string, error) {
	if expectsHeader(ta.tokenProvider) {
		serviceName, err := extractServiceName(ta.connectString)
		if err != nil {
			return "", err
		}
		remoteAddr, err := getRequiredSessionProperty(ta.sessionContext, "REMOTE_ADDRESS")
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(
			"date: %s\n(request-target): %s\nhost: %s",
			time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
			serviceName,
			remoteAddr,
		), nil
	}
	return "", nil
}

func (ta *tokenAuthenticator) signHeader(ctx context.Context, header string) (string, error) {
	if expectsHeader(ta.tokenProvider) && len(header) > 0 {
		keyPEM, err := ta.tokenProvider.(oracleProviders.OCITokenAuthenticationProvider).PrivateKey(ctx)
		if err != nil {
			return "", err
		}
		signer, err := getSigner(keyPEM)
		if err != nil {
			return "", err
		}
		signature, err := signTokenHeader(header, signer)
		if err != nil {
			return "", err
		}
		return signature, nil
	}
	return "", nil
}

func getRequiredSessionProperty(sessionContext *driverCommon.SessionContext, key string) (string, error) {
	value, ok := sessionContext.GetSessionProperties().GetProperty(key).(string)
	if !ok {
		return "", common.NewOracleError(oracleErrors.ValueRetrievalError, nil, key)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", common.NewOracleError(oracleErrors.ValueRetrievalError, nil, key)
	}
	return value, nil
}

func extractServiceName(connectString string) (string, error) {
	common.Odl.Debug("ConnectionString", "connectString", connectString)
	return extractAddressValue(connectString, "SERVICE_NAME")
}

func extractAddressValue(connectString, key string) (string, error) {
	upper := strings.ToUpper(connectString)
	idx := strings.Index(upper, key+"=")
	if idx == -1 {
		return "", common.NewOracleError(oracleErrors.ValueRetrievalError, nil, key)
	}
	start := idx + len(key) + 1
	end := strings.IndexAny(connectString[start:], ")")
	if end == -1 {
		return "", common.NewOracleError(oracleErrors.ValueRetrievalError, nil, key)
	}
	return strings.TrimSpace(connectString[start : start+end]), nil
}

func getSigner(keyPEM []byte) (crypto.Signer, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, common.NewOracleError(oracleErrors.InvalidPrivateKey, nil)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, common.NewOracleError(oracleErrors.InvalidPrivateKey, nil)
	}
	if _, ok := signer.(*rsa.PrivateKey); !ok {
		return nil, common.NewOracleError(oracleErrors.InvalidPrivateKey, nil)
	}
	return signer, nil
}

func validateJWTExpiration(token string) error {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}

	common.Odl.Debug("JWTToken", "token", token, "payload", payload)
	var claims struct {
		Exp *int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == nil {
		return nil
	}
	if time.Unix(*claims.Exp, 0).Before(time.Now()) {
		return common.NewOracleError(oracleErrors.ExpiredToken, nil)
	}
	return nil
}

func signTokenHeader(header string, signer crypto.Signer) (string, error) {
	sum := sha256.Sum256([]byte(header))
	signature, err := signer.Sign(rand.Reader, sum[:], crypto.SHA256)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(signature), nil
}
