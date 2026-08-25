/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below), to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the
** Software and the Larger Work(s), and to sublicense the foregoing rights on
** either these or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or
** substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package ttc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"reflect"
	"strings"
	"testing"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	oracleconfig "github.com/oracle/go-oracledb/v26/oracle/config"
	oracleProviders "github.com/oracle/go-oracledb/v26/oracle/providers"
)

type mockTokenAuthenticationProvider struct {
	token string
	err   error
}

func (m mockTokenAuthenticationProvider) Token(context.Context) (string, error) {
	return m.token, m.err
}

type mockSignedTokenAuthenticationProvider struct {
	mockTokenAuthenticationProvider
	privateKey    []byte
	privateKeyErr error
	tokenSeen     *string
}

func (m mockSignedTokenAuthenticationProvider) PrivateKeyForToken(
	_ context.Context,
	token string,
) ([]byte, error) {
	if m.tokenSeen != nil {
		*m.tokenSeen = token
	}
	return m.privateKey, m.privateKeyErr
}

func encodePrivateKeyPEM(t *testing.T, privateKey *rsa.PrivateKey) []byte {
	t.Helper()

	encoded, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey failed: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
}

func newTestProviderRegistry(providersToRegister ...oracleProviders.Provider) common.ProviderRegistry {
	registry := common.NewProviderRegistry()
	for _, provider := range providersToRegister {
		registry.RegisterProvider(provider)
	}
	return registry
}

func TestGetAuthenticator_UsesTokenAuthenticatorForSignedToken(t *testing.T) {
	t.Parallel()

	cfg := oracleconfig.NewOracleDriverConfig()
	cfg.ConnectDescriptor = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=127.0.0.1)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=freepdb1)))"

	authenticator, err := GetAuthenticator(cfg, newTestProviderRegistry(
		mockSignedTokenAuthenticationProvider{
			mockTokenAuthenticationProvider: mockTokenAuthenticationProvider{token: "token-value"},
		},
	))
	if err != nil {
		t.Fatalf("GetAuthenticator returned error: %v", err)
	}
	if _, ok := authenticator.(*tokenAuthenticator); !ok {
		t.Fatalf("expected tokenAuthenticator, got %T", authenticator)
	}
}

func TestGetAuthenticator_UsesTokenAuthenticatorForOAuth(t *testing.T) {
	t.Parallel()

	cfg := oracleconfig.NewOracleDriverConfig()
	cfg.ConnectDescriptor = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=127.0.0.1)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=freepdb1)))"

	authenticator, err := GetAuthenticator(cfg, newTestProviderRegistry(
		mockTokenAuthenticationProvider{token: "token-value"},
	))
	if err != nil {
		t.Fatalf("GetAuthenticator returned error: %v", err)
	}
	if _, ok := authenticator.(*tokenAuthenticator); !ok {
		t.Fatalf("expected tokenAuthenticator, got %T", authenticator)
	}
}

func TestGetAuthenticator_AcceptsLegacyDummyPasswordForTokenProvider(t *testing.T) {
	t.Parallel()

	cfg := oracleconfig.NewOracleDriverConfig()
	cfg.Credentials.Password = "password"

	authenticator, err := GetAuthenticator(cfg, newTestProviderRegistry(
		mockTokenAuthenticationProvider{token: "token-value"},
	))
	if err != nil {
		t.Fatalf("GetAuthenticator returned error: %v", err)
	}
	if _, ok := authenticator.(*tokenAuthenticator); !ok {
		t.Fatalf("expected tokenAuthenticator, got %T", authenticator)
	}
}

func TestProviderRegistryReturnsFirstRegisteredTokenProvider(t *testing.T) {
	t.Parallel()

	registry := newTestProviderRegistry(
		struct{}{},
		mockTokenAuthenticationProvider{token: "first-token"},
		mockTokenAuthenticationProvider{token: "second-token"},
	)
	gotProvider, err := registry.Provider(reflect.TypeOf((*oracleProviders.TokenAuthenticationProvider)(nil)).Elem())
	if err != nil {
		t.Fatalf("GetProvider returned error: %v", err)
	}
	provider := gotProvider.(oracleProviders.TokenAuthenticationProvider)

	got, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("Token returned error: %v", err)
	}
	if got != "first-token" {
		t.Fatalf("Token = %q, want %q", got, "first-token")
	}
}

func TestOAuthSetTokenKeyValsForOAUTHAddsTokenHeaderAndSignature(t *testing.T) {
	t.Parallel()

	oauth := NewOAuth().(*oAuth)
	oauth.keyValList = NewKeyValueList()
	header := "date: Mon, 10 Aug 2026 10:00:00 GMT\n(request-target): freepdb1\nhost: 127.0.0.1:1521"
	signature := base64.StdEncoding.EncodeToString([]byte("signature"))
	if err := oauth.setTokenKeyValsForOAUTH("token-value", header, signature); err != nil {
		t.Fatalf("setTokenKeyValsForOAUTH returned error: %v", err)
	}

	if oauth.keyValList.Len() != 3 {
		t.Fatalf("expected 3 key/value pairs, got %d", oauth.keyValList.Len())
	}

	got := map[string]string{}
	for e := oauth.keyValList.Front(); e != nil; e = e.Next() {
		kv := e.Value.(*driverCommon.KeyValue)
		got[driverCommon.B1ArrayToString(kv.Key)] = driverCommon.B1ArrayToString(kv.Value)
	}

	if got[authToken] != "token-value" {
		t.Fatalf("AUTH_TOKEN = %q, want token-value", got[authToken])
	}
	if got[authHeader] != header {
		t.Fatalf("AUTH_HEADER = %q, want %q", got[authHeader], header)
	}
	if got[authSignature] == "" {
		t.Fatal("AUTH_SIGNATURE should not be empty")
	}
	if _, err := base64.StdEncoding.DecodeString(got[authSignature]); err != nil {
		t.Fatalf("AUTH_SIGNATURE is not valid base64: %v", err)
	}
}

func TestSignedTokenProviderGenerateTokenHeader(t *testing.T) {
	t.Parallel()

	sessContext := driverCommon.NewSessionContext()
	sessContext.GetClientProperties().SetProperty(driverCommon.RemoteAddress, "192.0.2.10")
	sessContext.GetClientProperties().SetProperty(driverCommon.RemotePort, 1522)
	sessContext.GetClientProperties().SetProperty(driverCommon.ConnectDescriptor, "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=adb.example.com)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=freepdb1)))")

	authenticator := &tokenAuthenticator{
		tokenProvider:  mockSignedTokenAuthenticationProvider{},
		sessionContext: sessContext,
	}

	header, err := authenticator.generateTokenHeader()
	if err != nil {
		t.Fatalf("generateTokenHeader returned error: %v", err)
	}
	if !strings.Contains(header, "(request-target): freepdb1") {
		t.Fatalf("header missing service name: %q", header)
	}
	if !strings.Contains(header, "host: 192.0.2.10:1522") {
		t.Fatalf("header missing remote ip:port: %q", header)
	}
	if !strings.Contains(header, " GMT\n(request-target): ") {
		t.Fatalf("header date should use GMT format: %q", header)
	}
}

func TestProviderRegistryReturnsErrorWhenTokenProviderMissing(t *testing.T) {
	t.Parallel()

	registry := newTestProviderRegistry(
		struct{}{},
		struct{}{},
	)
	if _, err := registry.Provider(reflect.TypeOf((*oracleProviders.TokenAuthenticationProvider)(nil)).Elem()); err == nil {
		t.Fatal("expected missing token provider error, got nil")
	}
}

func TestOAuthSetTokenKeyValsForOAUTHAddsTokenOnlyWithoutHeader(t *testing.T) {
	t.Parallel()

	oauth := NewOAuth().(*oAuth)
	oauth.keyValList = NewKeyValueList()

	if err := oauth.setTokenKeyValsForOAUTH("token-value", "", ""); err != nil {
		t.Fatalf("setTokenKeyValsForOAUTH returned error: %v", err)
	}

	if oauth.keyValList.Len() != 1 {
		t.Fatalf("expected 1 key/value pair, got %d", oauth.keyValList.Len())
	}

	kv := oauth.keyValList.Front().Value.(*driverCommon.KeyValue)
	if got := driverCommon.B1ArrayToString(kv.Key); got != authToken {
		t.Fatalf("unexpected key %q, want %q", got, authToken)
	}
	if got := driverCommon.B1ArrayToString(kv.Value); got != "token-value" {
		t.Fatalf("unexpected value %q, want %q", got, "token-value")
	}
}

func TestTokenAuthenticatorSignHeaderForSignedProvider(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	keyPEM := encodePrivateKeyPEM(t, privateKey)
	var tokenSeen string
	authenticator := &tokenAuthenticator{
		tokenProvider: mockSignedTokenAuthenticationProvider{
			privateKey: keyPEM,
			tokenSeen:  &tokenSeen,
		},
	}

	got, err := authenticator.signHeader(
		context.Background(),
		"date: Mon, 10 Aug 2026 10:00:00 GMT",
		"token-value",
	)
	if err != nil {
		t.Fatalf("signHeader returned error: %v", err)
	}
	if tokenSeen != "token-value" {
		t.Fatalf("PrivateKeyForToken token = %q, want token-value", tokenSeen)
	}
	if got == "" {
		t.Fatal("expected non-empty signature")
	}
	if _, err := base64.StdEncoding.DecodeString(got); err != nil {
		t.Fatalf("signature is not valid base64: %v", err)
	}
}

func TestTokenAuthenticatorSignHeaderForOAuthProviderReturnsEmpty(t *testing.T) {
	t.Parallel()

	authenticator := &tokenAuthenticator{
		tokenProvider: mockTokenAuthenticationProvider{token: "token-value"},
	}

	got, err := authenticator.signHeader(
		context.Background(),
		"date: Mon, 10 Aug 2026 10:00:00 GMT",
		"token-value",
	)
	if err != nil {
		t.Fatalf("signHeader returned error: %v", err)
	}
	if got != "" {
		t.Fatalf("signHeader = %q, want empty signature", got)
	}
}

func TestValidateJWTExpirationExpired(t *testing.T) {
	t.Parallel()

	token := "eyJhbGciOiJub25lIn0.eyJleHAiOjF9."
	err := validateJWTExpiration(token)
	if err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired token error, got %v", err)
	}
}
