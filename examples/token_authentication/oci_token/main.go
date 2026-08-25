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

// Package main shows OCI IAM token authentication using a file-backed
// provider registered on an Oracle connector.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oracle/go-oracledb/v26/oracle"
	oracleProviders "github.com/oracle/go-oracledb/v26/oracle/providers"
)

const (
	tokenFileName         = "token"
	ociPrivateKeyFileName = "oci_db_key.pem"
)

// fileOAuthTokenProvider implements SignedTokenAuthenticationProvider interface.
type fileOCITokenProvider struct {
	tokenPath      string
	privateKeyPath string
}

// Token returns the token used for token authentication
func (p fileOCITokenProvider) Token(_ context.Context) (string, error) {
	return readTrimmedFile(p.tokenPath)
}

// PrivateKeyForToken return the private key associated to the token
func (p fileOCITokenProvider) PrivateKeyForToken(_ context.Context, token string) ([]byte, error) {
	keyPEM, err := os.ReadFile(p.privateKeyPath)
	if err != nil {
		return nil, err
	}
	return keyPEM, nil
}

func main() {
	connectDescriptor := requiredEnv("ORACLE_GO_OCI_TOKEN_CONNECT_DESCRIPTOR")
	tokenLocation := requiredEnv("ORACLE_GO_OCI_TOKEN_LOCATION")

	tokenPath := filepath.Join(tokenLocation, tokenFileName)
	privateKeyPath := filepath.Join(tokenLocation, ociPrivateKeyFileName)

	cfg := oracle.NewOracleDriverConfig()
	cfg.ConnectDescriptor = connectDescriptor

	connector, err := oracle.NewOracleConnector(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Check that the connector implements ProviderRegistrar
	registrar, ok := connector.(oracleProviders.ProviderRegistrar)
	if !ok {
		log.Fatal("connector does not support provider registration")
	}
	// register the provider, the provider methods will be called by
	// the driver during token-based authentication
	registrar.RegisterProvider(fileOCITokenProvider{
		tokenPath:      tokenPath,
		privateKeyPath: privateKeyPath,
	})

	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}

	var result string
	if err := db.QueryRowContext(ctx, "SELECT USER FROM SYS.DUAL").Scan(&result); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Username: %s\n", result)
}

func requiredEnv(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		log.Fatalf("missing required environment variable %s", name)
	}
	return value
}

func readTrimmedFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(content)), nil
}
