/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

// Package main demonstrates connecting to Oracle using OAuth token
// authentication and verifying the session with SELECT USER FROM SYS.DUAL.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/oracle/go-oracledb/v26/oracle"
	"github.com/oracle/go-oracledb/v26/oracle/config"
	oracleProviders "github.com/oracle/go-oracledb/v26/oracle/providers"
)

type fileOAuthTokenProvider struct {
	tokenPath string
}

func (p fileOAuthTokenProvider) Token(context.Context) (string, error) {
	return readTrimmedFile(p.tokenPath)
}

func main() {
	connectDescriptor := requiredEnv("ORACLE_GO_OAUTH_CONNECT_DESCRIPTOR")
	tokenPath := requiredEnv("ORACLE_GO_OAUTH_TOKEN_FILE")

	cfg := oracle.NewOracleDriverConfig()
	cfg.ConnectDescriptor = connectDescriptor
	cfg.Credentials.TokenAuthentication = config.TokenAuthenticationOAuth

	connector, err := oracle.NewOracleConnector(cfg)
	if err != nil {
		log.Fatal(err)
	}
	registrar, ok := connector.(oracleProviders.ProviderRegistrar)
	if !ok {
		log.Fatal("connector does not support provider registration")
	}
	registrar.RegisterProvider(fileOAuthTokenProvider{tokenPath: tokenPath})

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
