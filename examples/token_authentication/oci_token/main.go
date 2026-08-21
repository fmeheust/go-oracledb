/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

// Package main demonstrates connecting to Oracle using OCI IAM token
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

type providerRegistrar interface {
	RegisterProvider(oracleProviders.Provider)
}

type fileOCITokenProvider struct {
	tokenPath      string
	privateKeyPath string
}

func (p fileOCITokenProvider) Token(context.Context) (string, error) {
	return readTrimmedFile(p.tokenPath)
}

func (p fileOCITokenProvider) PrivateKey(context.Context) ([]byte, error) {
	keyPEM, err := os.ReadFile(p.privateKeyPath)
	if err != nil {
		return nil, err
	}
	return keyPEM, nil
}

func main() {
	connectDescriptor := requiredEnv("ORACLE_GO_OCI_TOKEN_CONNECT_DESCRIPTOR")
	tokenPath := requiredEnv("ORACLE_GO_OCI_TOKEN_FILE")
	privateKeyPath := requiredEnv("ORACLE_GO_OCI_PRIVATE_KEY_FILE")

	cfg := oracle.NewOracleDriverConfig()
	cfg.ConnectDescriptor = connectDescriptor
	cfg.Credentials.TokenAuthentication = config.TokenAuthenticationOCI

	connector, err := oracle.NewOracleConnector(cfg)
	if err != nil {
		log.Fatal(err)
	}
	registrar, ok := connector.(providerRegistrar)
	if !ok {
		log.Fatal("connector does not support provider registration")
	}
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
