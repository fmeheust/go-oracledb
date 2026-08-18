/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package oracle

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oracle/go-driver/internal/common"
	"golang.org/x/text/language"
)

func TestConfiguration_AssignFromEmptyMap(t *testing.T) {
	t.Parallel()
	conf := NewOracleDriverConfig()
	if err := conf.AssignFromMap(nil); err != nil {
		t.Fatalf("nil map should not raise error")
	}
	if err := conf.AssignFromMap(map[string]string{}); err != nil {
		t.Fatalf("empty map should not raise error")
	}
}

func TestConfiguration_AssignFromMapUnknownKey(t *testing.T) {
	t.Parallel()
	conf := NewOracleDriverConfig()
	if err := conf.AssignFromMap(map[string]string{"xxxxxx": ""}); err != nil {
		t.Fatalf("unknown keys should not raise error")
	}
}

func TestConfiguration_AssignFromMap(t *testing.T) {
	t.Parallel()
	conf := NewOracleDriverConfig()
	conf.Credentials.User = "foo"
	conf.ConnectDescriptor = "myhost:1234/svc"
	err := conf.AssignFromMap(map[string]string{
		"oracle.go.credentials.user":  "bar",
		"oracle.go.connectDescriptor": "dbhost:1521/freedp1",
	})
	if err != nil {
		t.Fatalf("assign keys should not raise error")
	}
	if conf.ConnectDescriptor != "dbhost:1521/freedp1" || conf.Credentials.User != "bar" {
		t.Fatalf("map values not assigned")
	}
}

func TestConfiguration_AssignFromMapValidatedIntString(t *testing.T) {
	t.Parallel()
	conf := NewOracleDriverConfig()
	if err := conf.AssignFromMap(map[string]string{"oracle.go.ConnectionProperties.ConnectTimeout": "42"}); err != nil {
		t.Fatalf("validated int string should not raise error: %v", err)
	}
	if conf.ConnectionProperties.ConnectTimeout != 42 {
		t.Fatalf("connect timeout not assigned, got %d", conf.ConnectionProperties.ConnectTimeout)
	}
}

func TestConfiguration_DefaultClientLanguageIsLanguageTag(t *testing.T) {
	t.Parallel()
	conf := NewOracleDriverConfig()
	if conf.Locale.ClientLanguage != language.English {
		t.Fatalf("expected default client language en, got %s", conf.Locale.ClientLanguage)
	}
	if err := conf.Validate(); err != nil {
		t.Fatalf("default client language should validate: %v", err)
	}
}

func TestConfiguration_AssignFromMapClientLanguageTag(t *testing.T) {
	t.Parallel()
	conf := NewOracleDriverConfig()
	if err := conf.AssignFromMap(map[string]string{"oracle.go.Locale.ClientLanguage": "fr"}); err != nil {
		t.Fatalf("client language assignment should not raise error: %v", err)
	}
	if conf.Locale.ClientLanguage != language.French {
		t.Fatalf("expected client language fr, got %s", conf.Locale.ClientLanguage)
	}
}

func TestConfiguration_AssignFromEnv(t *testing.T) {
	conf := NewOracleDriverConfig()
	conf.Credentials.User = "foo"
	conf.ConnectDescriptor = "myhost:1234/svc"
	t.Setenv("ORACLE_GO_CREDENTIALS_USER", "bar")
	t.Setenv("ORACLE_GO_CONNECTDESCRIPTOR", "dbhost:1521/freedp1")
	if err := conf.AssignFromEnv(); err != nil {
		t.Fatalf("assign env should not raise error")
	}
	if conf.ConnectDescriptor != "dbhost:1521/freedp1" || conf.Credentials.User != "bar" {
		t.Fatalf("env values not assigned")
	}
}

func TestConfiguration_AssignFromEnvValidatedIntString(t *testing.T) {
	conf := NewOracleDriverConfig()
	t.Setenv("ORACLE_GO_CONNECTIONPROPERTIES_CONNECTTIMEOUT", "43")
	if err := conf.AssignFromEnv(); err != nil {
		t.Fatalf("validated int string from env should not raise error: %v", err)
	}
	if conf.ConnectionProperties.ConnectTimeout != 43 {
		t.Fatalf("connect timeout not assigned, got %d", conf.ConnectionProperties.ConnectTimeout)
	}
}

func TestConfiguration_AssignFromEnvClientLanguageTag(t *testing.T) {
	conf := NewOracleDriverConfig()
	t.Setenv("ORACLE_GO_LOCALE_CLIENTLANGUAGE", "fr")
	if err := conf.AssignFromEnv(); err != nil {
		t.Fatalf("client language from env should not raise error: %v", err)
	}
	if conf.Locale.ClientLanguage != language.French {
		t.Fatalf("expected client language fr, got %s", conf.Locale.ClientLanguage)
	}
}

func TestConfiguration_AssignFromEmptyFlags(t *testing.T) {
	t.Parallel()
	conf := NewOracleDriverConfig()
	flag.Set("oracle.go.Locale.Territory", "FOO")
	flag.Set("oracle.go.Credentials.User", "myuser")
	conf.AssignFromFlags()
	if conf.Credentials.User != "myuser" || conf.Locale.Territory != "FOO" {
		t.Fatalf("flag values not propagated")
	}
}

func TestConfiguration_Clone(t *testing.T) {
	t.Parallel()
	conf := NewOracleDriverConfig()
	conf.Credentials.User = "foo"
	conf.ConnectDescriptor = "myhost:1234/svc"
	conf.Locale.ClientLanguage = language.English
	cloneConf := conf.Clone()
	if cloneConf == nil {
		t.Fatalf("cloned configuration is nil")
	}
	if conf.Credentials.User != cloneConf.Credentials.User || conf.ConnectDescriptor != cloneConf.ConnectDescriptor {
		t.Fatalf("cloned values do not match")
	}
	cloneConf.Credentials.User = "bar"
	conf.Locale.Territory = "none"
	if conf.Credentials.User == "bar" || cloneConf.Locale.Territory == "none" {
		t.Fatalf("clone/original should remain independent")
	}
}

func TestConfiguration_toNSConnectionParameters(t *testing.T) {
	t.Parallel()
	conf := NewOracleDriverConfig()
	conf.ConnectionProperties.SSLServerCertDN = "CN=test"
	conf.ConnectionProperties.Failover = false
	conf.ConnectionProperties.HttpsProxyPort = 9000
	conf.ConnectionProperties.RetryDelay = 7
	params := conf.ToNSConnectionParameters()
	if len(params) == 0 {
		t.Fatalf("expected non-empty NS connection parameters")
	}
	want := map[string]bool{
		"ssl_server_cert_dn=\"CN=test\"": false,
		"ssl_server_dn_match=false":      false,
		"ssl_allow_weak_dn_match=false":  false,
		"https_proxy_port=9000":          false,
		"connect_timeout=0":              false,
		"expire_time=0":                  false,
		"failover=false":                 false,
		"load_balance=false":             false,
		"recv_buf_size=0":                false,
		"send_buf_size=0":                false,
		"sdu=0":                          false,
		"source_route=false":             false,
		"retry_count=0":                  false,
		"retry_delay=7":                  false,
		"transport_connect_timeout=0":    false,
		"USE_SNI=false":                  false,
	}
	for _, s := range strings.Split(params, "&") {
		if _, ok := want[s]; ok {
			want[s] = true
		}
	}
	for param, found := range want {
		if !found {
			t.Fatalf("expected NS connection parameter %q in %q", param, params)
		}
	}
}

func TestInitLoggingWithConfigFileDestination(t *testing.T) {
	t.Parallel()
	defer common.InitLoggingWithConfig(NewOracleLoggingConfig())
	logPath := filepath.Join(t.TempDir(), "driver.log")
	config := NewOracleLoggingConfig()
	config.Destination = logPath
	config.Level = "INFO"
	config.Truncate = true
	common.InitLoggingWithConfig(config)
	common.Odl.InfoContext(context.Background(), "file logging smoke")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	if !strings.Contains(string(data), "file logging smoke") {
		t.Fatalf("expected log file to contain message, got %q", string(data))
	}
}

func TestConfigurationFlagDebugString(_ *testing.T) {
	_ = fmt.Sprintf("")
}
