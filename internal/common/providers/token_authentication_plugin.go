package plugins

import (
	"errors"

	"github.com/oracle/go-oracledb/v26/oracle/config"
	"github.com/oracle/go-oracledb/v26/oracle/plugins"
)

// Since we only have two types for now, we can use a variable per type for now,
// if the number of possible types increases we should change it

// oAuthTokenPlugin token plugin for token aithentication type TokenAuthenticationOAuth
var oAuthTokenPlugin plugins.TokenAuthenticationPlugin = nil

// ociTokenPlugin token plugin for token aithentication type TokenAuthenticationOCI
var ociTokenPlugin plugins.OCITokenAuthenticationPlugin = nil

func RegisterTokenAuthenticationPlugin(tokenType config.TokenAuthenticationType, plugin plugins.TokenAuthenticationPlugin) error {
	if tokenType.IsValid() {
		if tokenType == config.TokenAuthenticationOCI {
			if ociPlugin, ok := plugin.(plugins.OCITokenAuthenticationPlugin); !ok {
				return errors.New("OCI_TOKEN plugin should implement OCITokenAuthenticationPlugin interface")
			} else {
				ociTokenPlugin = ociPlugin
			}
		} else {
			oAuthTokenPlugin = plugin
		}
		return nil
	}
	return errors.New("Invalid toke type")
}

func UnregisterTokenAuthenticationPlugin(tokenType config.TokenAuthenticationType) {
	switch tokenType {
	case config.TokenAuthenticationOAuth:
		oAuthTokenPlugin = nil
	case config.TokenAuthenticationOCI:
		ociTokenPlugin = nil
	}
}

func GetTokenPlugin(tokenType config.TokenAuthenticationType) plugins.TokenAuthenticationPlugin {
	switch tokenType {
	case config.TokenAuthenticationOAuth:
		return oAuthTokenPlugin
	case config.TokenAuthenticationOCI:
		return ociTokenPlugin
	default:
		return nil
	}
}
