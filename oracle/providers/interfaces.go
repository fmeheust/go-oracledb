package providers

import "context"

// ProviderRegistrar registers runtime providers used by the connector during
// connection establishment.
//
// RegisterProvider adds provider to the connector so it is available to future
// connection attempts.
type ProviderRegistrar interface {
	RegisterProvider(Provider)
}

// Provider is the marker interface implemented by connector-extensible runtime
// providers.
type Provider interface{}

/*** TOKEN AUTHENTICATI%ON ***/

// TokenAuthenticationProvider returns the token used for token-based database
// authentication flows.
type TokenAuthenticationProvider interface {
	Provider
	// Token returns the token string to authenticate with and any retrieval error.
	Token(context.Context) (string, error)
}

// OCITokenAuthenticationProvider extends TokenAuthenticationProvider with the
// private key material required for OCI IAM token signing.
type OCITokenAuthenticationProvider interface {
	TokenAuthenticationProvider
	// PrivateKey returns the PEM-encoded private key bytes used to sign OCI token
	// headers and any retrieval error.
	PrivateKey(context.Context) ([]byte, error)
}

/*** END OF TOKEN AUTHENTICATI%ON ***/
