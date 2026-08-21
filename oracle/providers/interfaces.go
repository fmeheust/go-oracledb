package providers

import "context"

type Provider interface{}

/*** TOKEN AUTHENTICATI%ON ***/

type TokenAuthenticationProvider interface {
	Provider
	Token(context.Context) (string, error)
}

type OCITokenAuthenticationProvider interface {
	TokenAuthenticationProvider
	PrivateKey(context.Context) ([]byte, error)
}

/*** END OF TOKEN AUTHENTICATI%ON ***/
