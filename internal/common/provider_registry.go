package common

import (
	"sync"

	"github.com/oracle/go-oracledb/v26/oracle/providers"
)

const maxProviders = 10

// ProviderRegistry stores connector runtime providers and exposes safe access
// to registered providers for future connection attempts.
type ProviderRegistry interface {
	// RegisterProvider appends provider to the registry.
	// When the registry already contains maxProviders entries, the oldest
	// registered provider is removed before provider is appended.
	//
	// Parameters:
	//   - provider: the provider to register.
	RegisterProvider(provider providers.Provider)
	// Providers returns a copy of the currently registered providers.
	//
	// Returns:
	//   - the registered providers in insertion order.
	Providers() []providers.Provider
}

// providerRegistry is the default thread-safe ProviderRegistry implementation.
type providerRegistry struct {
	providers             []providers.Provider
	providerRegistryMutex sync.RWMutex
}

// NewProviderRegistry creates an empty provider registry.
//
// Returns:
//   - the initialized provider registry.
func NewProviderRegistry() *providerRegistry {
	return &providerRegistry{}
}

// RegisterProvider appends provider to the registry.
// When the registry already contains maxProviders entries, the oldest
// registered provider is removed before provider is appended.
//
// Parameters:
//   - provider: the provider to register.
func (providerRegistry *providerRegistry) RegisterProvider(provider providers.Provider) {
	providerRegistry.providerRegistryMutex.Lock()
	defer providerRegistry.providerRegistryMutex.Unlock()
	if len(providerRegistry.providers) >= maxProviders {
		providerRegistry.providers = providerRegistry.providers[1:]
	}
	providerRegistry.providers = append(providerRegistry.providers, provider)
}

// Providers returns a copy of the currently registered providers.
//
// Returns:
//   - the registered providers in insertion order.
func (providerRegistry *providerRegistry) Providers() []providers.Provider {
	providerRegistry.providerRegistryMutex.RLock()
	defer providerRegistry.providerRegistryMutex.RUnlock()
	providerRegistryCopy := make([]providers.Provider, len(providerRegistry.providers))
	copy(providerRegistryCopy, providerRegistry.providers)
	return providerRegistryCopy
}
