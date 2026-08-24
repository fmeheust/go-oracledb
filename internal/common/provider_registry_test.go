package common

import (
	"testing"

	oracleProviders "github.com/oracle/go-oracledb/v26/oracle/providers"
)

type mockProviderRegistryProvider struct {
	name string
}

// TestProviderRegistryRegisterProviderPreservesInsertionOrder verifies that
// providers are returned in the same order they were registered.
func TestProviderRegistryRegisterProviderPreservesInsertionOrder(t *testing.T) {
	t.Parallel()

	registry := NewProviderRegistry()
	first := mockProviderRegistryProvider{name: "first"}
	second := mockProviderRegistryProvider{name: "second"}

	registry.RegisterProvider(first)
	registry.RegisterProvider(second)

	got := registry.Providers()
	if len(got) != 2 {
		t.Fatalf("expected 2 providers, got %d", len(got))
	}
	if provider, ok := got[0].(mockProviderRegistryProvider); !ok || provider.name != "first" {
		t.Fatalf("unexpected first provider: got %#v", got[0])
	}
	if provider, ok := got[1].(mockProviderRegistryProvider); !ok || provider.name != "second" {
		t.Fatalf("unexpected second provider: got %#v", got[1])
	}
}

// TestProviderRegistryRegisterProviderEvictsOldestWhenCapacityExceeded
// verifies that registering more than maxProviders providers removes the
// oldest provider and preserves the order of the remaining providers.
func TestProviderRegistryRegisterProviderEvictsOldestWhenCapacityExceeded(t *testing.T) {
	t.Parallel()

	registry := NewProviderRegistry()
	for i := 0; i < maxProviders; i++ {
		registry.RegisterProvider(mockProviderRegistryProvider{name: string(rune('a' + i))})
	}
	registry.RegisterProvider(mockProviderRegistryProvider{name: "overflow"})

	got := registry.Providers()
	if len(got) != maxProviders {
		t.Fatalf("expected %d providers after overflow, got %d", maxProviders, len(got))
	}
	if provider, ok := got[0].(mockProviderRegistryProvider); !ok || provider.name != "b" {
		t.Fatalf("expected oldest provider to be evicted, got first provider %#v", got[0])
	}
	if provider, ok := got[len(got)-1].(mockProviderRegistryProvider); !ok || provider.name != "overflow" {
		t.Fatalf("expected newest provider at the end, got %#v", got[len(got)-1])
	}
}

// TestProviderRegistryProvidersReturnsCopy verifies that Providers returns a
// defensive copy, so callers cannot mutate the registry's internal slice.
func TestProviderRegistryProvidersReturnsCopy(t *testing.T) {
	t.Parallel()

	registry := NewProviderRegistry()
	original := mockProviderRegistryProvider{name: "original"}
	registry.RegisterProvider(original)

	snapshot := registry.Providers()
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 provider in snapshot, got %d", len(snapshot))
	}

	snapshot[0] = mockProviderRegistryProvider{name: "mutated"}
	got := registry.Providers()
	if len(got) != 1 {
		t.Fatalf("expected 1 provider after snapshot mutation, got %d", len(got))
	}
	if provider, ok := got[0].(mockProviderRegistryProvider); !ok || provider.name != "original" {
		t.Fatalf("expected registry to remain unchanged, got %#v", got[0])
	}
}

// TestProviderRegistryProvidersEmptyWhenUninitialized verifies that a newly
// created registry reports no providers and returns an empty slice.
func TestProviderRegistryProvidersEmptyWhenUninitialized(t *testing.T) {
	t.Parallel()

	registry := NewProviderRegistry()

	got := registry.Providers()
	if len(got) != 0 {
		t.Fatalf("expected no providers, got %d", len(got))
	}
	if got == nil {
		t.Fatal("expected Providers to return an empty slice copy, got nil")
	}
}

var _ oracleProviders.Provider = mockProviderRegistryProvider{}
