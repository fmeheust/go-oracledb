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
