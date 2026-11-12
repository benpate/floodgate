package floodgate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMemoryStore_UnknownHost verifies which operations create a record for a host that has
// none, so that the reference Store matches the contract that a database-backed Store follows
func TestMemoryStore_UnknownHost(t *testing.T) {

	ctx := context.Background()
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

	// These record trouble for a host that already has a record, and must not create one
	writesNothing := map[string]func(*MemoryStore) error{
		"RecordSuccess": func(store *MemoryStore) error {
			return store.RecordSuccess(ctx, testHost, now)
		},
		"MarkUnhealthy": func(store *MemoryStore) error {
			return store.MarkUnhealthy(ctx, testHost, now.Add(time.Minute), time.Minute)
		},
		"MarkDormant": func(store *MemoryStore) error {
			return store.MarkDormant(ctx, testHost, now, now.Add(time.Hour))
		},
		"RecordProbeFailure": func(store *MemoryStore) error {
			return store.RecordProbeFailure(ctx, testHost, now.Add(time.Hour))
		},
	}

	for name, operation := range writesNothing {
		t.Run(name+" writes nothing", func(t *testing.T) {
			store := NewMemoryStore()
			require.NoError(t, operation(store))
			require.Empty(t, store.hosts)
		})
	}

	// These are the first thing known about a host, and must create its record
	creates := map[string]func(*MemoryStore) error{
		"RecordFailure": func(store *MemoryStore) error {
			_, err := store.RecordFailure(ctx, testHost, now, "2026-11-11")
			return err
		},
		"SetRateLimit": func(store *MemoryStore) error {
			return store.SetRateLimit(ctx, testHost, now.Add(time.Minute))
		},
		"SetPaused": func(store *MemoryStore) error {
			return store.SetPaused(ctx, testHost, true)
		},
		"SetNodeInfo": func(store *MemoryStore) error {
			return store.SetNodeInfo(ctx, testHost, "software", "1.0")
		},
		"SetSignatureFormat": func(store *MemoryStore) error {
			return store.SetSignatureFormat(ctx, testHost, "rfc9421")
		},
	}

	for name, operation := range creates {
		t.Run(name+" creates the record", func(t *testing.T) {
			store := NewMemoryStore()
			require.NoError(t, operation(store))
			require.Equal(t, testHost, store.hosts[testHost].Host)
		})
	}
}
