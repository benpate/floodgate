package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/benpate/floodgate"
	"github.com/stretchr/testify/require"
)

// TestRun_Records verifies that a script's operations reach the Store, by the records it leaves
func TestRun_Records(t *testing.T) {

	script := []byte{
		opRecordFailure, 0, 0, // one.example fails
		opMarkUnhealthy, 0, 5, // and is unhealthy for five minutes
		opSetPaused, 1, 1, // two.example is paused
		opRecordFailure, 2, 0, // three.example fails
		opMarkDormant, 2, 1, // and is dormant, with a check due in an hour
		opAdvanceClock, 0, 2, // two hours pass
	}

	snapshot, err := Run(context.Background(), floodgate.NewMemoryStore(), script)
	require.NoError(t, err)

	start := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

	require.Len(t, snapshot.Hosts, 3)
	require.Equal(t, floodgate.Host{
		Host:         "one.example",
		IsUnhealthy:  true,
		TestAfter:    start.Add(5 * time.Minute).Unix(),
		Cooldown:     300,
		FailureCount: 1,
		FailureDates: []string{"2026-11-11"},
		LastFailure:  start.Unix(),
	}, snapshot.Hosts[0])
	require.True(t, snapshot.Hosts[1].IsPaused)
	require.Equal(t, start.Unix(), snapshot.Hosts[2].DormantSince)
	require.Equal(t, []string{"three.example", "two.example"}, snapshot.Blocked)
	require.Equal(t, []string{"three.example"}, snapshot.ProbesDue)
}

// TestRun_EmptyScript verifies that no script leaves an empty Store
func TestRun_EmptyScript(t *testing.T) {

	snapshot, err := Run(context.Background(), floodgate.NewMemoryStore(), []byte{})
	require.NoError(t, err)
	require.Equal(t, []floodgate.Host{{}, {}, {}}, snapshot.Hosts)
	require.Empty(t, snapshot.Blocked)
	require.Empty(t, snapshot.ProbesDue)
}

// TestRun_StopsAtMaxSteps verifies that the steps beyond the limit are ignored
func TestRun_StopsAtMaxSteps(t *testing.T) {

	script := make([]byte, 0, (MaxSteps+10)*stepSize)

	for range MaxSteps + 10 {
		script = append(script, opRecordFailure, 0, 0)
	}

	snapshot, err := Run(context.Background(), floodgate.NewMemoryStore(), script)
	require.NoError(t, err)
	require.Equal(t, MaxSteps, snapshot.Hosts[0].FailureCount)
}

// forgetfulStore is a Store whose RecordFailure returns a record that it did not store
type forgetfulStore struct {
	*floodgate.MemoryStore
}

// RecordFailure counts the failure, then reports a count that is one too high
func (store forgetfulStore) RecordFailure(ctx context.Context, host string, now time.Time, date string) (floodgate.Host, error) {
	record, err := store.MemoryStore.RecordFailure(ctx, host, now, date)
	record.FailureCount++
	return record, err
}

// TestRun_ReturnedRecordMustBeStored verifies that Run catches a RecordFailure whose answer is not what was stored
func TestRun_ReturnedRecordMustBeStored(t *testing.T) {

	store := forgetfulStore{MemoryStore: floodgate.NewMemoryStore()}

	_, err := Run(context.Background(), store, []byte{opRecordFailure, 0, 0})
	require.Error(t, err)
}

// brokenStore is a Store whose named method fails
type brokenStore struct {
	*floodgate.MemoryStore
	broken string
}

var errBroken = errors.New("the store is broken")

// Load fails when it is the broken method
func (store brokenStore) Load(ctx context.Context, host string) (floodgate.Host, error) {

	if store.broken == "Load" {
		return floodgate.Host{}, errBroken
	}

	return store.MemoryStore.Load(ctx, host)
}

// RecordSuccess fails when it is the broken method
func (store brokenStore) RecordSuccess(ctx context.Context, host string, now time.Time) error {

	if store.broken == "RecordSuccess" {
		return errBroken
	}

	return store.MemoryStore.RecordSuccess(ctx, host, now)
}

// TestRun_StoreErrors verifies that a failing Store fails the run, in a step and in the final read
func TestRun_StoreErrors(t *testing.T) {

	for _, broken := range []string{"Load", "RecordSuccess"} {
		t.Run(broken, func(t *testing.T) {
			store := brokenStore{MemoryStore: floodgate.NewMemoryStore(), broken: broken}
			_, err := Run(context.Background(), store, []byte{opRecordSuccess, 0, 0})
			require.ErrorIs(t, err, errBroken)
		})
	}
}

// FuzzRun_MemoryStore verifies that no script makes the reference Store fail, and that the same
// script always leaves the same records
func FuzzRun_MemoryStore(f *testing.F) {

	f.Add([]byte{})
	f.Add([]byte{opRecordFailure, 0, 0, opMarkUnhealthy, 0, 5, opRecordSuccess, 0, 0})
	f.Add([]byte{opMarkDormant, 1, 3, opMarkDormant, 1, 9, opRecordProbeFailure, 1, 200})
	f.Add([]byte{opSetRateLimit, 2, 250, opSetRateLimit, 2, 3, opAdvanceClock, 0, 255})
	f.Add([]byte{opSetPaused, 0, 1, opSetPaused, 0, 0, opSetNodeInfo, 0, 7, opSetSignatureFormat, 0, 2})

	f.Fuzz(func(t *testing.T, script []byte) {

		first, err := Run(context.Background(), floodgate.NewMemoryStore(), script)
		require.NoError(t, err)

		second, err := Run(context.Background(), floodgate.NewMemoryStore(), script)
		require.NoError(t, err)
		require.Equal(t, first, second)

		// Every host that is due for a check is dormant, and so is blocked
		require.Subset(t, first.Blocked, first.ProbesDue)
		require.Len(t, first.Hosts, len(Hosts()))
	})
}
