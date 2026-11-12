package floodgate

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testClock is a clock that a test moves by hand
type testClock struct {
	current time.Time
}

// now returns the clock's current time
func (clock *testClock) now() time.Time {
	return clock.current
}

// advance moves the clock forward
func (clock *testClock) advance(duration time.Duration) {
	clock.current = clock.current.Add(duration)
}

// newTestGate returns a Gate over an empty MemoryStore, with no jitter and a clock at noon UTC
func newTestGate(options ...GateOption) (Gate, *MemoryStore, *testClock) {

	store := NewMemoryStore()
	clock := &testClock{current: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	options = append([]GateOption{WithClock(clock.now), WithJitter(0)}, options...)

	return New(store, options...), store, clock
}

// fail reports the given number of failures for a host
func fail(t *testing.T, gate Gate, host string, count int) {

	t.Helper()

	for range count {
		require.NoError(t, gate.Failure(context.Background(), host))
	}
}

// check returns the Gate's decision for a host
func check(t *testing.T, gate Gate, host string) Decision {

	t.Helper()

	decision, err := gate.Check(context.Background(), host)
	require.NoError(t, err)

	return decision
}

const testHost = "remote.example"

// TestGate_UnknownHostSends verifies that a host with no record is healthy, and that its success writes nothing
func TestGate_UnknownHostSends(t *testing.T) {

	gate, store, _ := newTestGate()

	decision := check(t, gate, testHost)
	require.True(t, decision.CanSend())
	require.Equal(t, testHost, decision.Host.Host)

	require.NoError(t, gate.Success(context.Background(), decision.Host))
	require.Empty(t, store.hosts, "a healthy host must not cost a record")
}

// TestGate_UnhealthyAtThreshold verifies that a host stays healthy below the failure threshold
// and becomes unhealthy for the first cooldown when it is reached
func TestGate_UnhealthyAtThreshold(t *testing.T) {

	gate, _, clock := newTestGate()

	fail(t, gate, testHost, 4)
	require.True(t, check(t, gate, testHost).CanSend(), "four failures leave the host healthy")

	fail(t, gate, testHost, 1)
	decision := check(t, gate, testHost)
	require.Equal(t, ActionRetry, decision.Action)
	require.Equal(t, clock.current.Add(time.Minute), decision.RetryAt)
	require.Equal(t, StateUnhealthy, decision.Host.State(clock.current))
}

// TestGate_InFlightFailuresDoNotExtendCooldown verifies that failures reported while the
// host is unhealthy leave its test time alone
func TestGate_InFlightFailuresDoNotExtendCooldown(t *testing.T) {

	gate, _, clock := newTestGate()

	fail(t, gate, testHost, 5)
	testAt := check(t, gate, testHost).RetryAt

	clock.advance(30 * time.Second)
	fail(t, gate, testHost, 20)

	require.Equal(t, testAt, check(t, gate, testHost).RetryAt)
}

// TestGate_TestingThenHealthy verifies that a trial is let through after the cooldown, and that
// its success marks the host healthy and clears the history
func TestGate_TestingThenHealthy(t *testing.T) {

	gate, store, clock := newTestGate()

	fail(t, gate, testHost, 5)
	clock.advance(time.Minute)

	decision := check(t, gate, testHost)
	require.True(t, decision.CanSend(), "the cooldown has passed, so a trial goes through")
	require.Equal(t, StateTesting, decision.Host.State(clock.current))

	require.NoError(t, gate.Success(context.Background(), decision.Host))

	record := store.hosts[testHost]
	require.Equal(t, StateHealthy, record.State(clock.current))
	require.Zero(t, record.FailureCount)
	require.Empty(t, record.FailureDates)
	require.Equal(t, clock.current.Unix(), record.LastRevived)
}

// TestGate_FailedTrialDoublesCooldown verifies that each failed trial makes the host unhealthy for
// twice as long, up to the longest cooldown
func TestGate_FailedTrialDoublesCooldown(t *testing.T) {

	gate, store, clock := newTestGate()
	fail(t, gate, testHost, 5)

	for _, expected := range []time.Duration{2, 4, 8, 16, 32, 32, 32} {

		// Wait out the cooldown, then fail the trial
		clock.current = time.Unix(store.hosts[testHost].TestAfter, 0).UTC()
		fail(t, gate, testHost, 1)

		decision := check(t, gate, testHost)
		require.Equal(t, ActionRetry, decision.Action)
		require.Equal(t, clock.current.Add(expected*time.Minute), decision.RetryAt, "cooldown of %d minutes", expected)
	}
}

// TestGate_DormantAfterSevenDays verifies that a host failing on seven separate days is
// marked dormant, that its deliveries are dropped, and that a success before then resets the count
func TestGate_DormantAfterSevenDays(t *testing.T) {

	gate, store, clock := newTestGate()

	// Six days of failure, then a success, then six more: never seven in a row
	for range 6 {
		fail(t, gate, testHost, 1)
		clock.advance(24 * time.Hour)
	}

	require.NoError(t, gate.Revive(context.Background(), testHost))

	for range 6 {
		fail(t, gate, testHost, 1)
		clock.advance(24 * time.Hour)
	}

	require.False(t, store.hosts[testHost].IsDormant())

	// Several failures on one day count as one day
	fail(t, gate, testHost, 3)
	record := store.hosts[testHost]
	require.True(t, record.IsDormant(), "the seventh separate day marks the host dormant")
	require.Equal(t, clock.current.Unix(), record.DormantSince)
	require.Equal(t, clock.current.Add(7*24*time.Hour).Unix(), record.NextProbe)

	require.Equal(t, ActionDrop, check(t, gate, testHost).Action)

	blocked, err := gate.Blocked(context.Background())
	require.NoError(t, err)
	require.True(t, blocked.Contains(testHost))
	require.False(t, blocked.Contains("other.example"))
}

// TestGate_RateLimit verifies that a rate limit reschedules without counting as a failure, that
// it is bounded, and that a later report never shortens it
func TestGate_RateLimit(t *testing.T) {

	gate, store, clock := newTestGate()
	ctx := context.Background()

	until, err := gate.RateLimited(ctx, testHost, 10*time.Minute)
	require.NoError(t, err)
	require.Equal(t, clock.current.Add(10*time.Minute), until)

	decision := check(t, gate, testHost)
	require.Equal(t, ActionWait, decision.Action)
	require.Equal(t, until, decision.RetryAt)
	require.Zero(t, store.hosts[testHost].FailureCount, "a rate limit is not a failure")

	// A shorter wait keeps the pause that is already there
	_, err = gate.RateLimited(ctx, testHost, time.Minute)
	require.NoError(t, err)
	require.Equal(t, until, check(t, gate, testHost).RetryAt)

	// No wait named means a short one; a very long wait is capped
	until, err = gate.RateLimited(ctx, "quiet.example", 0)
	require.NoError(t, err)
	require.Equal(t, clock.current.Add(time.Minute), until)

	until, err = gate.RateLimited(ctx, "greedy.example", 48*time.Hour)
	require.NoError(t, err)
	require.Equal(t, clock.current.Add(time.Hour), until)

	// The pause ends by itself
	clock.advance(11 * time.Minute)
	require.True(t, check(t, gate, testHost).CanSend())

	// A rate-limited host is not blocked, so fan-out still queues for it
	blocked, err := gate.Blocked(ctx)
	require.NoError(t, err)
	require.False(t, blocked.Contains("greedy.example"))
}

// TestGate_Paused verifies that a host the operator paused is dropped and listed as blocked, until it is resumed
func TestGate_Paused(t *testing.T) {

	gate, _, _ := newTestGate()
	ctx := context.Background()

	require.NoError(t, gate.SetPaused(ctx, testHost, true))
	require.Equal(t, ActionDrop, check(t, gate, testHost).Action)

	blocked, err := gate.Blocked(ctx)
	require.NoError(t, err)
	require.True(t, blocked.Contains(testHost))

	require.NoError(t, gate.SetPaused(ctx, testHost, false))
	require.True(t, check(t, gate, testHost).CanSend())
}

// TestGate_ProbeSchedule verifies that a dormant host is checked weekly four times and then
// monthly, and that a successful check revives it
func TestGate_ProbeSchedule(t *testing.T) {

	gate, store, clock := newTestGate(WithDormantDays(1))
	ctx := context.Background()
	const day = 24 * time.Hour

	fail(t, gate, testHost, 1)
	require.True(t, store.hosts[testHost].IsDormant())

	// Nothing is due before the first week has passed
	due, err := gate.ProbesDue(ctx)
	require.NoError(t, err)
	require.Empty(t, slices.Collect(due))

	for check, expected := range []time.Duration{7 * day, 7 * day, 7 * day, 30 * day, 30 * day} {

		clock.current = time.Unix(store.hosts[testHost].NextProbe, 0).UTC()

		due, err := gate.ProbesDue(ctx)
		require.NoError(t, err)

		hosts := slices.Collect(due)
		require.Len(t, hosts, 1)

		require.NoError(t, gate.ProbeFailed(ctx, hosts[0]))
		require.Equal(t, clock.current.Add(expected).Unix(), store.hosts[testHost].NextProbe, "after check %d", check+1)
	}

	require.NoError(t, gate.ProbeSucceeded(ctx, testHost, "mastodon", "4.7.0"))

	record := store.hosts[testHost]
	require.False(t, record.IsDormant())
	require.Zero(t, record.ProbeCount)
	require.Equal(t, "mastodon", record.Software)
	require.Equal(t, "4.7.0", record.Version)
	require.True(t, check(t, gate, testHost).CanSend())
}

// TestGate_ReviveUnknownHost verifies that reviving a host with no record creates none
func TestGate_ReviveUnknownHost(t *testing.T) {

	gate, store, _ := newTestGate()

	require.NoError(t, gate.Revive(context.Background(), testHost))
	require.Empty(t, store.hosts)
}

// TestGate_Jitter verifies that a rescheduled delivery is spread within the jitter, and never before the pause ends
func TestGate_Jitter(t *testing.T) {

	gate, _, clock := newTestGate(WithJitter(time.Minute))

	until, err := gate.RateLimited(context.Background(), testHost, 10*time.Minute)
	require.NoError(t, err)

	distinct := map[time.Time]bool{}

	for range 50 {
		retryAt := check(t, gate, testHost).RetryAt
		require.False(t, retryAt.Before(until))
		require.True(t, retryAt.Before(until.Add(time.Minute)))
		distinct[retryAt] = true
	}

	require.Greater(t, len(distinct), 1, "fifty reschedules must not all land on one instant")
	require.Equal(t, clock.current.Add(10*time.Minute), until)
}

// TestGate_SignatureFormat verifies that the format a host accepts is stored and read back
func TestGate_SignatureFormat(t *testing.T) {

	gate, _, _ := newTestGate()

	require.NoError(t, gate.SetSignatureFormat(context.Background(), testHost, "rfc9421"))
	require.Equal(t, "rfc9421", check(t, gate, testHost).Host.SignatureFormat)
}

// TestGate_ConcurrentFailures verifies that failures reported at once are all counted
func TestGate_ConcurrentFailures(t *testing.T) {

	gate, store, _ := newTestGate(WithFailureThreshold(1000))

	var group sync.WaitGroup

	for range 100 {
		group.Go(func() {
			_ = gate.Failure(context.Background(), testHost)
		})
	}

	group.Wait()
	require.Equal(t, 100, store.hosts[testHost].FailureCount)
}

// brokenStore is a Store whose every call fails
type brokenStore struct{}

var errBroken = errors.New("store is down")

func (brokenStore) Load(context.Context, string) (Host, error) { return Host{}, errBroken }
func (brokenStore) RangeBlocked(context.Context) (iter.Seq[Host], error) {
	return nil, errBroken
}
func (brokenStore) RangeProbesDue(context.Context, time.Time) (iter.Seq[Host], error) {
	return nil, errBroken
}
func (brokenStore) RecordSuccess(context.Context, string, time.Time) error { return errBroken }
func (brokenStore) RecordFailure(context.Context, string, time.Time, string) (Host, error) {
	return Host{}, errBroken
}
func (brokenStore) MarkUnhealthy(context.Context, string, time.Time, time.Duration) error {
	return errBroken
}
func (brokenStore) MarkDormant(context.Context, string, time.Time, time.Time) error {
	return errBroken
}
func (brokenStore) RecordProbeFailure(context.Context, string, time.Time) error { return errBroken }
func (brokenStore) SetRateLimit(context.Context, string, time.Time) error       { return errBroken }
func (brokenStore) SetPaused(context.Context, string, bool) error               { return errBroken }
func (brokenStore) SetNodeInfo(context.Context, string, string, string) error   { return errBroken }
func (brokenStore) SetSignatureFormat(context.Context, string, string) error    { return errBroken }

// TestGate_StoreErrors verifies that every Gate method returns the Store's error, and that a
// failed Check allows nothing
func TestGate_StoreErrors(t *testing.T) {

	gate := New(brokenStore{})
	ctx := context.Background()

	decision, err := gate.Check(ctx, testHost)
	require.ErrorIs(t, err, errBroken)
	require.False(t, decision.CanSend())

	_, err = gate.Blocked(ctx)
	require.ErrorIs(t, err, errBroken)

	_, err = gate.ProbesDue(ctx)
	require.ErrorIs(t, err, errBroken)

	_, err = gate.RateLimited(ctx, testHost, time.Minute)
	require.ErrorIs(t, err, errBroken)

	require.ErrorIs(t, gate.Failure(ctx, testHost), errBroken)
	require.ErrorIs(t, gate.Revive(ctx, testHost), errBroken)
	require.ErrorIs(t, gate.Success(ctx, Host{Host: testHost, FailureCount: 1}), errBroken)
	require.ErrorIs(t, gate.ProbeFailed(ctx, Host{Host: testHost}), errBroken)
	require.ErrorIs(t, gate.ProbeSucceeded(ctx, testHost, "", ""), errBroken)
	require.ErrorIs(t, gate.SetPaused(ctx, testHost, true), errBroken)
	require.ErrorIs(t, gate.SetSignatureFormat(ctx, testHost, "x"), errBroken)
}
