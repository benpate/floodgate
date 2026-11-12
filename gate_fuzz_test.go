package floodgate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The operations that a fuzz script can hold. A step's first byte selects one, modulo fuzzOpCount.
const (
	fuzzOpFailure        = iota // A delivery that was already running reports a failure
	fuzzOpDeliverOK             // A delivery is checked, sent, and succeeds
	fuzzOpDeliverFail           // A delivery is checked, sent, and fails
	fuzzOpSeconds               // The clock moves by the step's value, in seconds
	fuzzOpMinutes               // The clock moves by the step's value, in minutes
	fuzzOpHours                 // The clock moves by the step's value, in hours
	fuzzOpRateLimit             // The host asks for fewer requests
	fuzzOpPause                 // The operator stops deliveries
	fuzzOpResume                // The operator allows deliveries again
	fuzzOpProbeFailed           // A check of the dormant host fails
	fuzzOpProbeSucceeded        // A check of the host succeeds
	fuzzOpRevive                // The host sends a request of its own
	fuzzOpCount
)

// fuzzMaxSteps is the largest number of steps that a fuzz script runs
const fuzzMaxSteps = 256

// fuzzBystander is a host that no script touches
const fuzzBystander = "bystander.example"

// validActions is every answer that a Decision can carry
var validActions = []string{ActionSend, ActionRetry, ActionWait, ActionDrop}

// fuzzRun is one Gate, and what a script has done to it so far
type fuzzRun struct {
	gate           Gate
	store          *MemoryStore
	clock          *testClock
	isPaused       bool  // TRUE if the script's last pause was not followed by a resume
	rateLimitUntil int64 // The latest rate-limit pause seen in the host's record
}

// newFuzzRun returns a run over an empty MemoryStore
func newFuzzRun(options ...GateOption) *fuzzRun {
	gate, store, clock := newTestGate(options...)
	return &fuzzRun{gate: gate, store: store, clock: clock}
}

// script applies each two-byte step of a script, and calls the inspection after every one
func (run *fuzzRun) script(t *testing.T, script []byte, inspect func(*testing.T)) {

	t.Helper()

	for step := range min(len(script)/2, fuzzMaxSteps) {
		run.step(t, script[step*2], script[step*2+1])
		inspect(t)
	}
}

// step applies one operation to the Gate
func (run *fuzzRun) step(t *testing.T, operation byte, value byte) {

	t.Helper()

	switch ctx := context.Background(); operation % fuzzOpCount {

	case fuzzOpFailure:
		require.NoError(t, run.gate.Failure(ctx, testHost))

	case fuzzOpDeliverOK:
		run.deliverOK(t)

	case fuzzOpDeliverFail:
		if check(t, run.gate, testHost).CanSend() {
			require.NoError(t, run.gate.Failure(ctx, testHost))
		}

	case fuzzOpSeconds:
		run.clock.advance(time.Duration(value) * time.Second)

	case fuzzOpMinutes:
		run.clock.advance(time.Duration(value) * time.Minute)

	case fuzzOpHours:
		run.clock.advance(time.Duration(value) * time.Hour)

	// Values below 32 are a host that names no wait, or names one in the past
	case fuzzOpRateLimit:
		_, err := run.gate.RateLimited(ctx, testHost, time.Duration(int(value)-32)*time.Minute)
		require.NoError(t, err)

	case fuzzOpPause:
		require.NoError(t, run.gate.SetPaused(ctx, testHost, true))
		run.isPaused = true

	case fuzzOpResume:
		require.NoError(t, run.gate.SetPaused(ctx, testHost, false))
		run.isPaused = false

	case fuzzOpProbeFailed:
		run.probeFailed(t)

	case fuzzOpProbeSucceeded:
		require.NoError(t, run.gate.ProbeSucceeded(ctx, testHost, "software", "1.0"))
		require.False(t, run.record(t).hasHistory(), "a successful check clears the history")

	case fuzzOpRevive:
		require.NoError(t, run.gate.Revive(ctx, testHost))
		require.False(t, run.record(t).hasHistory(), "a revived host has no history")
	}
}

// deliverOK sends one delivery if the Gate allows it, and reports its success
func (run *fuzzRun) deliverOK(t *testing.T) {

	t.Helper()

	decision := check(t, run.gate, testHost)

	if !decision.CanSend() {
		return
	}

	before, existed := run.store.hosts[testHost]
	require.NoError(t, run.gate.Success(context.Background(), decision.Host))
	after, exists := run.store.hosts[testHost]

	// RULE: A success after trouble clears the history
	if decision.Host.hasHistory() {
		require.False(t, after.hasHistory(), "a success clears the history")
		return
	}

	// RULE: A success for a host with no history writes nothing
	require.Equal(t, existed, exists, "a healthy host's success must not create a record")
	require.Equal(t, before, after, "a healthy host's success must not change its record")
}

// probeFailed reports a failed check, as the daily task does for a dormant host
func (run *fuzzRun) probeFailed(t *testing.T) {

	t.Helper()

	before := run.record(t)

	if !before.IsDormant() {
		return
	}

	require.NoError(t, run.gate.ProbeFailed(context.Background(), before))
	require.Equal(t, before.ProbeCount+1, run.record(t).ProbeCount)
}

// record returns the host's record as the Store holds it
func (run *fuzzRun) record(t *testing.T) Host {

	t.Helper()

	record, err := run.store.Load(context.Background(), testHost)
	require.NoError(t, err)

	return record
}

// requireValidDecision verifies the least that every Gate must do: answer, with a known action
func (run *fuzzRun) requireValidDecision(t *testing.T) {

	t.Helper()

	decision := check(t, run.gate, testHost)
	require.Contains(t, validActions, decision.Action)
	require.Equal(t, testHost, decision.Host.Host)
}

// requireInvariants verifies everything that must hold after any step, for a Gate with the default settings
func (run *fuzzRun) requireInvariants(t *testing.T) {

	t.Helper()

	now := run.clock.now()
	record := run.record(t)
	decision := check(t, run.gate, testHost)

	require.Contains(t, validActions, decision.Action)
	require.Equal(t, run.isPaused, record.IsPaused, "only the operator pauses or resumes a host")

	// RULE: A paused or dormant host gets nothing
	if run.isPaused || record.IsDormant() {
		require.Equal(t, ActionDrop, decision.Action)
	}

	// RULE: A delivery that is held back is never rescheduled into the past
	if (decision.Action == ActionWait) || (decision.Action == ActionRetry) {
		require.True(t, decision.RetryAt.After(now), "retry at %s is not after %s", decision.RetryAt, now)
	}

	// RULE: Nothing is sent to a host that is rate-limited or cooling down
	if decision.CanSend() {
		require.False(t, record.IsRateLimited(now))
		require.NotEqual(t, StateUnhealthy, record.State(now))
	}

	// RULE: A host with no trouble on record is sent to
	if !record.hasHistory() && !run.isPaused && !record.IsRateLimited(now) {
		require.True(t, decision.CanSend())
	}

	// RULE: An unhealthy host earned it, and its cooldown stays within the Gate's limits
	if record.IsUnhealthy {
		cooldown := time.Duration(record.Cooldown) * time.Second
		require.GreaterOrEqual(t, cooldown, run.gate.cooldown)
		require.LessOrEqual(t, cooldown, run.gate.cooldownMax)
		require.LessOrEqual(t, record.TestAfter, now.Add(run.gate.cooldownMax).Unix())
		require.GreaterOrEqual(t, record.FailureCount, run.gate.failureThreshold)
	}

	// RULE: A dormant host failed on enough separate days
	if record.IsDormant() {
		require.GreaterOrEqual(t, len(record.FailureDates), run.gate.dormantDays)
		require.Greater(t, record.NextProbe, record.DormantSince)
	}

	// RULE: A rate-limit pause never moves earlier, and never runs past the longest pause allowed
	require.GreaterOrEqual(t, record.RateLimitUntil, run.rateLimitUntil)
	require.LessOrEqual(t, record.RateLimitUntil, now.Add(run.gate.rateLimitMax).Unix())
	run.rateLimitUntil = record.RateLimitUntil

	// Every failure date came from a counted failure
	require.GreaterOrEqual(t, record.FailureCount, len(record.FailureDates))

	// RULE: One host's trouble never touches another host
	bystander, err := run.store.Load(context.Background(), fuzzBystander)
	require.NoError(t, err)
	require.Equal(t, Host{}, bystander)
	require.True(t, check(t, run.gate, fuzzBystander).CanSend())
}

// fuzzSteps builds a script that repeats one group of steps
func fuzzSteps(count int, steps ...byte) []byte {

	result := make([]byte, 0, count*len(steps))

	for range count {
		result = append(result, steps...)
	}

	return result
}

// addScriptSeeds seeds a fuzz test with the scripts that the unit tests walk through by hand
func addScriptSeeds(add func(script []byte)) {

	// Nothing happens
	add([]byte{})

	// Five failures, a cooldown, and a trial that succeeds
	add(append(fuzzSteps(5, fuzzOpFailure, 0), fuzzOpMinutes, 1, fuzzOpDeliverOK, 0))

	// Failed trials double the cooldown until it stops growing
	add(append(fuzzSteps(5, fuzzOpFailure, 0), fuzzSteps(8, fuzzOpMinutes, 40, fuzzOpDeliverFail, 0)...))

	// A failure a day until the host is dormant, then failed checks, then a successful one
	dormant := fuzzSteps(8, fuzzOpFailure, 0, fuzzOpHours, 24)
	dormant = append(dormant, fuzzSteps(6, fuzzOpHours, 200, fuzzOpProbeFailed, 0)...)
	add(append(dormant, fuzzOpProbeSucceeded, 0, fuzzOpDeliverOK, 0))

	// A long rate limit, a shorter one, one with no wait named, and a delivery after each
	add([]byte{fuzzOpRateLimit, 255, fuzzOpDeliverOK, 0, fuzzOpRateLimit, 40, fuzzOpRateLimit, 0, fuzzOpHours, 2, fuzzOpDeliverOK, 0})

	// A rate limit that arrives while the host is being tested
	add(append(fuzzSteps(5, fuzzOpFailure, 0), fuzzOpMinutes, 1, fuzzOpRateLimit, 60, fuzzOpDeliverFail, 0, fuzzOpMinutes, 30, fuzzOpDeliverFail, 0))

	// The operator pauses a troubled host, the host revives itself, and the operator resumes it
	add(append(fuzzSteps(5, fuzzOpFailure, 0), fuzzOpPause, 0, fuzzOpRevive, 0, fuzzOpDeliverOK, 0, fuzzOpResume, 0, fuzzOpDeliverOK, 0))
}

// FuzzGate_Script runs scripts of deliveries, failures, pauses, checks, and passing time against
// a Gate with the default settings, and verifies after every step that its rules still hold
func FuzzGate_Script(f *testing.F) {

	addScriptSeeds(func(script []byte) { f.Add(script) })

	f.Fuzz(func(t *testing.T, script []byte) {
		run := newFuzzRun()
		run.script(t, script, run.requireInvariants)
	})
}

// FuzzGate_Options runs the same scripts against Gates with arbitrary settings, including zero,
// negative, and reversed ones, and verifies that the Gate still answers without a panic or an error
func FuzzGate_Options(f *testing.F) {

	addScriptSeeds(func(script []byte) {
		f.Add(5, int64(time.Minute), int64(32*time.Minute), 7, int64(time.Minute), int64(time.Hour), int64(time.Minute), int64(time.Hour), 4, int64(time.Hour), script)
		f.Add(0, int64(0), int64(0), 0, int64(0), int64(0), int64(0), int64(0), 0, int64(0), script)
		f.Add(-1, int64(time.Hour), int64(time.Second), -1, int64(-time.Minute), int64(-time.Hour), int64(-1), int64(-time.Hour), -1, int64(-1), script)
	})

	f.Fuzz(func(t *testing.T, failureThreshold int, cooldown int64, cooldownMax int64, dormantDays int, rateLimit int64, rateLimitMax int64, jitter int64, probeInterval int64, probeEarlyCount int, probeIntervalLate int64, script []byte) {

		run := newFuzzRun(
			WithFailureThreshold(failureThreshold),
			WithCooldown(time.Duration(cooldown), time.Duration(cooldownMax)),
			WithDormantDays(dormantDays),
			WithRateLimit(time.Duration(rateLimit), time.Duration(rateLimitMax)),
			WithJitter(time.Duration(jitter)),
			WithProbeSchedule(time.Duration(probeInterval), probeEarlyCount, time.Duration(probeIntervalLate)),
		)

		run.script(t, script, run.requireValidDecision)
	})
}

// expectedNextCooldown returns the cooldown, in seconds, that follows a failed trial under the
// default settings: twice the last one, and never outside the shortest and longest allowed
func expectedNextCooldown(previous int64) int64 {

	shortest := int64(defaultCooldown / time.Second)
	longest := int64(defaultCooldownMax / time.Second)

	// A cooldown that makes no sense starts over
	if previous <= 0 {
		return shortest
	}

	// Doubling anything this large reaches the limit, and must not overflow on the way
	if previous >= longest {
		return longest
	}

	return min(max(previous*2, shortest), longest)
}

// FuzzGate_ArbitraryRecord gives the Gate a record holding any values at all, as a damaged or
// hand-edited database could, and verifies that it still decides and still records a failure
func FuzzGate_ArbitraryRecord(f *testing.F) {

	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC).Unix()

	f.Add(false, int64(0), int64(0), int64(0), int64(0), false, 0, now)
	f.Add(true, now+60, int64(60), int64(0), int64(0), false, 5, now)
	f.Add(true, now-60, int64(960), int64(0), int64(0), false, 9, now)
	f.Add(true, now-60, int64(1<<62), int64(0), int64(0), false, 9, now)
	f.Add(true, now-60, int64(-1<<62), int64(0), int64(0), false, 9, now)
	f.Add(false, int64(0), int64(0), now, now+3600, true, 1<<62, now)
	f.Add(true, int64(1<<62), int64(1<<62), int64(-1), int64(1<<62), false, -1, int64(-1<<40))

	f.Fuzz(func(t *testing.T, isUnhealthy bool, testAfter int64, cooldown int64, dormantSince int64, rateLimitUntil int64, isPaused bool, failureCount int, nowUnix int64) {

		ctx := context.Background()
		gate, store, clock := newTestGate()
		clock.current = time.Unix(nowUnix, 0).UTC()

		record := Host{
			Host:           testHost,
			IsUnhealthy:    isUnhealthy,
			TestAfter:      testAfter,
			Cooldown:       cooldown,
			DormantSince:   dormantSince,
			RateLimitUntil: rateLimitUntil,
			IsPaused:       isPaused,
			FailureCount:   failureCount,
		}

		store.hosts[testHost] = record
		stateBefore := record.State(clock.now())

		// The Gate decides, whatever the record holds
		decision := check(t, gate, testHost)
		require.Contains(t, validActions, decision.Action)

		// RULE: A paused or dormant host gets nothing, whatever else its record says
		if isPaused || (dormantSince > 0) {
			require.Equal(t, ActionDrop, decision.Action)
		}

		// RULE: Nothing is sent to a host that is rate-limited or cooling down
		if decision.CanSend() {
			require.LessOrEqual(t, rateLimitUntil, nowUnix)
			require.NotEqual(t, StateUnhealthy, stateBefore)
		}

		// The Gate records a failure, whatever the record holds
		require.NoError(t, gate.Failure(ctx, testHost))

		after, err := store.Load(ctx, testHost)
		require.NoError(t, err)

		switch stateBefore {

		// RULE: A failure while cooling down leaves the cooldown alone
		case StateUnhealthy:
			require.Equal(t, cooldown, after.Cooldown)
			require.Equal(t, testAfter, after.TestAfter)

		// RULE: A failed trial doubles the cooldown, within the Gate's limits
		case StateTesting:
			require.True(t, after.IsUnhealthy)
			require.Equal(t, expectedNextCooldown(cooldown), after.Cooldown)

		// RULE: A healthy host becomes unhealthy at the threshold, for the first cooldown
		case StateHealthy:
			require.Equal(t, after.FailureCount >= defaultFailureThreshold, after.IsUnhealthy)
		}
	})
}
