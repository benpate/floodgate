package floodgate

import (
	"context"
	"iter"
	"math/rand/v2"
	"time"

	"github.com/benpate/derp"
)

// Gate decides whether deliveries may go to a remote host, and records how each one went
type Gate struct {
	store             Store
	failureThreshold  int
	cooldown          time.Duration
	cooldownMax       time.Duration
	dormantDays       int
	rateLimit         time.Duration
	rateLimitMax      time.Duration
	jitter            time.Duration
	probeInterval     time.Duration
	probeIntervalLate time.Duration
	probeEarlyCount   int
	now               func() time.Time
}

// New returns a Gate that keeps its records in the provided Store
func New(store Store, options ...GateOption) Gate {

	result := Gate{
		store:             store,
		failureThreshold:  defaultFailureThreshold,
		cooldown:          defaultCooldown,
		cooldownMax:       defaultCooldownMax,
		dormantDays:       defaultDormantDays,
		rateLimit:         defaultRateLimit,
		rateLimitMax:      defaultRateLimitMax,
		jitter:            defaultJitter,
		probeInterval:     defaultProbeInterval,
		probeIntervalLate: defaultProbeIntervalLate,
		probeEarlyCount:   defaultProbeEarlyCount,
		now:               time.Now,
	}

	for _, option := range options {
		option(&result)
	}

	return result
}

// Now returns the current time as this Gate reads it, which a test may have replaced
func (gate Gate) Now() time.Time {
	return gate.now()
}

// Blocked returns the hosts that nothing should be queued for, read from the Store once.
// A fan-out loads it at the start and checks every recipient against it in memory.
func (gate Gate) Blocked(ctx context.Context) (Blocklist, error) {

	hosts, err := gate.store.RangeBlocked(ctx)

	if err != nil {
		return nil, derp.Wrap(err, "floodgate.Gate.Blocked", "Loading blocked hosts")
	}

	result := make(Blocklist)

	for host := range hosts {
		result[host.Host] = struct{}{}
	}

	return result, nil
}

// Check decides what to do with one delivery to a host, at the moment it is about to be sent
func (gate Gate) Check(ctx context.Context, host string) (Decision, error) {

	record, err := gate.store.Load(ctx, host)

	if err != nil {
		return Decision{}, derp.Wrap(err, "floodgate.Gate.Check", "Loading host record")
	}

	// A host with no record has no name in it yet
	record.Host = host
	now := gate.now()

	// RULE: Nothing goes to a host that is dormant or that the operator has paused
	if record.IsBlocked() {
		return Decision{Action: ActionDrop, Host: record}, nil
	}

	// RULE: A host that asked for fewer requests is left alone until the time it named
	if record.IsRateLimited(now) {
		retryAt := time.Unix(record.RateLimitUntil, 0).UTC().Add(gate.randomJitter())
		return Decision{Action: ActionWait, RetryAt: retryAt, Host: record}, nil
	}

	// RULE: An unhealthy host fails fast, without connecting
	if record.State(now) == StateUnhealthy {
		retryAt := time.Unix(record.TestAfter, 0).UTC().Add(gate.randomJitter())
		return Decision{Action: ActionRetry, RetryAt: retryAt, Host: record}, nil
	}

	// A healthy host is sent to, and so is one that is testing. Several trials at once are fine.
	return Decision{Action: ActionSend, Host: record}, nil
}

// Success records that a delivery reached the host. It takes the record from the Decision,
// and writes nothing for a host with no history of trouble.
func (gate Gate) Success(ctx context.Context, record Host) error {

	// RULE: A healthy host costs no write per delivery
	if !record.hasHistory() {
		return nil
	}

	return gate.Revive(ctx, record.Host)
}

// Revive marks a host healthy and clears its history. Call it when a delivery or a check
// succeeds after trouble, or when the host sends a request of its own.
func (gate Gate) Revive(ctx context.Context, host string) error {

	if err := gate.store.RecordSuccess(ctx, host, gate.now()); err != nil {
		return derp.Wrap(err, "floodgate.Gate.Revive", "Recording success")
	}

	return nil
}

// Failure records that the host could not be reached or answered with a server error, and
// marks it unhealthy or dormant when that is due. Never call it for a rate limit or a refusal.
func (gate Gate) Failure(ctx context.Context, host string) error {

	const location = "floodgate.Gate.Failure"

	now := gate.now()
	record, err := gate.store.RecordFailure(ctx, host, now, now.UTC().Format(dateFormat))

	if err != nil {
		return derp.Wrap(err, location, "Recording failure")
	}

	if err := gate.markDormant(ctx, record, now); err != nil {
		return derp.Wrap(err, location, "Marking host dormant")
	}

	if err := gate.markUnhealthy(ctx, record, now); err != nil {
		return derp.Wrap(err, location, "Marking host unhealthy")
	}

	return nil
}

// markDormant marks a host dormant once it has failed on enough separate days
func (gate Gate) markDormant(ctx context.Context, record Host, now time.Time) error {

	if record.IsDormant() {
		return nil
	}

	if len(record.FailureDates) < gate.dormantDays {
		return nil
	}

	return gate.store.MarkDormant(ctx, record.Host, now, now.Add(gate.probeInterval))
}

// markUnhealthy marks a healthy host unhealthy when it reaches the failure threshold, or when a
// trial delivery fails. A host that is already unhealthy and cooling down is left alone.
func (gate Gate) markUnhealthy(ctx context.Context, record Host, now time.Time) error {

	switch record.State(now) {

	// Deliveries that were already in flight when the host became unhealthy do not extend the cooldown
	case StateUnhealthy:
		return nil

	// A failed trial makes the host unhealthy again, for twice as long
	case StateTesting:
		cooldown := gate.nextCooldown(record)
		return gate.store.MarkUnhealthy(ctx, record.Host, now.Add(cooldown), cooldown)
	}

	// Fall through means the host is healthy
	if record.FailureCount < gate.failureThreshold {
		return nil
	}

	return gate.store.MarkUnhealthy(ctx, record.Host, now.Add(gate.cooldown), gate.cooldown)
}

// nextCooldown returns twice the cooldown that the host last waited, within the
// shortest and longest cooldowns that this Gate allows
func (gate Gate) nextCooldown(record Host) time.Duration {

	// A damaged record could hold any number, and doubling one outside the limits could overflow
	if record.Cooldown <= 0 {
		return gate.cooldown
	}

	if record.Cooldown >= int64(gate.cooldownMax/time.Second) {
		return gate.cooldownMax
	}

	doubled := time.Duration(record.Cooldown) * time.Second * 2
	return min(max(doubled, gate.cooldown), gate.cooldownMax)
}

// RateLimited records that the host asked for fewer requests, and returns the time until which
// it is paused. A wait of zero or less means that the host named none.
func (gate Gate) RateLimited(ctx context.Context, host string, retryAfter time.Duration) (time.Time, error) {

	// RULE: A host that names no wait gets a short one, and no host can ask for a very long one
	if retryAfter <= 0 {
		retryAfter = gate.rateLimit
	}

	until := gate.now().Add(min(retryAfter, gate.rateLimitMax))

	// The Store keeps the later of this time and the one it already holds
	if err := gate.store.SetRateLimit(ctx, host, until); err != nil {
		return time.Time{}, derp.Wrap(err, "floodgate.Gate.RateLimited", "Recording rate limit")
	}

	return until, nil
}

// ProbesDue returns the dormant hosts that should be checked now
func (gate Gate) ProbesDue(ctx context.Context) (iter.Seq[Host], error) {

	hosts, err := gate.store.RangeProbesDue(ctx, gate.now())

	if err != nil {
		return nil, derp.Wrap(err, "floodgate.Gate.ProbesDue", "Loading hosts to check")
	}

	return hosts, nil
}

// ProbeFailed records that a check of a dormant host failed, and schedules the next one
func (gate Gate) ProbeFailed(ctx context.Context, record Host) error {

	// The record still shows the count before this check, so add it
	interval := gate.probeIntervalLate

	if record.ProbeCount+1 < gate.probeEarlyCount {
		interval = gate.probeInterval
	}

	if err := gate.store.RecordProbeFailure(ctx, record.Host, gate.now().Add(interval)); err != nil {
		return derp.Wrap(err, "floodgate.Gate.ProbeFailed", "Scheduling next check")
	}

	return nil
}

// ProbeSucceeded records that a check of a dormant host succeeded, along with the
// software that the host reported
func (gate Gate) ProbeSucceeded(ctx context.Context, host string, software string, version string) error {

	const location = "floodgate.Gate.ProbeSucceeded"

	if err := gate.Revive(ctx, host); err != nil {
		return derp.Wrap(err, location, "Reviving host")
	}

	if err := gate.store.SetNodeInfo(ctx, host, software, version); err != nil {
		return derp.Wrap(err, location, "Recording host software")
	}

	return nil
}

// SetPaused records whether the server operator has stopped deliveries to a host
func (gate Gate) SetPaused(ctx context.Context, host string, isPaused bool) error {

	if err := gate.store.SetPaused(ctx, host, isPaused); err != nil {
		return derp.Wrap(err, "floodgate.Gate.SetPaused", "Recording pause")
	}

	return nil
}

// SetSignatureFormat records the HTTP signature format that a host accepts
func (gate Gate) SetSignatureFormat(ctx context.Context, host string, format string) error {

	if err := gate.store.SetSignatureFormat(ctx, host, format); err != nil {
		return derp.Wrap(err, "floodgate.Gate.SetSignatureFormat", "Recording signature format")
	}

	return nil
}

// randomJitter returns a random delay, so that deliveries rescheduled for the same host do not
// all wake in the same instant
func (gate Gate) randomJitter() time.Duration {

	if gate.jitter <= 0 {
		return 0
	}

	// #nosec G404 -- spreading retries apart needs no unpredictability
	return time.Duration(rand.Int64N(int64(gate.jitter)))
}
