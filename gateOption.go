package floodgate

import "time"

// GateOption changes one setting of a Gate
type GateOption func(*Gate)

// WithFailureThreshold sets the number of consecutive failures that makes a host unhealthy
func WithFailureThreshold(count int) GateOption {
	return func(gate *Gate) {
		gate.failureThreshold = count
	}
}

// WithCooldown sets how long a host stays unhealthy before its first test, and the longest it
// waits after its cooldown has doubled
func WithCooldown(first time.Duration, longest time.Duration) GateOption {
	return func(gate *Gate) {
		gate.cooldown = first
		gate.cooldownMax = longest
	}
}

// WithDormantDays sets the number of separate days of failure that marks a host dormant
func WithDormantDays(days int) GateOption {
	return func(gate *Gate) {
		gate.dormantDays = days
	}
}

// WithRateLimit sets the pause used when a host names none, and the longest pause it can ask for
func WithRateLimit(fallback time.Duration, longest time.Duration) GateOption {
	return func(gate *Gate) {
		gate.rateLimit = fallback
		gate.rateLimitMax = longest
	}
}

// WithJitter sets the longest random delay added to a rescheduled delivery. Zero adds none.
func WithJitter(longest time.Duration) GateOption {
	return func(gate *Gate) {
		gate.jitter = longest
	}
}

// WithProbeSchedule sets the wait between the first `earlyCount` checks of a dormant
// host, and the wait between every check after those
func WithProbeSchedule(early time.Duration, earlyCount int, late time.Duration) GateOption {
	return func(gate *Gate) {
		gate.probeInterval = early
		gate.probeEarlyCount = earlyCount
		gate.probeIntervalLate = late
	}
}

// WithClock replaces the clock, so that a test can choose the time
func WithClock(now func() time.Time) GateOption {
	return func(gate *Gate) {
		gate.now = now
	}
}
