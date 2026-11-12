package floodgate

import "time"

// StateHealthy means that deliveries to the host are allowed
const StateHealthy = "HEALTHY"

// StateUnhealthy means that deliveries to the host are held back until its cooldown passes
const StateUnhealthy = "UNHEALTHY"

// StateTesting means that the cooldown has passed, and trial deliveries are let through
const StateTesting = "TESTING"

// ActionSend means that the delivery may go ahead
const ActionSend = "SEND"

// ActionRetry means that the host is unhealthy, so the delivery fails fast and is retried later
const ActionRetry = "RETRY"

// ActionWait means that the host asked for fewer requests, so the delivery is rescheduled
const ActionWait = "WAIT"

// ActionDrop means that the host is dormant or paused, so the delivery is abandoned
const ActionDrop = "DROP"

// defaultFailureThreshold is the number of consecutive failures that makes a host unhealthy
const defaultFailureThreshold = 5

// defaultCooldown is how long a host stays unhealthy before its first test
const defaultCooldown = time.Minute

// defaultCooldownMax is the longest that a host stays unhealthy between tests
const defaultCooldownMax = 32 * time.Minute

// defaultDormantDays is the number of separate days of failure that marks a host dormant
const defaultDormantDays = 7

// defaultRateLimit is how long a host is paused when it asks for fewer requests without saying for how long
const defaultRateLimit = time.Minute

// defaultRateLimitMax is the longest pause that a host can ask for
const defaultRateLimitMax = time.Hour

// defaultJitter is the longest random delay added to a rescheduled delivery
const defaultJitter = time.Minute

// defaultProbeInterval is the wait between the first few checks of a dormant host
const defaultProbeInterval = 7 * 24 * time.Hour

// defaultProbeIntervalLate is the wait between checks once the first few have failed
const defaultProbeIntervalLate = 30 * 24 * time.Hour

// defaultProbeEarlyCount is the number of checks made at the shorter interval
const defaultProbeEarlyCount = 4

// dateFormat is how a day of failure is written in a Host record
const dateFormat = "2006-01-02"
