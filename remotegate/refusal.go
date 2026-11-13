package remotegate

import (
	"time"

	"github.com/benpate/floodgate"
)

// Refusal is the error returned for a request that the Gate held back, so that it was never sent
type Refusal struct {
	Decision floodgate.Decision // The Gate's answer, and the record that it was made from
	Wait     time.Duration      // How long to wait before trying again, or zero when there is no point
}

// newRefusal returns the Refusal for a Decision that does not allow the request
func newRefusal(decision floodgate.Decision, now time.Time) Refusal {

	result := Refusal{Decision: decision}

	// RULE: A dormant or paused host has no time at which to try again
	if decision.Action == floodgate.ActionDrop {
		return result
	}

	result.Wait = max(decision.RetryAt.Sub(now), minimumWait)

	return result
}

// Error returns a description of the Refusal that names the host
func (refusal Refusal) Error() string {

	host := refusal.Decision.Host.Host

	switch refusal.Decision.Action {

	case floodgate.ActionWait:
		return "floodgate: " + host + " asked for fewer requests"

	case floodgate.ActionRetry:
		return "floodgate: " + host + " is unhealthy"
	}

	return "floodgate: " + host + " is dormant or paused"
}

// GetErrorCode returns the HTTP status code that stands for this Refusal, which derp.ErrorCode reads
func (refusal Refusal) GetErrorCode() int {

	switch refusal.Decision.Action {

	case floodgate.ActionWait:
		return CodeRateLimited

	case floodgate.ActionRetry:
		return CodeUnhealthy
	}

	return CodeBlocked
}

// GetRetryAfter returns how long to wait before trying again, which derp.RetryAfter reads
func (refusal Refusal) GetRetryAfter() time.Duration {
	return refusal.Wait
}
