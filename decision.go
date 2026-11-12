package floodgate

import "time"

// Decision is the answer to "may this server deliver to that host right now?"
type Decision struct {
	Action  string    // What to do with the delivery: ActionSend, ActionRetry, ActionWait, or ActionDrop
	RetryAt time.Time // For ActionWait and ActionRetry, when to try again
	Host    Host      // The record that the decision was made from, to hand back with the result
}

// CanSend returns TRUE if the delivery may go ahead
func (decision Decision) CanSend() bool {
	return decision.Action == ActionSend
}
