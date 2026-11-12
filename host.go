package floodgate

import "time"

// Host is the record of how deliveries to one remote host have gone. A host with no record is
// healthy, so the zero value describes a healthy host.
type Host struct {
	Host            string   `json:"host"              bson:"_id"`             // Hostname of the remote server, and the record's key
	IsUnhealthy     bool     `json:"isUnhealthy"       bson:"isUnhealthy"`     // TRUE while deliveries are held back
	TestAfter       int64    `json:"testAfter"         bson:"testAfter"`       // Unix seconds when an unhealthy host starts testing
	Cooldown        int64    `json:"cooldown"          bson:"cooldown"`        // Seconds that the host last waited before testing
	FailureCount    int      `json:"failureCount"      bson:"failureCount"`    // Consecutive failures since the last success
	FailureDates    []string `json:"failureDates"      bson:"failureDates"`    // Separate days (UTC) with a failure since the last success
	DormantSince    int64    `json:"dormantSince"      bson:"dormantSince"`    // Unix seconds when the host was marked dormant, or zero
	LastRevived     int64    `json:"lastRevived"       bson:"lastRevived"`     // Unix seconds when a success last cleared a history of trouble
	LastFailure     int64    `json:"lastFailure"       bson:"lastFailure"`     // Unix seconds of the last failed delivery
	IsPaused        bool     `json:"isPaused"          bson:"isPaused"`        // TRUE if the server operator has stopped deliveries to this host
	RateLimitUntil  int64    `json:"rateLimitUntil"    bson:"rateLimitUntil"`  // Unix seconds until which the host asked for no requests
	Software        string   `json:"software"          bson:"software"`        // Server software reported by NodeInfo
	Version         string   `json:"version"           bson:"version"`         // Software version reported by NodeInfo
	SignatureFormat string   `json:"signatureFormat"   bson:"signatureFormat"` // HTTP signature format that the host accepts, once known
	NextProbe       int64    `json:"nextProbe"         bson:"nextProbe"`       // Unix seconds when a dormant host is next checked
	ProbeCount      int      `json:"probeCount"        bson:"probeCount"`      // Number of checks made since the host became dormant
}

// State returns the host's state at the given time: healthy, unhealthy, or testing
func (host Host) State(now time.Time) string {

	if !host.IsUnhealthy {
		return StateHealthy
	}

	if now.Unix() < host.TestAfter {
		return StateUnhealthy
	}

	return StateTesting
}

// IsDormant returns TRUE if the host has failed for long enough that deliveries are abandoned
func (host Host) IsDormant() bool {
	return host.DormantSince > 0
}

// IsBlocked returns TRUE if nothing should be queued for the host at all
func (host Host) IsBlocked() bool {
	return host.IsDormant() || host.IsPaused
}

// IsRateLimited returns TRUE if the host has asked for no requests until a time after the one given
func (host Host) IsRateLimited(now time.Time) bool {
	return host.RateLimitUntil > now.Unix()
}

// hasHistory returns TRUE if a success must be written, because the record shows earlier trouble
func (host Host) hasHistory() bool {
	return host.IsUnhealthy || host.IsDormant() || (host.FailureCount > 0) || (len(host.FailureDates) > 0)
}
