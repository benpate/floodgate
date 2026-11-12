package storetest

import "github.com/benpate/floodgate"

// Snapshot is everything that a Store holds after a script has run
type Snapshot struct {
	Hosts     []floodgate.Host // The record of each host in Hosts(), in the same order
	Blocked   []string         // The hosts that RangeBlocked returns, sorted
	ProbesDue []string         // The hosts that RangeProbesDue returns at the script's final time, sorted
}
