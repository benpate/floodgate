package storetest

import (
	"context"
	"iter"
	"reflect"
	"slices"
	"strconv"
	"time"

	"github.com/benpate/derp"
	"github.com/benpate/floodgate"
)

// Hosts returns the hostnames that a script works on
func Hosts() []string {
	return []string{"one.example", "two.example", "three.example"}
}

// Run applies a script of Store operations to an empty Store, and returns what the Store holds
// afterwards. It fails when the Store does, or when RecordFailure returns a record it did not store.
func Run(ctx context.Context, store floodgate.Store, script []byte) (Snapshot, error) {

	const location = "storetest.Run"

	hosts := Hosts()
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

	// Apply each step in order
	for step := range min(len(script)/stepSize, MaxSteps) {

		operation := script[step*stepSize]
		host := hosts[int(script[step*stepSize+1])%len(hosts)]
		value := script[step*stepSize+2]

		// Moving the clock touches no record
		if operation%opCount == opAdvanceClock {
			now = now.Add(time.Duration(value) * time.Hour)
			continue
		}

		if err := apply(ctx, store, operation, host, value, now); err != nil {
			return Snapshot{}, derp.Wrap(err, location, "Applying step", "step: "+strconv.Itoa(step))
		}
	}

	// Read back everything that the Store holds
	result, err := snapshot(ctx, store, now)

	if err != nil {
		return Snapshot{}, derp.Wrap(err, location, "Reading the Store")
	}

	return result, nil
}

// apply runs one Store operation for one host
func apply(ctx context.Context, store floodgate.Store, operation byte, host string, value byte, now time.Time) error {

	// The value is read as hours, except where a case reads it another way
	switch hours := time.Duration(value) * time.Hour; operation % opCount {

	case opRecordFailure:
		return recordFailure(ctx, store, host, now)

	case opRecordSuccess:
		return store.RecordSuccess(ctx, host, now)

	case opMarkUnhealthy:
		cooldown := time.Duration(value) * time.Minute
		return store.MarkUnhealthy(ctx, host, now.Add(cooldown), cooldown)

	case opMarkDormant:
		return store.MarkDormant(ctx, host, now, now.Add(hours))

	case opRecordProbeFailure:
		return store.RecordProbeFailure(ctx, host, now.Add(hours))

	// Minutes either side of the present, because a pause that ends before an earlier one must not shorten it
	case opSetRateLimit:
		minutes := time.Duration(int(value)-128) * time.Minute
		return store.SetRateLimit(ctx, host, now.Add(minutes))

	case opSetPaused:
		return store.SetPaused(ctx, host, value%2 == 1)

	case opSetNodeInfo:
		return store.SetNodeInfo(ctx, host, "software-"+strconv.Itoa(int(value%4)), strconv.Itoa(int(value)))
	}

	// Fall through means opSetSignatureFormat
	return store.SetSignatureFormat(ctx, host, "format-"+strconv.Itoa(int(value%4)))
}

// recordFailure counts one failure, and checks that the record returned is the record stored
func recordFailure(ctx context.Context, store floodgate.Store, host string, now time.Time) error {

	const location = "storetest.recordFailure"

	returned, err := store.RecordFailure(ctx, host, now, now.Format(dateFormat))

	if err != nil {
		return derp.Wrap(err, location, "Recording failure")
	}

	stored, err := store.Load(ctx, host)

	if err != nil {
		return derp.Wrap(err, location, "Loading host record")
	}

	// RULE: The Gate decides from the returned record, so it must be the one that was stored
	if !sameRecord(returned, stored) {
		return derp.Internal(location, "RecordFailure returned a record that differs from the stored one", "host: "+host)
	}

	return nil
}

// sameRecord returns TRUE if two records hold the same values
func sameRecord(first floodgate.Host, second floodgate.Host) bool {

	if !slices.Equal(first.FailureDates, second.FailureDates) {
		return false
	}

	// With the dates compared, an empty list and no list are the same
	first.FailureDates = nil
	second.FailureDates = nil

	return reflect.DeepEqual(first, second)
}

// snapshot reads every record and both lists from a Store
func snapshot(ctx context.Context, store floodgate.Store, now time.Time) (Snapshot, error) {

	const location = "storetest.snapshot"

	result := Snapshot{}

	// Read each host's record
	for _, host := range Hosts() {

		record, err := store.Load(ctx, host)

		if err != nil {
			return Snapshot{}, derp.Wrap(err, location, "Loading host record", "host: "+host)
		}

		// A Store may hold "no failure dates" as an empty list or as none at all
		if len(record.FailureDates) == 0 {
			record.FailureDates = nil
		}

		result.Hosts = append(result.Hosts, record)
	}

	// Read the hosts that nothing should be queued for
	blocked, err := store.RangeBlocked(ctx)

	if err != nil {
		return Snapshot{}, derp.Wrap(err, location, "Loading blocked hosts")
	}

	result.Blocked = sortedNames(blocked)

	// Read the hosts that are due for a check
	probesDue, err := store.RangeProbesDue(ctx, now)

	if err != nil {
		return Snapshot{}, derp.Wrap(err, location, "Loading hosts to check")
	}

	result.ProbesDue = sortedNames(probesDue)

	return result, nil
}

// sortedNames returns the hostnames of a list of records, in order
func sortedNames(hosts iter.Seq[floodgate.Host]) []string {

	result := make([]string, 0)

	for host := range hosts {
		result = append(result, host.Host)
	}

	slices.Sort(result)

	return result
}
