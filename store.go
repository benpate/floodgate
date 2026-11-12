package floodgate

import (
	"context"
	"iter"
	"time"
)

// Store keeps the Host records. Every server of an application shares one Store, so each
// write must be one atomic operation on one record, or two reports at once lose a count.
type Store interface {

	// Load returns the record for a host. A host with no record yields a zero Host and no error.
	Load(ctx context.Context, host string) (Host, error)

	// RangeBlocked returns every host that is dormant or paused
	RangeBlocked(ctx context.Context) (iter.Seq[Host], error)

	// RangeProbesDue returns every dormant host whose next check is at or before the given time
	RangeProbesDue(ctx context.Context, now time.Time) (iter.Seq[Host], error)

	// RecordSuccess marks the host healthy and clears the failure history, the dormant mark and
	// the probe schedule. It writes nothing when the host has no record.
	RecordSuccess(ctx context.Context, host string, now time.Time) error

	// RecordFailure adds one to the failure count, adds the day to the failure dates if it is
	// new, creates the record if there is none, and returns the record as it stands afterwards
	RecordFailure(ctx context.Context, host string, now time.Time, date string) (Host, error)

	// MarkUnhealthy holds deliveries back until the given time, and records the cooldown that was
	// used. It writes nothing when the host has no record.
	MarkUnhealthy(ctx context.Context, host string, testAfter time.Time, cooldown time.Duration) error

	// MarkDormant marks the host dormant and schedules its first check. It changes nothing
	// when the host is already marked, or has no record.
	MarkDormant(ctx context.Context, host string, since time.Time, nextProbe time.Time) error

	// RecordProbeFailure adds one to the probe count and schedules the next check. It writes
	// nothing when the host has no record.
	RecordProbeFailure(ctx context.Context, host string, nextProbe time.Time) error

	// SetRateLimit pauses the host until the given time, creating the record if there is none.
	// It never shortens a pause: the later of the stored time and the new time is kept.
	SetRateLimit(ctx context.Context, host string, until time.Time) error

	// SetPaused records whether the server operator has stopped deliveries to the host,
	// creating the record if there is none
	SetPaused(ctx context.Context, host string, isPaused bool) error

	// SetNodeInfo records the software that the host runs, creating the record if there is none
	SetNodeInfo(ctx context.Context, host string, software string, version string) error

	// SetSignatureFormat records the HTTP signature format that the host accepts, creating the
	// record if there is none
	SetSignatureFormat(ctx context.Context, host string, format string) error
}
