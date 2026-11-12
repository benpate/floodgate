package floodgate

import (
	"context"
	"iter"
	"slices"
	"sync"
	"time"
)

// MemoryStore is a Store that keeps its records in one process's memory. It is for tests and
// single-process tools: servers that share work must share a Store that they can all reach.
type MemoryStore struct {
	mutex sync.Mutex
	hosts map[string]Host
}

// NewMemoryStore returns an empty MemoryStore
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{hosts: make(map[string]Host)}
}

// Load returns the record for a host, or a zero Host when there is none
func (store *MemoryStore) Load(_ context.Context, host string) (Host, error) {

	store.mutex.Lock()
	defer store.mutex.Unlock()

	return store.copyOf(host), nil
}

// RangeBlocked returns every host that is dormant or paused
func (store *MemoryStore) RangeBlocked(_ context.Context) (iter.Seq[Host], error) {
	return store.filter(func(host Host) bool {
		return host.IsBlocked()
	}), nil
}

// RangeProbesDue returns every dormant host whose next check is at or before the given time
func (store *MemoryStore) RangeProbesDue(_ context.Context, now time.Time) (iter.Seq[Host], error) {
	return store.filter(func(host Host) bool {
		return host.IsDormant() && (host.NextProbe <= now.Unix())
	}), nil
}

// RecordSuccess marks the host healthy and clears the failure history of a host that has a record
func (store *MemoryStore) RecordSuccess(_ context.Context, host string, now time.Time) error {

	store.mutex.Lock()
	defer store.mutex.Unlock()

	record, found := store.hosts[host]

	if !found {
		return nil
	}

	record.IsUnhealthy = false
	record.TestAfter = 0
	record.Cooldown = 0
	record.FailureCount = 0
	record.FailureDates = nil
	record.DormantSince = 0
	record.NextProbe = 0
	record.ProbeCount = 0
	record.LastRevived = now.Unix()
	store.hosts[host] = record

	return nil
}

// RecordFailure counts one failure and its day, and returns the record afterwards
func (store *MemoryStore) RecordFailure(_ context.Context, host string, now time.Time, date string) (Host, error) {

	store.mutex.Lock()
	defer store.mutex.Unlock()

	record := store.hosts[host]
	record.Host = host
	record.FailureCount++
	record.LastFailure = now.Unix()

	if !slices.Contains(record.FailureDates, date) {
		record.FailureDates = append(slices.Clone(record.FailureDates), date)
	}

	store.hosts[host] = record

	return store.copyOf(host), nil
}

// MarkUnhealthy holds deliveries back until the given time. It writes nothing when the host has no record.
func (store *MemoryStore) MarkUnhealthy(_ context.Context, host string, testAfter time.Time, cooldown time.Duration) error {

	store.updateExisting(host, func(record *Host) {
		record.IsUnhealthy = true
		record.TestAfter = testAfter.Unix()
		record.Cooldown = int64(cooldown / time.Second)
	})

	return nil
}

// MarkDormant marks the host dormant, unless it already is or it has no record
func (store *MemoryStore) MarkDormant(_ context.Context, host string, since time.Time, nextProbe time.Time) error {

	store.updateExisting(host, func(record *Host) {

		if record.IsDormant() {
			return
		}

		record.DormantSince = since.Unix()
		record.NextProbe = nextProbe.Unix()
		record.ProbeCount = 0
	})

	return nil
}

// RecordProbeFailure counts one failed check and schedules the next. It writes nothing when the host has no record.
func (store *MemoryStore) RecordProbeFailure(_ context.Context, host string, nextProbe time.Time) error {

	store.updateExisting(host, func(record *Host) {
		record.ProbeCount++
		record.NextProbe = nextProbe.Unix()
	})

	return nil
}

// SetRateLimit pauses the host until the given time, and never shortens a pause
func (store *MemoryStore) SetRateLimit(_ context.Context, host string, until time.Time) error {

	store.update(host, func(record *Host) {
		record.RateLimitUntil = max(record.RateLimitUntil, until.Unix())
	})

	return nil
}

// SetPaused records whether the server operator has stopped deliveries to the host
func (store *MemoryStore) SetPaused(_ context.Context, host string, isPaused bool) error {

	store.update(host, func(record *Host) {
		record.IsPaused = isPaused
	})

	return nil
}

// SetNodeInfo records the software that the host runs
func (store *MemoryStore) SetNodeInfo(_ context.Context, host string, software string, version string) error {

	store.update(host, func(record *Host) {
		record.Software = software
		record.Version = version
	})

	return nil
}

// SetSignatureFormat records the HTTP signature format that the host accepts
func (store *MemoryStore) SetSignatureFormat(_ context.Context, host string, format string) error {

	store.update(host, func(record *Host) {
		record.SignatureFormat = format
	})

	return nil
}

// update changes one host's record under the lock, creating the record if there is none
func (store *MemoryStore) update(host string, change func(*Host)) {

	store.mutex.Lock()
	defer store.mutex.Unlock()

	record := store.hosts[host]
	record.Host = host
	change(&record)
	store.hosts[host] = record
}

// updateExisting changes one host's record under the lock, and does nothing if there is no record
func (store *MemoryStore) updateExisting(host string, change func(*Host)) {

	store.mutex.Lock()
	defer store.mutex.Unlock()

	record, found := store.hosts[host]

	if !found {
		return
	}

	change(&record)
	store.hosts[host] = record
}

// filter returns a copy of every record that the test selects
func (store *MemoryStore) filter(test func(Host) bool) iter.Seq[Host] {

	store.mutex.Lock()
	defer store.mutex.Unlock()

	result := make([]Host, 0)

	for name, record := range store.hosts {
		if test(record) {
			result = append(result, store.copyOf(name))
		}
	}

	return slices.Values(result)
}

// copyOf returns a host's record with its own copy of the failure dates. The caller holds the lock.
func (store *MemoryStore) copyOf(host string) Host {
	record := store.hosts[host]
	record.FailureDates = slices.Clone(record.FailureDates)
	return record
}
