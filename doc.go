// Package floodgate is a circuit breaker for outbound deliveries, keyed by remote host.
//
// It answers one question before each delivery: may this server send to that host right now?
// The answer comes from a small record of how recent deliveries to the host went. A host that
// keeps failing is unhealthy for a cooldown, then tested, and marked dormant when it has failed
// on enough separate days. A host that asks for fewer requests is paused until the time it names.
//
// The names describe the remote host, not the gate: healthy, unhealthy, testing, and dormant.
// The circuit breaker terms "open" and "closed" are not used, because they read both ways.
//
// The package keeps no state of its own. Every record lives behind the Store interface, which
// the application implements over a database that all of its servers share, so that every
// server sees the same host the same way. MemoryStore is for tests and single-process tools.
package floodgate
