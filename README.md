# Floodgate

A circuit breaker for outbound deliveries, keyed by remote host. Before each delivery it answers one question: may this server send to that host right now?

```go
gate := floodgate.New(store)

decision, err := gate.Check(ctx, "remote.example")

switch decision.Action {
case floodgate.ActionSend:  // deliver, then report gate.Success or gate.Failure
case floodgate.ActionRetry: // the host is unhealthy; fail fast and retry at decision.RetryAt
case floodgate.ActionWait:  // the host asked for fewer requests; reschedule for decision.RetryAt
case floodgate.ActionDrop:  // the host is dormant or paused; abandon the delivery
}
```

## States

The names describe the remote host. The circuit-breaker terms "open" and "closed" are not used, because they read both ways.

- **Healthy:** deliveries are allowed.
- **Unhealthy:** deliveries are held back until a cooldown passes.
- **Testing:** the cooldown has passed, and trial deliveries are let through. A success makes the host healthy; a failure makes it unhealthy again for twice as long.
- **Dormant:** the host has failed on seven separate days. Its deliveries are abandoned, and it is checked on a slow schedule. This is a separate mark (`Host.IsDormant`), not a value that `Host.State` returns.

## Rules

- **Unhealthy hosts:** five consecutive failures make a host unhealthy for one minute. Each failed trial doubles the cooldown, up to 32 minutes.
- **Dormant hosts:** a host that fails on seven separate days with no success between them is marked dormant, and its deliveries are abandoned. It is checked again weekly for four checks, then every 30 days. `Revive` clears the mark.
- **Rate limits:** `RateLimited` pauses a host until the time it asked for, capped at one hour, or for one minute when it named none. A pause is never shortened, and is not a failure.
- **Operator pause:** `SetPaused` stops deliveries to a host until it is resumed.

Every number above is a default; see the `With...` options.

## Testing a Store

The `storetest` package checks a `Store` of your own against `MemoryStore`, which is the reference. `storetest.Run` turns a slice of bytes into a script of Store operations and returns a `Snapshot` of what the Store holds afterwards. Run the same script on both Stores and compare the two Snapshots; a native fuzz test can generate the scripts.

```go
expected, _ := storetest.Run(ctx, floodgate.NewMemoryStore(), script)
actual, _ := storetest.Run(ctx, myStore, script) // myStore must start empty
require.Equal(t, expected, actual)
```

## What counts as a failure

The caller decides. Report `Failure` when the host could not be reached or answered with a server error. Report `RateLimited` for a 429. Report neither for a refusal that says nothing about the host's health, such as a 401 or a 404.

## Storage

Floodgate keeps no state of its own. The application implements `Store` over a database that all of its servers share, and each write must be one atomic operation on the host's record. `MemoryStore` is for tests and single-process tools.

A healthy host costs no record and no write: `Success` writes only when the record shows earlier trouble.
