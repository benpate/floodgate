# Floodgate 🌊

<img src="https://github.com/benpate/floodgate/blob/main/meta/Tsunami_by_hokusai_19th_century.png" style="width:100%; margin-bottom:20px;" alt="The Great Wave off Kanagawa woodcut by Hokusai, 1831. https://en.wikipedia.org/wiki/The_Great_Wave_off_Kanagawa">

## HTTP Circuit Breaker for Go

Floodgate is a circuit breaker for HTTP requests.  It tracks host status and blocks requests to hosts that that are known to be unavailable, with an automatic recovery that periodically checks if unavailable hosts have recovered.

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

- **Healthy:** deliveries are allowed.
- **Unhealthy:** deliveries are held back until a cooldown passes.
- **Testing:** the cooldown has passed, and trial deliveries are let through. A success makes the host healthy; a failure makes it unhealthy again for twice as long.
- **Dormant:** the host has failed on seven separate days. Its deliveries are abandoned, and it is checked on a slow schedule. This is a separate mark (`Host.IsDormant`), not a value that `Host.State` returns.

## Sensible Default Rules

These rules can be modified using `With...` options

- **Rate limits:** `RateLimited` pauses a host until the time it asked for, capped at one hour, or for one minute when it named none.
- **Unhealthy hosts:** after five consecutive failures, a host is marked unhealthy for one minute, doubling for up to 32 minutes at a time.
- **Dormant hosts:** a host that fails on seven separate days with no success between them is marked dormant, and its deliveries are abandoned. It is checked again weekly for four checks, then every 30 days. `Revive` clears the mark.
- **Operator pause:** `SetPaused` stops deliveries to a host until it is resumed.


## Using it with `remote`

The `remotegate` subpackage connects a Gate to requests made with [remote](https://github.com/benpate/remote). Its option asks before the request is sent, refuses the request when it must be held back, and records the outcome afterwards. The caller never reports to the Gate.

```go
err := remote.Post(inboxURL).
	With(remotegate.Option(gate)). // build a new one for every request
	Body(body).
	Send()
```

A refused request is never sent. Its error is a `remotegate.Refusal`, which carries a status code and a wait, so code written for a server's own answers handles the Gate's too:

| The host is | Code | Wait |
| --- | --- | --- |
| rate-limited | 429 | until the pause ends |
| unhealthy | 503 | until the cooldown ends |
| dormant or paused | 423 | none |

`derp.IsTooManyRequests(err)` therefore returns the Gate's wait for a rate-limited host, the same way it returns a server's own `Retry-After`.

## Reporting Failures

The `remotegate` option does this reporting for you. Without it, callers report failures to floodgate after each transaction.  Report `Failure` when the host could not be reached or answered with a server error. Report `RateLimited` for a 429. Report neither for a refusal that says nothing about the host's health, such as a 401 or a 404.

## Storage

Floodgate keeps no state of its own. The application implements `Store` over a database that all of its servers share, and each write must be one atomic operation on the host's record. `MemoryStore` is for tests and single-process tools.

A healthy host costs no record and no write: `Success` writes only when the record shows earlier trouble.

## Testing a Store

The `storetest` package checks a `Store` of your own against `MemoryStore`, which is the reference. `storetest.Run` turns a slice of bytes into a script of Store operations and returns a `Snapshot` of what the Store holds afterwards. Run the same script on both Stores and compare the two Snapshots; a native fuzz test can generate the scripts.

```go
expected, _ := storetest.Run(ctx, floodgate.NewMemoryStore(), script)
actual, _ := storetest.Run(ctx, myStore, script) // myStore must start empty
require.Equal(t, expected, actual)
```

## Pull Requests Welcome

This library is a work in progress, and will benefit from your experience reports, use cases, and contributions. If you have an idea for making this library better, send in a pull request. We're all in this together! 🌊
