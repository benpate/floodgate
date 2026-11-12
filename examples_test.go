package floodgate

import (
	"context"
	"fmt"
	"time"
)

// exampleStart is the moment at which every example begins
var exampleStart = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

// ExampleGate_Check shows the whole life of one delivery: ask, send, and report how it went
func ExampleGate_Check() {

	ctx := context.Background()

	// In production, the Store is a database that all of your servers share
	gate := New(NewMemoryStore())

	// Ask before each delivery
	decision, err := gate.Check(ctx, "remote.example")

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(decision.Action)

	// Send the delivery, then report the result. Hand back the record from the Decision,
	// so that a host with no trouble on record costs no write.
	if decision.CanSend() {
		_ = gate.Success(ctx, decision.Host)
	}

	// Output: SEND
}

// ExampleGate_Failure shows a host becoming unhealthy after five failures in a row
func ExampleGate_Failure() {

	ctx := context.Background()
	gate := New(NewMemoryStore(), WithJitter(0), WithClock(func() time.Time { return exampleStart }))

	// Report each delivery that could not connect, timed out, or got a server error
	for range 5 {
		_ = gate.Failure(ctx, "remote.example")
	}

	// The next delivery fails fast, without connecting, and is retried after the cooldown
	decision, _ := gate.Check(ctx, "remote.example")

	fmt.Println(decision.Action)
	fmt.Println(decision.Host.State(exampleStart))
	fmt.Println(decision.RetryAt.Format(time.RFC3339))

	// Output:
	// RETRY
	// UNHEALTHY
	// 2026-11-11T12:01:00Z
}

// ExampleGate_RateLimited shows a host that answered "429 Too Many Requests"
func ExampleGate_RateLimited() {

	ctx := context.Background()
	gate := New(NewMemoryStore(), WithJitter(0), WithClock(func() time.Time { return exampleStart }))

	// Pass the wait that the host asked for, or zero if it named none
	until, _ := gate.RateLimited(ctx, "remote.example", 10*time.Minute)
	fmt.Println(until.Format(time.RFC3339))

	// Every delivery to that host now waits, on every server that shares the Store
	decision, _ := gate.Check(ctx, "remote.example")
	fmt.Println(decision.Action)

	// No host can ask for more than the longest pause, which is one hour by default
	until, _ = gate.RateLimited(ctx, "remote.example", 6*time.Hour)
	fmt.Println(until.Format(time.RFC3339))

	// Output:
	// 2026-11-11T12:10:00Z
	// WAIT
	// 2026-11-11T13:00:00Z
}

// ExampleGate_Blocked shows a fan-out skipping the hosts that nothing should be queued for
func ExampleGate_Blocked() {

	ctx := context.Background()
	gate := New(NewMemoryStore())

	// The server's operator stops deliveries to one host
	_ = gate.SetPaused(ctx, "paused.example", true)

	// Load the list once, then check every recipient against it in memory
	blocked, _ := gate.Blocked(ctx)

	for _, host := range []string{"paused.example", "remote.example"} {
		fmt.Println(host, blocked.Contains(host))
	}

	// Output:
	// paused.example true
	// remote.example false
}

// ExampleGate_ProbesDue shows a scheduled task checking whether a dormant host has come back
func ExampleGate_ProbesDue() {

	ctx := context.Background()
	now := exampleStart

	// For this example, one day of failure makes a host dormant. The default is seven.
	gate := New(NewMemoryStore(), WithDormantDays(1), WithClock(func() time.Time { return now }))
	_ = gate.Failure(ctx, "remote.example")

	// A dormant host gets nothing
	decision, _ := gate.Check(ctx, "remote.example")
	fmt.Println(decision.Action)

	// Eight days later, the first weekly check is due
	now = now.Add(8 * 24 * time.Hour)
	hosts, _ := gate.ProbesDue(ctx)

	for host := range hosts {

		fmt.Println("checking", host.Host)

		// Contact the host yourself, then report what happened: ProbeFailed or ProbeSucceeded
		_ = gate.ProbeSucceeded(ctx, host.Host, "mastodon", "4.5.0")
	}

	// The host is healthy again
	decision, _ = gate.Check(ctx, "remote.example")
	fmt.Println(decision.Action)

	// Output:
	// DROP
	// checking remote.example
	// SEND
}

// ExampleHost_State shows the three states that a host's record can be in
func ExampleHost_State() {

	// A host with no record, or with no trouble on record, is healthy
	fmt.Println(Host{}.State(exampleStart))

	// An unhealthy host is left alone until its cooldown passes, and is then tested
	record := Host{IsUnhealthy: true, TestAfter: exampleStart.Add(time.Minute).Unix()}

	fmt.Println(record.State(exampleStart))
	fmt.Println(record.State(exampleStart.Add(2 * time.Minute)))

	// Output:
	// HEALTHY
	// UNHEALTHY
	// TESTING
}
