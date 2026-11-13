package remotegate

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benpate/derp"
	"github.com/benpate/floodgate"
	"github.com/benpate/remote"
	"github.com/benpate/remote/options"
	"github.com/stretchr/testify/require"
)

// testHost is the key that every test server's record is stored under: its hostname, with no port
const testHost = "127.0.0.1"

// testStart is the time that every test's Gate reads
var testStart = time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)

// testServer is a remote host that answers every request the way the test last told it to
type testServer struct {
	*httptest.Server
	status     atomic.Int64
	retryAfter atomic.Value
	requests   atomic.Int64
}

// newTestServer starts a server that answers 200 until it is told otherwise
func newTestServer(t *testing.T) *testServer {

	t.Helper()

	server := &testServer{}
	server.status.Store(http.StatusOK)
	server.retryAfter.Store("")

	server.Server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {

		server.requests.Add(1)

		if retryAfter := server.retryAfter.Load().(string); retryAfter != "" {
			writer.Header().Set("Retry-After", retryAfter)
		}

		writer.WriteHeader(int(server.status.Load()))
	}))

	t.Cleanup(server.Close)

	return server
}

// newTestGate returns a Gate over an empty MemoryStore, with no jitter and a fixed clock
func newTestGate() (floodgate.Gate, *floodgate.MemoryStore) {

	store := floodgate.NewMemoryStore()
	gate := floodgate.New(store, floodgate.WithJitter(0), floodgate.WithClock(func() time.Time { return testStart }))

	return gate, store
}

// send posts one request through a new Option, as a caller does for every request
func send(gate floodgate.Gate, url string, extra ...remote.Option) error {
	return remote.Post(url).
		AllowPrivateIPs(true).
		With(Option(gate)).
		With(extra...).
		Send()
}

// record returns the test host's record
func record(t *testing.T, store *floodgate.MemoryStore) floodgate.Host {

	t.Helper()

	result, err := store.Load(context.Background(), testHost)
	require.NoError(t, err)

	return result
}

// TestOption_HealthyHostCostsNothing verifies that a success for a host with no trouble sends
// the request and writes no record
func TestOption_HealthyHostCostsNothing(t *testing.T) {

	server := newTestServer(t)
	gate, store := newTestGate()

	require.NoError(t, send(gate, server.URL))
	require.Equal(t, int64(1), server.requests.Load())
	require.Equal(t, floodgate.Host{}, record(t, store))
}

// TestOption_ServerErrorsMakeAHostUnhealthy verifies that server errors are counted, that the
// host is then refused without being contacted, and that the refusal reads as a 503 with a wait
func TestOption_ServerErrorsMakeAHostUnhealthy(t *testing.T) {

	server := newTestServer(t)
	gate, store := newTestGate()

	for _, status := range []int{500, 502, 503, 504, 408} {
		server.status.Store(int64(status))
		require.Error(t, send(gate, server.URL))
	}

	require.Equal(t, 5, record(t, store).FailureCount)
	require.True(t, record(t, store).IsUnhealthy)
	require.Equal(t, int64(5), server.requests.Load())

	// The sixth request is refused before it is sent
	err := send(gate, server.URL)

	require.Equal(t, int64(5), server.requests.Load(), "an unhealthy host is not contacted")
	require.Equal(t, CodeUnhealthy, derp.ErrorCode(err))
	require.True(t, derp.IsServerError(err))
	require.Equal(t, time.Minute, derp.RetryAfter(err))

	var refusal Refusal
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, floodgate.ActionRetry, refusal.Decision.Action)
	require.Equal(t, testHost, refusal.Decision.Host.Host)
	require.Contains(t, refusal.Error(), "unhealthy")

	// RULE: A refusal is not a failure, so it does not count against the host
	require.Equal(t, 5, record(t, store).FailureCount)
}

// TestOption_UnreachableHostIsAFailure verifies that a request with no response at all is counted
func TestOption_UnreachableHostIsAFailure(t *testing.T) {

	server := newTestServer(t)
	gate, store := newTestGate()

	// Nothing listens at the address once the server has stopped
	server.Close()

	require.Error(t, send(gate, server.URL))
	require.Equal(t, 1, record(t, store).FailureCount)
}

// TestOption_TimeoutIsAFailure verifies that a timeout is counted, even though the request's own
// context has ended by the time it is recorded
func TestOption_TimeoutIsAFailure(t *testing.T) {

	gate, store := newTestGate()
	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-release
	}))

	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := send(gate, server.URL, options.WithContext(ctx))

	require.Error(t, err)
	require.True(t, derp.IsTimeoutError(err))
	require.Equal(t, 1, record(t, store).FailureCount)
}

// TestOption_CanceledRequestIsNotAFailure verifies that a request this server gave up on does
// not count against the host
func TestOption_CanceledRequestIsNotAFailure(t *testing.T) {

	server := newTestServer(t)
	gate, store := newTestGate()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.Error(t, send(gate, server.URL, options.WithContext(ctx)))
	require.Equal(t, floodgate.Host{}, record(t, store))
}

// TestOption_RateLimit verifies that a 429 pauses the host for the time it names, that the next
// request is refused as a 429 carrying the rest of that wait, and that refusals never move the pause
func TestOption_RateLimit(t *testing.T) {

	server := newTestServer(t)
	gate, store := newTestGate()

	server.status.Store(http.StatusTooManyRequests)
	server.retryAfter.Store("120")

	// The host's own 429 comes back as it was sent
	err := send(gate, server.URL)
	isRateLimited, wait := derp.IsTooManyRequests(err)

	require.True(t, isRateLimited)
	require.Equal(t, 2*time.Minute, wait)
	require.Equal(t, testStart.Add(2*time.Minute).Unix(), record(t, store).RateLimitUntil)
	require.Zero(t, record(t, store).FailureCount, "a rate limit is not a failure")

	// Later requests are refused without contacting the host, and read the same way
	for range 3 {
		err = send(gate, server.URL)
		isRateLimited, wait = derp.IsTooManyRequests(err)

		require.True(t, isRateLimited)
		require.Equal(t, 2*time.Minute, wait)
	}

	require.Equal(t, int64(1), server.requests.Load())
	require.Equal(t, testStart.Add(2*time.Minute).Unix(), record(t, store).RateLimitUntil, "a refusal must not extend the pause")

	var refusal Refusal
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, floodgate.ActionWait, refusal.Decision.Action)
	require.Contains(t, refusal.Error(), "fewer requests")
}

// TestOption_RateLimitWithNoWaitNamed verifies that a 429 with no Retry-After pauses the host for a minute
func TestOption_RateLimitWithNoWaitNamed(t *testing.T) {

	server := newTestServer(t)
	gate, store := newTestGate()

	server.status.Store(http.StatusTooManyRequests)

	require.Error(t, send(gate, server.URL))
	require.Equal(t, testStart.Add(time.Minute).Unix(), record(t, store).RateLimitUntil)
}

// TestOption_OtherRefusalsSayNothing verifies that an answer about one request is not recorded
func TestOption_OtherRefusalsSayNothing(t *testing.T) {

	server := newTestServer(t)
	gate, store := newTestGate()

	for _, status := range []int{400, 401, 403, 404, 410, 422} {
		server.status.Store(int64(status))
		require.Error(t, send(gate, server.URL))
	}

	require.Equal(t, floodgate.Host{}, record(t, store))
}

// TestOption_PausedHostIsBlocked verifies that a paused host is refused with a code that does
// not read as a missing or deleted address
func TestOption_PausedHostIsBlocked(t *testing.T) {

	server := newTestServer(t)
	gate, _ := newTestGate()

	require.NoError(t, gate.SetPaused(context.Background(), testHost, true))

	err := send(gate, server.URL)

	require.Zero(t, server.requests.Load())
	require.Equal(t, CodeBlocked, derp.ErrorCode(err))
	require.False(t, derp.IsNotFoundOrGone(err))
	require.False(t, derp.IsServerError(err))
	require.Zero(t, derp.RetryAfter(err))

	var refusal Refusal
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, floodgate.ActionDrop, refusal.Decision.Action)
	require.Contains(t, refusal.Error(), "dormant or paused")
}

// TestOption_SuccessAfterTroubleClearsTheHistory verifies that a success for a host with
// failures on record makes it healthy again
func TestOption_SuccessAfterTroubleClearsTheHistory(t *testing.T) {

	server := newTestServer(t)
	gate, store := newTestGate()

	server.status.Store(http.StatusInternalServerError)
	require.Error(t, send(gate, server.URL))
	require.Equal(t, 1, record(t, store).FailureCount)

	server.status.Store(http.StatusOK)
	require.NoError(t, send(gate, server.URL))
	require.Zero(t, record(t, store).FailureCount)
	require.Equal(t, testStart.Unix(), record(t, store).LastRevived)
}

// TestOption_HostKeyIgnoresCaseAndPort verifies that the record is kept under the lower-cased
// hostname, however the URL spells it
func TestOption_HostKeyIgnoresCaseAndPort(t *testing.T) {

	server := newTestServer(t)
	gate, store := newTestGate()

	server.status.Store(http.StatusInternalServerError)

	// The same server, reached by a name written in capitals
	url := strings.Replace(server.URL, "127.0.0.1", "LOCALHOST", 1)
	require.Error(t, send(gate, url))

	result, err := store.Load(context.Background(), "localhost")
	require.NoError(t, err)
	require.Equal(t, 1, result.FailureCount)
}

// brokenStore is a Store that cannot be read
type brokenStore struct {
	*floodgate.MemoryStore
}

// Load always fails
func (brokenStore) Load(context.Context, string) (floodgate.Host, error) {
	return floodgate.Host{}, errors.New("the store is broken")
}

// TestOption_BrokenStoreDoesNotStopRequests verifies that the request is sent when the Gate cannot answer
func TestOption_BrokenStoreDoesNotStopRequests(t *testing.T) {

	server := newTestServer(t)
	gate := floodgate.New(brokenStore{MemoryStore: floodgate.NewMemoryStore()})

	require.NoError(t, send(gate, server.URL))
	require.Equal(t, int64(1), server.requests.Load())
}

// TestCheck_NoHost verifies that a URL with no host is left for remote to reject, with no record
func TestCheck_NoHost(t *testing.T) {

	gate, store := newTestGate()

	decision, err := check(gate, "")
	require.NoError(t, err)
	require.True(t, decision.CanSend())

	// Reports for a request with no host are ignored
	reportResponse(gate, decision.Host, &http.Response{StatusCode: http.StatusInternalServerError})
	reportError(gate, decision.Host, errors.New("no host"))

	blocked, err := gate.Blocked(context.Background())
	require.NoError(t, err)
	require.Empty(t, blocked)
	require.Equal(t, floodgate.Host{}, record(t, store))
}

// TestNewRefusal_WaitIsNeverTooShort verifies that a retry time that has already passed still asks for a short wait
func TestNewRefusal_WaitIsNeverTooShort(t *testing.T) {

	decision := floodgate.Decision{Action: floodgate.ActionRetry, RetryAt: testStart.Add(-time.Hour)}
	require.Equal(t, minimumWait, newRefusal(decision, testStart).GetRetryAfter())
}
