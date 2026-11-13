package remotegate

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/benpate/derp"
	"github.com/benpate/floodgate"
	"github.com/benpate/remote"
	"github.com/benpate/uri"
)

// Option returns a remote.Option that asks the Gate before one request and records its outcome.
// Build a new one for every request, because it carries the Gate's answer from one hook to the next.
func Option(gate floodgate.Gate) remote.Option {

	// The record that the Gate decided from, kept for the report that follows
	var record floodgate.Host

	return remote.Option{

		// Ask before the request is built, and refuse it without connecting
		BeforeRequest: func(transaction *remote.Transaction) error {

			decision, err := check(gate, uri.Hostname(transaction.RequestURL()))

			if err != nil {
				return err
			}

			record = decision.Host
			return nil
		},

		// A response arrived, so record what it says about the host
		AfterRequest: func(_ *remote.Transaction, response *http.Response) error {
			reportResponse(gate, record, response)
			return nil
		},

		// No response arrived, so the host could not be reached
		OnError: func(_ *remote.Transaction, _ *http.Request, err error) error {
			reportError(gate, record, err)
			return err
		},
	}
}

// check asks the Gate about a host, and returns a Refusal when the request must not be sent
func check(gate floodgate.Gate, host string) (floodgate.Decision, error) {

	// A request with no host fails on its own, and there is no record to keep for it
	allowed := floodgate.Decision{Action: floodgate.ActionSend, Host: floodgate.Host{Host: host}}

	if host == "" {
		return allowed, nil
	}

	ctx, cancel := gateContext()
	defer cancel()

	decision, err := gate.Check(ctx, host)

	// RULE: A Store that cannot answer must not stop requests
	if err != nil {
		derp.Report(derp.Wrap(err, "remotegate.check", "Checking host", "host: "+host))
		return allowed, nil
	}

	if decision.CanSend() {
		return decision, nil
	}

	return decision, newRefusal(decision, gate.Now())
}

// reportResponse records what a response says about the host that sent it
func reportResponse(gate floodgate.Gate, record floodgate.Host, response *http.Response) {

	const location = "remotegate.reportResponse"

	if (record.Host == "") || (response == nil) {
		return
	}

	ctx, cancel := gateContext()
	defer cancel()

	switch code := response.StatusCode; {

	// RULE: A rate limit is not a failure. The host is left alone until the time it names.
	case code == http.StatusTooManyRequests:
		if _, err := gate.RateLimited(ctx, record.Host, retryAfter(response)); err != nil {
			derp.Report(derp.Wrap(err, location, "Recording rate limit", "host: "+record.Host))
		}

	// RULE: A server error or a timeout says that the host is in trouble
	case (code >= http.StatusInternalServerError) || (code == http.StatusRequestTimeout):
		if err := gate.Failure(ctx, record.Host); err != nil {
			derp.Report(derp.Wrap(err, location, "Recording failure", "host: "+record.Host))
		}

	// RULE: A success clears the host's history, and costs nothing when it has none
	case (code >= http.StatusOK) && (code < http.StatusMultipleChoices):
		if err := gate.Success(ctx, record); err != nil {
			derp.Report(derp.Wrap(err, location, "Recording success", "host: "+record.Host))
		}
	}

	// Any other answer is about this one request, and says nothing about the host's health
}

// reportError records that a request got no response from the host
func reportError(gate floodgate.Gate, record floodgate.Host, requestError error) {

	if record.Host == "" {
		return
	}

	// RULE: A request that this server gave up on says nothing about the host
	if errors.Is(requestError, context.Canceled) {
		return
	}

	ctx, cancel := gateContext()
	defer cancel()

	if err := gate.Failure(ctx, record.Host); err != nil {
		derp.Report(derp.Wrap(err, "remotegate.reportError", "Recording failure", "host: "+record.Host))
	}
}

// retryAfter returns the wait that a rate-limited response asks for, or derp's default if it names none
func retryAfter(response *http.Response) time.Duration {

	// derp reads every header that servers use for this, in every date format
	report := derp.HTTPError{Response: derp.HTTPResponseReport{
		StatusCode: response.StatusCode,
		Header:     response.Header,
	}}

	return report.GetRetryAfter()
}

// gateContext returns a context for one call to the Gate. It never comes from the request,
// because the request's own context has already ended when a timeout is being recorded.
func gateContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), gateTimeout)
}
