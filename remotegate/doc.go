// Package remotegate connects a floodgate.Gate to requests made with the benpate/remote package.
//
// Option returns a remote.Option that does everything a caller would otherwise do by hand. It
// asks the Gate before the request is sent, refuses the request when the Gate says to hold it
// back, and afterwards records how the request went: a success, a failure, or a rate limit.
// The caller attaches the option and reads the error. It never reports to the Gate itself.
//
// A refused request returns a Refusal, which carries an HTTP status code and a wait, so that
// code written for a remote server's own answers handles the Gate's answers too: 429 for a host
// that asked for fewer requests, 503 for an unhealthy host, and 423 for a host that gets nothing.
//
// This package is separate from floodgate so that floodgate itself does not depend on remote.
package remotegate
