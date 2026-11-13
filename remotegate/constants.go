package remotegate

import (
	"net/http"
	"time"
)

// CodeRateLimited is the code of a Refusal for a host that asked for fewer requests
const CodeRateLimited = http.StatusTooManyRequests

// CodeUnhealthy is the code of a Refusal for a host that keeps failing and is cooling down
const CodeUnhealthy = http.StatusServiceUnavailable

// CodeBlocked is the code of a Refusal for a dormant or paused host. No inbox answers with
// it, and it does not mean that the address is gone, so the delivery can be dropped quietly.
const CodeBlocked = http.StatusLocked

// gateTimeout bounds each call that the option makes to the Gate
const gateTimeout = 10 * time.Second

// minimumWait is the shortest wait that a Refusal asks for
const minimumWait = time.Second
