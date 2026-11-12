package storetest

// MaxSteps is the largest number of operations that Run applies. The rest of a script is ignored.
const MaxSteps = 64

// stepSize is the number of bytes that describe one operation: what to do, to which host, and with what value
const stepSize = 3

// dateFormat is the layout of a failure date, which matches the one that floodgate.Gate writes
const dateFormat = "2006-01-02"

// The operations that a script can hold. A step's first byte selects one, modulo opCount.
const (
	opRecordFailure = iota
	opRecordSuccess
	opMarkUnhealthy
	opMarkDormant
	opRecordProbeFailure
	opSetRateLimit
	opSetPaused
	opSetNodeInfo
	opSetSignatureFormat
	opAdvanceClock
	opCount
)
