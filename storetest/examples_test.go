package storetest

import (
	"context"
	"fmt"
	"reflect"

	"github.com/benpate/floodgate"
)

// ExampleRun shows a Store being compared against the reference Store with one script
func ExampleRun() {

	ctx := context.Background()

	// Each step is three bytes: the operation, the host, and a value. This script records a
	// failure for the first host, then pauses the second.
	script := []byte{0, 0, 0, 6, 1, 1}

	expected, _ := Run(ctx, floodgate.NewMemoryStore(), script)

	// In a real test, this is your own Store, and it must start empty
	actual, _ := Run(ctx, floodgate.NewMemoryStore(), script)

	fmt.Println(expected.Hosts[0].Host, expected.Hosts[0].FailureCount)
	fmt.Println(expected.Blocked)
	fmt.Println(reflect.DeepEqual(expected, actual))

	// Output:
	// one.example 1
	// [two.example]
	// true
}
