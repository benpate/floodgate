// Package storetest checks that an implementation of floodgate.Store behaves like the others.
//
// Run turns a slice of bytes into a short script of Store operations, applies it, and returns
// a Snapshot of every record and both lists. The same script gives the same Snapshot on every
// correct Store, so a test compares a database-backed Store against floodgate.MemoryStore, which
// is the plain reference. Any difference is a defect in one of the two.
//
// The script is made of bytes so that a native fuzz test can generate it. The package imports
// no test framework, so that an application can call it from tests of its own Store.
package storetest
