// Package room hosts the multi-client mixing logic. See room.go for the full
// package overview; manager.go owns the set of live rooms keyed by id.
//
// Membership + mix clock live in room.go; the Manager (create-on-join,
// destroy-when-empty, multiple independent rooms) lives in manager.go. The
// package is PURE GO and reaches sessions only through the Participant interface,
// so the mixer is unit-testable without Pion or cgo.
package room
