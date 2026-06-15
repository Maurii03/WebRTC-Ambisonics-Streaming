// Package audio holds the server-side jitter buffer that reassembles and
// de-jitters a single client's decoded 16-channel soundfield.
//
// JitterBuffer (jitter.go) is a timestamp-indexed ring buffer ported from the
// browser's playback worklet (shared/worklet-receiver.js): decoded mono channels
// are written by their absolute sample timestamp, readout begins after a prefill
// (prebuffer) threshold, underruns emit silence, and overflow snaps the read
// pointer forward. A decode worker (package session, fed by package codec) writes
// per channel; the readout/mix clock reads whole frameSize frames.
//
// The package is PURE GO — no Pion, no cgo — so the buffer is unit-testable in
// isolation.
package audio
