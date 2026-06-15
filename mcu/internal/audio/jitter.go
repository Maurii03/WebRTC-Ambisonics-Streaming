package audio

import "sync"

// JitterBuffer is a 16-channel (configurable), timestamp-indexed ring buffer for
// reassembling and de-jittering a single client's decoded soundfield. It is a
// server-side port of the browser playback worklet (shared/worklet-receiver.js):
// frames are placed by their absolute sample timestamp, playback begins after a
// prefill (prebuffer) threshold, underruns emit silence, and overflow snaps the
// read pointer forward.
//
// Two key differences from the worklet, both because the server's roles differ:
//
//   - Writes are PER-CHANNEL. On the client a whole 16-channel frame is written
//     at once; on the server the 16 mono channels of one frameTs are decoded
//     independently (one Opus stream per channel) and arrive separately, so a
//     decode worker calls WriteChannel 16 times for a given frameTs. Channels
//     that never arrive stay silent (the slot is zeroed on read).
//
//   - Reads are WHOLE FRAMES. The worklet's process() reads 128-sample render
//     quanta; the server's mix/readout clock reads exactly one frameSize frame
//     per tick. The read pointer therefore stays frame-aligned, and the overflow
//     snap is rounded up to a frame boundary (see ReadFrame / WriteChannel).
//
// Concurrency: a single decode worker writes (WriteChannel) and the readout/mix
// clock reads (ReadFrame); the mutex makes that safe. The package is PURE GO (no
// Pion, no cgo) so it stays unit-testable in isolation.
type JitterBuffer struct {
	mu sync.Mutex

	channels  int   // soundfield channel count (16 for 3rd-order ambisonics)
	frameSize int   // samples per frame per channel (server-authoritative)
	bufLen    int   // ring capacity in sample-frames per channel
	prebuffer int64 // prefill depth in samples before readout starts
	maxDepth  int64 // latency cap: snap the read pointer forward when occupancy exceeds this

	// buf holds bufLen * channels int16 samples, interleaved as
	// buf[(sampleSlot)*channels + ch] where sampleSlot = absoluteTs % bufLen.
	buf []int16

	initTs  int64 // first frameTs seen; -1 until anchored
	writeTs int64 // highest (frameTs + len) written, in absolute samples
	readTs  int64 // next absolute sample to read for output
	started bool  // true once the prebuffer threshold has been met

	underruns uint64 // ReadFrame calls that hit an underrun (telemetry)
	overflows uint64 // WriteChannel calls that forced an overflow snap (telemetry)
}

// Stats is a snapshot of a JitterBuffer's state, for telemetry and tests.
type Stats struct {
	Started   bool
	Depth     int    // buffered samples not yet read (writeTs - readTs), >= 0
	Underruns uint64 // cumulative underrun reads
	Overflows uint64 // cumulative overflow snaps
	ReadTs    int64  // current read pointer (absolute samples)
	WriteTs   int64  // current write frontier (absolute samples)
}

// NewJitterBuffer builds a buffer for channels independent channels, reading
// frameSize-sample frames, with a ring capacity of bufLenSamples per channel and
// a prefill depth of prebufferSamples. bufLenSamples should comfortably exceed
// the expected network jitter plus the prebuffer (the session uses 2 s).
//
// It panics on non-positive sizes or bufLenSamples <= frameSize, since those are
// programmer errors, not runtime conditions.
func NewJitterBuffer(channels, frameSize, bufLenSamples, prebufferSamples int) *JitterBuffer {
	if channels <= 0 || frameSize <= 0 {
		panic("audio: channels and frameSize must be positive")
	}
	if bufLenSamples <= frameSize {
		panic("audio: bufLenSamples must exceed frameSize")
	}
	if prebufferSamples < 0 {
		prebufferSamples = 0
	}
	// Latency cap = prebuffer target + a generous drift tolerance (6 frames). Only
	// occupancy ABOVE this is treated as stale accumulated latency and snapped away
	// (see WriteChannel); the generous margin absorbs normal bursty delivery so the
	// drain doesn't fight it (drain→underrun→drain thrashing). Clamped below the
	// ring so it also serves as the hard overflow guard.
	maxDepth := prebufferSamples + 6*frameSize
	if cap := bufLenSamples - frameSize; maxDepth > cap {
		maxDepth = cap
	}
	return &JitterBuffer{
		channels:  channels,
		frameSize: frameSize,
		bufLen:    bufLenSamples,
		prebuffer: int64(prebufferSamples),
		maxDepth:  int64(maxDepth),
		buf:       make([]int16, bufLenSamples*channels),
		initTs:    -1,
	}
}

// WriteChannel places one decoded mono channel (chIdx) of the frame at frameTs
// into the ring. pcm is int16 PCM, typically frameSize samples. The 16 channels
// sharing a frameTs are written by 16 separate calls.
//
// The first write anchors the ring origin. Frames lying entirely behind the read
// pointer are dropped (the analogue of the client's frameTs <= lastFlushedTs
// guard) to avoid corrupting future ring slots via modular aliasing.
func (j *JitterBuffer) WriteChannel(frameTs uint32, chIdx int, pcm []int16) {
	if chIdx < 0 || chIdx >= j.channels || len(pcm) == 0 {
		return
	}
	n := int64(len(pcm))
	ts := int64(frameTs)

	j.mu.Lock()
	defer j.mu.Unlock()

	// Anchor the ring origin to the first frame's timestamp.
	if j.initTs < 0 {
		j.initTs = ts
		j.readTs = ts
		j.writeTs = ts
	}

	// Drop frames entirely in the past: their samples would alias onto slots
	// that belong to future timestamps and corrupt them.
	if ts+n <= j.readTs {
		return
	}

	// Write each sample at its absolute interleaved ring position.
	bufLen := int64(j.bufLen)
	for s := int64(0); s < n; s++ {
		slot := int((ts+s)%bufLen) * j.channels
		j.buf[slot+chIdx] = pcm[s]
	}

	// Advance the write frontier.
	if end := ts + n; end > j.writeTs {
		j.writeTs = end
	}

	// Bound the buffering latency. Reads advance only at the mix clock, so any
	// latency the buffer accumulates (a burst at startup, or growth while
	// underrunning) would otherwise sit here until the 2 s ring wraps — adding
	// delay that never drains. If occupancy exceeds the prebuffer target by more
	// than the drift tolerance (maxDepth), snap the read pointer forward to the
	// target depth, rounded UP to a frame boundary so whole-frame reads stay
	// aligned: a one-time skip that keeps latency low. maxDepth < bufLen, so this
	// also subsumes the ring-overflow guard.
	if j.writeTs-j.readTs > j.maxDepth {
		// Drain to a safe depth (prebuffer + 2 frames), not the bare prebuffer, so
		// the buffer doesn't immediately underrun after the snap.
		target := j.writeTs - (j.prebuffer + 2*int64(j.frameSize))
		if rem := (target - j.initTs) % int64(j.frameSize); rem != 0 {
			target += int64(j.frameSize) - rem
		}
		j.readTs = target
		j.overflows++
	}

	// Begin readout once the prebuffer threshold is reached.
	if !j.started && (j.writeTs-j.readTs) >= j.prebuffer {
		j.started = true
	}
}

// ReadFrame copies the next frameSize-sample frame for every channel into out
// (out[ch][0:frameSize]) and advances the read pointer. out must have at least
// channels slices, each of length >= frameSize.
//
// Return values:
//   - (false, false): still prebuffering — out is left untouched, nothing to emit.
//   - (true, true):   underrun — out is zero-filled and the read pointer is HELD
//     (so playback resumes in phase once data arrives).
//   - (true, false):  a real frame was copied and the read pointer advanced.
//
// Each consumed ring slot is zeroed on read, so a channel that never arrived for
// this frame reads back as silence.
func (j *JitterBuffer) ReadFrame(out [][]int16) (started, underrun bool) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if !j.started {
		return false, false
	}

	fs := int64(j.frameSize)
	if j.writeTs-j.readTs < fs {
		// Underrun: emit silence, hold the read pointer.
		for c := 0; c < j.channels && c < len(out); c++ {
			dst := out[c]
			for s := 0; s < j.frameSize && s < len(dst); s++ {
				dst[s] = 0
			}
		}
		j.underruns++
		return true, true
	}

	bufLen := int64(j.bufLen)
	for s := int64(0); s < fs; s++ {
		slot := int((j.readTs+s)%bufLen) * j.channels
		for c := 0; c < j.channels; c++ {
			if c < len(out) && int(s) < len(out[c]) {
				out[c][s] = j.buf[slot+c]
			}
			j.buf[slot+c] = 0 // clear the slot so missing channels read as silence
		}
	}
	j.readTs += fs
	return true, false
}

// Stats returns a snapshot of the buffer's current state.
func (j *JitterBuffer) Stats() Stats {
	j.mu.Lock()
	defer j.mu.Unlock()
	depth := int(j.writeTs - j.readTs)
	if depth < 0 {
		depth = 0
	}
	return Stats{
		Started:   j.started,
		Depth:     depth,
		Underruns: j.underruns,
		Overflows: j.overflows,
		ReadTs:    j.readTs,
		WriteTs:   j.writeTs,
	}
}
