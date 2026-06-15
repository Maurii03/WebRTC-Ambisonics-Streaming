// Package room hosts the multi-client mixing logic: one Room per conference,
// owning the set of connected participants and a single self-correcting mix-clock
// goroutine. Each tick the clock reads every participant's current 16-channel
// frame and sums the soundfields (B-format is linear → channel-wise addition).
//
// MINUS-ONE mixing (Phase 3): the clock accumulates the global sum S once in
// int32, then sends each client i the mix S − contribution_i — the soundfield of
// everyone EXCEPT itself — so there is no self-echo. By B-format linearity this is
// exact, and it costs one extra subtract per client instead of an independent N−1
// sum per client, so the mix stays O(N) overall and O(1) per client. The 16N
// re-encodes (parallelized in package session) are what scale with N — the thesis
// measurement. With a single client in a room, S − itself = silence (expected).
//
// The package is PURE GO — no Pion, no cgo. It talks to sessions only through the
// Participant interface, so the mixer is unit-testable with stub participants.
package room

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// sampleRate is the fixed soundfield sample rate (Hz); the mix-clock period is
// frameSize/sampleRate.
const sampleRate = 48000

// Participant is one client as the mixer sees it. Session implements this.
type Participant interface {
	// ID is a stable unique identifier (used as the roster key).
	ID() string
	// ReadInputFrame pulls this participant's next input frame (channels ×
	// frameSize) from its jitter buffer into out, advancing its read pointer.
	// It returns false while the participant is still prebuffering OR is gated as
	// silent by VAD; in that case it contributes silence to the mix and out is
	// treated as zero (so a minus-one mix leaves it hearing everyone else).
	ReadInputFrame(out [][]int16) bool
	// IsRecording reports whether this participant is currently inside a recording
	// window (it recently sent frames flagged isRec). The mix marks its output
	// isRec if ANY contributor is recording, so all receivers record the same span.
	IsRecording() bool
	// SubmitMix hands a mixed output frame (channels × frameSize) to this
	// participant's encode pipeline, stamped with the mix-timeline frameTs. isRec
	// flags the frame as part of a recording window. It must be non-blocking
	// (drop-oldest); the mix clock must never stall on it.
	SubmitMix(pcm [][]int16, frameTs uint32, isRec bool)
	// Metrics returns a telemetry snapshot for the /metrics endpoint.
	Metrics() ParticipantMetrics
}

// ParticipantMetrics is one client's telemetry snapshot (JSON-serialized by the
// /metrics endpoint).
type ParticipantMetrics struct {
	ID           string  `json:"id"`
	JBStarted    bool    `json:"jbStarted"`
	JBDepthMs    int     `json:"jbDepthMs"`
	Underruns    uint64  `json:"underruns"`
	Overflows    uint64  `json:"overflows"`
	DecodeCount  uint64  `json:"decodeCount"`
	EncodeCount  uint64  `json:"encodeCount"`
	DecodeUsAvg  float64 `json:"decodeUsAvg"`
	EncodeUsAvg  float64 `json:"encodeUsAvg"`
	PktsSent     uint64  `json:"pktsSent"`
	SendBuffered uint64  `json:"sendBufferedBytes"`
	InboundDrops uint64  `json:"inboundDrops"`
	MixDrops     uint64  `json:"mixDrops"`
	SendDrops    uint64  `json:"sendDrops"`
	Active       bool    `json:"active"` // last VAD decision (contributing to the mix)
}

// RoomMetrics is one room's telemetry snapshot.
type RoomMetrics struct {
	ID              string               `json:"id"`
	Members         int                  `json:"members"`
	Active          int                  `json:"active"` // contributors in the last mix tick
	MixTs           uint32               `json:"mixTs"`
	Ticks           uint64               `json:"ticks"`
	TickJitterMsAvg float64              `json:"tickJitterMsAvg"`
	TickJitterMsMax float64              `json:"tickJitterMsMax"`
	Sessions        []ParticipantMetrics `json:"sessions"`
}

// Room is one conference: a roster plus a single mix-clock goroutine.
type Room struct {
	id        string
	channels  int
	frameSize int

	mu      sync.Mutex
	members map[string]Participant
	clockOn bool

	done chan struct{}
	once sync.Once

	// Metrics written by mixClock, read by Metrics() (atomic).
	ticks       atomic.Uint64
	active      atomic.Int64
	curMixTs    atomic.Uint32
	jitNanosSum atomic.Int64
	jitNanosMax atomic.Int64
	jitCount    atomic.Int64
}

// NewRoom creates an empty room. The mix clock starts on the first Add.
func NewRoom(id string, channels, frameSize int) *Room {
	return &Room{
		id:        id,
		channels:  channels,
		frameSize: frameSize,
		members:   make(map[string]Participant),
		done:      make(chan struct{}),
	}
}

// Add registers a participant and starts the mix clock if this is the first one.
func (r *Room) Add(p Participant) {
	r.mu.Lock()
	r.members[p.ID()] = p
	startClock := !r.clockOn
	r.clockOn = true
	size := len(r.members)
	r.mu.Unlock()

	log.Printf("[room %s] + %s (size=%d)", r.id, p.ID(), size)
	if startClock {
		go r.mixClock()
	}
}

// Remove deregisters a participant by ID (no-op if absent).
func (r *Room) Remove(id string) {
	r.mu.Lock()
	_, ok := r.members[id]
	delete(r.members, id)
	size := len(r.members)
	r.mu.Unlock()
	if ok {
		log.Printf("[room %s] - %s (size=%d)", r.id, id, size)
	}
}

// Size reports the current member count.
func (r *Room) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.members)
}

// Close stops the mix clock. Idempotent.
func (r *Room) Close() {
	r.once.Do(func() { close(r.done) })
}

// snapshot copies the current roster so a tick iterates without holding the lock
// while it reads jitter buffers and submits mixes.
func (r *Room) snapshot() []Participant {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Participant, 0, len(r.members))
	for _, m := range r.members {
		out = append(out, m)
	}
	return out
}

// mixClock is the self-correcting per-room clock — the only place mixing happens.
// Deadlines are computed from a fixed monotonic base (base + tick*period), not by
// accumulating sleeps, so scheduling jitter does not compound. It does ONLY
// additions (never codec) and never blocks (SubmitMix is drop-oldest), so it
// cannot drift.
func (r *Room) mixClock() {
	period := time.Duration(r.frameSize) * time.Second / time.Duration(sampleRate)
	base := time.Now()

	acc := makeI32(r.channels, r.frameSize) // unsaturated global sum S
	out := makeI16(r.channels, r.frameSize) // per-client minus-one output (reused; SubmitMix copies)
	var inbufs [][][]int16                  // per-member input frames this tick (grows with the roster)

	var (
		tick  int64 = 1
		mixTs uint32
	)
	timer := time.NewTimer(time.Until(base.Add(period)))
	defer timer.Stop()

	for {
		select {
		case <-r.done:
			return
		case <-timer.C:
		}

		// Mix-clock jitter: how late did this tick fire vs its scheduled deadline.
		if late := time.Since(base.Add(time.Duration(tick) * period)); late > 0 {
			r.jitNanosSum.Add(int64(late))
			r.jitCount.Add(1)
			for {
				cur := r.jitNanosMax.Load()
				if int64(late) <= cur || r.jitNanosMax.CompareAndSwap(cur, int64(late)) {
					break
				}
			}
		} else {
			r.jitCount.Add(1)
		}

		if members := r.snapshot(); len(members) > 0 {
			for len(inbufs) < len(members) {
				inbufs = append(inbufs, makeI16(r.channels, r.frameSize))
			}
			ready := accumulate(members, r.channels, r.frameSize, inbufs, acc)
			// Recording: if ANY contributor is inside a recording window, flag the
			// whole mix output so every receiver records the same span.
			rec := false
			for _, m := range members {
				if m.IsRecording() {
					rec = true
					break
				}
			}
			for i, m := range members {
				// Minus-one: client i hears S minus its own contribution.
				subtractAndSaturate(acc, inbufs[i], out, r.channels, r.frameSize)
				m.SubmitMix(out, mixTs, rec) // copies out; reusing it for the next member is safe
			}
			mixTs += uint32(r.frameSize)
			r.ticks.Add(1)
			r.active.Store(int64(ready))
			r.curMixTs.Store(mixTs)

			// Periodic diagnostic (~1 s): how many contributed and how hot is the sum.
			if tick%int64(sampleRate/r.frameSize) == 0 {
				log.Printf("[room %s] mix: members=%d active=%d mixTs=%d sumPeak=%d",
					r.id, len(members), ready, mixTs, peakAbs32(acc[0]))
			}
		}

		tick++
		timer.Reset(time.Until(base.Add(time.Duration(tick) * period)))
	}
}

// Metrics returns a telemetry snapshot of the room and its members.
func (r *Room) Metrics() RoomMetrics {
	members := r.snapshot()
	sessions := make([]ParticipantMetrics, 0, len(members))
	for _, m := range members {
		sessions = append(sessions, m.Metrics())
	}
	var avgMs float64
	if n := r.jitCount.Load(); n > 0 {
		avgMs = float64(r.jitNanosSum.Load()) / float64(n) / 1e6
	}
	return RoomMetrics{
		ID:              r.id,
		Members:         len(members),
		Active:          int(r.active.Load()),
		MixTs:           r.curMixTs.Load(),
		Ticks:           r.ticks.Load(),
		TickJitterMsAvg: avgMs,
		TickJitterMsMax: float64(r.jitNanosMax.Load()) / 1e6,
		Sessions:        sessions,
	}
}

// accumulate reads every member's current input frame into its own buffer
// (inbufs[i]) and sums them, channel-wise, into the int32 accumulator acc — the
// global soundfield S, kept UNSATURATED so the minus-one subtraction below is
// exact. A member that is still prebuffering contributes silence; its buffer is
// zeroed so the later (acc − inbufs[i]) leaves it hearing everyone else. acc is
// caller-owned and reused. It returns how many members actually contributed.
func accumulate(members []Participant, channels, frameSize int, inbufs [][][]int16, acc [][]int32) int {
	for c := 0; c < channels; c++ {
		ac := acc[c]
		for s := 0; s < frameSize; s++ {
			ac[s] = 0
		}
	}
	ready := 0
	for i, m := range members {
		buf := inbufs[i]
		if m.ReadInputFrame(buf) {
			ready++
			for c := 0; c < channels; c++ {
				ac := acc[c]
				sc := buf[c]
				for s := 0; s < frameSize; s++ {
					ac[s] += int32(sc[s])
				}
			}
		} else {
			for c := 0; c < channels; c++ { // zero so (acc − buf) = acc (hears everyone)
				b := buf[c]
				for s := 0; s < frameSize; s++ {
					b[s] = 0
				}
			}
		}
	}
	return ready
}

// subtractAndSaturate writes out = saturate(acc − own): the global sum minus this
// client's own contribution, the minus-one mix. Subtracting before saturating
// keeps the result exact (the sum of all OTHER clients) regardless of headroom.
func subtractAndSaturate(acc [][]int32, own [][]int16, out [][]int16, channels, frameSize int) {
	for c := 0; c < channels; c++ {
		oc := out[c]
		ac := acc[c]
		ow := own[c]
		for s := 0; s < frameSize; s++ {
			oc[s] = saturate(ac[s] - int32(ow[s]))
		}
	}
}

// peakAbs32 returns the maximum absolute value in an int32 channel (diagnostics).
func peakAbs32(ch []int32) int {
	peak := 0
	for _, v := range ch {
		a := int(v)
		if a < 0 {
			a = -a
		}
		if a > peak {
			peak = a
		}
	}
	return peak
}

// saturate clamps an int32 accumulator sample to the int16 range (hard limiter).
func saturate(v int32) int16 {
	switch {
	case v > 32767:
		return 32767
	case v < -32768:
		return -32768
	default:
		return int16(v)
	}
}

func makeI16(channels, frameSize int) [][]int16 {
	b := make([][]int16, channels)
	for c := range b {
		b[c] = make([]int16, frameSize)
	}
	return b
}

func makeI32(channels, frameSize int) [][]int32 {
	b := make([][]int32, channels)
	for c := range b {
		b[c] = make([]int32, frameSize)
	}
	return b
}
