package room

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubParticipant is a fake Participant: it emits a fixed per-channel value as its
// input frame and records the mixes submitted to it.
type stubParticipant struct {
	id     string
	value  int16 // value written to every sample of every channel on read
	ready  bool  // ReadInputFrame return value (false = prebuffering → silence)
	frames int   // number of input frames produced (telemetry)

	recording bool // IsRecording() return value

	mu       sync.Mutex
	lastMix  [][]int16
	mixCount int
	lastTs   uint32
	lastRec  bool // isRec flag of the last submitted mix
}

func (p *stubParticipant) ID() string { return p.id }

func (p *stubParticipant) IsRecording() bool { return p.recording }

func (p *stubParticipant) ReadInputFrame(out [][]int16) bool {
	if !p.ready {
		return false
	}
	for c := range out {
		for s := range out[c] {
			out[c][s] = p.value
		}
	}
	p.frames++
	return true
}

func (p *stubParticipant) SubmitMix(pcm [][]int16, frameTs uint32, isRec bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Copy: the mixer reuses its mix buffer across ticks.
	cp := make([][]int16, len(pcm))
	for c := range pcm {
		cp[c] = append([]int16(nil), pcm[c]...)
	}
	p.lastMix = cp
	p.lastTs = frameTs
	p.lastRec = isRec
	p.mixCount++
}

func (p *stubParticipant) Metrics() ParticipantMetrics { return ParticipantMetrics{ID: p.id} }

func (p *stubParticipant) snapshotMix() ([][]int16, uint32, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastMix, p.lastTs, p.mixCount
}

// mixMinusOne is a test helper that runs one accumulate + per-member
// subtract-and-saturate pass and returns each member's minus-one output.
func mixMinusOne(members []Participant, channels, frameSize int) [][][]int16 {
	inbufs := make([][][]int16, len(members))
	for i := range inbufs {
		inbufs[i] = makeI16(channels, frameSize)
	}
	acc := makeI32(channels, frameSize)
	accumulate(members, channels, frameSize, inbufs, acc)

	outs := make([][][]int16, len(members))
	for i := range members {
		outs[i] = makeI16(channels, frameSize)
		subtractAndSaturate(acc, inbufs[i], outs[i], channels, frameSize)
	}
	return outs
}

// TestMinusOneEachHearsOthers verifies each client receives the sum of everyone
// EXCEPT itself (no self-echo).
func TestMinusOneEachHearsOthers(t *testing.T) {
	const channels, frameSize = 16, 8
	members := []Participant{
		&stubParticipant{id: "a", value: 1000, ready: true},
		&stubParticipant{id: "b", value: 2000, ready: true},
		&stubParticipant{id: "c", value: 3000, ready: true},
	}
	want := []int16{2000 + 3000, 1000 + 3000, 1000 + 2000} // S − own for a,b,c

	outs := mixMinusOne(members, channels, frameSize)
	for i := range members {
		for c := 0; c < channels; c++ {
			for s := 0; s < frameSize; s++ {
				if outs[i][c][s] != want[i] {
					t.Fatalf("member %d out[%d][%d] = %d, want %d", i, c, s, outs[i][c][s], want[i])
				}
			}
		}
	}
}

// TestMinusOneSingleClientIsSilent verifies S − itself = silence for N=1.
func TestMinusOneSingleClientIsSilent(t *testing.T) {
	const channels, frameSize = 4, 4
	members := []Participant{&stubParticipant{id: "a", value: 12345, ready: true}}
	outs := mixMinusOne(members, channels, frameSize)
	for c := 0; c < channels; c++ {
		for s := 0; s < frameSize; s++ {
			if outs[0][c][s] != 0 {
				t.Fatalf("N=1 out[%d][%d] = %d, want 0 (no self-echo)", c, s, outs[0][c][s])
			}
		}
	}
}

// TestMinusOneSkipsNotReady confirms a prebuffering peer contributes silence and
// still hears everyone else.
func TestMinusOneSkipsNotReady(t *testing.T) {
	const channels, frameSize = 4, 4
	members := []Participant{
		&stubParticipant{id: "a", value: 5000, ready: true},
		&stubParticipant{id: "b", value: 9999, ready: false}, // not ready → contributes 0
	}
	outs := mixMinusOne(members, channels, frameSize)
	// a hears everyone but itself: only b contributes, but b is silent → 0.
	// b hears everyone but itself: only a contributes 5000.
	for c := 0; c < channels; c++ {
		for s := 0; s < frameSize; s++ {
			if outs[0][c][s] != 0 {
				t.Fatalf("a out = %d, want 0 (b not contributing)", outs[0][c][s])
			}
			if outs[1][c][s] != 5000 {
				t.Fatalf("b out = %d, want 5000 (hears a)", outs[1][c][s])
			}
		}
	}
}

// TestMinusOneSaturates verifies the per-client mix saturates to int16.
func TestMinusOneSaturates(t *testing.T) {
	const channels, frameSize = 2, 2
	members := []Participant{
		&stubParticipant{id: "a", value: 1, ready: true},
		&stubParticipant{id: "b", value: 20000, ready: true},
		&stubParticipant{id: "c", value: 20000, ready: true}, // a hears 40000 → saturate
	}
	outs := mixMinusOne(members, channels, frameSize)
	for c := 0; c < channels; c++ {
		for s := 0; s < frameSize; s++ {
			if outs[0][c][s] != 32767 {
				t.Fatalf("a out = %d, want 32767 (saturated sum of b+c)", outs[0][c][s])
			}
		}
	}
}

// TestRoomClockDistributesMix runs the real mix clock for a short while and
// asserts each member receives the minus-one mix (the OTHER member's audio) on a
// monotonic frameTs timeline.
func TestRoomClockDistributesMix(t *testing.T) {
	const channels, frameSize = 16, 480 // 10 ms ticks for a quick test
	a := &stubParticipant{id: "a", value: 1000, ready: true}
	b := &stubParticipant{id: "b", value: 4000, ready: true}

	r := NewRoom("t", channels, frameSize)
	r.Add(a)
	r.Add(b)
	defer r.Close()

	// Let several ticks elapse.
	time.Sleep(120 * time.Millisecond)
	r.Close()

	mixA, tsA, countA := a.snapshotMix()
	mixB, _, countB := b.snapshotMix()
	if countA < 3 || countB < 3 {
		t.Fatalf("too few mixes distributed: a=%d b=%d (want >= 3 each)", countA, countB)
	}
	if mixA == nil || mixB == nil {
		t.Fatal("a or b received no mix")
	}
	// Minus-one: a hears b (4000), b hears a (1000) — never themselves.
	for c := 0; c < channels; c++ {
		if mixA[c][0] != 4000 {
			t.Fatalf("a's mix ch %d = %d, want 4000 (b's audio)", c, mixA[c][0])
		}
		if mixB[c][0] != 1000 {
			t.Fatalf("b's mix ch %d = %d, want 1000 (a's audio)", c, mixB[c][0])
		}
	}
	if tsA%uint32(frameSize) != 0 {
		t.Fatalf("frameTs %d not a multiple of frameSize %d", tsA, frameSize)
	}
}

// TestRecordingPropagates verifies the mix flags EVERY downlink isRec when any
// single contributor is recording, so all receivers record the same window.
func TestRecordingPropagates(t *testing.T) {
	const channels, frameSize = 16, 480
	a := &stubParticipant{id: "a", value: 1000, ready: true, recording: true}
	b := &stubParticipant{id: "b", value: 2000, ready: true} // not recording itself
	r := NewRoom("t", channels, frameSize)
	r.Add(a)
	r.Add(b)
	time.Sleep(120 * time.Millisecond)
	r.Close()

	a.mu.Lock()
	ar := a.lastRec
	a.mu.Unlock()
	b.mu.Lock()
	br := b.lastRec
	b.mu.Unlock()
	if !ar || !br {
		t.Fatalf("recording not propagated to all downlinks: a.lastRec=%v b.lastRec=%v (want both true)", ar, br)
	}
}

// TestManagerLifecycle checks rooms are created on join and destroyed when empty.
func TestManagerLifecycle(t *testing.T) {
	m := NewManager(16, 480)
	a := &stubParticipant{id: "a", ready: true}
	b := &stubParticipant{id: "b", ready: true}

	m.Join("r1", a)
	m.Join("r1", b)
	m.Join("r2", a)
	if got := m.Rooms(); got != 2 {
		t.Fatalf("rooms = %d, want 2", got)
	}

	m.Leave("r1", "a")
	if got := m.Rooms(); got != 2 {
		t.Fatalf("rooms after one leave = %d, want 2 (r1 still has b)", got)
	}
	m.Leave("r1", "b")
	if got := m.Rooms(); got != 1 {
		t.Fatalf("rooms after r1 empty = %d, want 1", got)
	}
	m.Leave("r2", "a")
	if got := m.Rooms(); got != 0 {
		t.Fatalf("rooms after all empty = %d, want 0", got)
	}
}

// TestManagerRoomsIndependent verifies two rooms mix independently: members hear
// only their own room's others, never the other room's audio.
func TestManagerRoomsIndependent(t *testing.T) {
	const channels, frameSize = 4, 480
	m := NewManager(channels, frameSize)
	a := &stubParticipant{id: "a", value: 1000, ready: true}
	b := &stubParticipant{id: "b", value: 2000, ready: true}
	c := &stubParticipant{id: "c", value: 3000, ready: true}
	m.Join("r1", a)
	m.Join("r1", b)
	m.Join("r2", c)
	defer func() { m.Leave("r1", "a"); m.Leave("r1", "b"); m.Leave("r2", "c") }()

	time.Sleep(120 * time.Millisecond)

	mixA, _, cntA := a.snapshotMix()
	mixB, _, cntB := b.snapshotMix()
	mixC, _, cntC := c.snapshotMix()
	if cntA < 3 || cntB < 3 || cntC < 3 {
		t.Fatalf("too few mixes: a=%d b=%d c=%d", cntA, cntB, cntC)
	}
	// r1: a hears only b (2000), b hears only a (1000) — c (3000, in r2) must not leak.
	if mixA[0][0] != 2000 {
		t.Fatalf("a heard %d, want 2000 (only b; r2's c must not leak)", mixA[0][0])
	}
	if mixB[0][0] != 1000 {
		t.Fatalf("b heard %d, want 1000 (only a; r2's c must not leak)", mixB[0][0])
	}
	// r2: c is alone → minus-one of itself = silence (r1 must not leak in).
	if mixC[0][0] != 0 {
		t.Fatalf("c heard %d, want 0 (alone in r2)", mixC[0][0])
	}
	if m.Rooms() != 2 {
		t.Fatalf("rooms = %d, want 2", m.Rooms())
	}
}

// TestRoomConcurrentAddRemove is a race-detector smoke test for the roster.
func TestRoomConcurrentAddRemove(t *testing.T) {
	r := NewRoom("t", 16, 480)
	defer r.Close()
	var wg sync.WaitGroup
	var n atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := &stubParticipant{id: string(rune('a' + i)), value: 100, ready: true}
			for j := 0; j < 50; j++ {
				r.Add(p)
				_ = r.Size()
				r.Remove(p.ID())
				n.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if n.Load() != 8*50 {
		t.Fatalf("iterations = %d, want %d", n.Load(), 8*50)
	}
}
