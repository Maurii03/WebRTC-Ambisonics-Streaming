package audio

import "testing"

// makeOut allocates the destination slices ReadFrame writes into.
func makeOut(channels, frameSize int) [][]int16 {
	out := make([][]int16, channels)
	for c := range out {
		out[c] = make([]int16, frameSize)
	}
	return out
}

// writeFrame writes all channels of one frameTs with a per-channel marker value,
// so a read can prove channel separation and timestamp placement.
func writeFrame(j *JitterBuffer, channels, frameSize int, frameTs uint32, value func(ch int) int16) {
	for c := 0; c < channels; c++ {
		pcm := make([]int16, frameSize)
		v := value(c)
		for s := range pcm {
			pcm[s] = v
		}
		j.WriteChannel(frameTs, c, pcm)
	}
}

// TestTimestampIndexingAndChannelSeparation writes several frames (including one
// out of order) and confirms each frame reads back at its timestamp with each
// channel's distinct value intact.
func TestTimestampIndexingAndChannelSeparation(t *testing.T) {
	const channels, frameSize = 16, 4
	// prebuffer 1 frame so readout starts immediately after the first write.
	j := NewJitterBuffer(channels, frameSize, 64, frameSize)

	// Frame value = frameIdx*100 + channel, so both axes are identifiable.
	val := func(frameIdx int) func(int) int16 {
		return func(ch int) int16 { return int16(frameIdx*100 + ch) }
	}

	// Write frame 0, then 2, then 1 (out of order within the window).
	writeFrame(j, channels, frameSize, 0, val(0))
	writeFrame(j, channels, frameSize, uint32(2*frameSize), val(2))
	writeFrame(j, channels, frameSize, uint32(1*frameSize), val(1))

	out := makeOut(channels, frameSize)
	for f := 0; f < 3; f++ {
		started, underrun := j.ReadFrame(out)
		if !started || underrun {
			t.Fatalf("frame %d: started=%v underrun=%v, want started w/o underrun", f, started, underrun)
		}
		for c := 0; c < channels; c++ {
			want := int16(f*100 + c)
			for s := 0; s < frameSize; s++ {
				if out[c][s] != want {
					t.Fatalf("frame %d ch %d sample %d = %d, want %d", f, c, s, out[c][s], want)
				}
			}
		}
	}
}

// TestPrefillGate verifies readout does not start until the prebuffer depth is met.
func TestPrefillGate(t *testing.T) {
	const channels, frameSize = 16, 8
	// prebuffer of 2 frames: one frame must not be enough to start.
	j := NewJitterBuffer(channels, frameSize, 128, 2*frameSize)

	writeFrame(j, channels, frameSize, 0, func(int) int16 { return 1 })
	out := makeOut(channels, frameSize)
	if started, _ := j.ReadFrame(out); started {
		t.Fatal("readout started after 1 frame, want still prebuffering")
	}

	writeFrame(j, channels, frameSize, uint32(frameSize), func(int) int16 { return 1 })
	if started, underrun := j.ReadFrame(out); !started || underrun {
		t.Fatalf("after 2 frames started=%v underrun=%v, want started w/o underrun", started, underrun)
	}
}

// TestUnderrunSilenceAndResume verifies that reading past the written data emits
// zeros, holds the read pointer, and resumes in phase once new data arrives.
func TestUnderrunSilenceAndResume(t *testing.T) {
	const channels, frameSize = 16, 4
	j := NewJitterBuffer(channels, frameSize, 64, frameSize)

	writeFrame(j, channels, frameSize, 0, func(int) int16 { return 7 })
	out := makeOut(channels, frameSize)

	// Read the one available frame.
	if started, underrun := j.ReadFrame(out); !started || underrun {
		t.Fatalf("first read started=%v underrun=%v", started, underrun)
	}
	readTsBefore := j.Stats().ReadTs

	// Next read underruns: zeros, read pointer held.
	if started, underrun := j.ReadFrame(out); !started || !underrun {
		t.Fatalf("underrun read started=%v underrun=%v, want both true", started, underrun)
	}
	for c := 0; c < channels; c++ {
		for s := 0; s < frameSize; s++ {
			if out[c][s] != 0 {
				t.Fatalf("underrun ch %d sample %d = %d, want 0 (silence)", c, s, out[c][s])
			}
		}
	}
	if got := j.Stats().ReadTs; got != readTsBefore {
		t.Fatalf("read pointer moved during underrun: %d -> %d", readTsBefore, got)
	}
	if n := j.Stats().Underruns; n != 1 {
		t.Fatalf("underruns = %d, want 1", n)
	}

	// New data at the held timestamp resumes in phase.
	writeFrame(j, channels, frameSize, uint32(readTsBefore), func(int) int16 { return 9 })
	if started, underrun := j.ReadFrame(out); !started || underrun {
		t.Fatalf("resume read started=%v underrun=%v", started, underrun)
	}
	for c := 0; c < channels; c++ {
		if out[c][0] != 9 {
			t.Fatalf("resume ch %d = %d, want 9", c, out[c][0])
		}
	}
}

// TestOverflowSnapForward verifies that writing far beyond capacity snaps the
// read pointer forward, keeps it frame-aligned, and preserves the newest frame.
func TestOverflowSnapForward(t *testing.T) {
	const channels, frameSize = 4, 4
	// Small ring: capacity 16 samples -> maxSpan = 12 samples = 3 frames.
	j := NewJitterBuffer(channels, frameSize, 16, frameSize)

	// Anchor at frame 0, then jump far ahead so depth would exceed maxSpan.
	writeFrame(j, channels, frameSize, 0, func(int) int16 { return 1 })
	farTs := uint32(10 * frameSize) // depth 11 frames >> 3-frame capacity
	writeFrame(j, channels, frameSize, farTs, func(int) int16 { return 5 })

	st := j.Stats()
	if st.Overflows == 0 {
		t.Fatal("expected an overflow snap, got none")
	}
	// Read pointer must stay frame-aligned relative to initTs (0 here).
	if st.ReadTs%int64(frameSize) != 0 {
		t.Fatalf("read pointer %d not frame-aligned (frameSize %d)", st.ReadTs, frameSize)
	}
	// After the snap, depth must be within the ring's capacity.
	if st.Depth > 16-frameSize {
		t.Fatalf("depth %d exceeds maxSpan %d after snap", st.Depth, 16-frameSize)
	}

	// The newest frame (at farTs) must still be readable intact.
	out := makeOut(channels, frameSize)
	var got bool
	for i := 0; i < 4 && !got; i++ {
		started, underrun := j.ReadFrame(out)
		if !started {
			t.Fatal("buffer should be started after writes")
		}
		if !underrun && out[0][0] == 5 {
			got = true
		}
	}
	if !got {
		t.Fatal("newest frame (value 5) was lost after overflow snap")
	}
}
