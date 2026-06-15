package codec

import (
	"math"
	"testing"

	"github.com/Maurii03/ambirtc-mcu/internal/proto"
)

// sineInt16 fills a frame with a full-amplitude * gain sine at freqHz.
func sineInt16(n int, freqHz, gain float64) []int16 {
	pcm := make([]int16, n)
	for i := range pcm {
		s := gain * math.Sin(2*math.Pi*freqHz*float64(i)/proto.SampleRate)
		pcm[i] = int16(s * 32767)
	}
	return pcm
}

// TestEncodeDecodeRoundTrip encodes a 20 ms / 960-sample 48 kHz mono frame and
// decodes it, asserting the decoded sample count and that the output is finite
// and non-silent (carries real energy, i.e. Opus reproduced the tone).
func TestEncodeDecodeRoundTrip(t *testing.T) {
	enc, err := NewEncoder(AppRestrictedLowdelay, proto.BitRate)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	dec, err := NewDecoder()
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}

	const frame = proto.DefaultFrameSize // 960 samples
	in := sineInt16(frame, 440, 0.5)

	// Opus has encoder look-ahead, so the first frame is partly ramp-in. Push a
	// few frames so the decoded frame we inspect carries the steady-state tone.
	pkt := make([]byte, MaxPacketBytes)
	out := make([]int16, MaxFrameSamples)
	var samples int
	for i := 0; i < 3; i++ {
		n, err := enc.Encode(in, pkt)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		if n <= 0 || n > MaxPacketBytes {
			t.Fatalf("Encode wrote %d bytes, want 1..%d", n, MaxPacketBytes)
		}
		samples, err = dec.Decode(pkt[:n], out)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}

	if samples != frame {
		t.Fatalf("decoded %d samples, want %d", samples, frame)
	}

	// Output must be finite and non-silent. int16 is inherently finite; assert a
	// non-trivial peak so we know we didn't decode silence.
	var peak int
	for _, s := range out[:samples] {
		a := int(s)
		if a < 0 {
			a = -a
		}
		if a > peak {
			peak = a
		}
	}
	if peak < 1000 {
		t.Fatalf("decoded peak amplitude = %d, want >= 1000 (output is silent?)", peak)
	}
}

// TestEncodeRejectsBadFrameSize confirms libopus refuses a non-Opus frame size,
// which our wrapper surfaces as an error.
func TestEncodeRejectsBadFrameSize(t *testing.T) {
	enc, err := NewEncoder(AppAudio, proto.BitRate)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	// 1000 is not in proto.ValidFrameSizes.
	if _, err := enc.Encode(make([]int16, 1000), make([]byte, MaxPacketBytes)); err == nil {
		t.Fatal("Encode(1000 samples) succeeded, want error")
	}
}

// TestFloat32Int16RoundTrip checks the documented float↔int16 boundary survives
// a round-trip within one quantization step.
func TestFloat32Int16RoundTrip(t *testing.T) {
	const n = 480
	src := make([]float32, n)
	for i := range src {
		src[i] = float32(math.Sin(2 * math.Pi * 100 * float64(i) / proto.SampleRate))
	}

	mid := make([]int16, n)
	back := make([]float32, n)
	Float32ToInt16(mid, src)
	Int16ToFloat32(back, mid)

	const tol = 1.0 / 32768.0 * 2 // two LSB of headroom for truncation
	for i := range src {
		if d := math.Abs(float64(src[i] - back[i])); d > tol {
			t.Fatalf("sample %d: |%.6f - %.6f| = %.6f > %.6f", i, src[i], back[i], d, tol)
		}
	}
}

// TestFloat32ToInt16Clamp verifies out-of-range floats saturate, not wrap.
func TestFloat32ToInt16Clamp(t *testing.T) {
	dst := make([]int16, 2)
	Float32ToInt16(dst, []float32{2.0, -2.0})
	if dst[0] != int16Max {
		t.Errorf("clamp high = %d, want %d", dst[0], int16Max)
	}
	if dst[1] != int16Min {
		t.Errorf("clamp low = %d, want %d", dst[1], int16Min)
	}
}
