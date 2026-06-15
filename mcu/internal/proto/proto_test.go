package proto

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestRoundTrip builds a packet with known fields, marshals it, parses it back,
// and asserts every field survives.
func TestRoundTrip(t *testing.T) {
	want := Packet{
		IsRec:   true,
		ChIdx:   5,
		SeqNum:  0xDEADBEEF,
		FrameTs: 48000,
		Payload: []byte{0x10, 0x20, 0x30, 0x40},
	}

	raw, err := want.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	if len(raw) != HeaderSize+len(want.Payload) {
		t.Fatalf("marshaled length = %d, want %d", len(raw), HeaderSize+len(want.Payload))
	}

	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.IsRec != want.IsRec || got.ChIdx != want.ChIdx ||
		got.SeqNum != want.SeqNum || got.FrameTs != want.FrameTs {
		t.Fatalf("parsed header = %+v, want %+v", got, want)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("parsed payload = % x, want % x", got.Payload, want.Payload)
	}
}

// TestByteLayout pins the exact on-wire byte positions so the format stays in
// lock-step with the browser client (shared/webrtc-node.js):
//
//	byte 0     = isRec<<7 | chIdx
//	bytes 1..4 = seqNum  uint32 LE
//	bytes 5..8 = frameTs uint32 LE
func TestByteLayout(t *testing.T) {
	p := Packet{IsRec: true, ChIdx: 0x0A, SeqNum: 0x01020304, FrameTs: 0x0A0B0C0D}
	raw, err := p.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}

	// Byte 0: isRec bit (0x80) OR channel index (0x0A).
	if raw[0] != (0x80 | 0x0A) {
		t.Errorf("byte0 = 0x%02X, want 0x%02X", raw[0], 0x80|0x0A)
	}
	// seqNum is little-endian at bytes 1..4.
	if got := binary.LittleEndian.Uint32(raw[1:5]); got != p.SeqNum {
		t.Errorf("seqNum LE = 0x%08X, want 0x%08X", got, p.SeqNum)
	}
	// Spot-check raw LE byte order for seqNum: 0x01020304 -> 04 03 02 01.
	if !bytes.Equal(raw[1:5], []byte{0x04, 0x03, 0x02, 0x01}) {
		t.Errorf("seqNum bytes = % x, want 04 03 02 01", raw[1:5])
	}
	// frameTs is little-endian at bytes 5..8.
	if got := binary.LittleEndian.Uint32(raw[5:9]); got != p.FrameTs {
		t.Errorf("frameTs LE = 0x%08X, want 0x%08X", got, p.FrameTs)
	}
	if !bytes.Equal(raw[5:9], []byte{0x0D, 0x0C, 0x0B, 0x0A}) {
		t.Errorf("frameTs bytes = % x, want 0D 0C 0B 0A", raw[5:9])
	}
}

// TestIsRecFlag checks both states of the recording bit and that it does not
// disturb the channel index.
func TestIsRecFlag(t *testing.T) {
	for _, isRec := range []bool{false, true} {
		raw, err := Packet{IsRec: isRec, ChIdx: 15}.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		wantHigh := byte(0)
		if isRec {
			wantHigh = 0x80
		}
		if raw[0]&0x80 != wantHigh {
			t.Errorf("isRec=%v: high bit = 0x%02X, want 0x%02X", isRec, raw[0]&0x80, wantHigh)
		}
		if raw[0]&chIdxMask != 15 {
			t.Errorf("isRec=%v: chIdx nibble = %d, want 15", isRec, raw[0]&chIdxMask)
		}
		got, err := Parse(raw)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if got.IsRec != isRec {
			t.Errorf("parsed IsRec = %v, want %v", got.IsRec, isRec)
		}
	}
}

// TestParseHeaderOnly verifies a bare 9-byte packet parses with an empty payload
// (this is exactly what the browser test client sends for the echo).
func TestParseHeaderOnly(t *testing.T) {
	raw, err := Packet{ChIdx: 3, SeqNum: 7, FrameTs: 960}.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	if len(raw) != HeaderSize {
		t.Fatalf("header-only length = %d, want %d", len(raw), HeaderSize)
	}
	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(got.Payload) != 0 {
		t.Errorf("payload len = %d, want 0", len(got.Payload))
	}
	if got.ChIdx != 3 || got.SeqNum != 7 || got.FrameTs != 960 {
		t.Errorf("parsed = %+v, want ChIdx=3 SeqNum=7 FrameTs=960", got)
	}
}

func TestParseShortPacket(t *testing.T) {
	if _, err := Parse(make([]byte, HeaderSize-1)); err != ErrShortPacket {
		t.Fatalf("Parse(8 bytes) err = %v, want ErrShortPacket", err)
	}
}

func TestMarshalChannelOutOfRange(t *testing.T) {
	if _, err := (Packet{ChIdx: 16}).MarshalBinary(); err != ErrChannelRange {
		t.Fatalf("MarshalBinary(ChIdx=16) err = %v, want ErrChannelRange", err)
	}
}

func TestIsValidFrameSize(t *testing.T) {
	for _, n := range ValidFrameSizes {
		if !IsValidFrameSize(n) {
			t.Errorf("IsValidFrameSize(%d) = false, want true", n)
		}
	}
	for _, n := range []int{0, 100, 128, 961, 3000, -960} {
		if IsValidFrameSize(n) {
			t.Errorf("IsValidFrameSize(%d) = true, want false", n)
		}
	}
	if !IsValidFrameSize(DefaultFrameSize) {
		t.Errorf("DefaultFrameSize %d must be a valid frame size", DefaultFrameSize)
	}
}

// TestAppendToReuse confirms AppendTo writes into a caller-provided buffer and
// is allocation-free when the buffer has capacity.
func TestAppendToReuse(t *testing.T) {
	buf := make([]byte, 0, 64)
	p := Packet{ChIdx: 1, SeqNum: 2, FrameTs: 3, Payload: []byte{9, 9}}
	out, err := p.AppendTo(buf)
	if err != nil {
		t.Fatalf("AppendTo: %v", err)
	}
	if &buf[:1][0] != &out[:1][0] {
		t.Errorf("AppendTo reallocated despite sufficient capacity")
	}
	got, err := Parse(out)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.ChIdx != 1 || got.SeqNum != 2 || got.FrameTs != 3 || !bytes.Equal(got.Payload, []byte{9, 9}) {
		t.Errorf("round-trip via AppendTo = %+v", got)
	}
}

// TestChannelBitRates pins each profile's order-tapered allocation and total
// budget; the browser mirror (multiuser/bundle.js channelBitrates) must match the
// default profile byte-for-byte.
func TestChannelBitRates(t *testing.T) {
	sum := func(b [Channels]int) int {
		t := 0
		for _, x := range b {
			t += x
		}
		return t
	}
	cases := []struct {
		p           BitrateProfile
		lo, mid, hi int
		total       int
	}{
		{BitrateUniform, 64000, 64000, 64000, 1024000},
		{BitrateHigh, 64000, 48000, 32000, 720000},
		{BitrateBalanced, 48000, 32000, 24000, 520000},
		{BitrateLow, 48000, 24000, 24000, 480000},
	}
	for _, c := range cases {
		b := ChannelBitRates(c.p)
		// Tiers: ch0-3 / ch4-8 / ch9-15.
		if b[0] != c.lo || b[3] != c.lo || b[4] != c.mid || b[8] != c.mid || b[9] != c.hi || b[15] != c.hi {
			t.Errorf("profile %d tiers = %d/%d/%d, want %d/%d/%d", c.p, b[0], b[4], b[9], c.lo, c.mid, c.hi)
		}
		if got := sum(b); got != c.total {
			t.Errorf("profile %d total = %d, want %d", c.p, got, c.total)
		}
	}
	// DefaultBitrateProfile must equal what the JS mirror hard-codes (520 kbps).
	if sum(ChannelBitRates(DefaultBitrateProfile)) != 520000 {
		t.Errorf("default profile total = %d, want 520000", sum(ChannelBitRates(DefaultBitrateProfile)))
	}
	for n := 1; n <= 4; n++ {
		if !IsValidBitrateProfile(n) {
			t.Errorf("profile %d should be valid", n)
		}
	}
	for _, n := range []int{0, 5, -1} {
		if IsValidBitrateProfile(n) {
			t.Errorf("profile %d should be invalid", n)
		}
	}
}
