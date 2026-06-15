package proto

import (
	"bytes"
	"testing"
)

// helper: build a payload of length n with a recognizable per-channel pattern.
func payload(ch uint8, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(int(ch)*7 + i)
	}
	return b
}

func TestBundleRoundTrip(t *testing.T) {
	// Mix of small (<128 → 1-byte varint) and large (≥128 → 2-byte varint) payloads.
	in := []ChannelPayload{
		{ChIdx: 9, Payload: payload(9, 200)}, // out of order on input
		{ChIdx: 0, Payload: payload(0, 20)},
		{ChIdx: 4, Payload: payload(4, 130)}, // crosses the 128 varint boundary
		{ChIdx: 15, Payload: payload(15, 1)},
	}
	const ts = 0x0123_4567
	buf, err := MarshalBundle(ts, in, false)
	if err != nil {
		t.Fatalf("MarshalBundle: %v", err)
	}
	gotTs, got, gotRec, err := ParseBundle(buf)
	if err != nil {
		t.Fatalf("ParseBundle: %v", err)
	}
	if gotRec {
		t.Fatalf("isRec: got true, want false")
	}
	if gotTs != ts {
		t.Fatalf("frameTs: got %d want %d", gotTs, ts)
	}
	if len(got) != len(in) {
		t.Fatalf("channel count: got %d want %d", len(got), len(in))
	}
	// got is ascending by chIdx; build a map from input to compare.
	want := map[uint8][]byte{}
	for _, c := range in {
		want[c.ChIdx] = c.Payload
	}
	var prev int = -1
	for _, c := range got {
		if int(c.ChIdx) <= prev {
			t.Fatalf("channels not ascending: %d after %d", c.ChIdx, prev)
		}
		prev = int(c.ChIdx)
		if !bytes.Equal(c.Payload, want[c.ChIdx]) {
			t.Fatalf("ch %d payload mismatch: got %d bytes want %d", c.ChIdx, len(c.Payload), len(want[c.ChIdx]))
		}
	}
}

func TestBundleSingleChannel(t *testing.T) {
	// k=1: no length varints, whole region is the one payload.
	buf, err := MarshalBundle(42, []ChannelPayload{{ChIdx: 7, Payload: payload(7, 50)}}, false)
	if err != nil {
		t.Fatalf("MarshalBundle: %v", err)
	}
	ts, got, _, err := ParseBundle(buf)
	if err != nil || ts != 42 || len(got) != 1 || got[0].ChIdx != 7 || len(got[0].Payload) != 50 {
		t.Fatalf("single-channel round trip failed: ts=%d got=%v err=%v", ts, got, err)
	}
}

func TestBundleErrors(t *testing.T) {
	if _, _, _, err := ParseBundle([]byte{1, 2, 3}); err != ErrBundleShort {
		t.Fatalf("short: got %v", err)
	}
	if _, _, _, err := ParseBundle([]byte{0x20, 0, 0, 0, 0, 0, 0}); err != ErrBundleVersion {
		t.Fatalf("version: got %v (want ErrBundleVersion)", err)
	}
	if _, err := MarshalBundle(0, nil, false); err != ErrBundleEmpty {
		t.Fatalf("empty: got %v", err)
	}
	dup := []ChannelPayload{{ChIdx: 3, Payload: []byte{1}}, {ChIdx: 3, Payload: []byte{2}}}
	if _, err := MarshalBundle(0, dup, false); err != ErrChannelRange {
		t.Fatalf("dup channel: got %v", err)
	}
}

// TestBundleIsRec verifies the recording flag round-trips and does not disturb
// the version, frameTs, or channel payloads.
func TestBundleIsRec(t *testing.T) {
	in := []ChannelPayload{{ChIdx: 0, Payload: payload(0, 30)}, {ChIdx: 5, Payload: payload(5, 40)}}
	const ts = 0xABCDEF
	for _, rec := range []bool{false, true} {
		buf, err := MarshalBundle(ts, in, rec)
		if err != nil {
			t.Fatalf("MarshalBundle(rec=%v): %v", rec, err)
		}
		if buf[0]>>4 != BundleVersion {
			t.Fatalf("rec=%v: version nibble corrupted: byte0=0x%02X", rec, buf[0])
		}
		gotTs, got, gotRec, err := ParseBundle(buf)
		if err != nil {
			t.Fatalf("ParseBundle(rec=%v): %v", rec, err)
		}
		if gotRec != rec {
			t.Errorf("isRec round-trip: got %v want %v", gotRec, rec)
		}
		if gotTs != ts || len(got) != len(in) || got[0].ChIdx != 0 || got[1].ChIdx != 5 {
			t.Errorf("rec=%v: payload/ts disturbed: ts=%d chans=%d", rec, gotTs, len(got))
		}
	}
}

func TestPlanLayout(t *testing.T) {
	cases := []struct {
		frame  int
		wantP  int
		wantMs float64
	}{
		{120, 2, 2.5}, {240, 2, 5}, {480, 2, 10}, {960, 4, 20}, {1920, 9, 40}, {2880, 11, 60},
	}
	for _, tc := range cases {
		plan := PlanLayout(tc.frame)
		if len(plan) != tc.wantP {
			t.Errorf("frame=%d (%.1fms): P=%d want %d", tc.frame, tc.wantMs, len(plan), tc.wantP)
		}

		// (a) every channel 0..15 is covered at least once.
		covered := map[uint8]int{}
		for _, pkt := range plan {
			for _, ch := range pkt {
				covered[ch]++
			}
		}
		for c := uint8(0); c < Channels; c++ {
			if covered[c] < 1 {
				t.Errorf("frame=%d: channel %d not covered", tc.frame, c)
			}
		}
		// (b) base layer ch0..3 appears in ≥2 packets (R=2).
		for c := uint8(0); c < 4; c++ {
			if covered[c] < 2 {
				t.Errorf("frame=%d: base ch %d only in %d packets (want ≥2)", tc.frame, c, covered[c])
			}
		}
		// (c) every packet fits the MTU budget at the reference per-channel size.
		ref := BitRate * tc.frame / (SampleRate * 8)
		for i, pkt := range plan {
			if got := len(pkt) * ref; got > MTUChannelBudget {
				t.Errorf("frame=%d pkt %d: %d bytes > MTU budget %d", tc.frame, i, got, MTUChannelBudget)
			}
		}
		// (d) the two base copies must land in DIFFERENT packets, so a single
		//     packet loss can never take all of ch0..3.
		basePkts := map[uint8][]int{}
		for i, pkt := range plan {
			for _, ch := range pkt {
				if ch < 4 {
					basePkts[ch] = append(basePkts[ch], i)
				}
			}
		}
		for c := uint8(0); c < 4; c++ {
			if len(basePkts[c]) < 2 || basePkts[c][0] == basePkts[c][1] {
				t.Errorf("frame=%d: base ch %d not in two distinct packets: %v", tc.frame, c, basePkts[c])
			}
		}
	}
}

// TestBundleLossKeepsBase simulates the receiver: marshal a frame per the P=2
// layout, drop ONE packet, and confirm the base layer (ch0–3) is still fully
// recoverable from the surviving packet (R=2), while only enhancement channels
// go missing.
func TestBundleLossKeepsBase(t *testing.T) {
	const frame = 480 // 10 ms → P=2
	const ts = 99_000
	plan := PlanLayout(frame)
	if len(plan) != 2 {
		t.Fatalf("expected P=2 for 10ms, got %d", len(plan))
	}
	ref := BitRate * frame / (SampleRate * 8)

	for dropped := 0; dropped < len(plan); dropped++ {
		// Build all packets, then "lose" the dropped one.
		recovered := map[uint8]bool{}
		for i, pkt := range plan {
			if i == dropped {
				continue
			}
			chans := make([]ChannelPayload, len(pkt))
			for j, ch := range pkt {
				chans[j] = ChannelPayload{ChIdx: ch, Payload: payload(ch, ref)}
			}
			buf, err := MarshalBundle(ts, chans, false)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			_, got, _, err := ParseBundle(buf)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			for _, c := range got {
				recovered[c.ChIdx] = true // receiver dedups: first copy wins
			}
		}
		// Base must survive any single-packet loss.
		for c := uint8(0); c < 4; c++ {
			if !recovered[c] {
				t.Errorf("dropping pkt %d lost base ch %d (R=2 should protect it)", dropped, c)
			}
		}
	}
}
