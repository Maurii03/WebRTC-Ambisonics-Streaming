package proto

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"sort"
)

// Bundled wire format (v2): one network packet carries MULTIPLE channels of the
// SAME audio frame, so a frame ships in a handful of packets instead of 16. It
// also carries the R=2 redundancy of the base layer (ch0–3) for loss resilience.
//
// Why a second format: the legacy per-channel header is 9 bytes × 16 = 144 bytes
// of header per frame. At 2.5 ms the Opus payload is only ~20 bytes/channel, so
// the header dwarfs the audio AND the packet rate explodes (16 × 400 = 6400
// pkt/s). Bundling amortizes one header over many channels and lets the base
// layer be replicated cheaply.
//
// Header (little-endian), followed by the Opus payloads concatenated in
// ascending channel order:
//
//	byte 0      : version (high nibble)=1 | flags (low nibble, reserved 0)
//	bytes 1–4   : frameTs uint32 LE      (samples; shared by every channel here)
//	bytes 5–6   : chMask  uint16 LE      (bit c set ⇒ channel c present)
//	bytes 7..   : (k−1) payload lengths as unsigned LEB128 varints, ascending
//	              chIdx order; the LAST channel's length is IMPLICIT (= the bytes
//	              left after the others) → saves 1–2 bytes per packet.
//	then        : the k Opus payloads, concatenated, ascending chIdx order.
//
// k = popcount(chMask). Deliberately NO per-channel seqNum (frameTs + chMask
// drive assembly and loss detection) and NO isRec bit (recording is a
// legacy-format feature). Result: ~7 bytes + (k−1) small varints PER PACKET vs
// 9×k bytes in the legacy format.
const (
	// BundleVersion is the high nibble of byte 0; bump on any layout change.
	BundleVersion = 1

	// MTUChannelBudget is the per-packet budget (bytes) for the SUM of the Opus
	// payloads, leaving headroom inside a ~1200 B DataChannel datagram for the
	// bundle header and the SCTP/DTLS/UDP/IP stack. Layout planning never packs
	// more channels than fit here (assuming the reference per-channel size).
	MTUChannelBudget = 1130

	// BundleFlagIsRec is bit 0 of byte 0's low (flags) nibble: set when this frame
	// belongs to a synchronized recording window, so every receiver records the
	// same frames. Mirrors the legacy header's isRec bit (0x80) without a new byte.
	BundleFlagIsRec = 0x01
)

var (
	ErrBundleShort   = errors.New("proto: bundle shorter than 7-byte header")
	ErrBundleVersion = errors.New("proto: unknown bundle version")
	ErrBundleLength  = errors.New("proto: bundle length table overruns payload")
	ErrBundleEmpty   = errors.New("proto: bundle has no channels")
)

// ChannelPayload pairs a channel index with its raw mono Opus payload.
type ChannelPayload struct {
	ChIdx   uint8
	Payload []byte
}

// MarshalBundle encodes one frame's channels into a single bundled packet.
// chans must be non-empty, hold ≤16 entries with unique indices in [0,15], and
// all belong to the same frameTs. The channels are emitted in ascending chIdx
// order regardless of input order. isRec sets the recording flag in byte 0.
func MarshalBundle(frameTs uint32, chans []ChannelPayload, isRec bool) ([]byte, error) {
	if len(chans) == 0 {
		return nil, ErrBundleEmpty
	}
	if len(chans) > Channels {
		return nil, ErrChannelRange
	}

	sorted := append([]ChannelPayload(nil), chans...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ChIdx < sorted[j].ChIdx })

	var mask uint16
	total := 0
	for _, c := range sorted {
		if c.ChIdx >= Channels {
			return nil, ErrChannelRange
		}
		bit := uint16(1) << c.ChIdx
		if mask&bit != 0 {
			return nil, ErrChannelRange // duplicate channel
		}
		mask |= bit
		total += len(c.Payload)
	}

	buf := make([]byte, 0, 7+2*len(sorted)+total)
	var hdr [7]byte
	hdr[0] = byte(BundleVersion << 4)
	if isRec {
		hdr[0] |= BundleFlagIsRec
	}
	binary.LittleEndian.PutUint32(hdr[1:5], frameTs)
	binary.LittleEndian.PutUint16(hdr[5:7], mask)
	buf = append(buf, hdr[:]...)

	// Lengths for the first k−1 channels; the last is implicit (rest of packet).
	var vbuf [binary.MaxVarintLen64]byte
	for i := 0; i < len(sorted)-1; i++ {
		n := binary.PutUvarint(vbuf[:], uint64(len(sorted[i].Payload)))
		buf = append(buf, vbuf[:n]...)
	}
	for _, c := range sorted {
		buf = append(buf, c.Payload...)
	}
	return buf, nil
}

// ParseBundle decodes one bundled packet. The returned payloads alias buf (no
// copy) — copy them if buf is reused. Channels are returned in ascending order.
// isRec reports whether the recording flag was set in byte 0.
func ParseBundle(buf []byte) (frameTs uint32, chans []ChannelPayload, isRec bool, err error) {
	if len(buf) < 7 {
		return 0, nil, false, ErrBundleShort
	}
	if buf[0]>>4 != BundleVersion {
		return 0, nil, false, ErrBundleVersion
	}
	isRec = buf[0]&BundleFlagIsRec != 0
	frameTs = binary.LittleEndian.Uint32(buf[1:5])
	mask := binary.LittleEndian.Uint16(buf[5:7])
	k := bits.OnesCount16(mask)
	if k == 0 {
		return frameTs, nil, isRec, ErrBundleEmpty
	}

	idxs := make([]uint8, 0, k)
	for c := uint8(0); c < Channels; c++ {
		if mask&(uint16(1)<<c) != 0 {
			idxs = append(idxs, c)
		}
	}

	off := 7
	lengths := make([]int, k)
	sum := 0
	for i := 0; i < k-1; i++ {
		v, n := binary.Uvarint(buf[off:])
		if n <= 0 {
			return 0, nil, false, ErrBundleLength
		}
		off += n
		lengths[i] = int(v)
		sum += int(v)
	}
	region := buf[off:]
	if sum > len(region) {
		return 0, nil, false, ErrBundleLength
	}
	lengths[k-1] = len(region) - sum // implicit last length

	chans = make([]ChannelPayload, k)
	p := 0
	for i := 0; i < k; i++ {
		chans[i] = ChannelPayload{ChIdx: idxs[i], Payload: region[p : p+lengths[i]]}
		p += lengths[i]
	}
	return frameTs, chans, isRec, nil
}

// MaxChannelsPerPacket reports how many channels of the given frame size fit in
// one bundled packet within MTUChannelBudget, using the reference per-channel
// Opus size at BitRate. Always ≥1.
func MaxChannelsPerPacket(frameSize int) int {
	ref := BitRate * frameSize / (SampleRate * 8) // bytes/channel at the reference bitrate
	if ref < 1 {
		ref = 1
	}
	m := MTUChannelBudget / ref
	if m < 1 {
		m = 1
	}
	if m > Channels {
		m = Channels
	}
	return m
}

// PlanLayout returns the bundled-packet plan for a frame size: each element is
// the ascending channel set for one network packet. The base layer (ch0–3) is
// included in TWO packets (R=2), so any single packet loss leaves the 1st-order
// soundfield intact; enhancement channels are grouped by ambisonic order
// (order2 = ch4–8, order3 = ch9–15). Packets never exceed the MTU budget.
//
//   - frames where base+order2 and base+order3 both fit one packet (≤10 ms):
//     P=2 — [base+order2] [base+order3], perfectly order-aligned.
//   - larger frames (MTU-bound): dedicated base copies + per-order groups, each
//     chunked to ≤ MaxChannelsPerPacket.
func PlanLayout(frameSize int) [][]uint8 {
	base := []uint8{0, 1, 2, 3}
	order2 := []uint8{4, 5, 6, 7, 8}
	order3 := []uint8{9, 10, 11, 12, 13, 14, 15}
	m := MaxChannelsPerPacket(frameSize)

	if len(base)+len(order2) <= m && len(base)+len(order3) <= m {
		return [][]uint8{
			concatU8(base, order2),
			concatU8(base, order3),
		}
	}

	var out [][]uint8
	out = append(out, chunkU8(base, m)...)   // base copy 1
	out = append(out, chunkU8(order2, m)...) // order 2
	out = append(out, chunkU8(order3, m)...) // order 3
	out = append(out, chunkU8(base, m)...)   // base copy 2 (R=2)
	return out
}

func concatU8(a, b []uint8) []uint8 {
	out := make([]uint8, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	return out
}

func chunkU8(s []uint8, n int) [][]uint8 {
	if n < 1 {
		n = 1
	}
	var out [][]uint8
	for i := 0; i < len(s); i += n {
		end := i + n
		if end > len(s) {
			end = len(s)
		}
		out = append(out, s[i:end])
	}
	return out
}
