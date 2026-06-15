// Package proto implements the AmbiRTC DataChannel wire format: a fixed 9-byte
// header followed by one channel's raw mono Opus payload.
//
// This package is PURE GO — it imports nothing outside the standard library (no
// Pion, no cgo). That keeps the wire format independently unit-testable and
// reusable from any context (server, tools, tests).
//
// Wire format (little-endian), one packet per encoded frame per channel. It
// mirrors the browser client in shared/webrtc-node.js (_onEncoderOutput builds
// it, _onDcMessage parses it) byte-for-byte:
//
//	Byte  0    : isRec(MSB, 0x80) | reserved(3 bits) | channelIndex(low 4 bits, 0–15)
//	Bytes 1–4  : seqNum   uint32 little-endian (per-channel monotonic counter)
//	Bytes 5–8  : frameTs  uint32 little-endian (frame timestamp in samples)
//	Bytes 9..  : raw mono Opus payload
package proto

import (
	"encoding/binary"
	"errors"
)

// Soundfield/codec format constants shared across the MCU. Third-order
// ambisonics (ACN ordering, SN3D normalization) is 16 channels; every stream is
// 48 kHz mono Opus.
const (
	Channels         = 16    // 3rd-order ambisonics (ACN/SN3D)
	SampleRate       = 48000 // Hz
	DefaultFrameSize = 960   // samples (20 ms @ 48 kHz)
	BitRate          = 64000 // bits/s per mono channel (reference; 16 × 64 = 1024 kbps total)
)

// BitrateProfile selects the per-channel Opus bitrate allocation, tapered by
// ambisonic order: bits go where the soundfield needs them (low orders carry the
// dominant energy). The tiers map to order 0-1 (ch 0-3) / order 2 (ch 4-8) /
// order 3 (ch 9-15). The MCU is authoritative and ships the resolved per-channel
// array to the browser in its offer, so uplink and downlink use the same profile.
type BitrateProfile int

const (
	BitrateUniform  BitrateProfile = 1 // 64/64/64 kbps → 16×64 = 1024 kbps (no taper)
	BitrateHigh     BitrateProfile = 2 // 64/48/32 kbps → 720 kbps
	BitrateBalanced BitrateProfile = 3 // 48/32/24 kbps → 520 kbps (default)
	BitrateLow      BitrateProfile = 4 // 48/24/24 kbps → 480 kbps

	DefaultBitrateProfile = BitrateBalanced
)

// IsValidBitrateProfile reports whether n selects a known profile (1–4).
func IsValidBitrateProfile(n int) bool {
	return n >= int(BitrateUniform) && n <= int(BitrateLow)
}

// bitrateTiers returns the (order 0-1, order 2, order 3) bitrates for a profile.
func bitrateTiers(p BitrateProfile) (lo, mid, hi int) {
	switch p {
	case BitrateUniform:
		return 64000, 64000, 64000
	case BitrateHigh:
		return 64000, 48000, 32000
	case BitrateLow:
		return 48000, 24000, 24000
	default: // BitrateBalanced
		return 48000, 32000, 24000
	}
}

// ChannelBitRates returns the per-channel Opus bitrate (bits/s) for the given
// profile, order-tapered (ACN ordering). The browser mirrors the default in
// multiuser/bundle.js (channelBitrates) but uses whatever the offer specifies.
func ChannelBitRates(p BitrateProfile) [Channels]int {
	lo, mid, hi := bitrateTiers(p)
	var b [Channels]int
	for c := 0; c < Channels; c++ {
		switch {
		case c <= 3:
			b[c] = lo
		case c <= 8:
			b[c] = mid
		default:
			b[c] = hi
		}
	}
	return b
}

// HeaderSize is the fixed per-packet header length in bytes.
const HeaderSize = 9

// Control-byte (byte 0) bit layout, matching ISREC_BIT / CHIDX_MASK in the client.
const (
	isRecBit  = 0x80 // MSB: recording flag
	chIdxMask = 0x0F // low 4 bits: channel index (0–15)
)

// ValidFrameSizes lists the Opus frame sizes (samples @ 48 kHz) the system
// accepts: 2.5, 5, 10, 20, 40, and 60 ms.
var ValidFrameSizes = []int{120, 240, 480, 960, 1920, 2880}

// IsValidFrameSize reports whether n is one of the allowed Opus frame sizes.
func IsValidFrameSize(n int) bool {
	for _, v := range ValidFrameSizes {
		if n == v {
			return true
		}
	}
	return false
}

// Errors returned by Parse and the marshalers.
var (
	ErrShortPacket  = errors.New("proto: buffer shorter than 9-byte header")
	ErrChannelRange = errors.New("proto: channel index out of range [0,15]")
)

// Packet is a decoded wire packet: the parsed 9-byte header plus the raw Opus
// payload (which may be empty, e.g. for a header-only test packet).
type Packet struct {
	IsRec   bool   // recording flag (header bit 0x80)
	ChIdx   uint8  // channel index, 0–15
	SeqNum  uint32 // per-channel monotonic sequence number
	FrameTs uint32 // frame timestamp in samples
	Payload []byte // raw mono Opus payload
}

// AppendTo appends the marshaled packet (9-byte header + payload) to dst and
// returns the extended slice. It allocates only when dst lacks capacity, so a
// reused buffer makes it allocation-free — suitable for the hot path. It
// returns ErrChannelRange if ChIdx > 15.
func (p Packet) AppendTo(dst []byte) ([]byte, error) {
	if p.ChIdx > chIdxMask {
		return dst, ErrChannelRange
	}
	var hdr [HeaderSize]byte
	hdr[0] = p.ChIdx & chIdxMask
	if p.IsRec {
		hdr[0] |= isRecBit
	}
	binary.LittleEndian.PutUint32(hdr[1:5], p.SeqNum)
	binary.LittleEndian.PutUint32(hdr[5:9], p.FrameTs)
	dst = append(dst, hdr[:]...)
	dst = append(dst, p.Payload...)
	return dst, nil
}

// MarshalBinary builds a fresh header+payload byte slice. It implements
// encoding.BinaryMarshaler.
func (p Packet) MarshalBinary() ([]byte, error) {
	return p.AppendTo(make([]byte, 0, HeaderSize+len(p.Payload)))
}

// Parse decodes the 9-byte header from buf and returns a Packet whose Payload
// is buf[9:] — a sub-slice of buf, NOT a copy. Do not retain Payload past buf's
// lifetime (copy it if you must). It returns ErrShortPacket only when buf is
// shorter than the header; a 9-byte header-only packet is valid and yields an
// empty (non-nil) payload.
//
// ChIdx is always 0–15 (the mask guarantees it), so unlike the JS client this
// never needs an explicit channel-range guard on the parse path.
func Parse(buf []byte) (Packet, error) {
	if len(buf) < HeaderSize {
		return Packet{}, ErrShortPacket
	}
	b0 := buf[0]
	return Packet{
		IsRec:   b0&isRecBit != 0,
		ChIdx:   b0 & chIdxMask,
		SeqNum:  binary.LittleEndian.Uint32(buf[1:5]),
		FrameTs: binary.LittleEndian.Uint32(buf[5:9]),
		Payload: buf[HeaderSize:],
	}, nil
}
