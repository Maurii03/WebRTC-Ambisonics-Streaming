// Package codec is a thin wrapper over gopkg.in/hraban/opus.v2 exposing a mono
// Opus encoder and decoder at 48 kHz — matching the per-channel streams on the
// wire (one independent mono Opus stream per ambisonics channel).
//
// PCM format boundary: libopus works in 16-bit signed PCM (int16). The browser
// client produces 32-bit float PCM (WebCodecs "f32-planar", samples in [-1, 1]).
// This package's encode/decode API is int16; cross the float boundary with
// Float32ToInt16 / Int16ToFloat32 (pcm.go). That conversion is the only place
// the two PCM worlds meet.
//
// cgo / single-binding rule: this package links libopus through hraban/opus
// (which pkg-configs `opus` and `opusfile`). Do NOT add any other libopus cgo
// binding to the build — notably pion/mediadevices/pkg/codec/opus — or the two
// bindings collide at link time with
// "multiple definition of bridge_encoder_set_bitrate".
package codec

import (
	"fmt"

	"github.com/Maurii03/ambirtc-mcu/internal/proto"
	opus "gopkg.in/hraban/opus.v2"
)

// monoChannels is the channel count for a single ambisonics-channel Opus stream.
const monoChannels = 1

// MaxPacketBytes bounds one encoded Opus packet. 4000 bytes comfortably exceeds
// a 60 ms / high-bitrate frame and follows libopus's own sizing guidance; use
// it to size encode output buffers.
const MaxPacketBytes = 4000

// MaxFrameSamples is the largest decoded mono frame we accept (120 ms @ 48 kHz).
// Size a decode buffer to this when the incoming frame size is unknown.
const MaxFrameSamples = 5760

// Application selects libopus's coding mode. It mirrors hraban/opus's
// Application type so callers need not import the opus package directly.
type Application int

const (
	// AppVoIP optimizes for speech intelligibility.
	AppVoIP = Application(opus.AppVoIP)
	// AppAudio optimizes for general/music fidelity.
	AppAudio = Application(opus.AppAudio)
	// AppRestrictedLowdelay disables the speech layer for lowest latency; this
	// is the right choice for the real-time MCU path.
	AppRestrictedLowdelay = Application(opus.AppRestrictedLowdelay)
)

// Encoder is a mono 48 kHz Opus encoder. It is NOT safe for concurrent use —
// use one Encoder per channel/goroutine.
type Encoder struct {
	enc *opus.Encoder
}

// NewEncoder builds a mono 48 kHz Opus encoder. application picks the coding
// mode; bitrate is bits/second (e.g. proto.BitRate = 64000) — pass 0 to leave
// libopus at its automatic default.
func NewEncoder(application Application, bitrate int) (*Encoder, error) {
	enc, err := opus.NewEncoder(proto.SampleRate, monoChannels, opus.Application(application))
	if err != nil {
		return nil, fmt.Errorf("codec: new encoder: %w", err)
	}
	if bitrate > 0 {
		if err := enc.SetBitrate(bitrate); err != nil {
			return nil, fmt.Errorf("codec: set bitrate %d: %w", bitrate, err)
		}
	}
	return &Encoder{enc: enc}, nil
}

// Encode encodes one mono frame of int16 PCM into out and returns the number of
// bytes written. len(pcm) must be a valid Opus frame size at 48 kHz (see
// proto.ValidFrameSizes); out should have len >= MaxPacketBytes. The caller
// owns out, enabling buffer reuse on the hot path.
func (e *Encoder) Encode(pcm []int16, out []byte) (int, error) {
	n, err := e.enc.Encode(pcm, out)
	if err != nil {
		return 0, fmt.Errorf("codec: encode %d samples: %w", len(pcm), err)
	}
	return n, nil
}

// Decoder is a mono 48 kHz Opus decoder. It is NOT safe for concurrent use —
// use one Decoder per channel/goroutine.
type Decoder struct {
	dec *opus.Decoder
}

// NewDecoder builds a mono 48 kHz Opus decoder.
func NewDecoder() (*Decoder, error) {
	dec, err := opus.NewDecoder(proto.SampleRate, monoChannels)
	if err != nil {
		return nil, fmt.Errorf("codec: new decoder: %w", err)
	}
	return &Decoder{dec: dec}, nil
}

// Decode decodes one Opus packet into pcm (int16, mono) and returns the number
// of samples decoded. pcm must be large enough for the frame; size it to
// MaxFrameSamples when the frame size is unknown. The caller owns pcm.
func (d *Decoder) Decode(payload []byte, pcm []int16) (int, error) {
	n, err := d.dec.Decode(payload, pcm)
	if err != nil {
		return 0, fmt.Errorf("codec: decode %d bytes: %w", len(payload), err)
	}
	return n, nil
}

// NewEncoders builds n independent mono encoders — one per soundfield channel.
// All share the same application/bitrate. Each Encoder is owned by a single
// goroutine (encoders are not safe for concurrent use). It fails fast, returning
// the first construction error.
func NewEncoders(n int, application Application, bitrate int) ([]*Encoder, error) {
	encs := make([]*Encoder, n)
	for i := range encs {
		enc, err := NewEncoder(application, bitrate)
		if err != nil {
			return nil, fmt.Errorf("codec: encoder %d: %w", i, err)
		}
		encs[i] = enc
	}
	return encs, nil
}

// NewEncodersPerChannel builds one mono encoder per entry in bitrates, each set
// to its own bitrate (bits/s; 0 leaves libopus at its automatic default). This is
// the multiuser/MCU path's order-tapered encoder bank — see proto.ChannelBitRates.
// Each Encoder is owned by a single goroutine (encoders are not concurrent-safe).
func NewEncodersPerChannel(application Application, bitrates []int) ([]*Encoder, error) {
	encs := make([]*Encoder, len(bitrates))
	for i, br := range bitrates {
		enc, err := NewEncoder(application, br)
		if err != nil {
			return nil, fmt.Errorf("codec: encoder %d: %w", i, err)
		}
		encs[i] = enc
	}
	return encs, nil
}

// NewDecoders builds n independent mono decoders — one per soundfield channel.
// Each Decoder is owned by a single goroutine (decoders are not safe for
// concurrent use). It fails fast, returning the first construction error.
func NewDecoders(n int) ([]*Decoder, error) {
	decs := make([]*Decoder, n)
	for i := range decs {
		dec, err := NewDecoder()
		if err != nil {
			return nil, fmt.Errorf("codec: decoder %d: %w", i, err)
		}
		decs[i] = dec
	}
	return decs, nil
}
