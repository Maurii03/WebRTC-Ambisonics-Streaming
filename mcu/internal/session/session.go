// Package session manages a single browser↔MCU WebRTC connection: one Pion
// PeerConnection plus the unordered/no-retransmit DataChannel that carries the
// ambisonics packet stream, and the per-client audio pipeline behind it.
//
// The MCU is the WebRTC offerer — it creates the DataChannel and the SDP offer,
// mirroring the role that creates "ambi-ch" in the browser client
// (shared/webrtc-node.js). The offer carries the server-authoritative frame size,
// which the browser applies (setFrameSize) before wiring its encoders.
//
// The DataChannel read callback ONLY parses the 9-byte header and pushes the
// payload to a bounded channel — it never decodes. Behind it run three goroutines
// (see pipeline.go): a decode worker that fills the per-client jitter buffer
// (package audio), an encode worker, and a send loop. The OUTPUT is driven not by
// this session but by the per-room mix clock (package room): on DataChannel open
// the session joins its room as a room.Participant, and the mix clock reads its
// jitter buffer (ReadInputFrame), sums every member's soundfield, and submits the
// mix back (SubmitMix) for re-encoding. With a single client in the room the mix
// equals that client's own field (loopback); with N it is the summed soundfield.
package session

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Maurii03/ambirtc-mcu/internal/audio"
	"github.com/Maurii03/ambirtc-mcu/internal/codec"
	"github.com/Maurii03/ambirtc-mcu/internal/proto"
	"github.com/Maurii03/ambirtc-mcu/internal/room"
	"github.com/pion/webrtc/v4"
)

// dataChannelLabel matches the browser client's channel name (shared/webrtc-node.js).
const dataChannelLabel = "ambi-ch"

// jitterBufferSeconds is the per-client jitter-buffer ring capacity, matching the
// browser worklet's BUF_SECONDS (shared/worklet-receiver.js).
const jitterBufferSeconds = 2

// prebufferFrames is the prefill depth (in frames) before readout starts, matching
// the browser's initial dynamic prebuffer (PREBUF_MIN_FRAMES).
const prebufferFrames = 2

// SignalSender delivers one signaling message (any JSON-encodable value) back to
// the browser over the out-of-band transport (the WebSocket). Implementations
// MUST be safe for concurrent use: the session calls it from Pion's ICE
// goroutine (trickled candidates) as well as from the negotiation path.
type SignalSender func(msg any) error

// Offer is the signaling payload the session emits to begin negotiation. It
// carries FrameSize so the browser aligns its codecs to the server-authoritative
// frame size (the client reads msg.frameSize).
type Offer struct {
	Type      string                    `json:"type"` // always "offer"
	SDP       webrtc.SessionDescription `json:"sdp"`
	FrameSize int                       `json:"frameSize"`
	Packing   string                    `json:"packing"`  // "legacy" (per-channel) or "bundled" (v2); tells the client how the downlink is framed
	Bitrates  []int                     `json:"bitrates"` // per-channel Opus bitrate (bits/s) the client mirrors on its uplink
}

// IceMessage carries one trickled ICE candidate (either direction).
type IceMessage struct {
	Type      string                  `json:"type"` // always "ice"
	Candidate webrtc.ICECandidateInit `json:"candidate"`
}

// Session owns a single PeerConnection, its DataChannel, and the per-client audio
// pipeline. The INPUT side (DataChannel → decode → jitter buffer) and OUTPUT side
// (mix from the room → encode → send) meet at the room's mix clock, which reads
// this session's jitter buffer and submits mixes back via the room.Participant
// interface (ReadInputFrame / SubmitMix, see pipeline.go). One Session per client.
type Session struct {
	id        string
	pc        *webrtc.PeerConnection
	send      SignalSender
	frameSize int

	rooms  *room.Manager // shared room manager; session joins roomID on DataChannel open
	roomID string

	dc *webrtc.DataChannel

	// Audio pipeline (built in New, run by the goroutines in pipeline.go).
	decoders  []*codec.Decoder
	encoders  []*codec.Encoder
	jb        *audio.JitterBuffer
	framePool sync.Pool

	// Per-channel highest input frameTs already decoded. The uplink DataChannel is
	// unordered, so the network can deliver an older frame after a newer one;
	// decoding it would desync the channel's stateful Opus decoder (audible as
	// "robotic" mix output). decodeLoop owns this (single goroutine → no lock) and
	// drops out-of-order input instead. -1 = nothing decoded yet.
	decInTs [proto.Channels]int64

	// Resolved per-channel Opus bitrate (the selected profile). Used for the encode
	// bank and sent to the client in the offer so the uplink matches.
	bitrates [proto.Channels]int

	inbound chan inboundPkt // DataChannel read → decode worker
	mixCh   chan *frame     // room mix clock → encode dispatcher
	sendCh  chan []byte     // encode workers → send loop
	done    chan struct{}   // closed by Close to stop all goroutines

	// Output encode is parallelized: a dispatcher fans each mix frame to
	// encWorkers workers that split the 16 channels (each owns disjoint channels,
	// encoders, and seq counters → no locking).
	encWorkers int
	encJobChs  []chan encodeJob
	encSeq     []uint32

	// Downlink wire format. When bundled, the per-frame channels are grouped into
	// layout packets (proto.PlanLayout, precomputed) with R=2 base redundancy
	// instead of one legacy packet per channel. The uplink is auto-detected
	// per-packet in handlePacket regardless of this.
	bundled bool
	layout  [][]uint8

	// congested gates sendLoop: set when the SCTP send buffer passes the high
	// watermark, cleared by OnBufferedAmountLow at the low watermark (pacing).
	congested atomic.Bool

	// VAD gate state is touched ONLY by the room mix-clock goroutine (via
	// ReadInputFrame), so it needs no lock; vadActive mirrors the last decision
	// for the /metrics reader (a different goroutine).
	vad       vadGate
	vadActive atomic.Bool

	// Telemetry counters (atomic; written by the pipeline goroutines, read by Metrics).
	decNanos     atomic.Uint64
	decCount     atomic.Uint64
	encNanos     atomic.Uint64
	encCount     atomic.Uint64
	pktsSent     atomic.Uint64
	inboundDrops atomic.Uint64
	reorderDrops atomic.Uint64 // out-of-order uplink packets dropped before decode
	mixDrops     atomic.Uint64
	sendDrops    atomic.Uint64

	// recDeadline is the UnixNano time until which this session counts as "in a
	// recording window": refreshed (with a hangover) whenever an inbound packet
	// carries isRec=1. The room mix reads IsRecording() to flag the downlink so
	// every receiver records the same span. 0 = not recording.
	recDeadline atomic.Int64

	startOnce sync.Once      // pipeline starts once, on first DataChannel open
	closeOnce sync.Once      // done is closed once
	wg        sync.WaitGroup // tracks the pipeline goroutines (decode+dispatcher+encWorkers+send)

	mu                sync.Mutex
	remoteDescription bool
	pendingCandidates []webrtc.ICECandidateInit
}

// Options holds the per-session audio/encode tuning knobs (server-authoritative).
type Options struct {
	FrameSize     int  // Opus frame size in samples (sent to the browser in the offer)
	EncodeWorkers int  // intra-session encode parallelism (16 channels split, clamped to [1,16])
	VADThreshold  int  // peak (int16) on channel 0 below which a frame is gated silent; 0 disables VAD
	VADHangover   int  // frames to remain active after the last loud frame
	Bundled       bool // downlink wire format: true = bundled v2 (multi-channel packets + R=2 base), false = legacy per-channel

	// BitrateProfile selects the order-tapered per-channel Opus bitrate (see
	// proto.BitrateProfile). 0 falls back to proto.DefaultBitrateProfile.
	BitrateProfile proto.BitrateProfile
}

// New creates a Session: it builds the PeerConnection, the 16 Opus decoders and
// encoders, the per-client jitter buffer, and the bounded pipeline channels, then
// wires the ICE/DataChannel handlers and creates the server-side DataChannel
// (unordered, no retransmits — matching the real-time client). opts carries the
// server-authoritative frame size, encode parallelism, and VAD gating; rooms is
// the shared room manager and roomID the conference this client joins (on
// DataChannel open). Call Start to emit the offer once signaling is ready.
func New(id string, api *webrtc.API, cfg webrtc.Configuration, opts Options, rooms *room.Manager, roomID string, send SignalSender) (*Session, error) {
	frameSize := opts.FrameSize
	encodeWorkers := opts.EncodeWorkers
	if encodeWorkers < 1 {
		encodeWorkers = 1
	}
	if encodeWorkers > proto.Channels {
		encodeWorkers = proto.Channels
	}
	pc, err := api.NewPeerConnection(cfg)
	if err != nil {
		return nil, fmt.Errorf("session %s: new peer connection: %w", id, err)
	}

	decoders, err := codec.NewDecoders(proto.Channels)
	if err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("session %s: %w", id, err)
	}
	// Order-tapered downlink bitrate (profile-selectable): fewer bits for the higher
	// ambisonic orders, easing the MCU's egress and each client's download. The
	// resolved array is also sent in the offer so the browser uplink uses the same
	// profile.
	bitrateProfile := opts.BitrateProfile
	if !proto.IsValidBitrateProfile(int(bitrateProfile)) {
		bitrateProfile = proto.DefaultBitrateProfile
	}
	chBitrates := proto.ChannelBitRates(bitrateProfile)
	encoders, err := codec.NewEncodersPerChannel(codec.AppRestrictedLowdelay, chBitrates[:])
	if err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("session %s: %w", id, err)
	}

	s := &Session{
		id:        id,
		pc:        pc,
		send:      send,
		frameSize: frameSize,
		rooms:     rooms,
		roomID:    roomID,
		decoders:  decoders,
		encoders:  encoders,
		jb: audio.NewJitterBuffer(
			proto.Channels, frameSize,
			jitterBufferSeconds*proto.SampleRate,
			prebufferFrames*frameSize,
		),
		inbound:    make(chan inboundPkt, inboundCap),
		mixCh:      make(chan *frame, mixCap),
		sendCh:     make(chan []byte, sendCap),
		done:       make(chan struct{}),
		encWorkers: encodeWorkers,
		encSeq:     make([]uint32, proto.Channels),
		vad:        vadGate{threshold: opts.VADThreshold, hangover: opts.VADHangover},
		bundled:    opts.Bundled,
		bitrates:   chBitrates,
	}
	for i := range s.decInTs {
		s.decInTs[i] = -1 // nothing decoded yet
	}
	if s.bundled {
		// Precompute the (constant per frame size) packetization plan once.
		s.layout = proto.PlanLayout(frameSize)
	}
	s.encJobChs = make([]chan encodeJob, encodeWorkers)
	for w := range s.encJobChs {
		s.encJobChs[w] = make(chan encodeJob, 2) // small buffer so the dispatcher never blocks
	}
	s.framePool.New = func() any {
		pcm := make([][]int16, proto.Channels)
		for c := range pcm {
			pcm[c] = make([]int16, frameSize)
		}
		return &frame{pcm: pcm}
	}

	// Trickle our local ICE candidates to the browser as they are gathered.
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			log.Printf("[session %s] local ICE gathering complete", s.id)
			return // nil marks end-of-candidates
		}
		log.Printf("[session %s] -> local ICE candidate: %s", s.id, c.String())
		if err := s.send(IceMessage{Type: "ice", Candidate: c.ToJSON()}); err != nil {
			log.Printf("[session %s] send ICE candidate: %v", s.id, err)
		}
	})

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("[session %s] ICE connection state: %s", s.id, state)
		if state == webrtc.ICEConnectionStateConnected {
			s.logSelectedPair()
		}
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("[session %s] connection state: %s", s.id, state)
	})

	// The MCU creates the DataChannel, so it owns the real-time parameters:
	// unordered + no retransmits, identical to the browser client.
	ordered := false
	maxRetransmits := uint16(0)
	dc, err := pc.CreateDataChannel(dataChannelLabel, &webrtc.DataChannelInit{
		Ordered:        &ordered,
		MaxRetransmits: &maxRetransmits,
	})
	if err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("session %s: create data channel: %w", id, err)
	}
	s.attachDataChannel(dc)

	return s, nil
}

func (s *Session) attachDataChannel(dc *webrtc.DataChannel) {
	s.dc = dc
	// Send pacing: resume sending once the SCTP buffer drains to the low watermark.
	dc.SetBufferedAmountLowThreshold(sendLowWater)
	dc.OnBufferedAmountLow(func() { s.congested.Store(false) })
	dc.OnOpen(func() {
		log.Printf("[session %s] DataChannel %q open (ordered=false, maxRetransmits=0)", s.id, dc.Label())
		s.startPipeline()
	})
	dc.OnClose(func() {
		log.Printf("[session %s] DataChannel %q closed", s.id, dc.Label())
	})
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		s.handlePacket(msg.Data)
	})
}

// handlePacket is the DataChannel hot path. It runs on Pion's SCTP read goroutine,
// so it does ONLY the cheap work: parse the 9-byte header, copy the payload (Parse
// returns a sub-slice of the Pion buffer we must not retain), and push it to the
// bounded inbound channel (drop-oldest if full). All decode/mix/encode work happens
// in the pipeline goroutines.
func (s *Session) handlePacket(data []byte) {
	// Auto-detect the wire format from byte 0's high nibble: legacy packets carry
	// isRec(bit7)|reserved(0)|chIdx, so their high nibble is 0 or 8; a bundled v2
	// packet's high nibble is BundleVersion (1). This lets the MCU accept both a
	// legacy client and a bundled client transparently, no negotiation needed.
	if len(data) > 0 && data[0]>>4 == proto.BundleVersion {
		frameTs, chans, isRec, err := proto.ParseBundle(data)
		if err != nil {
			log.Printf("[session %s] drop malformed bundle (%d bytes): %v", s.id, len(data), err)
			return
		}
		if isRec {
			s.markRecording()
		}
		for _, c := range chans {
			if len(c.Payload) == 0 {
				continue
			}
			payload := make([]byte, len(c.Payload))
			copy(payload, c.Payload)
			s.pushInbound(inboundPkt{chIdx: c.ChIdx, frameTs: frameTs, payload: payload})
		}
		return
	}

	pkt, err := proto.Parse(data)
	if err != nil {
		log.Printf("[session %s] drop malformed packet (%d bytes): %v", s.id, len(data), err)
		return
	}
	if pkt.IsRec {
		s.markRecording()
	}
	if len(pkt.Payload) == 0 {
		return // header-only / no audio payload
	}
	payload := make([]byte, len(pkt.Payload))
	copy(payload, pkt.Payload)
	s.pushInbound(inboundPkt{chIdx: pkt.ChIdx, frameTs: pkt.FrameTs, payload: payload})
}

// recHangover keeps the recording latch active briefly after the last isRec
// packet, so a momentary gap in the uplink does not split the downlink window.
const recHangover = 250 * time.Millisecond

// markRecording (re)arms the recording latch on an inbound isRec packet. Logs
// once per window (on the off→on transition) so an operator can confirm the
// uplink isRec actually reaches the MCU and the downlink mix will be flagged.
func (s *Session) markRecording() {
	wasRec := time.Now().UnixNano() < s.recDeadline.Load()
	s.recDeadline.Store(time.Now().Add(recHangover).UnixNano())
	if !wasRec {
		log.Printf("[session %s] recording window started (isRec uplink) — flagging downlink mix for all receivers", s.id)
	}
}

// IsRecording reports whether this session is currently inside a recording
// window (room.Participant). Read by the room mix to flag the downlink.
func (s *Session) IsRecording() bool {
	return time.Now().UnixNano() < s.recDeadline.Load()
}

// Start creates the SDP offer, sets it as the local description (which begins
// ICE gathering), and sends it to the browser. Call exactly once after New.
func (s *Session) Start() error {
	offer, err := s.pc.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("session %s: create offer: %w", s.id, err)
	}
	if err := s.pc.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("session %s: set local description: %w", s.id, err)
	}
	// Trickle ICE: send the offer now; candidates follow via OnICECandidate.
	packing := "legacy"
	if s.bundled {
		packing = "bundled"
	}
	if err := s.send(Offer{Type: "offer", SDP: *s.pc.LocalDescription(), FrameSize: s.frameSize, Packing: packing, Bitrates: s.bitrates[:]}); err != nil {
		return fmt.Errorf("session %s: send offer: %w", s.id, err)
	}
	return nil
}

// HandleAnswer applies the browser's SDP answer and drains any ICE candidates
// that arrived before the remote description was set.
func (s *Session) HandleAnswer(sdp webrtc.SessionDescription) error {
	log.Printf("[session %s] <- answer received, applying remote description", s.id)
	if err := s.pc.SetRemoteDescription(sdp); err != nil {
		return fmt.Errorf("session %s: set remote description: %w", s.id, err)
	}

	s.mu.Lock()
	s.remoteDescription = true
	pending := s.pendingCandidates
	s.pendingCandidates = nil
	s.mu.Unlock()

	for _, c := range pending {
		if err := s.pc.AddICECandidate(c); err != nil {
			log.Printf("[session %s] add buffered ICE: %v", s.id, err)
		}
	}
	return nil
}

// AddRemoteICE applies a trickled ICE candidate from the browser, buffering it
// until the remote description (answer) has been set.
func (s *Session) AddRemoteICE(c webrtc.ICECandidateInit) error {
	log.Printf("[session %s] <- remote ICE candidate: %s", s.id, c.Candidate)
	s.mu.Lock()
	if !s.remoteDescription {
		s.pendingCandidates = append(s.pendingCandidates, c)
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	if err := s.pc.AddICECandidate(c); err != nil {
		return fmt.Errorf("session %s: add ICE candidate: %w", s.id, err)
	}
	return nil
}

// logSelectedPair logs the nominated ICE candidate pair once ICE connects, so we
// can confirm the data path uses a host/LAN candidate (not loopback).
func (s *Session) logSelectedPair() {
	sctp := s.pc.SCTP()
	if sctp == nil {
		return
	}
	dtls := sctp.Transport()
	if dtls == nil {
		return
	}
	iceT := dtls.ICETransport()
	if iceT == nil {
		return
	}
	pair, err := iceT.GetSelectedCandidatePair()
	if err != nil || pair == nil {
		log.Printf("[session %s] selected candidate pair: <none yet> (%v)", s.id, err)
		return
	}
	log.Printf("[session %s] selected candidate pair: %s", s.id, pair.String())
}

// ID returns the session's stable identifier (room.Participant).
func (s *Session) ID() string { return s.id }

// Close deregisters the session from its room (so the mix clock stops touching
// it), stops the pipeline goroutines, and tears down the PeerConnection. It is
// idempotent and safe to call from the signaling goroutine (not a Pion callback),
// which is why wg.Wait here cannot deadlock against a Pion close callback.
// Deregistering first ensures the room no longer calls ReadInputFrame/SubmitMix
// on this session while it shuts down. Calling Leave when the session never
// joined (DataChannel never opened) is a harmless no-op.
func (s *Session) Close() error {
	s.rooms.Leave(s.roomID, s.id)
	s.closeOnce.Do(func() { close(s.done) })
	err := s.pc.Close()
	s.wg.Wait()
	return err
}
