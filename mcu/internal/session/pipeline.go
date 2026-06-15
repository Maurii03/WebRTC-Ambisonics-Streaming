package session

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/Maurii03/ambirtc-mcu/internal/codec"
	"github.com/Maurii03/ambirtc-mcu/internal/proto"
	"github.com/Maurii03/ambirtc-mcu/internal/room"
)

// Bounded-channel capacities and send-pacing thresholds. Every stage is bounded
// and drops oldest under backpressure so no stage can stall the one before it.
const (
	inboundCap = proto.Channels * 8 // parsed packets awaiting decode (~8 frames)
	mixCap     = 8                  // mix frames from the room awaiting encode
	sendCap    = proto.Channels * 8 // marshaled wire packets awaiting send

	// Send pacing with hysteresis: when the SCTP send buffer climbs past the high
	// watermark the session stops sending (drops, to avoid growing latency); it
	// resumes when OnBufferedAmountLow fires at the low watermark.
	sendHighWater = 1 << 20 // 1 MiB
	sendLowWater  = 1 << 18 // 256 KiB

	statsInterval = 5 * time.Second // per-session jitter-buffer occupancy log (drift watch)
)

// inboundPkt is a parsed wire packet handed from the DataChannel read callback to
// the decode worker. payload is an OWNED copy (Parse returns a sub-slice of the
// Pion buffer, which must not be retained past the callback).
type inboundPkt struct {
	chIdx   uint8
	frameTs uint32
	payload []byte
}

// frame is one assembled 16-channel soundfield (a minus-one mix from the room)
// handed from the room's mix clock to this session's encode stage. Pooled to
// avoid per-tick allocation (s.framePool). refs is the count of encode workers
// still to process this frame; the worker that decrements it to zero returns it
// to the pool (the encode stage fans one frame to K workers in parallel).
type frame struct {
	pcm     [][]int16 // channels × frameSize int16 PCM
	frameTs uint32    // mix-timeline timestamp (samples), set by the room
	rec     bool      // recording window: marks the downlink isRec so receivers record it
	refs    int32     // remaining encode workers (atomic); 0 → release to pool

	// encoded holds each channel's freshly-encoded Opus payload for the bundled
	// downlink. Workers write disjoint indices (channelRange) — no shared slot —
	// and the worker that drives refs to 0 reads them all (the atomic decrement
	// establishes happens-before) to marshal the bundles. Unused in legacy mode.
	encoded [proto.Channels][]byte
}

// encodeJob hands one mix frame to one encode worker. The worker encodes only the
// channels it owns (s.channelRange), so the 16 channels of a frame are encoded in
// parallel across the K workers.
type encodeJob struct {
	fr *frame
}

// startPipeline launches the per-session goroutines exactly once, on the first
// DataChannel open, then registers the session with its room so the per-room mix
// clock begins reading its jitter buffer and submitting mixes. The output encode
// is parallelized: a dispatcher fans each mix frame to encWorkers workers that
// split the 16 channels. Goroutines: decode + dispatcher + encWorkers + send.
// They run until Close closes s.done.
func (s *Session) startPipeline() {
	s.startOnce.Do(func() {
		s.wg.Add(4 + s.encWorkers)
		go s.decodeLoop()
		go s.encodeDispatcher()
		for w := 0; w < s.encWorkers; w++ {
			go s.encodeWorker(w)
		}
		go s.sendLoop()
		go s.statsLoop()
		log.Printf("[session %s] pipeline started (frameSize=%d, encodeWorkers=%d), joining room %q",
			s.id, s.frameSize, s.encWorkers, s.roomID)
		s.rooms.Join(s.roomID, s)
	})
}

// decodeLoop owns all 16 decoders (so they are never touched concurrently). It
// consumes parsed packets, decodes each to int16 PCM, and writes the channel into
// the jitter buffer at its frameTs. This is the INPUT side; the room mix clock
// reads the jitter buffer via ReadInputFrame.
func (s *Session) decodeLoop() {
	defer s.wg.Done()
	pcm := make([]int16, codec.MaxFrameSamples)
	for {
		select {
		case <-s.done:
			return
		case p := <-s.inbound:
			ci := int(p.chIdx)
			if ci < 0 || ci >= proto.Channels {
				continue
			}
			// Decode each channel strictly forward in time. The uplink is unordered,
			// so a reordered/late frame must NOT be fed to the stateful Opus decoder
			// (it would desync CELT → "robotic" mix). Drop it; the jitter buffer
			// leaves that slot silent and the mix carries on.
			if int64(p.frameTs) <= s.decInTs[ci] {
				s.reorderDrops.Add(1)
				continue
			}
			s.decInTs[ci] = int64(p.frameTs)
			t0 := time.Now()
			n, err := s.decoders[ci].Decode(p.payload, pcm)
			dur := time.Since(t0)
			if err != nil {
				log.Printf("[session %s] decode ch %d: %v", s.id, ci, err)
				continue
			}
			s.decNanos.Add(uint64(dur))
			s.decCount.Add(1)
			s.jb.WriteChannel(p.frameTs, ci, pcm[:n])
		}
	}
}

// encodeDispatcher reads each mix frame from the room and fans it to the encode
// workers, setting its refcount so the last worker to finish releases it. It does
// not wait, so workers across successive frames can overlap; per channel, encoding
// stays in order because one worker owns each channel.
func (s *Session) encodeDispatcher() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case fr := <-s.mixCh:
			atomic.StoreInt32(&fr.refs, int32(s.encWorkers))
			for w := 0; w < s.encWorkers; w++ {
				select {
				case s.encJobChs[w] <- encodeJob{fr: fr}:
				case <-s.done:
					return // shutting down; in-flight frame is GC'd with the session
				}
			}
		}
	}
}

// encodeWorker owns a disjoint slice of the 16 channels (and their encoders + seq
// counters, so no locking is needed). For each frame it encodes its channels,
// builds the 9-byte wire packets (mix-timeline frameTs + per-channel monotonic
// seqNum), and pushes them to the send stage; the worker that finishes the frame
// last returns it to the pool.
func (s *Session) encodeWorker(w int) {
	defer s.wg.Done()
	pkt := make([]byte, codec.MaxPacketBytes)
	lo, hi := s.channelRange(w)
	for {
		select {
		case <-s.done:
			return
		case job := <-s.encJobChs[w]:
			fr := job.fr
			for c := lo; c < hi; c++ {
				t0 := time.Now()
				n, err := s.encoders[c].Encode(fr.pcm[c], pkt)
				dur := time.Since(t0)
				if err != nil {
					log.Printf("[session %s] encode ch %d: %v", s.id, c, err)
					continue
				}
				s.encNanos.Add(uint64(dur))
				s.encCount.Add(1)

				if s.bundled {
					// Stash the payload for the frame; sendBundled (last worker) marshals it.
					fr.encoded[c] = append(fr.encoded[c][:0], pkt[:n]...)
					continue
				}
				// Legacy: one 9-byte-header packet per channel, sent immediately.
				p := proto.Packet{ChIdx: uint8(c), SeqNum: s.encSeq[c], FrameTs: fr.frameTs, IsRec: fr.rec, Payload: pkt[:n]}
				s.encSeq[c]++ // only this worker touches its channels' seq → race-free
				buf, err := p.MarshalBinary()
				if err != nil {
					log.Printf("[session %s] marshal ch %d: %v", s.id, c, err)
					continue
				}
				s.pushSend(buf)
			}
			// The worker that finishes the frame (refs→0) has happens-before on
			// every worker's encoded[] writes, so it can marshal the bundles.
			if atomic.AddInt32(&fr.refs, -1) == 0 {
				if s.bundled {
					s.sendBundled(fr)
				}
				s.releaseFrame(fr)
			}
		}
	}
}

// sendBundled marshals one frame's encoded channels into bundled packets per the
// precomputed layout (R=2 base, order-aligned, MTU-bounded) and queues them.
// Channels that failed to encode (empty payload) are simply omitted — the client
// treats them as lost and pads with silence. Called only by the last encode
// worker of a frame, so reading fr.encoded across channels is race-free.
func (s *Session) sendBundled(fr *frame) {
	for _, group := range s.layout {
		chans := make([]proto.ChannelPayload, 0, len(group))
		for _, ch := range group {
			if p := fr.encoded[ch]; len(p) > 0 {
				chans = append(chans, proto.ChannelPayload{ChIdx: ch, Payload: p})
			}
		}
		if len(chans) == 0 {
			continue
		}
		buf, err := proto.MarshalBundle(fr.frameTs, chans, fr.rec)
		if err != nil {
			log.Printf("[session %s] marshal bundle: %v", s.id, err)
			continue
		}
		s.pushSend(buf)
	}
}

// channelRange returns the [lo, hi) channel range owned by encode worker w,
// splitting the 16 channels as evenly as possible across s.encWorkers.
func (s *Session) channelRange(w int) (lo, hi int) {
	lo = w * proto.Channels / s.encWorkers
	hi = (w + 1) * proto.Channels / s.encWorkers
	return lo, hi
}

// sendLoop writes marshaled packets to the DataChannel with hysteresis pacing:
// while congested (SCTP send buffer above the high watermark) it drops packets
// rather than grow latency, and resumes once OnBufferedAmountLow clears the flag
// at the low watermark (wired in attachDataChannel). Dropping stale real-time
// audio is preferable to queueing it behind a backlog.
func (s *Session) sendLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case buf := <-s.sendCh:
			if s.congested.Load() {
				s.sendDrops.Add(1) // drop until the buffer drains
				continue
			}
			if err := s.dc.Send(buf); err != nil {
				log.Printf("[session %s] send: %v", s.id, err)
				continue
			}
			s.pktsSent.Add(1)
			if s.dc.BufferedAmount() > sendHighWater {
				s.congested.Store(true)
			}
		}
	}
}

// statsLoop periodically logs this session's jitter-buffer occupancy so a long
// session can be checked for drift: a stable buffer depth (hovering near the
// prebuffer) with only occasional underruns/overflows means no accumulation;
// a steadily growing depth or runaway overflow count would signal drift.
func (s *Session) statsLoop() {
	defer s.wg.Done()
	t := time.NewTicker(statsInterval)
	defer t.Stop()
	// Room occupancy for the client UI, pushed over the signaling socket: an
	// immediate send fills the stats box right after connect, then a faster
	// ticker keeps it fresh (the 5s log cadence would feel stale in the UI).
	s.sendRoomStats()
	roomTick := time.NewTicker(2 * time.Second)
	defer roomTick.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-roomTick.C:
			s.sendRoomStats()
		case <-t.C:
			st := s.jb.Stats()
			log.Printf("[session %s] jb: started=%v depth=%dms underruns=%d overflows=%d sendBuffered=%dB congested=%v drops(in/reorder/mix/send)=%d/%d/%d/%d",
				s.id, st.Started, st.Depth*1000/proto.SampleRate, st.Underruns, st.Overflows,
				s.dc.BufferedAmount(), s.congested.Load(),
				s.inboundDrops.Load(), s.reorderDrops.Load(), s.mixDrops.Load(), s.sendDrops.Load())
		}
	}
}

// sendRoomStats pushes this session's room occupancy to the client over the
// signaling WebSocket: members = sessions in the room, active = contributors to
// the last mix tick (i.e. currently transmitting audio). The client renders the
// split "transmitting vs listening". Send errors are ignored — the socket may
// be tearing down while the pipeline drains.
func (s *Session) sendRoomStats() {
	for _, rm := range s.rooms.Snapshot() {
		if rm.ID == s.roomID {
			_ = s.send(map[string]any{"type": "room_stats", "members": rm.Members, "active": rm.Active})
			return
		}
	}
}

// --- room.Participant implementation (input read + mix submit) ---

// ReadInputFrame pulls this session's next input frame from its jitter buffer into
// out (channels × frameSize), advancing the read pointer, then applies VAD gating.
// It returns false while the session is still prebuffering OR when the frame is
// gated as silent, so the room treats it as a silent (non-)contribution. Called
// only by the room mix-clock goroutine, so the VAD gate needs no lock.
func (s *Session) ReadInputFrame(out [][]int16) bool {
	started, _ := s.jb.ReadFrame(out)
	if !started {
		s.vadActive.Store(false)
		return false
	}
	active := s.vad.active(out)
	s.vadActive.Store(active)
	return active
}

// vadGate is a per-frame energy gate with hangover. Its state is touched only by
// the room mix-clock goroutine (via ReadInputFrame), so it needs no lock.
type vadGate struct {
	threshold int // peak (abs int16) on channel 0 below which a frame is silent; <=0 disables
	hangover  int // frames to stay active after the last loud frame
	remaining int // hangover countdown
}

// active reports whether the frame should contribute to the mix. Disabled
// (threshold<=0) → always active. Otherwise a frame whose channel-0 peak meets the
// threshold is active and re-arms the hangover; quieter frames stay active until
// the hangover elapses, then gate out (so a held tail of speech is not chopped).
func (g *vadGate) active(frame [][]int16) bool {
	if g.threshold <= 0 {
		return true
	}
	peak := 0
	for _, v := range frame[0] { // channel 0 = W (omni) ≈ overall loudness
		a := int(v)
		if a < 0 {
			a = -a
		}
		if a > peak {
			peak = a
		}
	}
	if peak >= g.threshold {
		g.remaining = g.hangover
		return true
	}
	if g.remaining > 0 {
		g.remaining--
		return true
	}
	return false
}

// Metrics returns this session's telemetry snapshot (room.Participant).
func (s *Session) Metrics() room.ParticipantMetrics {
	st := s.jb.Stats()
	usAvg := func(nanos, count uint64) float64 {
		if count == 0 {
			return 0
		}
		return float64(nanos) / float64(count) / 1e3 // ns → µs per call
	}
	dc, ec := s.decCount.Load(), s.encCount.Load()
	var sendBuffered uint64
	if s.dc != nil {
		sendBuffered = s.dc.BufferedAmount()
	}
	return room.ParticipantMetrics{
		ID:           s.id,
		JBStarted:    st.Started,
		JBDepthMs:    st.Depth * 1000 / proto.SampleRate,
		Underruns:    st.Underruns,
		Overflows:    st.Overflows,
		DecodeCount:  dc,
		EncodeCount:  ec,
		DecodeUsAvg:  usAvg(s.decNanos.Load(), dc),
		EncodeUsAvg:  usAvg(s.encNanos.Load(), ec),
		PktsSent:     s.pktsSent.Load(),
		SendBuffered: sendBuffered,
		InboundDrops: s.inboundDrops.Load(),
		MixDrops:     s.mixDrops.Load(),
		SendDrops:    s.sendDrops.Load(),
		Active:       s.vadActive.Load(),
	}
}

// SubmitMix hands a mixed output frame (channels × frameSize) from the room to
// this session's encode pipeline, stamped with the mix-timeline frameTs. It is
// non-blocking (drop-oldest), so the room mix clock never stalls on a slow client.
func (s *Session) SubmitMix(pcm [][]int16, frameTs uint32, isRec bool) {
	fr := s.acquireFrame()
	for c := 0; c < proto.Channels && c < len(pcm); c++ {
		copy(fr.pcm[c], pcm[c])
	}
	fr.frameTs = frameTs
	fr.rec = isRec
	s.pushFrame(fr)
}

// --- frame pool + bounded drop-oldest enqueues ---

func (s *Session) acquireFrame() *frame {
	fr := s.framePool.Get().(*frame)
	if s.bundled {
		// Clear last frame's payloads so a channel that fails to encode this
		// frame isn't sent with stale bytes (it must be omitted instead).
		for c := range fr.encoded {
			fr.encoded[c] = fr.encoded[c][:0]
		}
	}
	return fr
}
func (s *Session) releaseFrame(fr *frame) {
	if fr != nil {
		s.framePool.Put(fr)
	}
}

// pushInbound enqueues a parsed packet, dropping the oldest if inbound is full.
func (s *Session) pushInbound(p inboundPkt) {
	select {
	case s.inbound <- p:
		return
	default:
	}
	select { // make room by dropping the oldest
	case <-s.inbound:
		s.inboundDrops.Add(1)
	default:
	}
	select {
	case s.inbound <- p:
	default:
		s.inboundDrops.Add(1)
	}
}

// pushFrame enqueues a mix frame for encoding, recycling the oldest (or the new
// one) to the pool if mixCh is full.
func (s *Session) pushFrame(fr *frame) {
	select {
	case s.mixCh <- fr:
		return
	default:
	}
	select { // make room by dropping the oldest
	case old := <-s.mixCh:
		s.releaseFrame(old)
		s.mixDrops.Add(1)
	default:
	}
	select {
	case s.mixCh <- fr:
	default:
		s.releaseFrame(fr)
		s.mixDrops.Add(1)
	}
}

// pushSend enqueues a marshaled packet, dropping the oldest if sendCh is full.
// Called concurrently by the encode workers; channel ops are safe for concurrent use.
func (s *Session) pushSend(buf []byte) {
	select {
	case s.sendCh <- buf:
		return
	default:
	}
	select { // make room by dropping the oldest
	case <-s.sendCh:
		s.sendDrops.Add(1)
	default:
	}
	select {
	case s.sendCh <- buf:
	default:
		s.sendDrops.Add(1)
	}
}
