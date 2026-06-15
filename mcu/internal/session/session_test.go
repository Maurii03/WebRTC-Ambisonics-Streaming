package session

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Maurii03/ambirtc-mcu/internal/codec"
	"github.com/Maurii03/ambirtc-mcu/internal/proto"
	"github.com/Maurii03/ambirtc-mcu/internal/room"
	"github.com/pion/webrtc/v4"
)

func loopbackAPI() *webrtc.API {
	se := webrtc.SettingEngine{}
	se.SetIncludeLoopbackCandidate(true) // make the loopback work on 127.0.0.1 with no STUN
	return webrtc.NewAPI(webrtc.WithSettingEngine(se))
}

// TestHandlePacketSetsRecordingLatch is the missing end-to-end link in the
// recording chain: an inbound packet with isRec=1 (legacy header bit OR bundled
// flag) must arm the session's recording latch, which the room reads to flag the
// downlink (covered by room.TestRecordingPropagates). Without this the MCU never
// propagates and no receiver records.
func TestHandlePacketSetsRecordingLatch(t *testing.T) {
	api := loopbackAPI()
	rooms := room.NewManager(proto.Channels, proto.DefaultFrameSize)
	send := func(any) error { return nil }

	for _, bundled := range []bool{false, true} {
		sess, err := New("rec", api, webrtc.Configuration{},
			Options{FrameSize: proto.DefaultFrameSize, EncodeWorkers: 2, Bundled: bundled}, rooms, "recroom", send)
		if err != nil {
			t.Fatalf("bundled=%v New: %v", bundled, err)
		}
		if sess.IsRecording() {
			t.Errorf("bundled=%v: recording before any isRec packet", bundled)
		}

		var data []byte
		if bundled {
			data, _ = proto.MarshalBundle(960, []proto.ChannelPayload{{ChIdx: 0, Payload: []byte{1, 2, 3}}}, true)
		} else {
			data, _ = proto.Packet{ChIdx: 0, FrameTs: 960, IsRec: true, Payload: []byte{1, 2, 3}}.MarshalBinary()
		}
		sess.handlePacket(data)
		if !sess.IsRecording() {
			t.Errorf("bundled=%v: latch not armed after an isRec packet", bundled)
		}

		// A non-isRec packet must not re-arm an already-expired latch.
		sess.recDeadline.Store(0)
		var plain []byte
		if bundled {
			plain, _ = proto.MarshalBundle(1920, []proto.ChannelPayload{{ChIdx: 0, Payload: []byte{4, 5}}}, false)
		} else {
			plain, _ = proto.Packet{ChIdx: 0, FrameTs: 1920, IsRec: false, Payload: []byte{4, 5}}.MarshalBinary()
		}
		sess.handlePacket(plain)
		if sess.IsRecording() {
			t.Errorf("bundled=%v: latch armed by a non-isRec packet", bundled)
		}
		sess.Close()
	}
}

// TestSessionLoopbackPipeline drives the full per-client pipeline without a
// browser. A second Pion PeerConnection ("the browser", the answerer) connects to
// a real Session ("the MCU", the offerer) over in-process signaling, then encodes
// real 16-channel sine PCM and streams it as 9-byte wire packets at the frame
// cadence. It exercises parse → inbound → decode → jitter buffer → room mix clock
// → encode → send end to end and asserts the MCU sends frames BACK on all 16
// channels with a monotonic mix-timeline frameTs (+frameSize per frame). Because
// the minus-one mix for a single client is S − itself = silence, it also asserts
// the returned audio is NEAR-SILENT — i.e. the self-contribution is cancelled.
func TestSessionLoopbackPipeline(t *testing.T) { runLoopbackPipeline(t, false) }

// TestSessionLoopbackBundled runs the same end-to-end pipeline with the bundled
// v2 wire format on BOTH links: the in-process client streams bundled uplink
// (auto-detected by the MCU) and the MCU returns the bundled downlink (P packets
// per frame, R=2 base). It asserts every channel round-trips, the ch0 mix
// timeline is +frameSize monotonic (deduping the R=2 base copies), and the
// minus-one cancellation still holds.
func TestSessionLoopbackBundled(t *testing.T) { runLoopbackPipeline(t, true) }

func runLoopbackPipeline(t *testing.T, bundled bool) {
	const frameSize = proto.DefaultFrameSize // 960 / 20 ms
	api := loopbackAPI()
	cfg := webrtc.Configuration{}
	rooms := room.NewManager(proto.Channels, frameSize)

	client, err := api.NewPeerConnection(cfg)
	if err != nil {
		t.Fatalf("client PeerConnection: %v", err)
	}
	defer client.Close()

	// Collect returned packets per channel, plus an ordered ch0 frameTs log.
	var mu sync.Mutex
	perChannel := make([]int, proto.Channels)
	var ch0Returns [][]byte
	var ch0FrameTs []uint32
	seenTs := map[uint32]bool{} // dedup the R=2 base copies (bundled) by frameTs
	errCh := make(chan error, 8)

	var sessPtr atomic.Pointer[Session]

	// The browser receives the server-created channel; on open it streams encoded
	// audio and records what comes back.
	client.OnDataChannel(func(d *webrtc.DataChannel) {
		d.OnMessage(func(msg webrtc.DataChannelMessage) {
			if bundled {
				if len(msg.Data) == 0 || msg.Data[0]>>4 != proto.BundleVersion {
					errCh <- fmt.Errorf("expected bundled packet, got byte0=%#x", msg.Data)
					return
				}
				ts, chans, _, perr := proto.ParseBundle(msg.Data)
				if perr != nil {
					errCh <- fmt.Errorf("client ParseBundle: %w", perr)
					return
				}
				mu.Lock()
				for _, c := range chans {
					perChannel[c.ChIdx]++
					if c.ChIdx == 0 && !seenTs[ts] { // base is R=2: count one ch0 per frameTs
						seenTs[ts] = true
						cp := make([]byte, len(c.Payload))
						copy(cp, c.Payload)
						ch0Returns = append(ch0Returns, cp)
						ch0FrameTs = append(ch0FrameTs, ts)
					}
				}
				mu.Unlock()
				return
			}
			pkt, perr := proto.Parse(msg.Data)
			if perr != nil {
				errCh <- fmt.Errorf("client parse return: %w", perr)
				return
			}
			mu.Lock()
			perChannel[pkt.ChIdx]++
			if pkt.ChIdx == 0 {
				cp := make([]byte, len(pkt.Payload))
				copy(cp, pkt.Payload)
				ch0Returns = append(ch0Returns, cp)
				ch0FrameTs = append(ch0FrameTs, pkt.FrameTs)
			}
			mu.Unlock()
		})
		d.OnOpen(func() {
			if bundled {
				go streamEncodedAudioBundled(d, frameSize, errCh)
			} else {
				go streamEncodedAudio(d, frameSize, errCh)
			}
		})
	})

	// In-process signaling: route the session's offer/ICE to the client, mirroring
	// the real browser answerer.
	var sigMu sync.Mutex
	clientReady := false
	var clientPending []webrtc.ICECandidateInit
	addToClient := func(c webrtc.ICECandidateInit) {
		sigMu.Lock()
		if !clientReady {
			clientPending = append(clientPending, c)
			sigMu.Unlock()
			return
		}
		sigMu.Unlock()
		if err := client.AddICECandidate(c); err != nil {
			errCh <- fmt.Errorf("client AddICECandidate: %w", err)
		}
	}

	send := func(msg any) error {
		switch m := msg.(type) {
		case Offer:
			if m.FrameSize != frameSize {
				errCh <- fmt.Errorf("offer FrameSize = %d, want %d", m.FrameSize, frameSize)
			}
			go func() {
				if err := client.SetRemoteDescription(m.SDP); err != nil {
					errCh <- fmt.Errorf("client SetRemoteDescription: %w", err)
					return
				}
				sigMu.Lock()
				clientReady = true
				pending := clientPending
				clientPending = nil
				sigMu.Unlock()
				for _, c := range pending {
					if err := client.AddICECandidate(c); err != nil {
						errCh <- fmt.Errorf("client drain ICE: %w", err)
					}
				}
				answer, err := client.CreateAnswer(nil)
				if err != nil {
					errCh <- fmt.Errorf("client CreateAnswer: %w", err)
					return
				}
				if err := client.SetLocalDescription(answer); err != nil {
					errCh <- fmt.Errorf("client SetLocalDescription: %w", err)
					return
				}
				if s := sessPtr.Load(); s != nil {
					if err := s.HandleAnswer(*client.LocalDescription()); err != nil {
						errCh <- fmt.Errorf("session HandleAnswer: %w", err)
					}
				}
			}()
		case IceMessage:
			addToClient(m.Candidate)
		case map[string]any:
			// Telemetry pushes for the client UI (e.g. "room_stats"): not part
			// of the negotiation, ignored by this harness.
		default:
			errCh <- fmt.Errorf("unexpected signaling message %T", msg)
		}
		return nil
	}

	client.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		if s := sessPtr.Load(); s != nil {
			if err := s.AddRemoteICE(c.ToJSON()); err != nil {
				errCh <- fmt.Errorf("session AddRemoteICE: %w", err)
			}
		}
	})

	sess, err := New("test", api, cfg, Options{FrameSize: frameSize, EncodeWorkers: 4, Bundled: bundled}, rooms, "testroom", send)
	if err != nil {
		t.Fatalf("session.New: %v", err)
	}
	defer sess.Close()
	sessPtr.Store(sess)

	if err := sess.Start(); err != nil {
		t.Fatalf("session.Start: %v", err)
	}

	// Wait until we've received several ch0 frames back (or fail on a signaling
	// error / timeout). The MCU emits at the 20 ms frame cadence after a 2-frame
	// prebuffer, so ~10 returned frames take ~250 ms; allow generous slack.
	const wantCh0Frames = 8
	deadline := time.After(20 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-errCh:
			t.Fatalf("signaling/stream error: %v", err)
		case <-deadline:
			mu.Lock()
			got := len(ch0Returns)
			mu.Unlock()
			t.Fatalf("timed out: only %d ch0 frames returned (want >= %d)", got, wantCh0Frames)
		case <-tick.C:
			mu.Lock()
			enough := len(ch0Returns) >= wantCh0Frames
			mu.Unlock()
			if enough {
				goto verify
			}
		}
	}

verify:
	mu.Lock()
	defer mu.Unlock()

	// (a) Frames came back on all 16 channels.
	for c := 0; c < proto.Channels; c++ {
		if perChannel[c] == 0 {
			t.Fatalf("no packets returned on channel %d (per-channel counts: %v)", c, perChannel)
		}
	}

	// (b) ch0 server frameTs is monotonic and increments by exactly frameSize.
	for i := 1; i < len(ch0FrameTs); i++ {
		if d := int64(ch0FrameTs[i]) - int64(ch0FrameTs[i-1]); d != int64(frameSize) {
			t.Fatalf("frameTs step %d: %d -> %d (delta %d), want +%d",
				i, ch0FrameTs[i-1], ch0FrameTs[i], d, frameSize)
		}
	}

	// (c) The returned ch0 frames decode to NEAR-SILENCE: with one client the
	// minus-one mix is S − itself = 0, so the self-contribution must be cancelled.
	// The input sine was ~0.5 full-scale (peak ~16384); a correct minus-one mix
	// leaves only Opus silence artifacts, far below that.
	dec, err := codec.NewDecoder()
	if err != nil {
		t.Fatalf("verify decoder: %v", err)
	}
	pcm := make([]int16, codec.MaxFrameSamples)
	var peak int
	for _, payload := range ch0Returns {
		n, derr := dec.Decode(payload, pcm)
		if derr != nil {
			t.Fatalf("verify decode: %v", derr)
		}
		for _, s := range pcm[:n] {
			a := int(s)
			if a < 0 {
				a = -a
			}
			if a > peak {
				peak = a
			}
		}
	}
	const silenceCeil = 1000 // well below the ~16384 input peak
	if peak >= silenceCeil {
		t.Fatalf("returned ch0 peak amplitude = %d, want < %d (self-contribution not cancelled?)", peak, silenceCeil)
	}
}

// TestVADGate verifies the energy gate: disabled → always active; enabled → loud
// frames active and arm the hangover, quiet frames stay active through the
// hangover then gate out, and a loud frame re-arms.
func TestVADGate(t *testing.T) {
	mk := func(peak int16) [][]int16 {
		f := make([][]int16, proto.Channels)
		for c := range f {
			f[c] = make([]int16, 8)
		}
		f[0][3] = peak // channel 0 carries the VAD energy
		return f
	}

	dis := vadGate{threshold: 0}
	if !dis.active(mk(0)) {
		t.Fatal("disabled VAD must always be active")
	}

	g := vadGate{threshold: 100, hangover: 2}
	if !g.active(mk(5000)) {
		t.Fatal("loud frame must be active")
	}
	if !g.active(mk(10)) || !g.active(mk(10)) {
		t.Fatal("quiet frames within hangover must stay active")
	}
	if g.active(mk(10)) {
		t.Fatal("quiet frame past hangover must gate out (silent)")
	}
	if !g.active(mk(2000)) {
		t.Fatal("loud frame must re-arm the gate")
	}
}

// streamEncodedAudio plays the role of the browser TX path: it encodes a steady
// sine into all 16 channels and sends 9-byte wire packets at the frame cadence,
// for enough frames to fill and drain the MCU's jitter buffer.
func streamEncodedAudio(d *webrtc.DataChannel, frameSize int, errCh chan<- error) {
	encs, err := codec.NewEncoders(proto.Channels, codec.AppRestrictedLowdelay, proto.BitRate)
	if err != nil {
		errCh <- fmt.Errorf("stream encoders: %w", err)
		return
	}

	// Distinct per-channel tone so channels are not accidentally identical.
	in := make([][]int16, proto.Channels)
	for c := range in {
		in[c] = make([]int16, frameSize)
		freq := 220.0 + 40.0*float64(c)
		for s := range in[c] {
			in[c][s] = int16(0.5 * 32767 * math.Sin(2*math.Pi*freq*float64(s)/proto.SampleRate))
		}
	}

	out := make([]byte, codec.MaxPacketBytes)
	seq := make([]uint32, proto.Channels)
	period := time.Duration(frameSize) * time.Second / time.Duration(proto.SampleRate)
	tick := time.NewTicker(period)
	defer tick.Stop()

	const frames = 40 // ~800 ms at 20 ms/frame
	var frameTs uint32
	for f := 0; f < frames; f++ {
		<-tick.C
		for c := 0; c < proto.Channels; c++ {
			n, eerr := encs[c].Encode(in[c], out)
			if eerr != nil {
				errCh <- fmt.Errorf("stream encode ch %d: %w", c, eerr)
				return
			}
			buf, merr := (proto.Packet{ChIdx: uint8(c), SeqNum: seq[c], FrameTs: frameTs, Payload: out[:n]}).MarshalBinary()
			if merr != nil {
				errCh <- fmt.Errorf("stream marshal ch %d: %w", c, merr)
				return
			}
			seq[c]++
			if serr := d.Send(buf); serr != nil {
				// Channel may be closing as the test winds down; stop quietly.
				return
			}
		}
		frameTs += uint32(frameSize)
	}
}

// streamEncodedAudioBundled is the bundled-uplink counterpart: it encodes all 16
// channels per frame, then groups them into bundled v2 packets per the layout
// (R=2 base) and sends each. Exercises the MCU's auto-detect + ParseBundle path.
func streamEncodedAudioBundled(d *webrtc.DataChannel, frameSize int, errCh chan<- error) {
	encs, err := codec.NewEncoders(proto.Channels, codec.AppRestrictedLowdelay, proto.BitRate)
	if err != nil {
		errCh <- fmt.Errorf("stream encoders: %w", err)
		return
	}
	in := make([][]int16, proto.Channels)
	for c := range in {
		in[c] = make([]int16, frameSize)
		freq := 220.0 + 40.0*float64(c)
		for s := range in[c] {
			in[c][s] = int16(0.5 * 32767 * math.Sin(2*math.Pi*freq*float64(s)/proto.SampleRate))
		}
	}

	layout := proto.PlanLayout(frameSize)
	encoded := make([][]byte, proto.Channels)
	out := make([]byte, codec.MaxPacketBytes)
	period := time.Duration(frameSize) * time.Second / time.Duration(proto.SampleRate)
	tick := time.NewTicker(period)
	defer tick.Stop()

	const frames = 40
	var frameTs uint32
	for f := 0; f < frames; f++ {
		<-tick.C
		for c := 0; c < proto.Channels; c++ {
			n, eerr := encs[c].Encode(in[c], out)
			if eerr != nil {
				errCh <- fmt.Errorf("stream encode ch %d: %w", c, eerr)
				return
			}
			encoded[c] = append(encoded[c][:0], out[:n]...)
		}
		for _, group := range layout {
			chans := make([]proto.ChannelPayload, 0, len(group))
			for _, ch := range group {
				chans = append(chans, proto.ChannelPayload{ChIdx: ch, Payload: encoded[ch]})
			}
			buf, merr := proto.MarshalBundle(frameTs, chans, false)
			if merr != nil {
				errCh <- fmt.Errorf("stream marshal bundle: %w", merr)
				return
			}
			if serr := d.Send(buf); serr != nil {
				return // channel closing as the test winds down
			}
		}
		frameTs += uint32(frameSize)
	}
}
