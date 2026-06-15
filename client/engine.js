/**
 * @fileoverview webrtc-node.js
 *
 * Symmetric WebRTC Peer for bidirectional 16-channel Ambisonics streaming.
 *
 * Architecture overview:
 *
 *   Transmit path (TX):
 *     Audio Source (Mic AudioWorklet / WS bridge feedPlanarFrame)
 *       -> 16x AudioEncoder           (Opus mono CBR 64kbps, realtime)
 *       -> RTCDataChannel             (unordered, no-retransmit)
 *
 *   Receive path (RX):
 *     RTCDataChannel
 *       -> demux by channelIndex (header byte 0)
 *       -> 16x AudioDecoder           (Opus mono)
 *       -> Playback AudioWorkletNode  (worklet-receiver.js, jitter buffer)
 *       -> Omnitone HOARenderer       (HRTF binaural rendering)
 *       -> AudioContext.destination
 *
 * Single RTCDataChannel wire format (9-byte header):
 *   Byte  0    : [ isRec(MSB) | reserved(3) | channelIndex(low 4 bits, 0-15) ]
 *   Bytes 1-4  : seqNum        uint32  LE (per-channel monotonic counter)
 *   Bytes 5-8  : frameTs       uint32  LE (timestamp in samples)
 *   Bytes 9+   : raw Opus payload (mono)
 *
 * Performance constraints enforced throughout:
 *   - No DOM or UI work in audio/network hot paths
 *   - Transferable zero-copy postMessage to/from AudioWorklets
 *   - Long-lived routing, stats, and assembly structures are pre-allocated
 *   - Stats polling via setInterval (2 s) - never inside the hot path
 *   - TX worklet output is NEVER routed to audioCtx.destination
 */

'use strict';

// Constants

const CHANNELS    = 16;
const SAMPLE_RATE = 48_000;
const DEFAULT_FRAME_SIZE  = 960;  // 20ms @ 48kHz
const BIT_RATE    = 64_000;       // per mono channel (16 x 64 = 1024 kbps total)

// TX back-pressure latency bound. When the DataChannel backlog would buy more
// than this many milliseconds of audio (at the configured total bitrate), drop
// the newest frames instead of accumulating latency. The old fixed 512 KB cap
// allowed ~4 s of buffered audio on a saturated uplink; this keeps it real-time.
const TX_MAX_BUFFER_MS = 250;
const TX_MAX_BUFFER_FLOOR = 16_000; // bytes — absorb normal SCTP cwnd jitter

// Extra time the outgoing isRec flag stays set after the nominal recording window,
// so the last recorded frame's packet (emitted a few ms after capture, plus any
// send-buffer jitter) still carries the flag. Kept tight: every ms here is extra
// audio the receivers record beyond the sender's window, so a too-large value
// makes receiver.wav noticeably longer than sender.wav. The MCU adds its own
// ~250 ms downlink recHangover on top, so the receiver tail is this + ~250 ms.
const TX_REC_EMIT_HANGOVER_MS = 250;

// Bump whenever a worklet (sender/receiver) changes: appended as ?v= to the
// addModule URL so the browser refetches it. AudioWorklet modules are cached very
// aggressively (a plain reload keeps the old one), which silently ships stale
// worklet code — e.g. a receiver without the latency-drain fix.
const WORKLET_VERSION = '3';

// Cap on raw PDV samples kept for the session report (~83 min at 100 fps). Bounds
// memory on very long sessions; percentiles stay representative well before this.
const PDV_MAX_SAMPLES = 500_000;

const HEADER_SIZE = 9;    // bytes
const ISREC_BIT  = 0x80;  // byte 0: [isRec(MSB) | reserved(3) | chIdx(4 bit)]
const CHIDX_MASK = 0x0F;  // mask to extract the 4-bit channel index

// Pre-allocated buffer
const COPY_OPTIONS = { planeIndex: 0, format: 'f32-planar' };

// Main-thread frame assembly timeout before missing channels are padded.
// Computed dynamically per frame size / prebuffer depth — see
// _updateAssemblyTimeout(). The floor below guards against the browser's
// ~4ms setTimeout clamping at very small frame sizes.
const ASSEMBLY_TIMEOUT_FLOOR_MS = 4;

// Maximum number of incomplete 16-channel frames expected in flight.
const RX_FRAME_POOL_SIZE = 64;

// Per-channel decoder timestamp FIFO capacity. Power of two for cheap wrapping.
const TS_QUEUE_CAPACITY = 64;
const TS_QUEUE_MASK = TS_QUEUE_CAPACITY - 1;

// Per-channel RX sequence reorder window. Power of two for cheap ring indexing.
const RX_SEQ_WINDOW_SIZE = 64;
const RX_SEQ_WINDOW_MASK = RX_SEQ_WINDOW_SIZE - 1;

// Stats polling interval
const STATS_INTERVAL_MS = 2_000;

// Dynamic prebuffer adaptation
const PREBUF_MIN_FRAMES         = 2;
const PREBUF_MAX_FRAMES         = 6;

// PLC-hold: conceal a missing RX channel by repeating its last good frame at a
// fading gain, instead of hard silence — but only for frames ≤ PLC_MAX_FRAME_SIZE
// (≤20 ms). At 40/60 ms a held frame is too long to mask, so we keep the mute.
const PLC_MAX_FRAME_SIZE        = 960;  // 20 ms @ 48 kHz
const PLC_MAX_HOLDS             = 4;    // consecutive holds before reverting to mute
const PLC_DECAY                 = 0.5;  // per-hold gain factor (0.5, 0.25, …)

// Low-latency prebuffer adaptation (MCU client at ≤20 ms, where PLC cushions the
// occasional miss): aim the buffer at the MEASURED jitter floor and converge
// there, instead of blind ±steps. Other paths keep the conservative scheme.
const PREBUF_LOWLAT_FLOOR_FRAMES = 1.5; // floor can dip below 2 frames (PLC covers isolated misses)
const PREBUF_JITTER_K            = 3;   // target ≈ K×jitterMs + 1 frame (covers the jitter tail, not just the mean)
const PREBUF_WINDOW_MS          = 2_000;
const PREBUF_UNDERRUN_THRESHOLD = 2;    // underruns/window before growing


// AmbisonicsNode
//
// Lifecycle (three distinct phases, do not conflate):
//
//   1. WebRTC connection   initWebRTC() -> signaling -> ICE/DTLS -> DataChannel
//                          'connected' is emitted from dc.onopen only.
//   2. Audio pipeline      connect() builds codecs + RX graph (Omnitone) in the
//                          background. 'onAudioReady' fires when the graph is up.
//   3. Audio output        startAudio() resumes the context and unmutes. Called
//                          from a user gesture. Gain is the only thing it toggles.
//
// The AudioContext is created in app-node inside the Connect click (autoplay
// policy) and passed in via opts.audioContext. See _initAudioContext().

export class AmbisonicsNode {

  /**
   * @param {object}   opts
   * @param {WebSocket} opts.signalingSocket    Open signaling WebSocket
   * @param {string}   opts.roomId              Room identifier
   * @param {'offerer'|'answerer'} opts.role    WebRTC offer/answer role
   * @param {AudioContext} [opts.audioContext]  Pre-created context (from a gesture)
   * @param {string}   [opts.captureWorkletUrl='../shared/worklet-sender.js']
   * @param {string}   [opts.playbackWorkletUrl='../shared/worklet-receiver.js']
   * @param {AudioNode} [opts.outputNode]      RX endpoint the renderer connects to (default: audioCtx.destination)
   * @param {number}   [opts.frameSize=960]     Opus frame size in samples
   * @param {object[]} [opts.iceServers]        RTCConfiguration.iceServers array
   * @param {function} [opts.onStats]           Called every STATS_INTERVAL_MS with a snapshot
   * @param {function} [opts.onStateChange]     Called on state transitions
   * @param {function} [opts.onAudioReady]      Called when the RX graph is built
   * @param {function} [opts.onRecordingComplete] Called when a WAV export finishes
   */
  constructor(opts = {}) {
    // Configuration
    this._sigWs               = opts.signalingSocket    ?? null;
    this._roomId              = opts.roomId             ?? 'room-1';
    this._role                = opts.role               ?? 'offerer';
    // Worklet URLs resolve relative to the PAGE (not this module). The defaults
    // assume the page sits one level below shared/ (bidirectional/, multiuser/);
    // app-*.js pass them explicitly anyway.
    this._captureWorkletUrl   = opts.captureWorkletUrl  ?? '../shared/worklet-sender.js';
    this._playbackWorkletUrl  = opts.playbackWorkletUrl ?? '../shared/worklet-receiver.js';
    this._outputNode          = opts.outputNode ?? null; // RX endpoint override (default: destination)

    // Bundled wire format v2 (MCU-only). opts.bundle is the injected codec
    // { marshal, parse, planLayout, BUNDLE_VERSION } from multiuser/bundle.js, so
    // the shared engine stays format-agnostic and the bidirectional path (which
    // injects nothing) runs the legacy per-channel path unchanged. _packing flips
    // to 'bundled' only when the MCU offer advertises it AND a bundle was injected.
    this._bundle    = opts.bundle ?? null;
    this._packing   = 'legacy';
    this._txLayout  = null; // cached planLayout(frameSize)
    this._txBundleTs = null; this._txBundleAccum = null; this._txBundleCount = 0; // TX frame accumulator
    this._rxBundleTs = -1;   this._rxBundleSeen = null;  // RX per-frame dedup (R=2 base arrives twice)
    // PLC-hold (set from frameSize below): conceal a missing channel with its
    // last good frame, faded, for ≤20 ms frames; mute beyond.
    this._plcHold     = false;
    this._lastGoodPcm = new Array(CHANNELS).fill(null); // last decoded frame per channel
    this._holdCount   = new Int32Array(CHANNELS);       // consecutive holds per channel
    // Per-channel highest frameTs handed to the (stateful) Opus decoder. The
    // unordered DataChannel lets the internet reorder packets; decoding an OLDER
    // frame after a newer one desyncs CELT's inter-frame state → "robotic" audio
    // (only on real networks — localhost never reorders). Out-of-order packets are
    // dropped so each decoder only ever moves forward (the gap is concealed).
    this._lastDecodedTs  = new Float64Array(CHANNELS).fill(-1);
    this._rxReorderDrops = 0;
    this._iceServers          = opts.iceServers         ?? [{ urls: 'stun:stun.cloudflare.com:3478' }];
    // ICE transport policy. Defaults to 'relay' (force TURN) to preserve existing
    // behavior; the MCU loopback harness overrides this with 'all' so host/loopback
    // candidates can connect on localhost without a TURN server.
    this._iceTransportPolicy  = opts.iceTransportPolicy ?? 'relay';
    this._onStats             = opts.onStats            ?? null;
    this._onStateChange       = opts.onStateChange      ?? null;

    // Frame size, overridden via setFrameSize()
    this._frameSize = (opts.frameSize != null && Number.isInteger(opts.frameSize) && opts.frameSize > 0)
      ? opts.frameSize
      : DEFAULT_FRAME_SIZE;
    this._plcHold = this._frameSize <= PLC_MAX_FRAME_SIZE;

    // Per-channel encoder bitrate (bps). opts.bitrates is an order-tapered array
    // (more bits for the dominant low orders) injected by the MCU/multiuser path;
    // the MCU offer can override it per-session (so uplink matches the downlink
    // profile). The bidirectional path injects nothing → every channel = BIT_RATE.
    this._applyBitrates(opts.bitrates);

    // Peer state
    this._peerIsReady         = false;
    this._pendingOffer        = null;
    this._offerSent           = false;

    // WebRTC / Signaling
    /** @type {RTCPeerConnection|null} */
    this._pc = null;

    /** @type {RTCDataChannel|null} */
    this._dc = null;

    /** @type {RTCIceCandidateInit[]} ICE candidates received before remote SDP */
    this._pendingIceCandidates = [];

    // Web Audio
    // The context is normally created by app-node inside the Connect gesture and
    // passed in here, so it starts in 'running' state (autoplay policy). If absent,
    // _initAudioContext() creates one as a fallback.
    /** @type {AudioContext|null} */
    this._audioCtx = opts.audioContext ?? null;
    this._ownsAudioCtx = (opts.audioContext == null);

    /** @type {AudioWorkletNode|null} TX capture worklet (sender-processor) */
    this._captureWorklet = null;

    /** @type {AudioWorkletNode|null} RX playback worklet (receiver-processor) */
    this._playbackWorklet = null;

    /** @type {MediaStreamAudioSourceNode|null} */
    this._micSource = null;

    /** @type {MediaStream|null} */
    this._micStream = null;

    // Omnitone HOA renderer (_setupRxGraph)
    /** @type {object|null} Omnitone HOARenderer instance */
    this._hoaRenderer = null;

    // WebCodecs TX - 16 mono Opus encoders
    /** @type {AudioEncoder[]} */
    this._encoders = new Array(CHANNELS).fill(null);

    /** Per-channel monotonic sequence counters */
    this._seqNums = new Uint32Array(CHANNELS);

    /**
     * Running TX timestamp in samples. AudioData timestamps are generated from
     * this integer to keep the packet header immune to floating-point drift.
     */
    this._txFrameTsSamples = 0;

    // WebCodecs RX - 16 mono Opus decoders
    /** @type {AudioDecoder[]} */
    this._decoders = new Array(CHANNELS).fill(null);

    /**
     * FIFO timestamp queues, one per decoder. WebCodecs/Opus may expose decoded
     * timestamps shifted by codec pre-skip, so frame assembly is keyed by the
     * original network frameTs pushed before decode().
     *
     * @type {{values: Uint32Array, head: number, tail: number, size: number}[]}
     */
    this._tsQueues = Array.from({ length: CHANNELS }, () => ({
      values: new Uint32Array(TS_QUEUE_CAPACITY),
      head: 0,
      tail: 0,
      size: 0,
    }));

    /**
     * Frame assembly ring buffer.
     * Indexed by (frameTs / frameSize) % RX_FRAME_POOL_SIZE. Each cell holds at
     * most one in-flight slot. A strict slot.frameTs === frameTs check guards
     * against stale reads from a recycled cell whose previous occupant had a
     * different timestamp.
     * @type {Array<object|null>}
     */
    this._frameRing = new Array(RX_FRAME_POOL_SIZE).fill(null);

    /** Highest frame timestamp already dispatched or intentionally dropped. */
    this._lastFlushedTs = -1;

    /**
     * Pooled assembly slots avoid allocating the data/xfer arrays for each
     * 20 ms frame. The Float32Array channel payloads are still newly allocated
     * because they are transferred to the AudioWorklet and become detached.
     */
    this._frameSlotPool = Array.from({ length: RX_FRAME_POOL_SIZE }, () => ({
      data: new Array(CHANNELS).fill(null),
      xfer: new Array(CHANNELS).fill(null),
      count: 0,
      frameSize: this._frameSize,
      frameTs: -1,
      timer: null,
    }));

    /**
     * ArrayBuffer recycle queue. The receiver worklet transfers the 16
     * channel ArrayBuffers back here after copying their samples into its ring
     * buffer; _acquireFrameSlot draws from this pool instead of allocating fresh
     * Float32Arrays. Bounded to avoid unbounded growth on backpressure.
     */
    this._bufferRecycleQueue = [];
    this._bufferRecycleMax   = RX_FRAME_POOL_SIZE * CHANNELS;


    // Stats
    this._statsTimer      = null;
    this._prevBytesSent   = 0;
    this._prevBytesRecv   = 0;
    this._prevStatsTime   = 0;
    this._totalPktsSent   = 0;
    this._totalPktsRecv   = 0;
    this._totalPktsLost   = 0; // estimated by the RX sequence reorder window

    // Inter-arrival jitter (measured in _onDcMessage, channel 0 only)
    this._jitterMs     = 0;     // running EWMA, exposed via onStats
    this._jLastRecvMs  = 0;     // performance.now() of the last ch0 packet
    this._jLastSendMs  = 0;     // frameTs converted to ms of the last ch0 packet
    this._pdvSamples   = [];    // raw per-packet PDV (|D|, ms) for the session report


    // Per-channel sequence tracking for reorder-tolerant loss detection.
    this._seqRecvBase   = new Uint32Array(CHANNELS);
    this._seqRecvHigh   = new Uint32Array(CHANNELS);
    this._seqRecvSeen   = new Uint16Array(CHANNELS);
    this._hasSeqRecv    = new Uint8Array(CHANNELS);
    this._seqRecvWindow = new Uint8Array(CHANNELS * RX_SEQ_WINDOW_SIZE);

    // Lifecycle flags
    this._pipelineStarted = false;
    this._connected  = false; // true once DC is open and both paths are ready
    this._destroyed  = false;

    // Recording
    this._onRecordingComplete = opts.onRecordingComplete ?? null;
    this._onAudioReady        = opts.onAudioReady        ?? null;
    this._graphReady          = false; // true once Omnitone finishes loading

    this._transmissionStarted  = false;

    // TX recording state
    this._txRec        = { active: false, frames: [], total: 0, left: 0 };
    this._txRecTimeout = null; // safety-net timer in case no audio flows in
    // Wall-clock deadline (performance.now() ms) until which outgoing packets are
    // flagged isRec. The wire flag is driven by time, NOT by a per-frameTs lookup:
    // the Opus encoder shifts its output timestamp vs the encode-input timestamp
    // (a constant pre-skip), so a Set keyed at encode-input never matched at emit.
    this._txRecEmitUntilMs = 0;

    // RX recording state
    this._rxRecActive    = false;
    this._rxRecSet       = new Set(); // frameTs values seen with isRec=1
    this._rxRecFrames    = [];
    this._rxRecTimer     = null; // inactivity timeout
    this._rxRecStartedAt = null; // capture-start time of the current RX window → filename

    // Dynamic prebuffer adaptation
    this._dynPrebuffer  = PREBUF_MIN_FRAMES * this._frameSize; // current target in samples
    this._underrunCount = 0;           // underruns accumulated in the current window
    this._prebufTimer   = null;        // setInterval handle for the adaptation loop

    // Cached main-thread frame assembly timeout (ms). Recomputed by
    // _updateAssemblyTimeout() whenever the frame size or prebuffer depth
    // changes, so it never has to be derived inside the RX hot path.
    this._assemblyTimeoutMs = 0;
    this._updateAssemblyTimeout();

    // Signaling message handler
    this._handleSigMsgBind = this._handleSigMessage.bind(this);
    if (this._sigWs) {
      this._sigWs.addEventListener('message', this._handleSigMsgBind);
    }
  }


  // --- Public API ---

  /**
   * Initialises the WebRTC PeerConnection and DataChannel
   * 
   * @returns {Promise<void>}
   */
  async initWebRTC() {
    if (this._destroyed) throw new Error('AmbisonicsNode has been destroyed');
    if (this._pc) return;
    await this._setupPeerConnection();
    await this._tryStartNegotiation();
  }

  /**
   * Phase 2: build the audio pipeline. Runs concurrently with ICE negotiation.
   *
   * Builds the 16 encoders/decoders, wires the TX capture graph, and loads the
   * RX graph (Omnitone HRTF) as a background task. The output gain starts at 0,
   * so decoded audio is buffered silently until startAudio() unmutes it.
   *
   * Fires opts.onAudioReady() when the RX graph is ready. The UI must gate
   * "Play Audio" on BOTH onAudioReady AND onStateChange('connected').
   *
   * @param {MediaStream|null} [captureStream=null]
   * @returns {Promise<void>}
   */
  async connect(captureStream = null) {
    if (this._destroyed) throw new Error('AmbisonicsNode has been destroyed');
    this._pipelineStarted = true;

    await this._initAudioContext();
    this._buildEncoders();
    this._buildDecoders();
    this._resetRxAssembly();

    // Wire TX immediately. _onEncoderOutput drops silently until the DataChannel
    // is open, so it is safe to start the capture graph now.
    if (captureStream) {
      await this._setupTxGraph(captureStream);
    }
    this._transmissionStarted = true;

    // RX graph (Omnitone) loads in the background, concurrently with ICE.
    this._setupRxGraph().then(() => {
      this._graphReady = true;
      console.log('[AmbisonicsNode] RX graph ready (Omnitone loaded)');
      if (this._onAudioReady) this._onAudioReady();
    }).catch(err => {
      console.error('[AmbisonicsNode] _setupRxGraph failed:', err);
    });
  }

  /**
   * Phase 3: unmute local output. Must be called from a user gesture (Play click).
   *
   * The context was created+resumed inside the Connect gesture, so it is already
   * running; the resume() here is a cheap defensive no-op for the rare auto-suspend
   * case. This method re-anchors the jitter buffer and raises the gain from 0.
   * The gain target is applied by the caller via setGainLinear() (slider value).
   *
   * @returns {Promise<void>}
   */
  async startAudio() {
    if (this._destroyed)   throw new Error('[AmbisonicsNode] Destroyed');
    if (!this._audioCtx)   throw new Error('[AmbisonicsNode] AudioContext missing — call connect() first');
    if (!this._graphReady) throw new Error('[AmbisonicsNode] RX graph not ready — wait for onAudioReady');

    // Defensive: resolves instantly if already running (the normal case).
    if (this._audioCtx.state === 'suspended') {
      await this._audioCtx.resume().catch(() => {});
    }

    // Drop any buffer accumulated while suspended and re-anchor to the next frame.
    if (this._playbackWorklet) {
      this._playbackWorklet.port.postMessage({ type: 'reanchor' });
    }

    // Unmute. setGainLinear() is the single source of truth for output gain;
    // app-node calls updateGain() right after this to apply the slider value.
    this.setGainLinear(1.0);

    this._preAudioDrops = 0;
  }


  /**
   * Sets the active frame size. Must be called before connect() — changing the
   * frame size after the audio pipeline is running is not supported.
   *
   * @param {number} n  Frame size in samples
   */
  setFrameSize(n) {
    console.log(`[DEBUG] setFrameSize setting: ${n} samples. Connection state: ${this._connected}`);
    if (!Number.isInteger(n) || ![120, 240, 480, 960, 1920, 2880].includes(n)) {
      throw new RangeError(`[AmbisonicsNode] Invalid frameSize: ${n}`);
    }
    // Once the DataChannel is open, changing the frame size mid-session is not
    // supported (would desync encoders and decoders on both peers).
    if (this._connected) {
      console.warn('[AmbisonicsNode] setFrameSize() called after DataChannel open — ignored');
      return;
    }
    this._frameSize = n;
    this._plcHold = n <= PLC_MAX_FRAME_SIZE;

    // Reset dynamic prebuffer
    this._dynPrebuffer = PREBUF_MIN_FRAMES * n;

    // Frame size and prebuffer both changed — refresh the cached assembly timeout.
    this._updateAssemblyTimeout();

    // Update assembly pool frame size
    if (this._frameSlotPool) {
      this._frameSlotPool.forEach(slot => { slot.frameSize = n; });
    }
  }

  /**
   * Starts a ~10-second synchronized recording. The window is a whole number of
   * frames — N = ⌈10·SR/frameSize⌉ — so no audio is cut mid-frame.
   *
   * TX side: captures N pre-encode 16-channel planar PCM frames for the local WAV,
   * and flags every outgoing packet isRec=1 for the window's wall-clock duration
   * (a time latch, not a per-frameTs tag — the Opus encoder shifts output
   * timestamps), so the peer(s) record the same window. Exports
   * "sender YYYYMMDD-HHMMSS.wav" when done.
   *
   * RX side (every receiver): detects packets with isRec=1, saves the assembled
   * post-decode frames, and exports "receiver YYYYMMDD-HHMMSS.wav" once the
   * flagged burst ends.
   *
   * opts.onRecordingComplete({ side: 'tx'|'rx' }) fires on each peer after its
   * WAV is exported. The caller is expected to handle the 1 s arming delay.
   *
   * @throws {Error} if not connected or a recording is already in progress
   */
  startRecording() {
    if (!this._connected) throw new Error('[AmbisonicsNode] Not connected — cannot start recording');
    if (!this._transmissionStarted) throw new Error('[AmbisonicsNode] Audio pipeline not ready — call connect() first');
    if (this._txRec.active)  throw new Error('[AmbisonicsNode] Recording already in progress');

    const N = Math.ceil(10 * SAMPLE_RATE / this._frameSize);
    this._txRec.frames    = [];
    this._txRec.total     = N;
    this._txRec.left      = N;
    this._txRec.active    = true;
    this._txRec.startedAt = new Date(); // capture-start time → filename

    // Safety-net: if no audio is flowing the frame counter never reaches 0.
    // Force-complete after the nominal duration + 1 s so the button re-enables.
    const recDurationMs = Math.ceil(N * this._frameSize * 1000 / SAMPLE_RATE);
    // Flag outgoing packets isRec for the whole window plus a generous hangover,
    // so even late encoder emits at the tail still carry the flag for receivers.
    this._txRecEmitUntilMs = performance.now() + recDurationMs + TX_REC_EMIT_HANGOVER_MS;
    this._txRecTimeout = setTimeout(() => {
      this._txRecTimeout = null;
      if (!this._txRec.active) return;
      this._txRec.active = false;
      const frames    = this._txRec.frames;
      const startedAt = this._txRec.startedAt;
      this._txRec.frames = [];
      setTimeout(() => {
        if (frames.length > 0) this._exportWav(frames, 'sender', startedAt);
        if (this._onRecordingComplete) this._onRecordingComplete({ side: 'tx' });
      }, 0);
    }, recDurationMs + 1_000);

    console.log(`[AmbisonicsNode] TX recording started — ${N} frames (${(recDurationMs / 1000).toFixed(1)} s)`);
  }

  /**
   * Encodes one externally supplied 16-channel planar PCM frame. This is the
   * entry point for the WebSocket bridge.
   *
   * @param {Float32Array[]} channels 16 mono planes, normally 960 samples each.
   * @param {number} [frameTsSamples] Absolute frame timestamp in samples. When
   *   omitted, the node advances its own monotonic TX sample clock.
   */
  feedPlanarFrame(channels, frameTsSamples = this._txFrameTsSamples) {
    this._encodePlanarFrame(channels, frameTsSamples);
    this._txFrameTsSamples = frameTsSamples + (channels[0]?.length ?? this._frameSize);
  }

  /**
   * Tears down all resources: codecs, worklets, AudioContext, WebRTC, signaling.
   * Safe to call multiple times.
   *
   * @returns {Promise<void>}
   */
  async destroy() {
    if (this._destroyed) return;
    this._destroyed = true;
    this._connected = false;
    this._pipelineStarted = false;
    this._transmissionStarted = false;
    this._graphReady          = false;

    this._stopStatsPolling();
    this._stopPrebufAdaptation();
    this._underrunCount = 0;
    this._resetRxAssembly();
    this._pendingIceCandidates.length = 0;

    // Recording
    if (this._txRecTimeout) { clearTimeout(this._txRecTimeout); this._txRecTimeout = null; }
    if (this._rxRecTimer)   { clearTimeout(this._rxRecTimer);   this._rxRecTimer   = null; }
    this._txRec.active = false;
    this._txRec.frames = [];
    this._txRecEmitUntilMs = 0;
    this._rxRecActive    = false;
    this._rxRecFrames    = [];
    this._rxRecSet.clear();
    this._rxRecStartedAt = null;

    // Signaling
    if (this._sigWs) {
      this._sigWs.removeEventListener('message', this._handleSigMsgBind);
      if (
        this._sigWs.readyState === WebSocket.OPEN ||
        this._sigWs.readyState === WebSocket.CONNECTING
      ) {
        this._sigWs.close();
      }
      this._sigWs = null;
    }

    // WebRTC
    if (this._dc) { try { this._dc.close(); } catch (_) {} this._dc = null; }
    if (this._pc) { this._pc.close(); this._pc = null; }

    // TX encoders & RX decoders
    for (let c = 0; c < CHANNELS; c++) {
      if (this._encoders[c]) { try { this._encoders[c].close(); } catch (_) {} this._encoders[c] = null; }
      if (this._decoders[c]) { try { this._decoders[c].close(); } catch (_) {} this._decoders[c] = null; }
    }

    // Omnitone
    if (this._hoaRenderer) {
      try { this._hoaRenderer.dispose?.(); } catch (_) {}
      this._hoaRenderer = null;
    }

    // AudioWorklets
    if (this._captureWorklet)  { this._captureWorklet.disconnect();  this._captureWorklet  = null; }
    if (this._playbackWorklet) { this._playbackWorklet.disconnect(); this._playbackWorklet = null; }

    this._inputGain = null;

    // Mic
    if (this._micSource) { this._micSource.disconnect(); this._micSource = null; }
    if (this._micStream) { this._micStream.getTracks().forEach(t => t.stop()); this._micStream = null; }

    // AudioContext - must be last
    if (this._audioCtx) { await this._audioCtx.close(); this._audioCtx = null; }

    this._emitState('destroyed');
  }


  // --- Initialisation - Audio Context & Worklets ---

  /**
   * Registers both AudioWorklet modules on the shared AudioContext.
   * Uses the context passed to the constructor (created inside a user gesture);
   * only creates a new one as a fallback. A new context created here starts
   * suspended and may be unresumable on Firefox, so the gesture path is preferred.
   *
   * @private
   */
  async _initAudioContext() {
    if (!this._audioCtx) {
      this._audioCtx = new AudioContext({ sampleRate: SAMPLE_RATE, latencyHint: 'interactive' });
      this._ownsAudioCtx = true;
    }

    // Register both worklets in parallel - they are independent modules.
    // ?v= busts the browser's aggressive AudioWorklet module cache (see WORKLET_VERSION).
    const bust = (u) => u + (u.includes('?') ? '&' : '?') + 'v=' + WORKLET_VERSION;
    await Promise.all([
      this._audioCtx.audioWorklet.addModule(bust(this._captureWorkletUrl)),
      this._audioCtx.audioWorklet.addModule(bust(this._playbackWorkletUrl)),
    ]);
  }


  // --- TX Path - Capture AudioWorklet -> 16x AudioEncoder -> DataChannel --- // 

  /**
   * Builds the TX audio graph:
   *   MediaStreamAudioSourceNode -> captureWorklet (sender-processor)
   *
   * The worklet is NOT connected to audioCtx.destination - doing so would
   * route the mic signal to the speakers and cause a feedback loop.
   *
   * The worklet posts frames via zero-copy Transferable postMessage:
   *   { timestamp: number, channels: Float32Array[16] }
   * Each Float32Array carries one mono channel of frame_size samples.
   *
   * @private
   * @param {MediaStream} captureStream
   */
  async _setupTxGraph(captureStream) {
    this._micStream = captureStream;
    this._micSource = this._audioCtx.createMediaStreamSource(captureStream);

    this._captureWorklet = new AudioWorkletNode(
      this._audioCtx,
      'sender-processor',
      {
        numberOfInputs:   1,
        numberOfOutputs:  0, // no output
        channelCount:     CHANNELS,
        channelCountMode: 'explicit',
        channelInterpretation: 'discrete',
      }
    );

    // Inform the worklet of the agreed frame size
    this._captureWorklet.port.postMessage({ type: 'config', frameSize: this._frameSize });

    // Hot path: worklet -> encoders
    // The worklet transfers ownership of all 16 Float32Array buffers (zero-copy).
    // We must not access them after postMessage returns on the worklet side.
    this._captureWorklet.port.onmessage = ({ data }) => {
      this.feedPlanarFrame(data.channels, data.timestamp);
    };

    this._micSource.connect(this._captureWorklet);
  }

  /**
   * Hot path - called for every 20ms capture frame.
   * Encodes all 16 channels with their respective AudioEncoder.
   *
   * @private
   * @param {Float32Array[]} channels  16 mono Float32Arrays, _frameSize samples each
   * @param {number} frameTsSamples    Timestamp in samples
   */
  _encodePlanarFrame(channels, frameTsSamples) {
    if (this._destroyed) return;
    if (!this._encoders[0]) {
      // Frames arriving before startAudio(), encoders not yet built, count and warn periodically
      this._preAudioDrops = (this._preAudioDrops ?? 0) + 1;
      if (this._preAudioDrops === 1 || this._preAudioDrops % 50 === 0) {
        console.warn(`[AmbisonicsNode] feedPlanarFrame: encoders not ready — ${this._preAudioDrops} frame(s) dropped. Call startAudio() first.`);
      }
      return;
    }

    const frameSize = channels[0]?.length ?? this._frameSize;

    // TX recording: copy all 16 channels (pre-encode) for the local WAV export.
    // The outgoing wire flag is driven separately by the time-based _txRecEmitUntilMs
    // latch (see startRecording), so it survives the encoder's timestamp shift.
    if (this._txRec.active) {
      const copy = new Array(CHANNELS);
      for (let c = 0; c < CHANNELS; c++) {
        copy[c] = channels[c] ? channels[c].slice() : new Float32Array(frameSize);
      }
      this._txRec.frames.push({ ts: frameTsSamples, channels: copy });
      this._txRec.left--;

      if (this._txRec.left === 0) {
        if (this._txRecTimeout) { clearTimeout(this._txRecTimeout); this._txRecTimeout = null; }
        this._txRec.active = false;
        const frames    = this._txRec.frames;
        const startedAt = this._txRec.startedAt;
        this._txRec.frames = [];
        // Defer export off the hot path
        setTimeout(() => {
          if (frames.length > 0) this._exportWav(frames, 'sender', startedAt);
          if (this._onRecordingComplete) this._onRecordingComplete({ side: 'tx' });
        }, 0);
      }
    }

    const timestampUs = Math.round(frameTsSamples * 1_000_000 / SAMPLE_RATE);

    for (let c = 0; c < CHANNELS; c++) {
      // AudioData constructor copies or references the underlying buffer
      const ad = new AudioData({
        format:           'f32-planar',
        sampleRate:       SAMPLE_RATE,
        numberOfFrames:   frameSize,
        numberOfChannels: 1,
        timestamp:        timestampUs,
        data:             channels[c],
      });

      this._encoders[c].encode(ad);
      ad.close(); // release the AudioData
    }
  }

  /**
   * AudioEncoder output callback - fires once per encoded Opus packet.
   * Builds the 9-byte binary header and sends via the DataChannel.
   *
   * The outgoing ArrayBuffer is allocated at the exact header+payload size and
   * sent directly on the DataChannel.
   *
   * @private
   * @param {number}    chIdx  Channel index (0–15)
   * @param {EncodedAudioChunk} chunk
   */
  _onEncoderOutput(chIdx, chunk) {
    if (!this._dc || this._dc.readyState !== 'open') return;
    if (this._dc.bufferedAmount > this._maxTxBufferBytes) return; // back-pressure: drop on congestion

    const frameTs = Math.round(chunk.timestamp * SAMPLE_RATE / 1_000_000);

    if (this._packing === 'bundled') {
      this._accumulateBundle(chIdx, frameTs, chunk);
      return;
    }

    const seqNum  = this._seqNums[chIdx]++;
    const isRec   = performance.now() < this._txRecEmitUntilMs; // time-based recording latch

    const buf  = new ArrayBuffer(HEADER_SIZE + chunk.byteLength);
    const view = new DataView(buf);

    view.setUint8 (0, (isRec ? ISREC_BIT : 0) | (chIdx & CHIDX_MASK));
    view.setUint32(1, seqNum,  true); // Little-Endian
    view.setUint32(5, frameTs, true);

    chunk.copyTo(new Uint8Array(buf, HEADER_SIZE));
    this._dc.send(buf);

    this._totalPktsSent++;
  }

  /**
   * Bundled TX: accumulate one frame's 16 channel payloads (keyed by frameTs),
   * then emit them as a few bundled packets per the layout. Encoder outputs for a
   * frame arrive close together with the same timestamp; a payload whose frameTs
   * differs flushes the previous frame first (stragglers of an already-flushed
   * frame are dropped — the peer pads). The 16th channel flushes immediately.
   * @private
   */
  _accumulateBundle(chIdx, frameTs, chunk) {
    if (this._txBundleTs !== frameTs) {
      this._flushBundle();
      this._txBundleTs = frameTs;
      this._txBundleAccum = new Array(CHANNELS).fill(null);
      this._txBundleCount = 0;
    }
    if (this._txBundleAccum[chIdx] === null) this._txBundleCount++;
    const p = new Uint8Array(chunk.byteLength);
    chunk.copyTo(p);
    this._txBundleAccum[chIdx] = p;
    if (this._txBundleCount === CHANNELS) this._flushBundle();
  }

  /** @private Marshal+send the accumulated frame's bundles (R=2 base) and reset. */
  _flushBundle() {
    const ts = this._txBundleTs, accum = this._txBundleAccum;
    this._txBundleTs = null; this._txBundleAccum = null; this._txBundleCount = 0;
    if (ts === null || !accum || !this._dc || this._dc.readyState !== 'open') return;
    const layout = this._txLayout || (this._txLayout = this._bundle.planLayout(this._frameSize));
    const isRec  = performance.now() < this._txRecEmitUntilMs; // time-based recording latch
    for (const group of layout) {
      const chans = [];
      for (const ch of group) if (accum[ch]) chans.push({ chIdx: ch, payload: accum[ch] });
      if (chans.length === 0) continue;
      let buf;
      try { buf = this._bundle.marshal(ts, chans, isRec); }
      catch (e) { console.warn('[AmbisonicsNode] marshal bundle:', e.message); continue; }
      this._dc.send(buf);
      this._totalPktsSent++;
    }
  }

  /**
   * Instantiates 16 independent mono Opus AudioEncoders.
   *
   * Each encoder is configured for:
   *   - Mono (1 channel): preserves inter-channel phase relationships required
   *     by Ambisonics - stereo joint/intensity coding would corrupt the soundfield.
   *   - CBR 64 kbps: deterministic bitrate for bandwidth estimation.
   *   - Realtime latency mode: disables look-ahead, minimises algorithmic delay.
   *   - frameDuration: derived from frameSize to align Opus frame boundaries
   *     with our AudioWorklet frame boundaries.
   *
   * @private
   */
  /**
   * Sets the per-channel uplink bitrate (from opts or the MCU offer) and sizes the
   * TX back-pressure cap to the resulting total bitrate. Each channel falls back to
   * BIT_RATE if the array is absent/invalid. Safe to call before encoders are built
   * (the offer applies it, then _buildEncoders reads this._bitrates).
   * @private
   */
  _applyBitrates(arr) {
    this._bitrates = Array.from({ length: CHANNELS }, (_, c) =>
      (Array.isArray(arr) && arr[c] > 0) ? arr[c] : BIT_RATE);
    // TX back-pressure cap (bytes), bounded to TX_MAX_BUFFER_MS of the total
    // bitrate so a saturated uplink sheds the newest audio instead of accumulating
    // latency. See _onEncoderOutput.
    const totalBps = this._bitrates.reduce((a, b) => a + b, 0);
    this._maxTxBufferBytes = Math.max(
      TX_MAX_BUFFER_FLOOR, Math.round(totalBps / 8 * (TX_MAX_BUFFER_MS / 1000)));
  }

  _buildEncoders() {
    this._seqNums.fill(0);

    const frameDurationUs = Math.round(this._frameSize * 1_000_000 / SAMPLE_RATE); // microseconds

    // Adjust Opus complexity based on frame size (0-10)
    const complexity = this._frameSize >= 1920 ? 7
                     : this._frameSize >= 960  ? 5
                     : this._frameSize >= 480  ? 3
                     : this._frameSize >= 240  ? 2
                     : 1;


    for (let c = 0; c < CHANNELS; c++) {
      const chIdx = c; // capture for closure

      this._encoders[c] = new AudioEncoder({
        output: (chunk) => this._onEncoderOutput(chIdx, chunk),
        error:  (err)   => console.error(`[AmbisonicsNode] Encoder[${chIdx}] error:`, err),
      });

      this._encoders[c].configure({
        codec:            'opus',
        sampleRate:       SAMPLE_RATE,
        numberOfChannels: 1,
        bitrate:          this._bitrates[c],
        bitrateMode:      'constant',
        latencyMode:      'realtime',
        opus: {
          frameDuration:  frameDurationUs,
          complexity:     complexity,
          packetlossperc: 0,
        },
      });
    }
  }


  // --- RX Path - DataChannel -> 16x AudioDecoder -> Jitter Buffer Worklet -> Omnitone ---

  /**
   * Builds the RX audio graph:
   *   playbackWorklet (receiver-processor) -> Omnitone HOARenderer -> destination
   *
   * The playback worklet implements a timestamp-based jitter buffer and outputs
   * 16 independent mono channels. Omnitone merges them into a binaural stereo
   * output and routes to the speakers.
   *
   * @private
   */
  async _setupRxGraph() {
    // Playback AudioWorkletNode, 16 output channels, no inputs
    this._playbackWorklet = new AudioWorkletNode( this._audioCtx, 'receiver-processor', {
        numberOfInputs:   0,
        numberOfOutputs:  1,
        channelCount:     CHANNELS,
        channelCountMode: 'explicit',
        channelInterpretation: 'discrete',
        outputChannelCount: [CHANNELS],
      }
    );

    // Configure jitter buffer prebuffer threshold
    this._playbackWorklet.port.postMessage({
      type:      'config',
      frameSize: this._frameSize,
      prebuffer: this._dynPrebuffer,
    });

    // Worklet message handler:
    //   - 'underrun': jitter buffer ran dry; counted by the adaptation loop
    //   - 'recycle':  ArrayBuffers transferred back from the worklet for reuse
    this._playbackWorklet.port.onmessage = ({ data }) => {
      if (data.type === 'underrun') {
        this._underrunCount++;
        console.warn('[AmbisonicsNode] RX jitter buffer underrun');
        return;
      }
      if (data.type === 'depth') { this._rxRingSamples = data.samples; return; } // ring occupancy telemetry
      if (data.type === 'recycle' && data.buffers) {
        const q = this._bufferRecycleQueue;
        for (let i = 0; i < data.buffers.length; i++) {
          if (q.length < this._bufferRecycleMax) q.push(data.buffers[i]);
        }
      }
    };

    // Gain node — initialised at 0.0 so incoming audio is decoded and buffered
    // silently. startAudio() ramps it to 1.0 on the user's Play Audio gesture.
    this._inputGain = this._audioCtx.createGain();
    this._inputGain.gain.value = 0.0;
    this._inputGain.channelCount = CHANNELS;
    this._inputGain.channelCountMode = 'explicit';
    this._inputGain.channelInterpretation = 'discrete';
    
    this._playbackWorklet.connect(this._inputGain); // worklet -> gain

    // Omnitone HOA Renderer
    if (typeof Omnitone !== 'undefined') {
      this._hoaRenderer = Omnitone.createHOARenderer(this._audioCtx, {
        ambisonicOrder: 3,             // 3rd order = 16 channels
        renderingMode:  'ambisonic',
      });

      await this._hoaRenderer.initialize();
      this._hoaRenderer.setRenderingMode?.('ambisonic');
      this._hoaRenderer.input.channelCount          = CHANNELS;
      this._hoaRenderer.input.channelCountMode      = 'explicit';
      this._hoaRenderer.input.channelInterpretation = 'discrete';

      this._inputGain.connect(this._hoaRenderer.input); // gain -> omnitone

      // Route: playbackWorklet -> HOA input -> HOA output -> RX endpoint.
      // The endpoint is audioCtx.destination unless opts.outputNode injected an
      // alternative (e.g. a MediaStreamDestination feeding an <audio> element,
      // so the page can pick the physical output device on browsers without
      // AudioContext.setSinkId — Firefox).
      this._hoaRenderer.output.connect(this._outputNode ?? this._audioCtx.destination);
    } else {
      // Fallback: connect worklet channel 0 directly (mono monitor, no spatialisation)
      console.warn('[AmbisonicsNode] Omnitone not found - bypassing spatial rendering');
      this._inputGain.connect(this._outputNode ?? this._audioCtx.destination);
    }

    this._startPrebufAdaptation();
  }

  /**
   * Instantiates 16 independent mono Opus AudioDecoders.
   * Decoded PCM output is forwarded to the jitter buffer worklet via
   * zero-copy Transferable postMessage.
   *
   * @private
   */
  _buildDecoders() {
    this._resetRxSeqTracking();
    this._lastDecodedTs.fill(-1); // fresh decoders → forget the previous order baseline

    for (let c = 0; c < CHANNELS; c++) {
      const chIdx = c;

      this._decoders[c] = new AudioDecoder({
        output: (audioData) => this._onDecoderOutput(chIdx, audioData),
        error:  (err)       => console.error(`[AmbisonicsNode] Decoder[${chIdx}] error:`, err),
      });

      this._decoders[c].configure({
        codec:            'opus',
        sampleRate:       SAMPLE_RATE,
        numberOfChannels: 1,
      });
    }
  }

  /**
   * Hot path - DataChannel message handler.
   *
   * Parses the 9-byte header, updates reorder-tolerant packet-loss telemetry,
   * and dispatches the Opus payload to the correct AudioDecoder. The fixed
   * header is parsed directly from the incoming ArrayBuffer.
   *
   * @private
   * @param {ArrayBuffer} buf  Raw DataChannel message payload
   */
  _onDcMessage(buf) {
    if (!(buf instanceof ArrayBuffer)) return;

    // Bundled v2: auto-detect by the version nibble, only when a bundle codec was
    // injected (so the bidirectional engine never enters this path).
    if (this._bundle && buf.byteLength >= 7 && (new DataView(buf).getUint8(0) >> 4) === this._bundle.BUNDLE_VERSION) {
      this._onBundle(buf);
      return;
    }

    if (buf.byteLength <= HEADER_SIZE) return;

    const view     = new DataView(buf);
    const byte0    = view.getUint8(0);
    const chIdx    = byte0 & CHIDX_MASK;
    const isRec    = (byte0 & ISREC_BIT) ? 1 : 0;
    const seqNum   = view.getUint32(1, true); // LE
    const frameTs  = view.getUint32(5, true); // LE


    if (chIdx >= CHANNELS) return; // guard against malformed packets

    // inter-arrival jitter measurement
    if (chIdx === 0) {
      const recvMs = performance.now();
      const sendMs = frameTs * 1000 / SAMPLE_RATE;   // samples → ms
      if (this._jLastRecvMs > 0) {
        // D = difference between receive spacing and send spacing
        const D = (recvMs - this._jLastRecvMs) - (sendMs - this._jLastSendMs);
        this._jitterMs += (Math.abs(D) - this._jitterMs) * (1 / 16);
      }
      this._jLastRecvMs = recvMs;
      this._jLastSendMs = sendMs;
    }

    this._trackRxSeq(chIdx, seqNum);
    this._totalPktsRecv++;

    if (!this._graphReady) return;
    if (frameTs <= this._lastFlushedTs) return;

    // Discard packets received before the RX graph is ready
    if (!this._decoders[chIdx]) {
      this._preRxDrops = (this._preRxDrops ?? 0) + 1;
      if (this._preRxDrops === 1 || this._preRxDrops % 100 === 0) {
        console.warn(`[AmbisonicsNode] RX packet dropped (decoders not ready) — ${this._preRxDrops} total, ch${chIdx}. Press Play to start audio.`);
      }
      return;
    }

    // RX recording: mark this frameTs for capture when its slot is dispatched.
    if (isRec === 1) this._markRxRecording(frameTs);

    // Feed the stateful decoder only forward in time (drop reordered/late packets).
    if (!this._acceptInOrder(chIdx, frameTs)) return;

    // Push the original network sample timestamp before decode
    this._pushDecoderTimestamp(chIdx, frameTs);

    // Dispatch Opus payload to the channel's decoder
    const opus = new EncodedAudioChunk({
      type:      'key', // Opus frames are independently decodable
      timestamp: Math.round(frameTs * 1_000_000 / SAMPLE_RATE), // samples -> µs
      data:      new Uint8Array(buf, HEADER_SIZE),
    });

    this._decoders[chIdx].decode(opus);
  }

  /**
   * Bundled RX: parse one v2 packet and dispatch each channel to its decoder.
   * The R=2 base layer (ch0–3) arrives in two packets per frame; it must be
   * decoded ONLY ONCE per frameTs — feeding the same Opus frame to a decoder
   * twice would desync its sequential state — so a per-frame set dedups it.
   * @private
   */
  _onBundle(buf) {
    let parsed;
    try { parsed = this._bundle.parse(new Uint8Array(buf)); }
    catch (e) { console.warn('[AmbisonicsNode] bad bundle:', e.message); return; }

    const { frameTs, chans, isRec } = parsed;
    this._totalPktsRecv++;
    if (!this._graphReady) return;
    if (frameTs <= this._lastFlushedTs) return;

    // RX recording: the MCU sets isRec on the mix when any contributor is recording.
    if (isRec) this._markRxRecording(frameTs);

    if (this._rxBundleTs !== frameTs) { // new frame → reset the dedup set
      this._rxBundleTs = frameTs;
      this._rxBundleSeen = new Set();
    }

    for (let i = 0; i < chans.length; i++) {
      const c = chans[i];
      if (this._rxBundleSeen.has(c.chIdx)) continue; // already decoded for this frame (R=2)
      this._rxBundleSeen.add(c.chIdx);

      if (c.chIdx === 0) this._updateJitter(frameTs);
      if (!this._decoders[c.chIdx]) continue;
      if (!this._acceptInOrder(c.chIdx, frameTs)) continue; // forward-only decode

      this._pushDecoderTimestamp(c.chIdx, frameTs);
      this._decoders[c.chIdx].decode(new EncodedAudioChunk({
        type:      'key',
        timestamp: Math.round(frameTs * 1_000_000 / SAMPLE_RATE),
        data:      c.payload,
      }));
    }
  }

  /**
   * Gate a channel's stateful Opus decoder to STRICTLY ascending frameTs. On the
   * unordered DataChannel the network can deliver a frame out of order; feeding an
   * older frame after a newer one desyncs CELT's overlap/prediction state and
   * produces "robotic" artifacts. Returns false for an out-of-order (late) packet
   * so the caller drops it — that frame is then concealed by the assembler/PLC,
   * which is far less audible than corrupting the decoder. Shared by both RX paths.
   * @private
   */
  _acceptInOrder(chIdx, frameTs) {
    if (frameTs <= this._lastDecodedTs[chIdx]) {
      this._rxReorderDrops++;
      if (this._rxReorderDrops === 1 || this._rxReorderDrops % 200 === 0) {
        console.warn(`[AmbisonicsNode] RX reorder: ${this._rxReorderDrops} out-of-order packet(s) dropped (network reordering; frame concealed instead of desyncing the decoder)`);
      }
      return false;
    }
    this._lastDecodedTs[chIdx] = frameTs;
    return true;
  }

  /** @private Inter-arrival jitter EWMA on the ch0 timeline (shared by both RX paths). */
  _updateJitter(frameTs) {
    const recvMs = performance.now();
    const sendMs = frameTs * 1000 / SAMPLE_RATE;
    if (this._jLastRecvMs > 0) {
      // D = packet delay variation: how much this packet's arrival spacing differs
      // from its send spacing. |D| feeds the EWMA jitter AND is collected raw for
      // the session report (PDV percentiles). Bounded so a long session can't grow
      // the array without limit.
      const D = Math.abs((recvMs - this._jLastRecvMs) - (sendMs - this._jLastSendMs));
      this._jitterMs += (D - this._jitterMs) * (1 / 16);
      if (this._pdvSamples.length < PDV_MAX_SAMPLES) this._pdvSamples.push(D);
    }
    this._jLastRecvMs = recvMs;
    this._jLastSendMs = sendMs;
  }

  /** Raw per-packet PDV samples (|D|, ms) collected since this node connected. */
  getPdvSamples() { return this._pdvSamples; }

  /**
   * AudioDecoder output callback - fires once per decoded PCM frame.
   * Reassembles all 16 mono decoder outputs by original network frameTs before
   * forwarding a complete Ambisonics frame to worklet-receiver.js.
   *
   * @private
   * @param {number}    chIdx      Channel index (0–15)
   * @param {AudioData} audioData  Decoded mono PCM frame from WebCodecs
   */
  _onDecoderOutput(chIdx, audioData) {
    const frameSize = audioData.numberOfFrames;

    // Pull the original frameTs from the DataChannel header
    const frameTs = this._shiftDecoderTimestamp(chIdx);
    if (frameTs < 0 || frameTs <= this._lastFlushedTs) {
      audioData.close();
      return;
    }

    if (!this._graphReady) {
      audioData.close();
      return;
    }

    const ringIdx = Math.floor(frameTs / this._frameSize) % RX_FRAME_POOL_SIZE;
    let frame = this._frameRing[ringIdx];
    if (!frame || frame.frameTs !== frameTs) {
      if (frame && frame.frameTs !== frameTs) {
        this._flushFrame(frame.frameTs);
      }
      frame = this._acquireFrameSlot(frameTs, frameSize);
      this._frameRing[ringIdx] = frame;
      frame.timer = setTimeout(() => this._flushFrame(frameTs), this._assemblyTimeoutMs);
    }

    // Duplicate decoded output for the same channel/timestamp.
    if (frame.data[chIdx] !== null) {
      audioData.close();
      return;
    }

    // Reuse a recycled ArrayBuffer if one of the right size is available,
    // otherwise allocate. frameSize*4 bytes = Float32 mono plane.
    const needBytes = frameSize * 4;
    let mono = null;
    while (this._bufferRecycleQueue.length > 0) {
      const candidate = this._bufferRecycleQueue.pop();
      if (candidate.byteLength === needBytes) {
        mono = new Float32Array(candidate);
        break;
      }
      // Wrong size (frame-size change): discard and keep searching.
    }
    if (mono === null) mono = new Float32Array(frameSize);

    try {
      audioData.copyTo(mono, COPY_OPTIONS);
    } catch (_) {
      // Fallback for TypeError or RangeError
      audioData.copyTo(mono);
    }
    audioData.close();

    frame.data[chIdx] = mono;
    frame.count++;

    // Keep the last good frame for PLC-hold concealment of a future miss (≤20 ms).
    if (this._plcHold) {
      let lg = this._lastGoodPcm[chIdx];
      if (!lg || lg.length !== mono.length) lg = this._lastGoodPcm[chIdx] = new Float32Array(mono.length);
      lg.set(mono);
      this._holdCount[chIdx] = 0;
    }

    if (frame.count === CHANNELS) {
      clearTimeout(frame.timer);
      frame.timer = null;
      this._frameRing[ringIdx] = null;
      this._lastFlushedTs = frameTs;
      this._dispatchFrame(frame);
    }
  }

  /**
   * Returns a pooled 16-channel assembly slot for one network frameTs.
   *
   * @private
   * @param {number} frameTs
   * @param {number} frameSize
   * @returns {object}
   */
  _acquireFrameSlot(frameTs, frameSize) {
    const frame = this._frameSlotPool.pop() ?? {
      data: new Array(CHANNELS).fill(null),
      xfer: new Array(CHANNELS).fill(null),
      count: 0,
      frameSize,
      frameTs,
      timer: null,
    };

    frame.count = 0;
    frame.frameSize = frameSize;
    frame.frameTs = frameTs;
    frame.timer = null;
    return frame;
  }

  /**
   * Timeout path for incomplete Ambisonics frames. Missing channels are padded
   * with silence to always have exactly 16 planes.
   *
   * @private
   * @param {number} frameTs
   */
  _flushFrame(frameTs) {
    const ringIdx = Math.floor(frameTs / this._frameSize) % RX_FRAME_POOL_SIZE;
    const frame = this._frameRing[ringIdx];
    if (!frame || frame.frameTs !== frameTs) return; // already dispatched or evicted

    this._frameRing[ringIdx] = null;
    frame.timer = null;

    if (frameTs <= this._lastFlushedTs) {
      this._releaseFrameSlot(frame);
      return;
    }

    const missing = CHANNELS - frame.count;
    if (missing > 0) {
      // Name the missing channels: a chronic fixed set (esp. ch0 = W, the omni
      // that carries most energy) points to a structural send/decode gap, while
      // a shifting random set points to the decoders falling behind under load.
      // Conceal each gap: PLC-hold (repeat the channel's last good frame, fading
      // over consecutive holds) for ≤20 ms frames, else mute. Past PLC_MAX_HOLDS
      // we revert to mute so a long outage doesn't turn into a stuck tone.
      const missingChs = [];
      for (let c = 0; c < CHANNELS; c++) {
        if (frame.data[c] !== null) continue;
        missingChs.push(c);
        const lg = this._lastGoodPcm[c];
        if (this._plcHold && lg && lg.length === frame.frameSize && this._holdCount[c] < PLC_MAX_HOLDS) {
          const gain = Math.pow(PLC_DECAY, this._holdCount[c] + 1);
          const held = new Float32Array(frame.frameSize);
          for (let i = 0; i < frame.frameSize; i++) held[i] = lg[i] * gain;
          frame.data[c] = held;
          this._holdCount[c]++;
        } else {
          frame.data[c] = new Float32Array(frame.frameSize); // mute
        }
      }
      const how = this._plcHold ? 'PLC-hold' : 'silence';
      console.warn(`[AmbisonicsNode] Frame ${frameTs}: ${missing} channel(s) timed out [${missingChs.join(',')}]; concealed with ${how}`);
    }

    this._lastFlushedTs = frameTs;
    this._dispatchFrame(frame);
  }

  /**
   * Marks the start/continuation of an RX recording window, driven by an incoming
   * isRec flag (legacy header bit or bundled flag). Skipped while this node is
   * itself the recorder: the sender records its pre-encode audio, and on the MCU
   * it would otherwise also RX-record the minus-one mix that the room flags isRec.
   * The guard tracks the TX emit latch (+ margin for the MCU's downlink hangover)
   * rather than _txRec.active, which clears at frame N while the downlink stays
   * flagged for the hangover tail. The frame capture + finalize happen in
   * _dispatchFrame.
   * @private
   */
  _markRxRecording(frameTs) {
    // Don't RX-record on the node that initiated the recording.
    if (this._txRec.active || performance.now() < this._txRecEmitUntilMs + 500) return;
    this._rxRecSet.add(frameTs);
    if (!this._rxRecActive) {
      this._rxRecActive    = true;
      this._rxRecStartedAt = new Date();
      console.log('[AmbisonicsNode] RX recording started');
    }
  }

  /**
   * Posts one complete 16-channel frame to the receiver worklet. The transfer
   * list is reused. After postMessage returns the channel ArrayBuffers are detached.
   *
   * @private
   * @param {object} frame
   */
  _dispatchFrame(frame) {
    if (!this._playbackWorklet || !this._graphReady) {
      this._releaseFrameSlot(frame);
      return;
    }

    // RX recording: copy all 16 channel arrays before postMessage
    if (this._rxRecActive && this._rxRecSet.has(frame.frameTs)) {
      this._rxRecSet.delete(frame.frameTs);
      const copy = new Array(CHANNELS);
      for (let c = 0; c < CHANNELS; c++) {
        copy[c] = frame.data[c] ? frame.data[c].slice() : new Float32Array(frame.frameSize);
      }
      this._rxRecFrames.push({ ts: frame.frameTs, channels: copy });

      // Reset inactivity timer: export 500ms after the last isRec frame lands
      if (this._rxRecTimer) clearTimeout(this._rxRecTimer);
      this._rxRecTimer = setTimeout(() => {
        this._rxRecTimer  = null;
        this._rxRecActive = false;
        const frames    = this._rxRecFrames;
        const startedAt = this._rxRecStartedAt;
        this._rxRecFrames = [];
        this._rxRecSet.clear();
        this._rxRecStartedAt = null;
        console.log(`[AmbisonicsNode] RX recording complete — ${frames.length} frames`);
        if (frames.length > 0) this._exportWav(frames, 'receiver', startedAt);
        if (this._onRecordingComplete) this._onRecordingComplete({ side: 'rx' });
      }, 500);
    }

    for (let c = 0; c < CHANNELS; c++) {
      frame.xfer[c] = frame.data[c].buffer;
    }

    try {
      this._playbackWorklet.port.postMessage(
        { type: 'frame', timestamp: frame.frameTs, channels: frame.data },
        frame.xfer,
      );
    } finally {
      this._releaseFrameSlot(frame);
    }
  }

  /**
   * Clears a frame assembly slot and returns it to the pool.
   *
   * @private
   * @param {object} frame
   */
  _releaseFrameSlot(frame) {
    if (frame.timer) {
      clearTimeout(frame.timer);
      frame.timer = null;
    }

    for (let c = 0; c < CHANNELS; c++) {
      frame.data[c] = null;
      frame.xfer[c] = null;
    }

    frame.count = 0;
    frame.frameSize = this._frameSize;
    frame.frameTs = -1;

    if (this._frameSlotPool.length < RX_FRAME_POOL_SIZE) {
      this._frameSlotPool.push(frame);
    }
  }


  // --- WebRTC - PeerConnection & DataChannel ---

  /**
   * Creates the RTCPeerConnection and a single unordered/no-retransmit
   * DataChannel that carries all 16 mono Opus streams.
   *
   * DataChannel configuration:
   *   ordered: false         - packets are timestamped and sorted by the jitter
   *                            buffer, so out-of-order delivery is fine.
   *   maxRetransmits: 0      - lost packets are never retransmitted; keep
   *                            latency low.
   *
   * @private
   */
  async _setupPeerConnection() {
    this._pc = new RTCPeerConnection({
      iceServers: this._iceServers,
      iceTransportPolicy: this._iceTransportPolicy // 'relay' by default; 'all' for localhost loopback
    });

    // DataChannel: offerer creates it, answerer receives via ondatachannel
    if (this._role === 'offerer') {
      this._dc = this._pc.createDataChannel('ambi-ch', {
        ordered:        false,
        maxRetransmits: 0,
      });
      this._dc.binaryType = 'arraybuffer';
      this._attachDcHandlers(this._dc);
    } else {
      this._pc.ondatachannel = ({ channel }) => {
        this._dc = channel;
        this._dc.binaryType = 'arraybuffer';
        this._attachDcHandlers(this._dc);
      };
    }

    // ICE candidate
    this._pc.onicecandidate = ({ candidate }) => {
      if (candidate && this._sigWs?.readyState === WebSocket.OPEN) {
        this._sigWs.send(JSON.stringify({
          type:      'ice',
          room:      this._roomId,
          candidate,
        }));
      }
    };

    // Connection state monitoring
    this._pc.onconnectionstatechange = () => {
      const s = this._pc.connectionState;

      if (s === 'failed' || s === 'disconnected') {
        this._connected = false;
        this._stopStatsPolling();
        this._emitState(s);
        return;
      }

      // Recovery from a transient ICE blip: the PC returns to 'connected' but
      // the DataChannel never re-opens (it never closed), so dc.onopen cannot
      // re-emit — without this the UI stays on "Reconnecting..." forever while
      // audio actually flows.
      if (s === 'connected' && !this._connected && this._dc?.readyState === 'open') {
        this._connected = true;
        this._startStatsPolling();
        this._emitState('connected');
      }
    };
  }

  /**
   * Attaches open/close/error/message handlers to a DataChannel instance.
   *
   * @private
   * @param {RTCDataChannel} dc
   */
  _attachDcHandlers(dc) {
    dc.onopen  = () => {
      if (this._connected) return;
      console.log('[AmbisonicsNode] DataChannel open');

      this._connected = true;
      this._startStatsPolling();
      this._emitState('connected');
    };

    dc.onclose = () => console.log('[AmbisonicsNode] DataChannel closed');
    dc.onerror = (e) => console.error('[AmbisonicsNode] DataChannel error:', e.error ?? e);

    dc.onmessage = ({ data }) => this._onDcMessage(data);
  }

  async _handleSigMessage(event) {
    let msg;
    try { msg = JSON.parse(event.data); } catch { return; }

    switch (msg.type) {
      case 'peer_ready':
        this._peerIsReady = true;
        await this._tryStartNegotiation();
        break;

      case 'peer_disconnected':
        console.log('[AmbisonicsNode] Remote peer disconnected via signaling');
        this._emitState('peer-stopped');
        break;

      case 'offer':
        if (this._role !== 'answerer') break;
        if (msg.frameSize) {
          this.setFrameSize(msg.frameSize); // Align incoming frame size
        }
        // MCU-authoritative uplink bitrate profile: match the downlink so a single
        // server flag controls both directions. Applied before _buildEncoders.
        if (Array.isArray(msg.bitrates) && msg.bitrates.length === CHANNELS) {
          this._applyBitrates(msg.bitrates);
        }
        // Enable the bundled wire format only if the MCU advertises it AND a
        // bundle codec was injected (multiuser/app-mcu.js). Otherwise stay legacy.
        if (this._bundle && msg.packing === 'bundled') {
          this._packing  = 'bundled';
          this._txLayout = this._bundle.planLayout(this._frameSize);
          console.log(`[AmbisonicsNode] bundled packing on (frameSize=${this._frameSize}, ${this._txLayout.length} packets/frame)`);
        }
        this._pendingOffer = msg.sdp;
        await this._tryStartNegotiation();
        break;

      case 'answer':
        if (!this._pc) break;
        await this._pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
        await this._drainPendingIceCandidates();
        break;

      case 'ice':
        if (!msg.candidate) break;
        if (!this._pc || !this._pc.remoteDescription) {
          this._pendingIceCandidates.push(msg.candidate);
          break;
        }
        await this._addIceCandidate(msg.candidate);
        break;
    }
  }

  async _tryStartNegotiation() {
    if (!this._pc) return;

    if (this._role === 'offerer' && this._peerIsReady && !this._offerSent) {
      this._offerSent = true;
      const offer = await this._pc.createOffer();
      await this._pc.setLocalDescription(offer);
      this._sigWs.send(JSON.stringify({
        type: 'offer',
        room: this._roomId,
        sdp: this._pc.localDescription,
        frameSize: this._frameSize
      }));
    } 
    else if (this._role === 'answerer' && this._pendingOffer) {
      const sdp = this._pendingOffer;
      this._pendingOffer = null;
      await this._pc.setRemoteDescription(new RTCSessionDescription(sdp));
      await this._drainPendingIceCandidates();
      const answer = await this._pc.createAnswer();
      await this._pc.setLocalDescription(answer);
      this._sigWs.send(JSON.stringify({
        type: 'answer',
        room: this._roomId,
        sdp: this._pc.localDescription,
        frameSize: this._frameSize
      }));
    }
  }


  // --- Stats - async polling, decoupled from the hot path ---

  /**
   * Starts periodic RTCStats polling (every 2 seconds).
   *
   * Stats snapshot shape:
   * {
   *   rttMs:      number,   // current RTT in milliseconds
   *   txKbps:     number,   // TX bitrate (kbps) since last poll
   *   rxKbps:     number,   // RX bitrate (kbps) since last poll
   *   pktsSent:   number,   // cumulative packets sent
   *   pktsRecv:   number,   // cumulative packets received
   *   pktsLost:   number,   // estimated cumulative packets lost (RX seq window)
   *   prebufferMs: number,  // current RX jitter-buffer target depth (ms)
   *   lossRate:   number,   // loss rate [0–1] since construction
   * }
   *
   * @private
   */
  _startStatsPolling() {
    this._stopStatsPolling();
    this._prevBytesSent = 0;
    this._prevBytesRecv = 0;
    this._prevStatsTime = performance.now();
    this._jitterMs    = 0;
    this._jLastRecvMs = 0;
    this._jLastSendMs = 0;

    this._statsTimer = setInterval(async () => {
      if (!this._pc) return;

      const stats = await this._pc.getStats();
      const now   = performance.now();
      const dt    = (now - this._prevStatsTime) / 1_000; // seconds

      let bytesSent    = 0;
      let bytesRecv    = 0;
      const candidateTypes = new Map();
      const pairs = new Map();   // id -> candidate-pair stat
      let selectedPairId = null; // transport.selectedCandidatePairId (most authoritative)

      stats.forEach(r => {
        if (r.type === 'candidate-pair') pairs.set(r.id, r);
        if (r.type === 'transport' && r.selectedCandidatePairId) selectedPairId = r.selectedCandidatePairId;
        if (r.type === 'local-candidate') candidateTypes.set(r.id, r.candidateType);
        if (r.type === 'data-channel') {
          bytesSent += r.bytesSent     ?? 0;
          bytesRecv += r.bytesReceived ?? 0;
        }
      });

      // Pick the ACTIVE pair, not just any succeeded one: relay fallback pairs also
      // reach "succeeded", so taking an arbitrary succeeded pair mislabels a direct
      // P2P link as TURN. Prefer transport.selectedCandidatePairId, then the pair
      // flagged selected, then a nominated+succeeded pair (the one carrying bytes).
      let activePair = selectedPairId ? pairs.get(selectedPairId) : null;
      if (!activePair) for (const p of pairs.values()) if (p.selected === true) { activePair = p; break; }
      if (!activePair) for (const p of pairs.values()) {
        if (p.nominated && p.state === 'succeeded' &&
            (!activePair || (p.bytesReceived ?? 0) > (activePair.bytesReceived ?? 0))) activePair = p;
      }

      const rttMs = (activePair && activePair.currentRoundTripTime != null) ? activePair.currentRoundTripTime * 1_000 : 0;
      const localCandidateId = activePair ? activePair.localCandidateId : null;
      let medianJitter = this._jitterMs;
      const transportType = localCandidateId ? candidateTypes.get(localCandidateId) : null;
      const bufferPressure = this._dc ? this._dc.bufferedAmount : 0;

      const txKbps = dt > 0 ? ((bytesSent - this._prevBytesSent) * 8 / dt / 1_000) : 0;
      const rxKbps = dt > 0 ? ((bytesRecv - this._prevBytesRecv) * 8 / dt / 1_000) : 0;

      this._prevBytesSent = bytesSent;
      this._prevBytesRecv = bytesRecv;
      this._prevStatsTime = now;

      const lossRate = this._totalPktsRecv > 0
        ? this._totalPktsLost / (this._totalPktsRecv + this._totalPktsLost)
        : 0;

      // Client RX latency breakdown (to locate where buffering hides): the WebCodecs
      // decoder backlog (chunks queued but not yet decoded — UPSTREAM of the jitter
      // buffer, so the worklet drain can't bound it) and the worklet ring depth
      // (reported by the worklet itself).
      let decoderQueue = 0;
      for (let c = 0; c < CHANNELS; c++) {
        if (this._decoders[c]) decoderQueue += this._decoders[c].decodeQueueSize || 0;
      }
      const frameMs = this._frameSize * 1000 / SAMPLE_RATE;
      const decoderQueueMs = (decoderQueue / CHANNELS) * frameMs; // avg per-channel backlog
      const rxRingMs = (this._rxRingSamples ?? 0) * 1000 / SAMPLE_RATE;

      if (this._onStats) {
        this._onStats({
          rttMs,
          medianJitter,
          transportType,
          bufferPressure,
          prebufferMs: this._dynPrebuffer * 1000 / SAMPLE_RATE,
          decoderQueueMs,
          rxRingMs,
          txKbps,
          rxKbps,
          pktsSent: this._totalPktsSent,
          pktsRecv: this._totalPktsRecv,
          pktsLost: this._totalPktsLost,
          lossRate,
        });
      }
    }, STATS_INTERVAL_MS);
  }

  /** @private */
  _stopStatsPolling() {
    if (this._statsTimer) {
      clearInterval(this._statsTimer);
      this._statsTimer = null;
    }
  }

  // --- Dynamic Prebuffer Adaptation ---

  /**
   * Recomputes the cached frame assembly timeout from the current frame size
   * and prebuffer depth.
   *
   *   base    = frameDurationMs + 15ms        (decode + scheduling margin)
   *   cap     = 0.75 × prebufferMs            (never eat more than 3/4 of the
   *                                            jitter buffer on one stalled frame)
   *   timeout = clamp(base, floor, cap)
   *
   * The 4ms floor documents the intent of not dropping below the browser's
   * effective setTimeout resolution, rather than relying on implicit clamping.
   * Called whenever _frameSize or _dynPrebuffer changes.
   *
   * @private
   */
  _updateAssemblyTimeout() {
    const frameDurationMs = this._frameSize * 1000 / SAMPLE_RATE;
    const base    = frameDurationMs + 15;
    const prebufMs = this._dynPrebuffer * 1000 / SAMPLE_RATE;
    this._assemblyTimeoutMs = Math.max(
      ASSEMBLY_TIMEOUT_FLOOR_MS,
      Math.min(base, prebufMs * 0.75),
    );
  }

  /**
   * Starts the 2-second adaptation loop that adjusts the jitter buffer's fill
   * threshold in response to observed underruns.
   *
   * Growth path  (count > PREBUF_UNDERRUN_THRESHOLD):
   * Increase by 1 frame, cap at PREBUF_MAX_FRAMES × frameSize. Sends a
   * 'reanchor' to the worklet first so it pauses, then re-fills to the new
   * (deeper) threshold before resuming playback. This produces a brief
   * dropout but prevents continuous stuttering on a congested link.
   *
   * Decay path (count === 0 AND prebuffer above floor):
   * Decrease by half a frame, floor at PREBUF_MIN_FRAMES × frameSize.
   * Only a 'config' message is sent — no reanchor — so the buffer simply
   * drains naturally to the new, shallower threshold without interrupting
   * playback.
   *
   * @private
   */
  _startPrebufAdaptation() {
    this._stopPrebufAdaptation();
    this._underrunCount = 0;
    this._updateAssemblyTimeout();

    this._prebufTimer = setInterval(() => {
      if (this._destroyed || !this._playbackWorklet) return;

      const count = this._underrunCount;
      this._underrunCount = 0;

      const frameSize = this._frameSize;
      const maxPrebuf = PREBUF_MAX_FRAMES * frameSize;

      // Low-latency, jitter-driven adaptation: only for the MCU client at ≤20 ms,
      // where PLC conceals the occasional miss. Aim the buffer at the measured
      // jitter floor (K×jitter + 1 frame) and converge there. Other paths fall
      // through to the conservative blind-step scheme below (unchanged).
      if (this._bundle && this._plcHold) {
        const floor    = Math.round(PREBUF_LOWLAT_FLOOR_FRAMES * frameSize);
        const jitterSm = Math.round(this._jitterMs * SAMPLE_RATE / 1000);
        const target   = Math.max(floor, Math.min(maxPrebuf, PREBUF_JITTER_K * jitterSm + frameSize));

        if (count > PREBUF_UNDERRUN_THRESHOLD) {
          // Genuinely too shallow: jump to cover the jitter (at least +1 frame).
          // The buffer is empty on underrun, so reanchor + refill at the new depth.
          const next = Math.max(floor, Math.min(maxPrebuf, Math.max(target, this._dynPrebuffer + frameSize)));
          if (next !== this._dynPrebuffer) {
            this._dynPrebuffer = next;
            this._updateAssemblyTimeout();
            console.log(`[AmbisonicsNode] Prebuffer ↑ ${(next / SAMPLE_RATE * 1000).toFixed(0)} ms (jitter ${this._jitterMs.toFixed(1)} ms, ${count} underruns)`);
            this._playbackWorklet.port.postMessage({ type: 'reanchor' });
            this._playbackWorklet.port.postMessage({ type: 'config', prebuffer: next });
          }
        } else if (count === 0 && this._dynPrebuffer > target) {
          // Stable: bleed toward the jitter-safe target (½ frame/window) and stop
          // there — no overshoot, no reanchor (the buffer drains naturally).
          const next = Math.max(target, this._dynPrebuffer - Math.ceil(frameSize / 2));
          if (next !== this._dynPrebuffer) {
            this._dynPrebuffer = next;
            this._updateAssemblyTimeout();
            console.log(`[AmbisonicsNode] Prebuffer ↓ ${(next / SAMPLE_RATE * 1000).toFixed(0)} ms (target ${(target / SAMPLE_RATE * 1000).toFixed(0)} ms, stable)`);
            this._playbackWorklet.port.postMessage({ type: 'config', prebuffer: next });
          }
        }
        // 1..PREBUF_UNDERRUN_THRESHOLD underruns: PLC absorbed them → hold steady.
        return;
      }

      const minPrebuf = PREBUF_MIN_FRAMES * frameSize;
      if (count > PREBUF_UNDERRUN_THRESHOLD) {
        // Network is unstable: grow the prebuffer by one frame.
        const next = Math.min(this._dynPrebuffer + frameSize, maxPrebuf);
        if (next !== this._dynPrebuffer) {
          this._dynPrebuffer = next;
          this._updateAssemblyTimeout();
          const ms = (next / SAMPLE_RATE * 1000).toFixed(0);
          console.log(`[AmbisonicsNode] Prebuffer ↑ ${ms} ms (${count} underruns in window)`);

          // Reanchor first: worklet resets and waits for the buffer to fill
          // to the new, deeper threshold before it starts playing again.
          this._playbackWorklet.port.postMessage({ type: 'reanchor' });
          this._playbackWorklet.port.postMessage({ type: 'config', prebuffer: next });
        }
      } else if (count === 0 && this._dynPrebuffer > minPrebuf) {
        // Network is stable: gently bleed the prebuffer back toward the floor.
        const next = Math.max(
          this._dynPrebuffer - Math.ceil(frameSize / 2),
          minPrebuf,
        );
        if (next !== this._dynPrebuffer) {
          this._dynPrebuffer = next;
          this._updateAssemblyTimeout();
          const ms = (next / SAMPLE_RATE * 1000).toFixed(0);
          console.log(`[AmbisonicsNode] Prebuffer ↓ ${ms} ms (window stable)`);

          // No reanchor: the buffer drains to the new threshold naturally,
          // so playback continues without interruption.
          this._playbackWorklet.port.postMessage({ type: 'config', prebuffer: next });
        }
      }
    }, PREBUF_WINDOW_MS);
  }

  /**
   * Stops the prebuffer adaptation loop. Safe to call when already stopped.
   *
   * @private
   */
  _stopPrebufAdaptation() {
    if (this._prebufTimer) {
      clearInterval(this._prebufTimer);
      this._prebufTimer = null;
    }
  }


  // --- Internal helpers ---

  /**
   * Adds an ICE candidate immediately when remote SDP is available, otherwise
   * queues it. This avoids losing early ICE candidates during signaling.
   *
   * @private
   * @param {RTCIceCandidateInit} candidate
   * @returns {Promise<void>}
   */
  async _addIceCandidate(candidate) {
    if (!this._pc) return;

    if (!this._pc.remoteDescription) {
      this._pendingIceCandidates.push(candidate);
      return;
    }

    await this._pc.addIceCandidate(new RTCIceCandidate(candidate)).catch(() => {});
  }

  /**
   * Drains ICE candidates that arrived before setRemoteDescription().
   *
   * @private
   * @returns {Promise<void>}
   */
  async _drainPendingIceCandidates() {
    if (!this._pc || !this._pc.remoteDescription) return;

    for (let i = 0; i < this._pendingIceCandidates.length; i++) {
      await this._pc
        .addIceCandidate(new RTCIceCandidate(this._pendingIceCandidates[i]))
        .catch(() => {});
    }
    this._pendingIceCandidates.length = 0;
  }

  /**
   * Pushes one original network frameTs into a per-channel decoder FIFO.
   *
   * @private
   * @param {number} chIdx
   * @param {number} frameTs
   */
  _pushDecoderTimestamp(chIdx, frameTs) {
    const q = this._tsQueues[chIdx];

    if (q.size === TS_QUEUE_CAPACITY) {
      q.head = (q.head + 1) & TS_QUEUE_MASK;
      q.size--;
      this._totalPktsLost++;
    }

    q.values[q.tail] = frameTs;
    q.tail = (q.tail + 1) & TS_QUEUE_MASK;
    q.size++;
  }

  /**
   * Pops one original network frameTs for a decoder output.
   *
   * @private
   * @param {number} chIdx
   * @returns {number} frameTs, or -1 if the queue is empty
   */
  _shiftDecoderTimestamp(chIdx) {
    const q = this._tsQueues[chIdx];
    if (q.size === 0) return -1;

    const frameTs = q.values[q.head];
    q.head = (q.head + 1) & TS_QUEUE_MASK;
    q.size--;
    return frameTs;
  }

  /**
   * Resets RX timestamp queues and frame assembly state. Called before starting
   * a new session and during teardown so timeout callbacks cannot fire late.
   *
   * @private
   */
  _resetRxAssembly() {
    for (let i = 0; i < RX_FRAME_POOL_SIZE; i++) {
      const frame = this._frameRing[i];
      if (frame) {
        this._releaseFrameSlot(frame);
        this._frameRing[i] = null;
      }
    }

    for (let c = 0; c < CHANNELS; c++) {
      this._tsQueues[c].head = 0;
      this._tsQueues[c].tail = 0;
      this._tsQueues[c].size = 0;
    }

    this._lastFlushedTs = -1;
    if (this._bufferRecycleQueue) this._bufferRecycleQueue.length = 0;
    this._resetRxSeqTracking();

    // Flush in-flight decoder operations to prevent stale outputs from
    // consuming timestamps from the freshly cleared queues (FIFO desync).
    for (let c = 0; c < CHANNELS; c++) {
      const decoder = this._decoders[c];
      if (decoder && decoder.state !== 'closed') {
        try {
          // Aborts all pending decodes; decoder becomes 'unconfigured'
          decoder.reset();
        } catch (_) { /* ignore */ }

        // Immediately reconfigure so the decoder is ready for new work
        try {
          decoder.configure({
            codec:            'opus',
            sampleRate:       SAMPLE_RATE,
            numberOfChannels: 1,
          });
        } catch (err) {
          console.error(`[AmbisonicsNode] Failed to reconfigure decoder ${c} after reset:`, err);
        }
      }
    }
  }

  /**
   * Resets the fixed per-channel RX sequence reorder windows.
   *
   * @private
   */
  _resetRxSeqTracking() {
    this._seqRecvBase.fill(0);
    this._seqRecvHigh.fill(0);
    this._seqRecvSeen.fill(0);
    this._hasSeqRecv.fill(0);
    this._seqRecvWindow.fill(0);
  }

  /**
   * Tracks packet sequence numbers without assuming ordered delivery.
   *
   * Each channel owns a fixed ring window. Out-of-order packets are accepted
   * while they are still inside the window; a sequence is counted as lost only
   * when newer traffic advances the window past that sequence.
   *
   * @private
   * @param {number} chIdx
   * @param {number} seqNum
   */
  _trackRxSeq(chIdx, seqNum) {
    const windowOffset = chIdx * RX_SEQ_WINDOW_SIZE;

    if (!this._hasSeqRecv[chIdx]) {
      this._seqRecvBase[chIdx] = seqNum;
      this._seqRecvHigh[chIdx] = seqNum;
      this._seqRecvWindow[windowOffset + (seqNum & RX_SEQ_WINDOW_MASK)] = 1;
      this._seqRecvSeen[chIdx] = 1;
      this._hasSeqRecv[chIdx] = 1;
      return;
    }

    const base = this._seqRecvBase[chIdx];
    const offsetFromBase = (seqNum - base) >>> 0;

    if (offsetFromBase >= RX_SEQ_WINDOW_SIZE) {
      const high = this._seqRecvHigh[chIdx];
      if (!this._isSeqNewer(seqNum, high)) return;

      const advance = offsetFromBase - RX_SEQ_WINDOW_SIZE + 1;
      if (advance > RX_SEQ_WINDOW_SIZE) {
        this._totalPktsLost += advance - this._seqRecvSeen[chIdx];
        this._clearRxSeqWindow(chIdx);
        this._seqRecvSeen[chIdx] = 0;
        this._seqRecvBase[chIdx] = (seqNum - RX_SEQ_WINDOW_SIZE + 1) >>> 0;
      } else {
        this._advanceRxSeqWindow(chIdx, advance);
      }

      this._seqRecvHigh[chIdx] = seqNum;
    } else if (this._isSeqNewer(seqNum, this._seqRecvHigh[chIdx])) {
      this._seqRecvHigh[chIdx] = seqNum;
    }

    const slot = windowOffset + (seqNum & RX_SEQ_WINDOW_MASK);
    if (this._seqRecvWindow[slot] === 0) {
      this._seqRecvWindow[slot] = 1;
      this._seqRecvSeen[chIdx]++;
    }
  }

  /**
   * Expires the oldest sequence slots and counts only still-unseen packets.
   *
   * @private
   * @param {number} chIdx
   * @param {number} count
   */
  _advanceRxSeqWindow(chIdx, count) {
    const windowOffset = chIdx * RX_SEQ_WINDOW_SIZE;
    let base = this._seqRecvBase[chIdx];

    for (let i = 0; i < count; i++) {
      const slot = windowOffset + (base & RX_SEQ_WINDOW_MASK);
      if (this._seqRecvWindow[slot] === 0) {
        this._totalPktsLost++;
      } else {
        this._seqRecvWindow[slot] = 0;
        this._seqRecvSeen[chIdx]--;
      }
      base = (base + 1) >>> 0;
    }

    this._seqRecvBase[chIdx] = base;
  }

  /**
   * Clears one channel's sequence ring after a large discontinuity.
   *
   * @private
   * @param {number} chIdx
   */
  _clearRxSeqWindow(chIdx) {
    const windowOffset = chIdx * RX_SEQ_WINDOW_SIZE;
    for (let i = 0; i < RX_SEQ_WINDOW_SIZE; i++) {
      this._seqRecvWindow[windowOffset + i] = 0;
    }
  }

  /**
   * Compare sequence numbers, handling wraparound.
   *
   * @private
   * @param {number} seqNum
   * @param {number} lastSeq
   * @returns {boolean}
   */
  _isSeqNewer(seqNum, lastSeq) {
    return ((seqNum - lastSeq) | 0) > 0;
  }

  /**
   * Emits a state-change event to the caller if opts.onStateChange was provided.
   * 
   * @private
   * @param {string} state
   */
  _emitState(state) {
    if (this._onStateChange) this._onStateChange(state);
  }

  /**
   * Builds a WAVE_FORMAT_EXTENSIBLE 48 kHz / 16-channel / Float32 WAV from the
   * recorded frames and triggers a browser download. role is 'sender' or
   * 'receiver'; startedAt is the capture-start time used in the filename
   * "<role> YYYYMMDD-HHMMSS.wav".
   */
  _exportWav(frames, role, startedAt) {
    if (!frames.length) return;

    const t   = startedAt instanceof Date ? startedAt : new Date();
    const pad = num => String(num).padStart(2, '0');
    const stamp = `${t.getFullYear()}${pad(t.getMonth() + 1)}${pad(t.getDate())}-${pad(t.getHours())}${pad(t.getMinutes())}${pad(t.getSeconds())}`;
    const filename = `${role} ${stamp}.wav`;

    frames.sort((a, b) => a.ts - b.ts);

    const frameSize = frames[0].channels[0].length;
    const firstTs   = frames[0].ts;

    const complete = [];
    let expectedTs = firstTs;
    for (const frame of frames) {
      while (expectedTs < frame.ts) {
        complete.push(Array.from({ length: CHANNELS }, () => new Float32Array(frameSize)));
        expectedTs += frameSize;
      }
      complete.push(frame.channels);
      expectedTs = frame.ts + frameSize;
    }

    const numFrames  = complete.length;
    const dataBytes  = numFrames * frameSize * CHANNELS * 4;

    const buf  = new ArrayBuffer(68 + dataBytes);
    const view = new DataView(buf);
    let off = 0;

    const w4 = s => { for (let i = 0; i < 4; i++) view.setUint8(off++, s.charCodeAt(i)); };
    const u16 = v => { view.setUint16(off, v, true); off += 2; };
    const u32 = v => { view.setUint32(off, v, true); off += 4; };

    w4('RIFF'); u32(60 + dataBytes); w4('WAVE');

    w4('fmt '); u32(40);
    u16(0xFFFE);                            // wFormatTag: EXTENSIBLE
    u16(CHANNELS);                          // nChannels
    u32(SAMPLE_RATE);                       // nSamplesPerSec
    u32(SAMPLE_RATE * CHANNELS * 4);        // nAvgBytesPerSec
    u16(CHANNELS * 4);                      // nBlockAlign
    u16(32);                                // wBitsPerSample
    u16(22);                                // cbSize
    u16(32);                                // wValidBitsPerSample
    u32(0);                                 // dwChannelMask
    [3,0,0,0, 0,0, 16,0, 128,0, 0,170,0,56,155,113].forEach(b => view.setUint8(off++, b));

    w4('data'); u32(dataBytes);

    const pcm = new Float32Array(buf, 68);
    let idx = 0;
    for (const chs of complete) {
      for (let s = 0; s < frameSize; s++) {
        for (let c = 0; c < CHANNELS; c++) {
          pcm[idx++] = chs[c][s];
        }
      }
    }

    const blob = new Blob([buf], { type: 'audio/wav' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 30_000);

    const durationS = (numFrames * frameSize / SAMPLE_RATE).toFixed(2);
    console.log(`[AmbisonicsNode] Exported ${filename}: ${numFrames} frames, ${durationS} s, ${(dataBytes / 1024 / 1024).toFixed(1)} MB`);
  }

  /**
   * True once startAudio() has been called and the AudioContext is live.
   * Use this to skip work (e.g. WS-bridge de-interleave) before encoders exist.
   * @returns {boolean}
   */
  get isAudioStarted() { return this._transmissionStarted;}

  setRotationMatrix3(matrix3) {
    if (this._hoaRenderer && typeof this._hoaRenderer.setRotationMatrix3 === 'function') {
      this._hoaRenderer.setRotationMatrix3(matrix3);
    }
  }

  /**
   * Sets the output gain with a short linear ramp. Deterministic: cancels any
   * pending automation and anchors at the current value before ramping, which
   * avoids a known Firefox stall when ramping from a 0 baseline.
   *
   * @param {number} gainValue Linear gain (1.0 = unity)
   */
  setGainLinear(gainValue) {
    if (this._inputGain && this._audioCtx) {
      const t = this._audioCtx.currentTime;
      const g = this._inputGain.gain;

      g.cancelScheduledValues(t);
      g.setValueAtTime(g.value, t);
      g.linearRampToValueAtTime(gainValue, t + 0.05);
    }
  }

  updateIceServers(servers) {
    this._iceServers = servers;
    if (this._pc) {
      this._pc.setConfiguration({ iceServers: this._iceServers });
    }
  }
}