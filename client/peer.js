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
   * @param {string}   [opts.captureWorkletUrl='worklet-sender.js']
   * @param {string}   [opts.playbackWorkletUrl='worklet-receiver.js']
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
    this._captureWorkletUrl   = opts.captureWorkletUrl  ?? 'worklet-sender.js';
    this._playbackWorkletUrl  = opts.playbackWorkletUrl ?? 'worklet-receiver.js';
    this._iceServers          = opts.iceServers         ?? [{ urls: 'stun:stun.cloudflare.com:3478' }];
    this._onStats             = opts.onStats            ?? null;
    this._onStateChange       = opts.onStateChange      ?? null;

    // Frame size, overridden via setFrameSize()
    this._frameSize = (opts.frameSize != null && Number.isInteger(opts.frameSize) && opts.frameSize > 0)
      ? opts.frameSize
      : DEFAULT_FRAME_SIZE;

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

    /** Sync recording flag packed into the MSB of header byte 0. */
    this._isRecFlag = 0;

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

    // RX recording state
    this._rxRecActive  = false;
    this._rxRecSet     = new Set(); // frameTs values seen with isRec=1
    this._rxRecFrames  = [];
    this._rxRecTimer   = null; // inactivity timeout

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
   * Sets the sync recording flag packed into the MSB of header byte 0.
   *
   * @param {boolean} active
   */
  setRecordingActive(active) {
    this._isRecFlag = active ? 1 : 0;
  }

  /**
   * Starts a 10-second synchronized recording on both peers.
   *
   * TX side: captures N = ⌊10·SR/frameSize⌋ raw PCM frames before encoding
   * and exports them as tx-ambisonics.wav when done.
   *
   * RX side (remote peer): automatically detects packets with isRec=1, saves
   * assembled decoded frames, and exports them as rx-ambisonics.wav once the
   * recording burst ends.
   *
   * opts.onRecordingComplete({ side: 'tx'|'rx' }) is called on each peer when
   * its respective WAV has been exported.
   *
   * @throws {Error} if not connected or a recording is already in progress
   */
  startRecording() {
    if (!this._connected) throw new Error('[AmbisonicsNode] Not connected — cannot start recording');
    if (!this._transmissionStarted) throw new Error('[AmbisonicsNode] Audio pipeline not ready — call connect() first');
    if (this._txRec.active)  throw new Error('[AmbisonicsNode] Recording already in progress');

    const N = Math.floor(10 * SAMPLE_RATE / this._frameSize);
    this._txRec.frames = [];
    this._txRec.total  = N;
    this._txRec.left   = N;
    this._txRec.active = true;
    this._isRecFlag    = 1;

    // Safety-net: if no audio is flowing the frame counter never reaches 0.
    // Force-complete after 11 s so the button is always re-enabled.
    const recDurationMs = Math.ceil(N * this._frameSize * 1000 / SAMPLE_RATE);
    this._txRecTimeout = setTimeout(() => {
      this._txRecTimeout = null;
      if (!this._txRec.active) return;
      this._txRec.active = false;
      this._isRecFlag    = 0;
      const frames = this._txRec.frames;
      this._txRec.frames = [];
      setTimeout(() => {
        if (frames.length > 0) this._exportWav(frames, 'tx-ambisonics.wav');
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
    this._rxRecActive  = false;
    this._rxRecFrames  = [];
    this._rxRecSet.clear();

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

    // Register both worklets in parallel - they are independent modules
    await Promise.all([
      this._audioCtx.audioWorklet.addModule(this._captureWorkletUrl),
      this._audioCtx.audioWorklet.addModule(this._playbackWorkletUrl),
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

    // TX recording: copy all 16 channels before AudioData creation
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
        this._isRecFlag    = 0;
        const frames = this._txRec.frames;
        this._txRec.frames = [];
        // Defer export off the hot path
        setTimeout(() => {
          if (frames.length > 0) this._exportWav(frames, 'tx-ambisonics.wav');
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
    if (this._dc.bufferedAmount > 512_000) return; // back-pressure: drop on congestion

    const frameTs = Math.round(chunk.timestamp * SAMPLE_RATE / 1_000_000);
    const seqNum  = this._seqNums[chIdx]++;

    const buf  = new ArrayBuffer(HEADER_SIZE + chunk.byteLength);
    const view = new DataView(buf);

    view.setUint8 (0, (this._isRecFlag ? ISREC_BIT : 0) | (chIdx & CHIDX_MASK));
    view.setUint32(1, seqNum,  true); // Little-Endian
    view.setUint32(5, frameTs, true);

    chunk.copyTo(new Uint8Array(buf, HEADER_SIZE));
    this._dc.send(buf);

    this._totalPktsSent++;
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
        bitrate:          BIT_RATE,
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

      // Route: playbackWorklet -> HOA input -> HOA output -> destination
      this._hoaRenderer.output.connect(this._audioCtx.destination);
    } else {
      // Fallback: connect worklet channel 0 directly (mono monitor, no spatialisation)
      console.warn('[AmbisonicsNode] Omnitone not found - bypassing spatial rendering');
      this._inputGain.connect(this._audioCtx.destination);
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
    if (!(buf instanceof ArrayBuffer) || buf.byteLength <= HEADER_SIZE) return;

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
    if (isRec === 1) {
      this._rxRecSet.add(frameTs);
      if (!this._rxRecActive) {
        this._rxRecActive = true;
        console.log('[AmbisonicsNode] RX recording started');
      }
    }

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
      console.warn(`[AmbisonicsNode] Frame ${frameTs}: ${missing} channel(s) timed out; padding with silence`);
      for (let c = 0; c < CHANNELS; c++) {
        if (frame.data[c] === null) frame.data[c] = new Float32Array(frame.frameSize);
      }
    }

    this._lastFlushedTs = frameTs;
    this._dispatchFrame(frame);
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
        const frames = this._rxRecFrames;
        this._rxRecFrames = [];
        this._rxRecSet.clear();
        console.log(`[AmbisonicsNode] RX recording complete — ${frames.length} frames`);
        if (frames.length > 0) this._exportWav(frames, 'rx-ambisonics.wav');
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
      iceTransportPolicy: 'relay' // Uncomment to force TURN usage for testing
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

      let rttMs        = 0;
      let bytesSent    = 0;
      let bytesRecv    = 0;
      let localCandidateId = null;
      const candidateTypes = new Map();

      stats.forEach(r => {
        if (
          r.type === 'candidate-pair' &&
          r.state === 'succeeded' &&
          r.currentRoundTripTime != null
        ) {
          rttMs = r.currentRoundTripTime * 1_000;
          localCandidateId = r.localCandidateId;
        }
        if (r.type === 'local-candidate') {
          candidateTypes.set(r.id, r.candidateType);
        }
        if (r.type === 'data-channel') {
          bytesSent += r.bytesSent      ?? 0;
          bytesRecv += r.bytesReceived  ?? 0;
        }
      });

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

      if (this._onStats) {
        this._onStats({
          rttMs,
          medianJitter,
          transportType,
          bufferPressure,
          prebufferMs: this._dynPrebuffer * 1000 / SAMPLE_RATE,
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
      const minPrebuf = PREBUF_MIN_FRAMES * frameSize;
      const maxPrebuf = PREBUF_MAX_FRAMES * frameSize;

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
   * Builds a WAVE_FORMAT_EXTENSIBLE IEEE-float WAV file from an array of
   * recorded frames and triggers a browser download.
   */
  _exportWav(frames, filename) {
    if (!frames.length) return;

    const now = new Date();
    const pad = num => String(num).padStart(2, '0');
    const timestamp = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}_${pad(now.getHours())}-${pad(now.getMinutes())}-${pad(now.getSeconds())}`;
    filename = filename.replace('.wav', `_${timestamp}.wav`);
    
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