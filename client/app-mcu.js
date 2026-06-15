'use strict';

import { AmbisonicsNode } from './engine.js';
import { marshalBundle, parseBundle, planLayout, BUNDLE_VERSION, channelBitrates } from './bundle.js';

// Python audio bridge (ASIO/PipeWire) — 16-channel planar Float32.
// MUST run at the same frame size as the MCU.
const WS_BRIDGE_URL = 'ws://127.0.0.1:9090';
// Fallback MCU endpoint when the page is served from a different origin.
const DEFAULT_MCU_WSS = 'wss://ambisonics-mcu.duckdns.org/ws';

function defaultSignalingUrl() {
  const { protocol, host } = window.location;
  if (protocol === 'https:') return `wss://${host}/ws`;
  return DEFAULT_MCU_WSS;
}
const SAMPLE_RATE = 48000;

// State
let ambisonicsNode = null;
let sourceMode = null;       // 'userMedia' | 'websocket'
let selectedDeviceId = null;
let signalingSocket = null;
let localWsBridge = null;
let prewarmedCtx = null;     // AudioContext created+resumed inside the Connect gesture
let mcuFrameSize = null;     // authoritative frame size from the MCU's offer
let mcuPacking = 'legacy';   // wire format advertised by the MCU offer: 'legacy' | 'bundled'
let mcuBitrates = null;      // per-channel Opus bitrate array from the MCU offer (uplink mirrors it)

// Session report: RTT sampled per stats tick, PDV pulled from the engine on stop.
let rttSamples = [];
let lastTransportType = null;
let sessionStartMs = 0;
let lastReport = null;
let recCountdownTimer = null; // interval driving the "Recording… {N}s" countdown

// RX terminates on outputTap instead of ctx.destination so output can be
// rerouted at any time: direct (Chrome setSinkId) or via a hidden <audio>
// element (Firefox HTMLMediaElement.setSinkId).
let outputTap = null;
let outputDest = null;
let outputEl = null;

// Signaling ping/pong RTT — fallback when getStats candidate-pair RTT is unavailable (Firefox).
let wsRttMs = 0;

const $ = id => document.getElementById(id);

const escHtml = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

function log(msg, type = '') {
  const el = $('log');
  const ts = new Date().toLocaleTimeString('en', { hour12: false });
  const cls = type ? ` class="${type}"` : '';
  const safe = escHtml(msg);
  el.innerHTML += `<div><span class="ts">[${ts}]</span>${cls ? `<span${cls}> ${safe}</span>` : ' ' + safe}</div>`;
  el.scrollTop = el.scrollHeight;
}

// Mirror console.warn/error into the on-page Log with deduplication+throttling.
(function mirrorConsoleToLog() {
  const safeStr = (o) => {
    if (o instanceof Error) return o.message || String(o);
    if (typeof o === 'object' && o !== null) { try { return JSON.stringify(o); } catch { return String(o); } }
    return String(o);
  };
  const WINDOW = 2000;
  const seen = new Map(); // message -> { t, n }
  // Benign PLC/padding noise — shown in DevTools but filtered from the UI log.
  const UI_LOG_SKIP = [/channel\(s\) timed out/, /padding with silence/];
  const mirror = (orig, type) => (...args) => {
    orig(...args);
    const msg = args.map(safeStr).join(' ');
    if (UI_LOG_SKIP.some(re => re.test(msg))) return;
    const now = Date.now();
    const rec = seen.get(msg);
    if (rec && now - rec.t < WINDOW) { rec.n++; rec.t = now; return; } // throttle a burst
    if (rec && rec.n > 1) log(`${msg} (×${rec.n})`, type);             // flush prior burst count
    else log(msg, type);
    seen.set(msg, { t: now, n: 1 });
  };
  console.warn = mirror(console.warn.bind(console), 'warn');
  console.error = mirror(console.error.bind(console), 'err');
})();

function resetConnectUi(stateText = 'idle') {
  if (prewarmedCtx && !ambisonicsNode) {
    prewarmedCtx.close?.().catch(() => {});
    prewarmedCtx = null;
    teardownOutputRoute();
  }
  $('wsUrl').disabled = false;
  $('roomId').disabled = false;
  $('connectBtn').disabled = false;
  $('dot').className = 'dot';
  $('stateText').textContent = stateText;
}

const delay = (ms) => new Promise(r => setTimeout(r, ms));

// --- Session report (Download Report) ---

// Nearest-rank percentile of an UNSORTED numeric array (ms). Returns NaN if empty.
function percentile(arr, p) {
  if (!arr.length) return NaN;
  const s = [...arr].sort((a, b) => a - b);
  const idx = Math.min(s.length - 1, Math.max(0, Math.ceil(p / 100 * s.length) - 1));
  return s[idx];
}

// Summary stats for a numeric array (ms): count, p50/p95/p99, min/mean/max, stddev.
function summarize(arr) {
  const n = arr.length;
  if (!n) return { n: 0 };
  const sum = arr.reduce((a, b) => a + b, 0);
  const mean = sum / n;
  const variance = arr.reduce((a, b) => a + (b - mean) ** 2, 0) / n;
  return {
    n,
    p50: percentile(arr, 50), p95: percentile(arr, 95), p99: percentile(arr, 99),
    min: Math.min(...arr), max: Math.max(...arr), mean, std: Math.sqrt(variance),
  };
}

// Format a session report as plain text from the frozen lastReport snapshot.
function buildReportText(r) {
  const f1 = (x) => Number.isFinite(x) ? x.toFixed(1) : 'n/a';
  const lat = summarize(r.rtt);
  const pdv = summarize(r.pdv);
  const frameMs = r.frameSize ? (r.frameSize * 1000 / 48000).toFixed(1) : '?';
  const kbps = r.bitrates ? Math.round(r.bitrates.reduce((a, b) => a + b, 0) / 1000) : '?';
  const tiers = r.bitrates ? `${r.bitrates[0] / 1000}/${r.bitrates[4] / 1000}/${r.bitrates[9] / 1000}` : '?';
  const L = [];
  L.push('AmbiRTC MCU — Session Report');
  L.push(`Generated:        ${new Date().toISOString()}`);
  L.push(`Session duration: ${(r.durationMs / 1000).toFixed(1)} s`);
  L.push('');
  L.push('Configuration');
  L.push(`  Signaling:   ${r.wsUrl}`);
  L.push(`  Room:        ${r.room}`);
  L.push(`  Frame size:  ${r.frameSize ?? '?'} samples (${frameMs} ms)`);
  L.push(`  Packing:     ${r.packing}`);
  L.push(`  Bitrate:     ${kbps} kbps/stream (${tiers} per order)`);
  L.push(`  Transport:   ${r.transport ?? 'unknown'}`);
  L.push('');
  L.push('Latency — transport RTT (round-trip), ms   [one-way ≈ RTT/2]');
  if (lat.n) {
    L.push(`  samples:          ${lat.n}  (sampled once per stats tick)`);
    L.push(`  p50 / p95 / p99:  ${f1(lat.p50)} / ${f1(lat.p95)} / ${f1(lat.p99)}`);
    L.push(`  min / mean / max: ${f1(lat.min)} / ${f1(lat.mean)} / ${f1(lat.max)}`);
  } else {
    L.push('  (no RTT samples — the browser may not expose candidate-pair RTT and no ping RTT was measured)');
  }
  L.push('');
  L.push('Packet Delay Variation (PDV) — |inter-arrival jitter| per packet, ms');
  if (pdv.n) {
    L.push(`  samples:          ${pdv.n}  (one per received frame)`);
    L.push(`  p50 / p95 / p99:  ${f1(pdv.p50)} / ${f1(pdv.p95)} / ${f1(pdv.p99)}`);
    L.push(`  mean / max / std: ${f1(pdv.mean)} / ${f1(pdv.max)} / ${f1(pdv.std)}`);
  } else {
    L.push('  (no PDV samples — no audio frames were received this session)');
  }
  L.push('');
  return L.join('\n');
}

// Trigger a browser download of `text` as a .txt file.
function downloadText(filename, text) {
  const blob = new Blob([text], { type: 'text/plain' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

async function fetchIceServers(wsURL) {
  let httpURL;
  try {
    const u = new URL(wsURL);
    u.protocol = u.protocol === 'wss:' ? 'https:' : 'http:';
    u.pathname = '/api/turn-credentials';
    u.search = '';
    httpURL = u.toString();
  } catch {
    return [];
  }
  try {
    const res = await fetch(httpURL, { signal: AbortSignal.timeout(4000) });
    if (!res.ok) return [];
    const data = await res.json();
    return Array.isArray(data.iceServers) ? data.iceServers : [];
  } catch {
    return [];
  }
}

let outputDeviceId = '';        // '' = OS default
let outputLabel = 'System default';

async function applyOutput(deviceId, label) {
  outputDeviceId = deviceId;
  outputLabel = label;
  if (!prewarmedCtx || !outputTap) {
    log(`Output device saved: ${label} — applied on Connect`, 'inf'); // picked before connecting
    return;
  }
  try {
    if (typeof prewarmedCtx.setSinkId === 'function') {
      await prewarmedCtx.setSinkId(deviceId); // Chrome: '' resets to the system default
    } else if (typeof HTMLMediaElement.prototype.setSinkId === 'function') {
      if (!deviceId) { // back to OS default: drop the element, go direct
        outputTap.disconnect();
        outputTap.connect(prewarmedCtx.destination);
        if (outputEl) { outputEl.pause(); outputEl.srcObject = null; outputEl = null; }
      } else {
        await routeToElementSink(deviceId);
      }
    } else {
      log('This browser cannot redirect audio output — set the device in the OS Sound settings.', 'err');
      return;
    }
    log(`Audio output: ${label}`, 'ok');
  } catch (err) {
    log('Output device error: ' + err.message, 'err');
  }
}

// Element is recreated on every reroute — a MediaStream element never drops
// back to the live edge once it lags. setSinkId applied before srcObject/play.
async function routeToElementSink(deviceId) {
  if (outputEl) { outputEl.pause(); outputEl.srcObject = null; }
  if (!outputDest) outputDest = prewarmedCtx.createMediaStreamDestination();
  const el = new Audio();
  await el.setSinkId(deviceId);
  el.srcObject = outputDest.stream;
  outputTap.disconnect();
  outputTap.connect(outputDest);
  try {
    await el.play();
    outputEl = el;
  } catch (err) {
    outputTap.disconnect();
    outputTap.connect(prewarmedCtx.destination); // fall back to OS default, don't lose audio
    throw err;
  }
}

function teardownOutputRoute() {
  if (outputEl) { outputEl.pause(); outputEl.srcObject = null; }
  outputEl = null;
  outputDest = null;
  if (outputTap) { try { outputTap.disconnect(); } catch { /* ctx already closed */ } outputTap = null; }
}

function logOutputDevice() {
  if (outputDeviceId) {
    log(`Audio output: ${outputLabel}`, 'ok');
    return;
  }
  log('Audio output: System default', 'inf');
  const maxCh = prewarmedCtx?.destination?.maxChannelCount ?? 0;
  if (maxCh > 2) {
    log(`Warning: the default output has ${maxCh} channels (BlackHole/aggregate?) — if silent, pick the headphones via Scan Devices`, 'warn');
  }
}

function updateRoomStats({ members = 0, active = 0 }) {
  const listeners = Math.max(0, members - active);
  $('statRoom').textContent = `TX ${active} · RX ${listeners}`;
}

function renderOutputButton(listEl, deviceId, label) {
  const b = document.createElement('button');
  b.className = 'device-item' + (deviceId === outputDeviceId ? ' selected' : '');
  b.textContent = label;
  b.onclick = () => {
    listEl.querySelectorAll('.device-item').forEach(x => x.classList.remove('selected'));
    b.classList.add('selected');
    listEl.classList.add('has-selection'); // CSS hides the non-selected buttons
    applyOutput(deviceId, label);
  };
  listEl.appendChild(b);
}

async function scanOutputDevices() {
  const btn = $('scanOutputBtn');
  const orig = btn.textContent;
  btn.disabled = true; btn.textContent = 'Scanning…'; btn.classList.add('loading');
  try {
    const tmp = await navigator.mediaDevices.getUserMedia({ audio: true }); // grant labels
    tmp.getTracks().forEach(t => t.stop());
  } catch (err) {
    log('Device access error: ' + err.message, 'err');
    btn.disabled = false; btn.textContent = orig; btn.classList.remove('loading');
    return;
  }

  const listEl = $('outputDeviceList');
  listEl.innerHTML = '';
  listEl.classList.remove('has-selection');

  let outs = [];
  try {
    outs = (await navigator.mediaDevices.enumerateDevices()).filter(d => d.kind === 'audiooutput');
  } catch { /* ignore */ }

  btn.disabled = false; btn.textContent = orig; btn.classList.remove('loading');

  if (outs.length) {
    // Chrome/Edge path (incl. Windows): System default + every enumerated device.
    renderOutputButton(listEl, '', 'System default');
    outs.forEach(d => renderOutputButton(listEl, d.deviceId, d.label || 'Unknown Output'));
    return;
  }

  // Firefox path: no enumerable outputs → open the browser's native picker.
  if (navigator.mediaDevices?.selectAudioOutput) {
    try {
      const d = await navigator.mediaDevices.selectAudioOutput();
      const label = d.label || 'Selected output';
      await applyOutput(d.deviceId, label);
      renderOutputButton(listEl, d.deviceId, label); // marks itself selected (id === outputDeviceId)
      listEl.classList.add('has-selection');
    } catch (err) {
      log('Output picker: ' + err.message, 'err'); // NotAllowedError when dismissed
    }
  } else {
    log('This browser does not expose output devices — set it in the OS Sound settings.', 'warn');
  }
}


async function loadDevices() {
  const scanBtn = $('scanBtn');
  const originalText = scanBtn.textContent;
  scanBtn.disabled = true;
  scanBtn.textContent = 'Scanning...';
  scanBtn.classList.add('loading');

  try {
    const tempStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    tempStream.getTracks().forEach(t => t.stop());
  } catch (err) {
    log('Device access error: ' + err.message, 'err');
    scanBtn.disabled = false;
    scanBtn.textContent = originalText;
    scanBtn.classList.remove('loading');
    return;
  }

  if (localWsBridge) { localWsBridge.close(); localWsBridge = null; }
  if (sourceMode === 'websocket') sourceMode = null;
  $('wsBridgeBtn').classList.remove('selected');
  $('wsBridgeBtn').classList.remove('connected');
  $('chInfo').textContent = '';

  const devices = await navigator.mediaDevices.enumerateDevices();
  scanBtn.disabled = false;
  scanBtn.textContent = originalText;
  scanBtn.classList.remove('loading');

  const audioInputs = devices.filter(d => d.kind === 'audioinput');
  const list = $('deviceList');
  list.innerHTML = '';
  list.classList.remove('has-selection');

  if (!audioInputs.length) {
    log('No audio input devices found.', 'err');
    return;
  }

  audioInputs.forEach(device => {
    const btn = document.createElement('button');
    btn.className = 'device-item';
    if (sourceMode === 'userMedia' && selectedDeviceId === device.deviceId) {
      btn.classList.add('selected');
    }
    btn.textContent = device.label || 'Unknown Device';
    btn.onclick = () => {
      if (sourceMode === 'userMedia' && selectedDeviceId === device.deviceId) {
        selectedDeviceId = null;
        sourceMode = null;
        btn.classList.remove('selected');
        list.classList.remove('has-selection');
        $('chInfo').textContent = '';
        return;
      }
      if (localWsBridge) { localWsBridge.close(); localWsBridge = null; $('wsBridgeBtn').classList.remove('selected'); }
      selectedDeviceId = device.deviceId;
      sourceMode = 'userMedia';
      list.querySelectorAll('.device-item').forEach(b => b.classList.remove('selected'));
      btn.classList.add('selected');
      list.classList.add('has-selection');
    };
    list.appendChild(btn);
  });
}

function toggleWSBridge() {
  if (sourceMode === 'websocket') {
    sourceMode = null;
    if (localWsBridge) { localWsBridge.close(); localWsBridge = null; }
    $('wsBridgeBtn').classList.remove('selected');
    $('wsBridgeBtn').classList.remove('connected');
    $('chInfo').textContent = '';
    return;
  }
  sourceMode = 'websocket';
  selectedDeviceId = null;
  const list = $('deviceList');
  if (list) { list.innerHTML = ''; list.classList.remove('has-selection'); }
  $('wsBridgeBtn').classList.add('selected');
  $('chInfo').textContent = `WS Bridge @ ${WS_BRIDGE_URL}`;
}

function startRecordingFlow() {
  if (!ambisonicsNode) return;
  const btn = $('recordBtn');
  btn.disabled = true;
  btn.classList.add('recording');
  btn.textContent = 'Arming…';
  log('Recording armed — starting in 1s', 'inf');

  setTimeout(() => {
    try {
      ambisonicsNode.startRecording();
    } catch (err) {
      log('Recording error: ' + (err?.message ?? err), 'err');
      btn.textContent = 'Record';
      btn.classList.remove('recording');
      btn.disabled = false;
      return;
    }
    let remaining = 10; // nominal seconds; onRecordingComplete is authoritative
    btn.textContent = `Recording… ${remaining}s`;
    recCountdownTimer = setInterval(() => {
      remaining -= 1;
      if (remaining > 0) {
        btn.textContent = `Recording… ${remaining}s`;
      } else {
        clearInterval(recCountdownTimer); recCountdownTimer = null;
        btn.textContent = 'Exporting…';
      }
    }, 1000);
    log('Recording started — ~10s capture in progress', 'inf');
  }, 1000);
}

async function connectNode() {
  const wsBase = $('wsUrl').value.trim();
  const roomId = $('roomId').value.trim();

  if (signalingSocket && signalingSocket.readyState === WebSocket.CONNECTING) return;
  if (!wsBase) { log('Please enter the MCU signaling URL (e.g. ws://host:8080/ws).', 'err'); return; }
  if (!roomId) { log('Please enter a room ID.', 'err'); return; }

  $('wsUrl').disabled = true;
  $('roomId').disabled = true;
  $('connectBtn').disabled = true;
  mcuFrameSize = null;

  rttSamples = [];
  lastTransportType = null;
  sessionStartMs = performance.now();
  $('downloadReportBtn').style.display = 'none';

  // Prewarm AudioContext inside the user gesture to satisfy autoplay policy.
  try {
    if (!prewarmedCtx) prewarmedCtx = new AudioContext({ sampleRate: SAMPLE_RATE, latencyHint: 'interactive' });
    prewarmedCtx.resume?.().catch(() => {});
    if (outputTap) { try { outputTap.disconnect(); } catch { /* old ctx */ } }
    outputTap = prewarmedCtx.createGain();
    outputTap.connect(prewarmedCtx.destination);
  } catch (err) {
    console.warn('AudioContext prewarm failed:', err);
    prewarmedCtx = null;
  }

  $('dot').className = 'dot waiting blink';
  $('stateText').textContent = 'connecting...';

  const iceServers = await fetchIceServers(wsBase);
  log(iceServers.length
    ? `ICE: ${iceServers.length} server set from MCU (TURN available as last-resort fallback)`
    : 'ICE: direct/STUN only (fine on localhost/LAN)', 'inf');

  const url = `${wsBase}${wsBase.includes('?') ? '&' : '?'}room=${encodeURIComponent(roomId)}`;
  let socket;
  try {
    socket = new WebSocket(url);
  } catch (err) {
    log('Connection error: ' + err.message, 'err');
    resetConnectUi('error');
    return;
  }
  signalingSocket = socket;

  let resolveOffer;
  const offerReceived = new Promise(r => { resolveOffer = r; });
  let pingTimer = null;
  socket.addEventListener('message', (ev) => {
    try {
      const m = JSON.parse(ev.data);
      if (m.type === 'offer') {
        if (m.frameSize) mcuFrameSize = m.frameSize;
        if (m.packing) mcuPacking = m.packing;
        if (Array.isArray(m.bitrates)) mcuBitrates = m.bitrates;
        resolveOffer();
      } else if (m.type === 'pong' && typeof m.t === 'number') {
        wsRttMs = performance.now() - m.t; // signaling-layer RTT (see wsRttMs)
      } else if (m.type === 'room_stats') {
        updateRoomStats(m);
      }
    } catch { /* not JSON / not a control message */ }
  });

  let webrtcConnected = false;
  let audioGraphReady = false;
  let audioStarted = false;
  const maybeAutoStart = async () => {
    if (!webrtcConnected || !audioGraphReady || audioStarted) return;
    audioStarted = true;
    try {
      await ambisonicsNode.startAudio();
      updateSceneRotation();
      updateGain();
      $('recordBtn').disabled = false;
      log('Audio output started', 'ok');
      if (outputDeviceId) await applyOutput(outputDeviceId, outputLabel);
      else logOutputDevice();
    } catch (err) {
      log('Error starting audio: ' + (err?.message ?? err), 'err');
      audioStarted = false;
    }
  };

  ambisonicsNode = new AmbisonicsNode({
    signalingSocket: socket,
    roomId,
    role: 'answerer',
    audioContext: prewarmedCtx,
    outputNode: outputTap, // page-owned RX endpoint (re-routable; see applyOutput)
    bundle: { marshal: marshalBundle, parse: parseBundle, planLayout, BUNDLE_VERSION },
    bitrates: channelBitrates(),
    iceServers,
    iceTransportPolicy: 'all', // prefer direct; relay only as a fallback (never forced)
    captureWorkletUrl: './worklet-sender.js',
    playbackWorkletUrl: './worklet-receiver.js',
    onAudioReady: () => {
      audioGraphReady = true;
      maybeAutoStart();
    },
    onRecordingComplete: ({ side }) => {
      if (side === 'tx') {
        if (recCountdownTimer) { clearInterval(recCountdownTimer); recCountdownTimer = null; }
        $('recordBtn').textContent = 'Record';
        $('recordBtn').classList.remove('recording');
        $('recordBtn').disabled = false;
        log('Sender recording exported (sender ….wav)', 'ok');
      } else {
        log('Receiver recording exported (receiver ….wav)', 'ok');
      }
    },
    onStateChange: (state) => {
      const d = $('dot');
      if (state === 'connected') {
        d.className = 'dot active';
        $('stateText').textContent = 'Connected';
        log('Connected to the MCU', 'ok');
        webrtcConnected = true;
        maybeAutoStart();
      } else if (state === 'disconnected') {
        d.className = 'dot waiting';
        $('stateText').textContent = 'Reconnecting...';
        log('Transport temporarily disconnected, attempting recovery', 'warn');
      } else if (state === 'failed') {
        d.className = 'dot error';
        $('stateText').textContent = 'Connection failed';
        log('Connection failed', 'err');
        stopAudio();
      } else if (state === 'peer-stopped') {
        d.className = 'dot';
        $('stateText').textContent = 'Stopped';
        log('Disconnected from the MCU. Returning to idle.', 'inf');
        stopAudio();
      }
    },
    onStats: (stats) => {
      console.log(`[lat] decoderQueue=${(stats.decoderQueueMs ?? 0).toFixed(0)}ms · rxRing=${(stats.rxRingMs ?? 0).toFixed(0)}ms · prebuf=${(stats.prebufferMs ?? 0).toFixed(0)}ms`);

      const formatPkts = (count) => (count > 10_000
        ? (count / 1000).toFixed(1).replace('.', ',') + ' k'
        : count.toLocaleString('en-US'));

      const rtt = (stats.rttMs && stats.rttMs > 0) ? stats.rttMs : wsRttMs;
      if (rtt > 0) rttSamples.push(rtt);
      if (stats.transportType) lastTransportType = stats.transportType === 'relay' ? 'TURN (relay)' : 'P2P (direct)';
      const jitVal = stats.medianJitter != null ? stats.medianJitter : 0;
      const rttStr = rtt > 100 ? `${Math.round(rtt)}ms` : `${rtt.toFixed(1)}ms`;
      const jitStr = jitVal > 10 ? `${Math.round(jitVal)}ms` : `${jitVal.toFixed(1)}ms`;
      $('statRtt').textContent = rtt > 0 ? `${rttStr} (${jitStr})` : '—';
      const rttLabelEl = $('statRttLabel');
      if (stats.transportType === 'relay') {
        rttLabelEl.textContent = 'RTT (Jitter) · TURN';
        $('statRtt').className = 'stat-value warn';
      } else if (stats.transportType) {
        rttLabelEl.textContent = 'RTT (Jitter) · P2P';
        $('statRtt').className = 'stat-value';
      } else {
        rttLabelEl.textContent = 'RTT (Jitter)';
        $('statRtt').className = 'stat-value';
      }

      $('statPrebuffer').textContent = stats.prebufferMs != null ? stats.prebufferMs.toFixed(0) + ' ms' : '—';
      $('statTxPkts').textContent = formatPkts(stats.pktsSent);
      $('statRxPkts').textContent = formatPkts(stats.pktsRecv);

      const lossPct = stats.lossRate * 100;
      const lossEl = $('statLoss');
      lossEl.textContent = lossPct.toFixed(2) + '%';
      lossEl.className = 'stat-value' + (lossPct > 2 ? ' alert' : lossPct > 0.5 ? ' warn' : '');

      const buf = stats.bufferPressure;
      const bufEl = $('statTxBuf');
      bufEl.textContent = buf > 1024 ? (buf / 1024).toFixed(1) + ' KB' : buf + ' B';
      bufEl.className = 'stat-value' + (buf > 102400 ? ' alert' : buf > 51200 ? ' warn' : '');

      $('statTxRxBitrate').textContent =
        `${stats.txKbps ? stats.txKbps.toFixed(0) : 0}/${stats.rxKbps ? stats.rxKbps.toFixed(0) : 0} kbps`;
    },
  });

  socket.onerror = () => {
    log('WebSocket error — is the MCU running at this URL?', 'err');
    if (!webrtcConnected) resetConnectUi('error');
  };
  socket.onclose = () => {
    if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
    if (signalingSocket === socket) signalingSocket = null;
  };

  socket.addEventListener('open', async () => {
    log(`Signaling open → ${url}`, 'inf');
    // RTT probe: the MCU echoes {"type":"pong","t"} for every ping.
    pingTimer = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ type: 'ping', t: performance.now() }));
      }
    }, 2000);
    try {
      await ambisonicsNode.initWebRTC();
        await Promise.race([offerReceived, delay(5000)]);
      if (mcuFrameSize) log(`MCU frame size: ${mcuFrameSize} samples · packing: ${mcuPacking}`, 'inf');
      if (mcuBitrates) log(`MCU bitrate: ${Math.round(mcuBitrates.reduce((a, b) => a + b, 0) / 1000)} kbps/stream (${mcuBitrates[0] / 1000}/${mcuBitrates[4] / 1000}/${mcuBitrates[9] / 1000} per order)`, 'inf');

      $('stopBtn').disabled = false;
      $('stateText').textContent = 'Loading audio engine…';
      await startMediaPipeline();
    } catch (err) {
      log('Setup failed: ' + (err?.message ?? err), 'err');
      resetConnectUi('error');
    }
  });
}

async function startMediaPipeline() {
  if (!ambisonicsNode) { log('Engine not initialized.', 'err'); return; }

  $('scanBtn').disabled = true;
  $('wsBridgeBtn').disabled = true;
  $('dot').className = 'dot waiting';
  $('stateText').textContent = 'starting media...';

  try {
    let stream = null;

    if (sourceMode === 'userMedia') {
      stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          deviceId: selectedDeviceId ? { exact: selectedDeviceId } : undefined,
          echoCancellation: false,
          noiseSuppression: false,
          autoGainControl: false,
          channelCount: 16,
        },
      });
      const settings = stream.getAudioTracks()[0].getSettings();
      const sr = settings.sampleRate || 48000;
      $('chInfo').textContent = `ch=${settings.channelCount} | sr=${sr / 1000}kHz`;
      if (settings.channelCount !== 16) {
        log(`Note: device gave ${settings.channelCount} channels, not 16 — channel 0 is fanned out across the soundfield.`, 'warn');
      }
    } else if (sourceMode === 'websocket') {
      const bridgeFrameSize = await new Promise((resolve, reject) => {
        localWsBridge = new WebSocket(WS_BRIDGE_URL);
        localWsBridge.binaryType = 'arraybuffer';
        localWsBridge.onerror = () => reject(new Error('WS Bridge connection error. Is the Python script running?'));
        localWsBridge.onmessage = ({ data }) => {
          if (typeof data !== 'string') return;
          try {
            const msg = JSON.parse(data);
            if (msg.type === 'config' && Number.isInteger(msg.frameSize)) resolve(msg.frameSize);
          } catch { reject(new Error('WS Bridge sent invalid config: ' + data)); }
        };
      });

      log(`WS Bridge frame size: ${bridgeFrameSize} samples`, 'inf');
      if (mcuFrameSize && bridgeFrameSize !== mcuFrameSize) {
        log(`Frame size mismatch: the Python bridge is running at ${bridgeFrameSize} samples but the MCU expects ${mcuFrameSize}. Restart the bridge with frame size ${mcuFrameSize} — e.g. "python asio_ws_bridge.py ${mcuFrameSize}" (Windows) or "pw-jack python pipewire_ws_bridge.py ${mcuFrameSize}" (Linux) — then reconnect.`, 'err');
        if (localWsBridge) { localWsBridge.close(); localWsBridge = null; }
        await stopAudio(); // terminate the connection — the audio would be broken otherwise
        return;
      }

      const HEADER_BYTES = 8;
      const EXPECTED_BLOB = HEADER_BYTES + bridgeFrameSize * 16 * 4;
      const planes = Array.from({ length: 16 }, () => new Float32Array(bridgeFrameSize));

      $('wsBridgeBtn').classList.remove('selected');
      $('wsBridgeBtn').classList.add('connected');
      localWsBridge.onerror = () => {
        log('Error with WS Bridge, is the Python script running?', 'err');
        $('wsBridgeBtn').classList.remove('connected');
        $('wsBridgeBtn').classList.add('selected');
      };
      localWsBridge.onclose = () => {
        log('WS Bridge disconnected', 'err');
        $('wsBridgeBtn').classList.remove('connected');
        $('wsBridgeBtn').classList.remove('selected');
      };
      localWsBridge.onmessage = ({ data }) => {
        if (!ambisonicsNode || typeof data === 'string') return;
        if (!ambisonicsNode.isAudioStarted) return; // encoders not ready yet
        if (data.byteLength !== EXPECTED_BLOB) {
          log(`Invalid WS Bridge frame: expected ${EXPECTED_BLOB} bytes, got ${data.byteLength}`, 'err');
          return;
        }
        const timestamp = Number(new DataView(data, 0, HEADER_BYTES).getBigUint64(0, true));
        const src = new Float32Array(data, HEADER_BYTES);
        for (let s = 0; s < bridgeFrameSize; s++) {
          const base = s * 16;
          for (let c = 0; c < 16; c++) {
            planes[c][s] = src[base + c];
          }
        }
        ambisonicsNode.feedPlanarFrame(planes, timestamp);
      };
    } else {
      log('No input source selected — receive-only (you will hear the other clients, send nothing).', 'inf');
    }

    await ambisonicsNode.connect(stream);
  } catch (err) {
    log('Error starting audio: ' + (err?.message ?? err), 'err');
    $('scanBtn').disabled = false;
    $('wsBridgeBtn').disabled = false;
    $('dot').className = 'dot error';
    $('stateText').textContent = 'audio error';
  }
}

async function stopAudio() {
  if (!ambisonicsNode) return;
  $('stopBtn').disabled = true;
  log('Closing session...', 'inf');

    lastReport = {
    rtt: rttSamples.slice(),
    pdv: (ambisonicsNode.getPdvSamples?.() ?? []).slice(),
    durationMs: sessionStartMs ? performance.now() - sessionStartMs : 0,
    wsUrl: $('wsUrl').value.trim(),
    room: $('roomId').value.trim(),
    frameSize: mcuFrameSize,
    packing: mcuPacking,
    bitrates: mcuBitrates,
    transport: lastTransportType,
  };
  $('downloadReportBtn').style.display = '';

  await ambisonicsNode.destroy();
  ambisonicsNode = null;
  signalingSocket = null;
  teardownOutputRoute();
  prewarmedCtx = null;
  mcuFrameSize = null;

  if (localWsBridge) { localWsBridge.close(); localWsBridge = null; }

  $('wsUrl').disabled = false;
  $('roomId').disabled = false;
  $('connectBtn').disabled = false;
  $('scanBtn').disabled = false;
  $('wsBridgeBtn').disabled = false;
  $('wsBridgeBtn').classList.remove('selected');
  $('wsBridgeBtn').classList.remove('connected');
  if (recCountdownTimer) { clearInterval(recCountdownTimer); recCountdownTimer = null; }
  $('recordBtn').disabled = true;
  $('recordBtn').textContent = 'Record';
  $('recordBtn').classList.remove('recording');

  outputDeviceId = '';
  outputLabel = 'System default';
  $('outputDeviceList').innerHTML = '';
  $('outputDeviceList').classList.remove('has-selection');

  $('dot').className = 'dot';
  $('stateText').textContent = 'idle';

  for (const id of ['statRtt', 'statRoom', 'statPrebuffer', 'statTxPkts', 'statTxBuf', 'statRxPkts', 'statLoss', 'statTxRxBitrate']) {
    $(id).textContent = '—';
  }
  wsRttMs = 0;
  $('statRtt').className = 'stat-value';
  $('statRttLabel').textContent = 'RTT (Jitter)';
  $('statTxBuf').className = 'stat-value';
  $('statLoss').className = 'stat-value';

  log('Session terminated.', 'ok');
}

function toggleSpatialControls() {
  const panel = $('spatialControls');
  const isHidden = panel.style.display === 'none';
  panel.style.display = isHidden ? 'block' : 'none';
  $('spatialToggleBtn').classList.toggle('active', isHidden);
}

const _cross = (a, b) => [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]];
const _norm = (a) => {
  const n = Math.sqrt(a[0] ** 2 + a[1] ** 2 + a[2] ** 2);
  return n < 1e-8 ? [1, 0, 0] : [a[0] / n, a[1] / n, a[2] / n];
};

function updateSceneRotation() {
  if (!ambisonicsNode) return;
  const theta = parseFloat($('azimuthSlider').value) * (Math.PI / 180);
  const phi = parseFloat($('elevationSlider').value) * (Math.PI / 180);
  const forward = [Math.sin(theta) * Math.cos(phi), Math.sin(phi), Math.cos(theta) * Math.cos(phi)];
  const right = _norm(_cross(forward, [0, 1, 0]));
  const up = _norm(_cross(right, forward));
  const m = new Float32Array(9);
  m[0] = right[0];   m[1] = right[1];   m[2] = right[2];
  m[3] = up[0];      m[4] = up[1];      m[5] = up[2];
  m[6] = forward[0]; m[7] = forward[1]; m[8] = forward[2];
  ambisonicsNode.setRotationMatrix3(m);
}

function updateGain() {
  if (!ambisonicsNode) return;
  const dB = parseFloat($('gainSlider').value);
  ambisonicsNode.setGainLinear(Math.pow(10, dB / 20));
}

function onSpatialSliderInput(name) {
  switch (name) {
    case 'azimuth':   $('azimuthValue').textContent = $('azimuthSlider').value + '°';   updateSceneRotation(); break;
    case 'elevation': $('elevationValue').textContent = $('elevationSlider').value + '°'; updateSceneRotation(); break;
    case 'gain':      $('gainValue').textContent = $('gainSlider').value + ' dB';        updateGain(); break;
  }
}

window.addEventListener('DOMContentLoaded', () => {
  if (!$('wsUrl').value.trim()) $('wsUrl').value = defaultSignalingUrl();

  $('scanBtn').addEventListener('click', loadDevices);
  $('wsBridgeBtn').addEventListener('click', toggleWSBridge);
  $('scanOutputBtn').addEventListener('click', scanOutputDevices);
  const canCtxSink = typeof AudioContext.prototype.setSinkId === 'function';
  const canElSink  = typeof HTMLMediaElement.prototype.setSinkId === 'function';
  if (!canCtxSink && !canElSink) $('outputRow').style.display = 'none';
  $('clearLogBtn').addEventListener('click', () => { $('log').innerHTML = ''; });
  $('spatialToggleBtn').addEventListener('click', toggleSpatialControls);
  $('azimuthSlider').addEventListener('input', () => onSpatialSliderInput('azimuth'));
  $('elevationSlider').addEventListener('input', () => onSpatialSliderInput('elevation'));
  $('gainSlider').addEventListener('input', () => onSpatialSliderInput('gain'));
  $('connectBtn').addEventListener('click', connectNode);
  $('stopBtn').addEventListener('click', stopAudio);
  $('recordBtn').addEventListener('click', startRecordingFlow);
  $('downloadReportBtn').addEventListener('click', () => {
    if (!lastReport) return;
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    downloadText(`ambirtc-report-${ts}.txt`, buildReportText(lastReport));
  });

  if (location.protocol === 'file:') {
    log('⚠ Served as file:// — serve from the repo root: "python3 -m http.server 8000", then open http://localhost:8000/client/', 'err');
  }
});
