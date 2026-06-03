// node/app-node.js
'use strict';

import { AmbisonicsNode } from './webrtc-node.js';

// Costants
const WS_BRIDGE_URL = 'ws://127.0.0.1:9090';

// State
let ambisonicsNode = null;
let sourceMode = null;       // 'userMedia' or 'websocket'
let selectedDeviceId = null;
let signalingSocket = null;
let localWsBridge = null;
let prewarmedCtx = null;       // AudioContext created+resumed inside the Connect gesture

const SAMPLE_RATE = 48000;

// Global Project Configuration
const CONFIG = {
  SIGNALING_URL: 'wss://ambisonics-node.duckdns.org',
  API_URL: 'https://ambisonics-node.duckdns.org'
};

// Helper shortcut function
const $ = id => document.getElementById(id);


// Log function for UI
function log(msg, type = '') {
  const el = $('log');
  const ts = new Date().toLocaleTimeString('en', { hour12: false });
  const cls = type ? ` class="${type}"` : '';
  el.innerHTML += `<div><span class="ts">[${ts}]</span>${cls ? `<span${cls}> ${msg}</span>` : ' ' + msg}</div>`;
  el.scrollTop = el.scrollHeight;
}

function resetConnectUi(stateText = 'idle') {
  // If a context was prewarmed in the gesture but no node took ownership
  // (connection failed before role assignment), close it to avoid a leak.
  if (prewarmedCtx && !ambisonicsNode) {
    prewarmedCtx.close?.().catch(() => {});
    prewarmedCtx = null;
  }

  $('wsUrl').disabled = false;
  $('roomId').disabled = false;
  $('connectBtn').disabled = false;
  $('dot').className = 'dot';
  $('stateText').textContent = stateText;
}

// Device Scanning
async function loadDevices() {
  const scanBtn = $('scanBtn');
  const originalText = scanBtn.textContent;
  
  // Visual loading feedback for hardware device enumeration
  scanBtn.disabled = true;
  scanBtn.textContent = 'Scanning...';
  scanBtn.classList.add('loading');

  try {
    // Ping all devices to get labels
    const tempStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    tempStream.getTracks().forEach(t => t.stop()); // Shutdown immediately
  } catch (err) {
    log('Device access error: ' + err.message, 'err');
    scanBtn.disabled = false;
    scanBtn.textContent = originalText;
    scanBtn.classList.remove('loading');
    return;
  }

  // Clear and disconnect local WS Bridge logically and visually if active
  if (localWsBridge) {
    localWsBridge.close();
    localWsBridge = null;
  }
  if (sourceMode === 'websocket') {
    sourceMode = null;
  }
  $('wsBridgeBtn').classList.remove('selected');
  $('wsBridgeBtn').classList.remove('connected');
  $('chInfo').textContent = '';

  const devices = await navigator.mediaDevices.enumerateDevices();
  // Restore button state after scanning completes successfully
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

  // Buttons for each device
  audioInputs.forEach(device => {
    const btn = document.createElement('button');
    btn.className = 'device-item';

    if (sourceMode === 'userMedia' && selectedDeviceId === device.deviceId) {
      btn.classList.add('selected');
    }

    btn.textContent = device.label || 'Unknown Device';

    btn.onclick = () => {
      // Toggle selection
      if (sourceMode === 'userMedia' && selectedDeviceId === device.deviceId) {
        selectedDeviceId = null;
        sourceMode = null;
        btn.classList.remove('selected');
        list.classList.remove('has-selection');
        $('chInfo').textContent = '';
        $('frameSizeSelect').style.display = 'none';
        return;
      }

      // Close WS Bridge if open
      if (localWsBridge) {
        localWsBridge.close();
        localWsBridge = null;
        $('wsBridgeBtn').classList.remove('selected');
      }

      selectedDeviceId = device.deviceId;
      sourceMode = 'userMedia';
      
      // Remove other buttons from UI
      document.querySelectorAll('.device-item').forEach(b => b.classList.remove('selected'));
      btn.classList.add('selected');
      list.classList.add('has-selection');
      $('frameSizeSelect').style.display = 'block';
      
      console.debug(`Selected device: ${device.label}`);
    };

    list.appendChild(btn);
  });

  console.debug(`Found ${audioInputs.length} audio input devices`);
}

// WS Bridge Selection
function toggleWSBridge() {
  // Toggle if selected
  if (sourceMode === 'websocket') {
    sourceMode = null;
    if (localWsBridge) {
      localWsBridge.close();
      localWsBridge = null;
    }
    $('wsBridgeBtn').classList.remove('selected');
    $('wsBridgeBtn').classList.remove('connected');
    $('chInfo').textContent = '';
    $('frameSizeSelect').style.display = 'none';
    console.debug('WS Bridge de-selected');
    return;
  }

  sourceMode = 'websocket';
  selectedDeviceId = null;
  $('frameSizeSelect').style.display = 'none';

  // Remove device buttons from UI
  const list = $('deviceList');
  if (list) {
    list.innerHTML = ''; 
    list.classList.remove('has-selection');
  }

  $('wsBridgeBtn').classList.add('selected');
  $('chInfo').textContent = `WS Bridge @ ${WS_BRIDGE_URL}`;
  
  console.debug('Source set to WS Bridge');
}

// Connection to Signaling Server and Node Initialization
function connectNode() {
  const wsUrl = $('wsUrl').value.trim();
  const roomId = $('roomId').value.trim();

  if (signalingSocket && signalingSocket.readyState === WebSocket.CONNECTING) {
    console.debug('Connection already in progress, wait a moment');
    return;
  }

  if (!wsUrl) {
    log('Please enter a valid signaling server URL.', 'err');
    return;
  } else if (!roomId) {
    log('Please enter a valid room ID.', 'err');
    return;
  }

  $('wsUrl').disabled = true;
  $('roomId').disabled = true;
  $('connectBtn').disabled = true;
  $('frameSizeSelect').disabled = true;

  // AudioContext creation
  try {
    if (!prewarmedCtx) {
      prewarmedCtx = new AudioContext({ sampleRate: SAMPLE_RATE, latencyHint: 'interactive' });
    }
    prewarmedCtx.resume?.().catch(() => {});
  } catch (err) {
    console.warn('AudioContext prewarm failed:', err);
    prewarmedCtx = null; // fall back to node-created context
  }

  $('dot').className = 'dot waiting blink';
  $('stateText').textContent = 'connecting...';
  console.log(`Connecting to signaling server ${wsUrl}`);

  // Open WebSocket connection
  let roleAssigned = false;
  let failureHandled = false;
  try {
    signalingSocket = new WebSocket(wsUrl);
  } catch (err) {
    log('Connection error: ' + err.message, 'err');
    resetConnectUi('error');
    return;
  }

  const socket = signalingSocket;

  socket.onopen = () => {
    // Request to join roomId
    socket.send(JSON.stringify({
      type: 'join_node',
      room: roomId
    }));
  };

  socket.onmessage = async (event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch (err) {
      console.log('Invalid response from signaling server: ' + event.data);
      failureHandled = true;
      resetConnectUi('error');
      socket.close();
      return;
    }

    if (msg.type === 'error') {
      log(`Error from signaling server: ${msg.message}`, 'err');
      if (msg.suggestedRoom) {
        $('roomId').value = msg.suggestedRoom;
        log(`Free room available: ${msg.suggestedRoom}`, 'inf');
      }
      failureHandled = true;
      resetConnectUi('error');
      if (socket.readyState === WebSocket.OPEN) {
        socket.close();
      }
      return;
    }

    if (msg.type === 'role_assigned') {
      roleAssigned = true;
      socket.onmessage = null;
      const assignedRole = msg.role;
      const assignedRoom = msg.room || roomId;
      $('roomId').value = assignedRoom;
      console.debug(`Handshake completed. Role assigned by server: ${assignedRole}`);

      // Initialize AmbisonicsNode
      let _webrtcConnected = false;
      let _audioGraphReady = false;

      $('playBtn').disabled  = true;
      $('recordBtn').disabled = true;

      // Play Audio needs the DataChannel to be open and Omnitone loaded
      function _enablePlayBtn() {
        if (_webrtcConnected && _audioGraphReady) {
          $('playBtn').disabled = false;
          log('Ready — press Play Audio to start output', 'inf');
        }
      }
      
      // Initialize AmbisonicsNode
      ambisonicsNode = new AmbisonicsNode({
        signalingSocket: socket,
        roomId: assignedRoom,
        role: assignedRole,
        captureWorkletUrl: 'worklet-sender.js',
        playbackWorkletUrl: 'worklet-receiver.js',
        audioContext: prewarmedCtx, // created+resumed in the Connect gesture

        onAudioReady: () => {
          _audioGraphReady = true;
          console.debug('[AmbisonicsNode] onAudioReady — Omnitone loaded');
          _enablePlayBtn();
        },
        onStateChange: (state) => { // state can be 'connected', 'disconnected', 'failed', 'signaling-error'
          console.debug(`[WebRTC State] ${state}`);
          const d = $('dot');

          if (state === 'connected') {
            d.className = 'dot active';
            $('stateText').textContent = 'Connected';
            log('Connected successfully', 'ok');
            _webrtcConnected = true;
            _enablePlayBtn();

          } else if (state === 'disconnected') {
            d.className = 'dot waiting';
            $('stateText').textContent = 'Reconnecting...';
            log(`Remote peer temporarily disconnected, attempting auto-recovery`, 'warn');

          } else if (state === 'failed') {
            d.className = 'dot error';
            $('stateText').textContent = 'Connection failed';
            log(`Connection failed (State: ${state})`, 'err');
            stopAudio();

          } else if (state === 'signaling-error') {
            d.className = 'dot error';
            $('stateText').textContent = 'Signaling error';
            log(`Signaling handshake error occurred`, 'err');
            stopAudio();
          } else if (state === 'peer-stopped') {
            d.className = 'dot';
            $('stateText').textContent = 'Peer stopped';
            log(`Remote peer disconnected via signaling. Returning to idle.`, 'inf');
            stopAudio();
          }
        },
        onStats: (stats) => {
          const formatPkts = (count) => {
            if (count > 10_000) {
              return (count / 1000).toFixed(1).replace('.', ',') + ' k';
            }
            return count.toLocaleString('en-US'); // Matches the "1,123" explicit layout format
          };

          // RTT, with active transport (P2P or TURN)
          $('statRtt').textContent = stats.rttMs ? stats.rttMs.toFixed(1) + ' ms' : '—';
          const rttLabelEl = $('statRttLabel');
          if (stats.transportType === 'relay') {
            rttLabelEl.textContent = 'RTT · TURN';
            $('statRtt').className = 'stat-value warn';
          } else if (stats.transportType) {
            rttLabelEl.textContent = 'RTT · P2P';
            $('statRtt').className = 'stat-value';
          } else {
            rttLabelEl.textContent = 'RTT';
            $('statRtt').className = 'stat-value';
          }

          // Median Jitter
          $('statJitter').textContent = stats.medianJitter != null ? stats.medianJitter.toFixed(1) + ' ms' : '—';

          // Prebuffer depth — current RX jitter-buffer target (dynamic).
          $('statPrebuffer').textContent = stats.prebufferMs != null
            ? stats.prebufferMs.toFixed(0) + ' ms'
            : '—';
          
          // TX Packets
          $('statTxPkts').textContent = formatPkts(stats.pktsSent);

          // RX Packets (%Loss)
          const lossPct = stats.lossRate * 100;
          const rxEl = $('statRxPkts');
          rxEl.textContent = `${formatPkts(stats.pktsRecv)}, ${lossPct.toFixed(2)}%`;
          rxEl.className = 'stat-value' + (lossPct > 2 ? ' alert' : lossPct > 0.5 ? ' warn' : '');
          
          // TX Buffer Pressure
          const buf = stats.bufferPressure;
          const bufEl = $('statTxBuf');
          bufEl.textContent = buf > 1024 ? (buf / 1024).toFixed(1) + ' KB' : buf + ' B';
          bufEl.className = 'stat-value' + (buf > 102400 ? ' alert' : buf > 51200 ? ' warn' : '');
          
          // TX Bitrate
          $('statTxBitrate').textContent = stats.txKbps ? stats.txKbps.toFixed(0) + ' kbps' : '0 kbps';
          
          // RX Bitrate
          $('statRxBitrate').textContent = stats.rxKbps ? stats.rxKbps.toFixed(0) + ' kbps' : '0 kbps';
        },
        onRecordingComplete: ({ side }) => {
          if (side === 'tx') {
            // Re-enable record button on the peer that initiated the recording
            $('recordBtn').textContent = 'Record';
            $('recordBtn').classList.remove('recording');
            $('recordBtn').disabled = false;
            log('TX recording exported as tx-ambisonics.wav', 'ok');
          } else {
            log('RX recording exported as rx-ambisonics.wav', 'ok');
          }
        }
      });

      try {
        const turnRes = await fetch(`${CONFIG.API_URL}/api/turn-credentials`);
        if (turnRes.ok) {
          const turnData = await turnRes.json();
          ambisonicsNode.updateIceServers(turnData.iceServers);
          console.debug('[TURN] Credentials loaded and applied to PeerConnection');
        } else {
          console.warn('[TURN] Credentials endpoint error, falling back to native STUN');
        }
      } catch (e) {
        console.warn('[TURN] Could not reach credentials endpoint, falling back to STUN:', e);
      }

      await ambisonicsNode.initWebRTC();

      $('stopBtn').disabled = false;
      $('playBtn').disabled  = true;
      $('recordBtn').disabled = true;
      
      console.debug(`AmbisonicsNode initialized, starting media pipeline.`);
      $('stateText').textContent = 'Loading audio engine…';
      _startMediaPipeline();
    }
  };

  socket.onerror = () => {
    log('Network error on WebSocket. Check if the server is running.', 'err');
    if (!roleAssigned) {
      failureHandled = true;
      resetConnectUi('error');
    }
  };

  socket.onclose = () => {
    if (signalingSocket === socket) {
      signalingSocket = null;
    }
    if (!roleAssigned && !failureHandled && !ambisonicsNode) {
      log('Connection to signaling server closed before completion.', 'err');
      resetConnectUi('disconnected');
    }
  };
}

// Start Audio
async function _startMediaPipeline() {
  if (!ambisonicsNode) {
    log('Error: WebRTC engine not initialized.', 'err');
    return;
  }

  $('scanBtn').disabled = true;
  $('wsBridgeBtn').disabled = true;

  $('dot').className = 'dot waiting';
  $('stateText').textContent = 'starting media...';

  try {
    let stream = null;

    if (sourceMode === 'userMedia') {
      console.debug('Acquiring userMedia...');
      const customFrameSize = parseInt($('frameSizeSelect').value, 10) || 960;
      ambisonicsNode.setFrameSize(customFrameSize);
      console.log(`[DEBUG userMedia] FrameSize UI: ${customFrameSize} samples`);

      stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          deviceId: selectedDeviceId ? { exact: selectedDeviceId } : undefined,
          echoCancellation: false,
          noiseSuppression: false,
          autoGainControl: false,
          channelCount: 16
        }
      });
      console.debug('userMedia acquired successfully.');

      const track = stream.getAudioTracks()[0];
      const settings = track.getSettings();
      const sr = settings.sampleRate || 48000;
      $('chInfo').textContent = `ch=${settings.channelCount} | sr=${sr/1000}kHz`;

      if (settings.channelCount !== 16) {
        log(`ATTENTION: Device delivered ${settings.channelCount} channels instead of 16. Higher-Order Ambisonics matrix layout will be compromised.`, 'warn');
      }
    } 
    else if (sourceMode === 'websocket') {
      console.debug('Acquiring WebSocket stream...');

      // Catch JSON config message for frame size
      const frameSize = await new Promise((resolve, reject) => {
        localWsBridge = new WebSocket(WS_BRIDGE_URL);
        localWsBridge.binaryType = 'arraybuffer';
 
        localWsBridge.onerror = () => {
          reject(new Error('WS Bridge connection error. Is the Python script running?'));
        };
 
        localWsBridge.onmessage = ({ data }) => {
          if (typeof data !== 'string') return; // ignore any binary data before config
          try {
            const msg = JSON.parse(data);
            if (msg.type === 'config' && Number.isInteger(msg.frameSize)) {
              resolve(msg.frameSize);
            }
          } catch {
            reject(new Error('WS Bridge sent invalid config: ' + data));
          }
        };
      });

      console.debug(`WS Bridge config received: frameSize=${frameSize}`);
      ambisonicsNode.setFrameSize(frameSize);
      console.log(`[DEBUG websocket] FrameSize: ${frameSize} samples`);

      // Wire format: 8-byte LE uint64 sample timestamp + 16ch interleaved Float32.
      const HEADER_BYTES  = 8;
      const EXPECTED_BLOB = HEADER_BYTES + frameSize * 16 * 4;

      // Reusable planar buffers. feedPlanarFrame() copies each plane into an
      // AudioData before returning, so we can safely reuse these across frames
      // (no per-frame allocation). 16 mono planes of frameSize samples.
      const planes = Array.from({ length: 16 }, () => new Float32Array(frameSize));

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
        if (!ambisonicsNode) return;
        if (typeof data === 'string') return; // skip any late text messages
        if (!ambisonicsNode.isAudioStarted) return; // skip until encoders are ready

        if (data.byteLength !== EXPECTED_BLOB) {
          log('Invalid data received from WS Bridge. Expected ' + EXPECTED_BLOB + ' bytes, got ' + data.byteLength, 'err');
          return;
        }

        // Read the LE uint64 sample timestamp
        const timestamp = Number(new DataView(data, 0, HEADER_BYTES).getBigUint64(0, true));

        // De-interleave
        const src = new Float32Array(data, HEADER_BYTES);
        for (let s = 0; s < frameSize; s++) {
          const base = s * 16;
          for (let c = 0; c < 16; c++) {
            planes[c][s] = src[base + c];
          }
        }

        // Pass the bridge timestamp so the jitter buffer stays aligned to real
        // capture time: dropped frames become silence gaps, not discontinuities.
        ambisonicsNode.feedPlanarFrame(planes, timestamp);
      };
    } 
    else {
      log('No input source selected. Starting in receive-only mode.', 'inf');
    }

    await ambisonicsNode.connect(stream);
    
    console.debug('Audio context and WebRTC pipeline initialized.');
    if (ambisonicsNode && !ambisonicsNode._connected) {
      $('stateText').textContent = 'Waiting for Peer';
    }

  } catch (err) {
    log('Error starting audio: ' + err.message, err);
    $('scanBtn').disabled = false;
    $('wsBridgeBtn').disabled = false;
    $('frameSizeSelect').disabled = false;
    $('dot').className = 'dot error';
    $('stateText').textContent = 'audio error';
  }
}

// Play Audio — phase 3. The context is already running (created in the Connect
// gesture), so this just unmutes: startAudio() raises the gain, updateGain()
// then applies the slider's dB value.
async function playAudio() {
  if (!ambisonicsNode) return;

  const btn = $('playBtn');
  btn.disabled = true;
  btn.textContent = 'Starting…';

  try {
    await ambisonicsNode.startAudio();

    // Apply the spatial rotation and the slider gain value.
    updateSceneRotation();
    updateGain();

    btn.textContent = 'Audio Playing';
    btn.classList.add('playing');
    log('Audio output started', 'ok');
    console.debug('Play Audio: output graph unmuted');
  } catch (err) {
    log('Error starting audio output: ' + err.message, 'err');
    btn.disabled = false;
    btn.textContent = 'Play Audio';
  }
}


// Start synchronized recording on both peers
function startRecording() {
  if (!ambisonicsNode) return;
  try {
    ambisonicsNode.startRecording();
    $('recordBtn').textContent = 'Recording…';
    $('recordBtn').classList.add('recording');
    $('recordBtn').disabled = true;
    log('Recording started — 10s capture in progress', 'inf');
  } catch (err) {
    log('Recording error: ' + err.message, 'err');
  }
}

// Stop Audio and Cleanup
async function stopAudio() {
  if (!ambisonicsNode) return;
  
  $('stopBtn').disabled = true;
  log('Closing audio session...', 'inf');
  
  await ambisonicsNode.destroy();
  ambisonicsNode = null;
  signalingSocket = null;

  // destroy() closed the context we handed over; drop our reference so the next
  // Connect creates a fresh one inside that click's gesture.
  prewarmedCtx = null;

  if (localWsBridge) {
    localWsBridge.close();
    localWsBridge = null;
  }
  
  $('wsUrl').disabled = false;
  $('roomId').disabled = false;
  $('connectBtn').disabled = false;
  $('scanBtn').disabled = false;
  $('frameSizeSelect').disabled = false;
  $('wsBridgeBtn').disabled = false;
  $('wsBridgeBtn').classList.remove('selected');
  $('wsBridgeBtn').classList.remove('connected');
  $('playBtn').disabled = true;
  $('playBtn').textContent = 'Play Audio';
  $('playBtn').classList.remove('playing');

  $('recordBtn').disabled = true;
  $('recordBtn').textContent = 'Record';
  $('recordBtn').classList.remove('recording');

  $('dot').className = 'dot';
  $('stateText').textContent = 'idle';
  
  $('statRtt').textContent = '—';
  $('statRtt').className = 'stat-value';
  $('statRttLabel').textContent = 'RTT';
  $('statJitter').textContent = '—';
  $('statPrebuffer').textContent = '—';
  $('statTxPkts').textContent = '—';
  $('statTxBuf').textContent = '—';
  $('statTxBuf').className = 'stat-value';
  $('statTxBitrate').textContent = '—';
  $('statRxPkts').textContent = '—';
  $('statRxPkts').className = 'stat-value';
  $('statRxBitrate').textContent = '—';
  
  log('Session terminated.', 'ok');
}

// Spatial Controls & HOA Math

function toggleSpatialControls() {
  const panel = $('spatialControls');
  const isHidden = panel.style.display === 'none';
  panel.style.display = isHidden ? 'block' : 'none';
  $('spatialToggleBtn').classList.toggle('active', isHidden);
}

// Vectorial functions for rotation matrix calculations
function _crossProduct(a, b) {
  return [
    a[1] * b[2] - a[2] * b[1],
    a[2] * b[0] - a[0] * b[2],
    a[0] * b[1] - a[1] * b[0],
  ];
}

function _normalize(a) {
  const n = Math.sqrt(a[0] ** 2 + a[1] ** 2 + a[2] ** 2);
  if (n < 1e-8) return [1, 0, 0];
  return [a[0] / n, a[1] / n, a[2] / n];
}

function updateSceneRotation() {
  if (!ambisonicsNode) return;

  const theta = parseFloat($('azimuthSlider').value) * (Math.PI / 180);
  const phi = parseFloat($('elevationSlider').value) * (Math.PI / 180);

  const forward = [
    Math.sin(theta) * Math.cos(phi),
    Math.sin(phi),
    Math.cos(theta) * Math.cos(phi),
  ];
  const right = _normalize(_crossProduct(forward, [0, 1, 0]));
  const up = _normalize(_crossProduct(right, forward));

  const m = new Float32Array(9);
  m[0] = right[0]; m[1] = right[1]; m[2] = right[2];
  m[3] = up[0];    m[4] = up[1];    m[5] = up[2];
  m[6] = forward[0]; m[7] = forward[1]; m[8] = forward[2];

  ambisonicsNode.setRotationMatrix3(m);
}

function updateGain() {
  if (!ambisonicsNode) return;
  const dB = parseFloat($('gainSlider').value);
  const linear = Math.pow(10, dB / 20);
  ambisonicsNode.setGainLinear(linear);
}

function onSpatialSliderInput(name) {
  switch (name) {
    case 'azimuth':
      $('azimuthValue').textContent = $('azimuthSlider').value + '°';
      updateSceneRotation();
      break;
    case 'elevation':
      $('elevationValue').textContent = $('elevationSlider').value + '°';
      updateSceneRotation();
      break;
    case 'gain':
      $('gainValue').textContent = $('gainSlider').value + ' dB';
      updateGain();
      break;
  }
}

// Event Listeners
window.addEventListener('DOMContentLoaded', () => {
  if (!$('wsUrl').value.trim()) {
    $('wsUrl').value = CONFIG.SIGNALING_URL;
  }

  $('scanBtn').addEventListener('click', loadDevices);
  $('wsBridgeBtn').addEventListener('click', toggleWSBridge);

  $('clearLogBtn').addEventListener('click', () => {
    $('log').innerHTML = '';
  });

  $('spatialToggleBtn').addEventListener('click', toggleSpatialControls);

  $('azimuthSlider').addEventListener('input', () => onSpatialSliderInput('azimuth'));
  $('elevationSlider').addEventListener('input', () => onSpatialSliderInput('elevation'));
  $('gainSlider').addEventListener('input', () => onSpatialSliderInput('gain'));

  $('azimuthSlider').addEventListener('change', () => console.debug(`Azimut set to: ${$('azimuthSlider').value}°`));
  $('elevationSlider').addEventListener('change', () => console.debug(`Elevation set to: ${$('elevationSlider').value}°`));
  $('gainSlider').addEventListener('change', () => console.debug(`Gain set to: ${$('gainSlider').value} dB`));

  $('connectBtn').addEventListener('click', connectNode);
  $('playBtn').addEventListener('click', playAudio);
  $('stopBtn').addEventListener('click', stopAudio);
  $('recordBtn').addEventListener('click', startRecording);
});