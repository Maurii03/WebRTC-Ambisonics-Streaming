# WebRTC Ambisonics Audio

Real-time, low-latency streaming of **third-order ambisonics (16-channel) audio**
between web browsers over a WebRTC **DataChannel**, with binaural rendering in the
browser. The system captures a physical ambisonic microphone (e.g. a Zylia ZM-1),
transports the full 16-channel soundfield to a remote peer, and renders it
binaurally with head-relative rotation — in either a one-way
(*sender → receiver*) or a symmetric **bidirectional (*peer ↔ peer*)**
configuration.

---

## Repository structure

```
.
├── README.md                  This file
├── env.example                Server environment template (Cloudflare TURN keys)
├── index.html                 Top-level entry page
├── server.js                  WebSocket signaling + TURN credential service
│
├── Python Bridge/
│   ├── asio_ws_bridge.py       Windows: ReaRoute (ASIO) → WebSocket bridge
│   ├── pipewire_ws_bridge.py   Linux: PipeWire → WebSocket bridge
│   └── requirements.txt        Python dependencies for the bridges
│
└── client/
    ├── index.html              Web client markup
    ├── style.css               Styles
    ├── main.js                 App bootstrap: UI, signaling client, source
    │                           selection, recording controls, stats
    ├── peer.js                 AmbisonicsNode: RTCPeerConnection + DataChannel,
    │                           16× WebCodecs Opus enc/dec, Omnitone, prebuffer
    │                           adaptation, recording
    ├── worklet-sender.js        AudioWorklet: accumulates 16 ch into frames
    └── worklet-receiver.js      AudioWorklet: timestamp jitter buffer + playback
```

---

## Prerequisites

**Signaling / TURN server**
- Node.js (LTS) and npm
- A Cloudflare Calls TURN key (Key ID + API token)

**Web client**
- A recent **Firefox** (reference target). Requires `AudioWorklet` and WebCodecs
  `AudioEncoder` / `AudioDecoder` with Opus **encode and decode** available.
- Any static file server to serve `client/`.

**Capture chain**
- REAPER with an AmbiX encoder (e.g. the Zylia Ambisonics Converter), project at
  **48 kHz**.
- Virtual audio device per OS: **ReaRoute** (Windows, provided by REAPER),
  **BlackHole 16ch** (macOS), **PipeWire** (Linux).
- Python 3 for the Windows bridges (dependencies in
  `Python Bridge/requirements.txt`).

---

## Setup

### 1. Signaling & TURN server

```bash
npm install ws express cors dotenv
 
cp env.example .env          # then fill in your Cloudflare credentials
```
 
`.env` (based on `env.example`) holds your Cloudflare Calls TURN credentials:
 
```
CF_TURN_KEY_ID=your-cloudflare-turn-key-id
CF_API_TOKEN=your-cloudflare-api-token
```

`server.js` loads `.env` automatically via `dotenv`. The server reads `CF_TURN_KEY_ID` and
`CF_API_TOKEN` on startup and exits if either is missing. Run `node server.js`
from the directory that contains `.env` (that is where `dotenv` looks for it).

### 2. Client configuration

The client points at a signaling server and a TURN endpoint defined in
`client/main.js`:

```js
const WS_BRIDGE_URL = 'ws://127.0.0.1:9090';   // local capture bridge
const CONFIG = {
  SIGNALING_URL: 'wss://<your-server>',         // WebSocket signaling
  API_URL:       'https://<your-server>',        // TURN credential REST endpoint
};
```

Set `SIGNALING_URL` / `API_URL` to your own server (for local development,
e.g. `ws://localhost:8080` and `http://localhost:8080`). `WS_BRIDGE_URL` is the
local Python bridge and normally needs no change.

### 3. Capture chain (per OS)

**Windows — ReaRoute**
0. During Reaper installation select `ReaRoute ASIO driver`.
1. Start **REAPER**, configure your microphone in `Preferences > Audio > Device`.
2. Route the 16-channel AmbiX bus to **ReaRoute outputs 1–16**.
   1. Click on `Routing` button.
   2. Click on `Add new hardware output > ReaRoute 1/2`.
   3. On the bottom left corner click on `Multichannel source > 16 channels > 1-16`
3. Set the input of the channel to the microphone.
4. Then start the bridge (next section). *Order matters* — the ReaRoute device is
   exposed by REAPER, so launching the bridge first attaches it to a dead device.

**macOS — BlackHole**
1. In `Preferences > Audio > Device` check box `Allow use of different input and output devices` and select BlackHole 16ch
2. Route REAPER's 16-channel output to **BlackHole 16ch**.
   1. Click on `Routing` button.
   2. Click on `Add new hardware output > Output 1 (BlackHole 16ch) / Output 2 (BlackHole 16ch)`.
   3. On the bottom left corner click on `Multichannel source > 16 channels > 1-16`
3. In the client, select **BlackHole** as the capture device — it is captured via
   `getUserMedia` (no bridge needed).
4. Keep the system/browser **output** on physical headphones, changing the **input** to BlackHole 16ch is not needed

---

## Running

### Server

```bash
node server.js            # defaults to port 8080
node server.js 9000       # optional custom port
```

Exposes `GET /api/turn-credentials` and the signaling WebSocket on the same port.

### Capture bridge (Windows)

```bash
python -m venv venv
.\venv\Scripts\activate # or Activate.ps1 on PowerShell
pip install -r "Python Bridge/requirements.txt"

# Windows
python "Python Bridge/asio_ws_bridge.py"          # default frame size 960 (20 ms)
python "Python Bridge/asio_ws_bridge.py" 480      # optional: 10 ms frames
```

Valid frame sizes (samples @ 48 kHz): **120 / 240 / 480 / 960 / 1920 / 2880**
(2.5 / 5 / 10 / 20 / 40 / 60 ms). The bridge listens on `ws://127.0.0.1:9090`,
sends a one-time `config` message with the negotiated frame size, then streams
binary frames: an 8-byte little-endian timestamp header followed by interleaved
16-channel 32-bit float samples.

On macOS no bridge runs — the browser captures BlackHole directly.

### Client

Serve the `client/` directory with any static server and open it in Firefox:

```bash
cd client
python3 -m http.server 5173
# open http://localhost:5173
```

`server.js` handles signaling and TURN only, it doesn't serve the static client.

---

## Usage

1. **Choose a room.** If a room is occupied the server replies with a suggested free room name.
1. **Select the audio source** — a capture device via `getUserMedia` (macOS), or
    the **WS Bridge** (Windows).
2. **Connect**, wait for the DataChannel to open and the renderer to initialize, then press **Play Audio** to unmute output.
3. **Rotate the soundfield** with the azimuth / elevation / gain controls.
4. **Record a test**: the Record button captures a synchronized pair of 16-channel WAVs on both peers, exporting a 10s clip.

---

## Acknowledgements

- [Omnitone](https://github.com/GoogleChrome/omnitone) — ambisonic binaural rendering.
- REAPER & ReaRoute, BlackHole, and PipeWire — capture and routing.
- Cloudflare Calls — TURN relay.