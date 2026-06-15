# WebRTC Ambisonics Streaming

Real-time streaming of **third-order ambisonics (16-channel) audio** between web browsers over a WebRTC DataChannel, with binaural rendering in the browser. The system captures a physical ambisonic microphone (e.g. Zylia ZM-1), transports the full soundfield to a multi-user room managed by a Go MCU, and renders it binaurally with head-relative rotation.

---

## Architecture

```
Browser (client/)  ──WebRTC DataChannel──▶  Go MCU (mcu/)
                                              │ per-room B-format mix
                                              │ minus-one downmix per client
                                              ▼
                                         Browser (client/)

Python bridge (bridges/)
  ASIO / PipeWire → ws://127.0.0.1:9090 → Browser
```

- **MCU** — WebRTC offerer, per-room ambisonics mixer, Opus encode/decode, WebSocket signaling, TURN credentials. Written in Go (Pion).
- **Client** — browser answerer: mic or WS bridge as input, Omnitone binaural renderer as output. One page, no build step.
- **Bridges** — optional Python scripts that forward a 16-channel ASIO (Windows) or PipeWire (Linux) device into the browser over a local WebSocket.
- **`server.js`** — lightweight Node.js signaling server for direct peer-to-peer sessions (no MCU, no mixing).

---

## Repository structure

```
.
├── server.js              Node.js peer-to-peer signaling + Cloudflare TURN
├── env.example            Environment template for server.js
│
├── client/
│   ├── index.html         Web client UI
│   ├── style.css
│   ├── app-mcu.js         UI logic: connects to the MCU as WebRTC answerer
│   ├── engine.js          AmbisonicsNode — RTCPeerConnection, Opus, Omnitone
│   ├── bundle.js          Bundled wire-format codec (MCU packing mode)
│   ├── worklet-sender.js  AudioWorklet: frames 16-ch capture into Opus packets
│   └── worklet-receiver.js AudioWorklet: jitter buffer + binaural playback
│
├── bridges/
│   ├── asio_ws_bridge.py      Windows: ReaRoute (ASIO) → WebSocket
│   ├── pipewire_ws_bridge.py  Linux: PipeWire → WebSocket
│   └── requirements.txt
│
└── mcu/                   Go MCU (module: github.com/Maurii03/ambirtc-mcu)
    ├── cmd/mcu/            Entry point + config flags
    └── internal/
        ├── proto/          Wire protocol (9-byte header + raw Opus payload)
        ├── codec/          Opus encode/decode via hraban/opus (cgo)
        ├── signaling/      WebSocket SDP/ICE endpoint
        ├── session/        Per-connection PeerConnection + DataChannel
        ├── room/           Per-room mixer (B-format channel-wise sum)
        ├── audio/          Jitter buffer + 16-channel frame assembly
        └── turn/           Cloudflare TURN credential endpoint
```

---

## Prerequisites

### MCU (Go)

- Go 1.22+
- libopus + libopusfile

**macOS:**
```sh
brew install pkg-config opus opusfile
```

**Linux (Debian/Ubuntu):**
```sh
sudo apt install -y build-essential pkg-config libopus-dev libopusfile-dev
```

### Node.js signaling server (peer-to-peer only)

- Node.js LTS
- Cloudflare Calls TURN key (Key ID + API token)

### Browser client

- Chrome, Edge, or Firefox (recent versions)
- Any static file server

### Capture chain

- REAPER with an AmbiX encoder, project at 48 kHz
- Virtual audio device: **ReaRoute** (Windows), **BlackHole 16ch** (macOS), **PipeWire** (Linux)
- Python 3 for the Windows/Linux bridge

---

## Setup

### MCU

```sh
cd mcu
go build ./...
go test ./...        # optional — runs proto, codec, and echo tests
```

Config flags (flag > env > default):

| Flag              | Env              | Default | Notes                              |
|-------------------|------------------|---------|------------------------------------|
| `-addr`           | `MCU_ADDR`       | `:8080` | listen address                     |
| `-frame-size`     | `MCU_FRAME_SIZE` | `960`   | Opus frame size in samples (20 ms) |
| `-public-ip`      | `MCU_PUBLIC_IP`  | —       | required for TURN relay            |
| `-udp-port`       | `MCU_UDP_PORT`   | `3478`  | TURN UDP port                      |

### Node.js signaling server (peer-to-peer only)

```sh
npm install ws express cors dotenv
cp env.example .env   # fill in Cloudflare credentials
```

`.env`:
```
CF_TURN_KEY_ID=your-cloudflare-turn-key-id
CF_API_TOKEN=your-cloudflare-api-token
```

---

## Running

### MCU

```sh
cd mcu
go run ./cmd/mcu
# or with flags:
go run ./cmd/mcu -addr :8080 -frame-size 960 -public-ip 1.2.3.4
```

### Node.js server (peer-to-peer)

```sh
node server.js          # port 8080
node server.js 9000     # custom port
```

### Capture bridge (Windows / Linux)

```sh
cd bridges
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Windows
python asio_ws_bridge.py         # default frame size 960 (20 ms)
python asio_ws_bridge.py 480     # 10 ms frames

# Linux
pw-jack python pipewire_ws_bridge.py
pw-jack python pipewire_ws_bridge.py 480
```

Valid frame sizes (samples @ 48 kHz): **120 / 240 / 480 / 960 / 1920 / 2880**.  
The bridge must run at the same frame size as the MCU (`-frame-size`).

### Client

```sh
cd client
python3 -m http.server 8000
# open http://localhost:8000
```

---

## Usage

1. Enter the MCU signaling URL (`wss://host/ws`) and a room ID, then click **Connect**.
2. Select the audio input: a microphone via **Scan Devices**, or the Python bridge via **Connect WS Bridge**.
3. Audio output starts automatically once the WebRTC connection is established.
4. Use the **Spatial Controls** panel to rotate the soundfield (azimuth, elevation) and adjust gain.
5. **Record** captures a synchronized 10 s export on both sender and receiver sides.

### macOS capture (no bridge needed)

1. Install [BlackHole 16ch](https://github.com/ExistentialAudio/BlackHole).
2. In REAPER: `Preferences > Audio > Device` — route the 16-channel AmbiX bus to BlackHole 16ch outputs 1–16.
3. In the client, select **BlackHole 16ch** as the input device.

### Windows capture (ASIO bridge)

1. During REAPER installation, select the **ReaRoute ASIO driver**.
2. Route the 16-channel AmbiX bus to ReaRoute outputs 1–16.
3. Start REAPER **before** the bridge (ReaRoute is exposed by REAPER).
4. Start `asio_ws_bridge.py`, then connect in the browser.

---

## Acknowledgements

- [Omnitone](https://github.com/GoogleChrome/omnitone) — ambisonic binaural rendering
- [Pion](https://github.com/pion/webrtc) — WebRTC in Go
- [hraban/opus](https://github.com/hraban/opus) — Go Opus bindings
- REAPER, ReaRoute, BlackHole, PipeWire — capture and routing
- Cloudflare Calls — TURN relay
