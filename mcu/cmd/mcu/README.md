# AmbiRTC MCU — Phase 0 (foundations)

A Go (Pion) MCU server for the 16-channel third-order ambisonics (ACN/SN3D,
48 kHz) communication system. **Phase 0 is foundations only**: a compiling
skeleton, two unit-tested primitives (the wire protocol and the Opus codec
wrapper), and a browser↔server DataChannel **echo** that proves the Pion +
signaling path. No audio mixing, jitter buffering, or multi-client room logic
yet — those are later phases (`internal/room`, `internal/audio` are stubs).

Module root: this directory's parent, `mcu/` (module `github.com/Maurii03/ambirtc-mcu`).
Run all commands from `mcu/`.

## Layout

```
cmd/mcu/            entry point + config (this dir)
internal/proto/     IMPLEMENTED — 9-byte packet build/parse + format constants (pure Go, no cgo)
internal/codec/     IMPLEMENTED — mono Opus encode/decode @ 48 kHz over hraban/opus (cgo)
internal/signaling/ IMPLEMENTED — WebSocket SDP/ICE endpoint (MCU is the WebRTC offerer)
internal/session/   IMPLEMENTED — per-connection PeerConnection + DataChannel echo
internal/room/      STUB — future per-room mixer (channel-wise B-format summation, O(1)/client)
internal/audio/     STUB — future jitter buffer + 16-channel frame assembly (port of worklet-receiver.js)
testclient/         minimal browser echo test client (manual verification)
```

## Wire format (source of truth: `shared/webrtc-node.js`)

9-byte header + raw mono Opus payload, one packet per channel per frame:

| Bytes | Field    | Encoding                                                    |
|-------|----------|-------------------------------------------------------------|
| 0     | control  | `isRec`(bit 7, `0x80`) \| reserved(3) \| `chIdx`(low 4 bits) |
| 1–4   | seqNum   | uint32 little-endian (per-channel monotonic)                |
| 5–8   | frameTs  | uint32 little-endian (timestamp in samples)                 |
| 9…    | payload  | raw mono Opus                                               |

Constants: 16 channels, 48000 Hz, default frame 960 (20 ms); valid frame sizes
`{120, 240, 480, 960, 1920, 2880}`; 64 kbps per channel. DataChannel `ambi-ch`,
**unordered, no retransmits**.

PCM boundary: libopus uses **int16**; the browser uses **float32** (WebCodecs
`f32-planar`). Convert with `codec.Float32ToInt16` / `codec.Int16ToFloat32`.

## Prerequisites

cgo is required (`hraban/opus` links libopus). `CGO_ENABLED=1` is the default
when cgo is used; cross-compilation is not supported.

**macOS (Homebrew):**

```sh
brew install pkg-config opus opusfile
```

> `opusfile` is required even though we only use the encoder/decoder: `hraban/opus`
> compiles its Stream API in the same package with `#cgo pkg-config: opusfile`.

**Linux (Debian/Ubuntu):**

```sh
sudo apt update
sudo apt install -y build-essential pkg-config libopus-dev libopusfile-dev
# install Go 1.22+ from go.dev if the distro package is older
```

Go 1.22+ (developed with 1.23; the module's go directive is 1.24, satisfied
automatically by Go's toolchain download).

## Build, test, run

```sh
go build ./...        # compiles cleanly (cgo)
go test ./...         # proto + codec round-trip tests, plus the loopback echo test
go run ./cmd/mcu      # starts the signaling WS + WebRTC endpoint; logs the listen address
```

Config (flag overrides env; env overrides default):

| Flag           | Env              | Default | Notes                                            |
|----------------|------------------|---------|--------------------------------------------------|
| `-addr`        | `MCU_ADDR`       | `:8080` | listen address                                   |
| `-frame-size`  | `MCU_FRAME_SIZE` | `960`   | server-authoritative; validated, unused this phase |

## Verify the echo (browser)

1. Run the server:

   ```sh
   go run ./cmd/mcu
   ```

2. Serve the test client (separate terminal) and open it:

   ```sh
   cd testclient && python3 -m http.server 5173
   # open http://localhost:5173
   ```

3. Click **Connect**. Expected:
   - server logs: `DataChannel "ambi-ch" open`, then
     `rx ch= 5 seq=12345 ts=48000 isRec=true payload=0B -> echo`;
   - browser logs: the sent 9-byte packet, the echoed reply, and
     `✓ echo matches sent bytes`.

The same path is covered automatically by the loopback test in
`internal/session` (a second Pion peer plays the browser), so `go test ./...`
proves the echo without a browser.
