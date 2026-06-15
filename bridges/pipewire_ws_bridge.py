"""
pipewire_ws_bridge.py

Local-Loop Bridge for Linux: PipeWire/JACK → Python → WebSocket → Browser

Launch with the pw-jack wrapper so sounddevice registers itself as a JACK
client and PipeWire exposes its virtual input ports:

    PIPEWIRE_LATENCY="960/48000" pw-jack venv/bin/python pipewire_ws_bridge.py

Then connect the 16 source ports (e.g. from Reaper / a DAW) to the script's
input ports with qpwgraph, Carla, or jack_connect.

-- ARCHITECTURE OVERVIEW --
1. PortAudio/JACK thread, high-priority
    - _audio_callback()     transfers frames to the event loop via call_soon_threadsafe

2. Asyncio event loop, main thread
    - _broadcaster()        reads from the queue and broadcasts to all connected clients
    - _ws_handler()         manages client connections and disconnections

3. asyncio.Queue, thread-safe communication channel
    - implements drop-oldest policy to prevent latency buildup

-- BINARY PAYLOAD LAYOUT --
Each WebSocket message is a binary blob: an 8-byte little-endian uint64 sample
timestamp header, followed by one frame of interleaved audio samples:
    [ ts(uint64 LE) ][ s0 ch0, s0 ch1, ..., s0 ch15, s1 ch0, ..., s959 ch15 ]

- Header       : 8 bytes, sample timestamp of the frame's first sample
- Frame size   : 960 samples (default; overridden by the CLI arg)
- Channels     : 16 (3rd Order Ambisonics)
- Sample format: float32, little-endian, normalised to [-1.0, +1.0]
- Total size   : 8 + 960 * 16 * 4 = 61,448 bytes per frame

On connect the bridge first sends a JSON config message {"type":"config",
"frameSize":N} so the browser learns the frame size, then streams the binary
frames above. This matches asio_ws_bridge.py and the browser client
(multiuser/app-mcu.js) exactly. The timestamp advances by OUTPUT_FRAME for
every frame PRODUCED (including frames dropped on queue saturation) so the
browser jitter buffer can detect real gaps and insert silence instead of
replaying dropped frames as if contiguous.
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import queue
from typing import Optional

import numpy as np
import sounddevice as sd
import websockets
from websockets.legacy.server import WebSocketServerProtocol


# Configuration
SAMPLE_RATE:    int = 48_000        # Hz
NUM_CHANNELS:   int = 16            # 3rd Order Ambisonics
OUTPUT_FRAME:   int = 960           # WebSocket payload size in samples (20 ms)
JACK_BLOCK:     int = 256           # JACK period size - set freely to match your DAW.
WS_HOST:        str = "127.0.0.1"   # listen address (localhost only)
WS_PORT:        int = 9090          # WebSocket port

# Maximum buffered frames; excess frames are dropped (drop-oldest) to keep
# end-to-end latency bounded.
QUEUE_MAX_FRAMES: int = 8

# PortAudio status warnings occurrences
_PA_STATUS_LOG_INTERVAL: int = 50

# Valid Opus frame durations in samples at 48 kHz (must match the MCU -frame-size)
_VALID_FRAME_SIZES = {120, 240, 480, 960, 1920, 2880}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pw_bridge")


def list_audio_devices() -> None:
    """Logs all PortAudio-visible devices (useful for debugging)."""
    log.info("--- Available Audio Devices ---")
    for idx, dev in enumerate(sd.query_devices()):
        parts = []
        if dev["max_input_channels"]  > 0:
            parts.append(f"IN x{dev['max_input_channels']}")
        if dev["max_output_channels"] > 0:
            parts.append(f"OUT x{dev['max_output_channels']}")
        log.info("  [%2d] %-45s  %s", idx, dev["name"], " | ".join(parts))

def find_suitable_device(required_channels: int) -> Optional[int]:
    """
    Finds the best input device that supports at least `required_channels`.

    Search order:
      1. JACK host-API default input device
      2. Any other JACK device with enough input channels
      3. PortAudio system default input device
      4. Any device with enough input channels

    Returns the device index, or None if nothing qualifies.
    """
    all_devices = sd.query_devices()

    # Try JACK API devices
    for api_info in sd.query_hostapis():
        if 'JACK' not in api_info['name'].upper():
            continue

        # JACK default input
        jack_default = api_info.get('default_input_device')
        if jack_default is not None:
            dev = all_devices[jack_default]
            if dev['max_input_channels'] >= required_channels:
                log.info(
                    "Using JACK default input: [%d] %s (%d ch)",
                    jack_default, dev['name'], dev['max_input_channels'],
                )
                return jack_default
            log.warning(
                "JACK default input [%d] '%s' has only %d input channels "
                "(need %d) - scanning for alternatives…",
                jack_default, dev['name'], dev['max_input_channels'],
                required_channels,
            )

        # Scan all devices belonging to this JACK host API
        for idx in api_info.get('devices', []):
            dev = all_devices[idx]
            if dev['max_input_channels'] >= required_channels:
                log.info(
                    "Using JACK device: [%d] %s (%d ch)",
                    idx, dev['name'], dev['max_input_channels'],
                )
                return idx

    log.warning("JACK host API not found - falling back to system defaults.")

    # PortAudio system default
    try:
        default_idx = sd.default.device[0]  # default input
        if default_idx is not None:
            dev = all_devices[default_idx]
            if dev['max_input_channels'] >= required_channels:
                log.info(
                    "Using system default input: [%d] %s (%d ch)",
                    default_idx, dev['name'], dev['max_input_channels'],
                )
                return default_idx
    except Exception:
        pass

    # Brute-force scan of all devices
    for idx, dev in enumerate(all_devices):
        if dev['max_input_channels'] >= required_channels:
            log.info(
                "Using fallback device: [%d] %s (%d ch)",
                idx, dev['name'], dev['max_input_channels'],
            )
            return idx

    return None


class AudioBridge:
    """
    Bridges the PortAudio/JACK real-time thread and the asyncio event loop.

    The JACK callback thread never touches asyncio or WebSocket objects directly;
    all communication goes through call_soon_threadsafe → asyncio.Queue.
    """

    def __init__(self) -> None:
        self._loop:          Optional[asyncio.AbstractEventLoop]  = None
        self._queue:         Optional[asyncio.Queue]              = None
        self._clients:       set[WebSocketServerProtocol]         = set()
        self._stream:        Optional[sd.InputStream]             = None
        self._dropped_frames: int                                 = 0
        self._pa_status_count: int                                = 0
        self._accum:      np.ndarray = np.zeros((OUTPUT_FRAME, NUM_CHANNELS), dtype=np.float32)
        self._accum_fill: int        = 0

        # Free-running sample clock for the binary timestamp header.
        self._sample_clock: int = 0

    # JACK / PortAudio callback
    def _audio_callback(
        self,
        indata:    np.ndarray,
        frames:    int,
        time_info,
        status:    sd.CallbackFlags,
    ) -> None:
        """
        Invoked by PortAudio on the real-time JACK thread.
        Copies the buffer and schedules it for broadcast on the asyncio loop.
        """
        if status:
            self._pa_status_count += 1
            if self._pa_status_count <= 3 or self._pa_status_count % _PA_STATUS_LOG_INTERVAL == 0:
                log.warning(
                    "PortAudio status: %s (occurrence #%d)",
                    status, self._pa_status_count,
                )

        if self._loop is None or self._queue is None:
            return

        # Ensure input is contiguous float32 without allocating if already correct
        block = indata.astype(np.float32, copy=False)

        offset = 0
        while offset < frames:
            space   = OUTPUT_FRAME - self._accum_fill
            to_copy = min(space, frames - offset)
 
            self._accum[self._accum_fill : self._accum_fill + to_copy] = \
                block[offset : offset + to_copy]
 
            self._accum_fill += to_copy
            offset           += to_copy
 
            if self._accum_fill == OUTPUT_FRAME:
                # Frame complete. Prepend an 8-byte LE uint64 sample-timestamp
                # header so the browser can detect gaps from dropped frames.
                # Serialise before resetting (bytes owns its memory).
                ts = self._sample_clock
                header: bytes = ts.to_bytes(8, byteorder="little", signed=False)
                raw: bytes = header + self._accum.tobytes()
                self._accum_fill = 0
                # Advance the clock for every frame produced (even if dropped below).
                self._sample_clock += OUTPUT_FRAME

                def _enqueue(blob: bytes = raw) -> None:
                    """Thread-safe enqueue with drop-oldest on saturation."""
                    if self._queue.full():
                        try:
                            self._queue.get_nowait()
                            self._dropped_frames += 1
                            if self._dropped_frames % 100 == 0:
                                log.warning(
                                    "Queue saturated - %d frames dropped total "
                                    "(increase QUEUE_MAX_FRAMES or reduce WS load)",
                                    self._dropped_frames,
                                )
                        except (asyncio.QueueEmpty, queue.Empty):
                            pass
                    try:
                        self._queue.put_nowait(blob)
                    except asyncio.QueueFull:
                        pass  # rare race condition; safe to ignore
 
                self._loop.call_soon_threadsafe(_enqueue)

    # WebSocket handlers
    async def _ws_handler(self, websocket: WebSocketServerProtocol) -> None:
        """Tracks connected clients; cleans up on disconnect."""
        remote = websocket.remote_address
        log.info("Client connected:    %s:%s", *remote)

        # Send the frame-size config as the FIRST message — the browser blocks
        # on this before it starts decoding binary frames.
        config_msg = json.dumps({"type": "config", "frameSize": OUTPUT_FRAME})
        await websocket.send(config_msg)
        log.debug("Sent config to %s:%s  frameSize=%d", *remote, OUTPUT_FRAME)

        # Reset the shared broadcast clock to 0 only for the first client of a
        # fresh session (empty set): a clean ts=0 start without jumping the
        # timeline backwards for a client already streaming when another joins.
        if not self._clients:
            self._sample_clock = 0

        self._clients.add(websocket)
        try:
            await websocket.wait_closed()
        finally:
            self._clients.discard(websocket)
            log.info("Client disconnected: %s:%s", *remote)

    async def _broadcaster(self) -> None:
        """Dequeues audio blobs and fans them out to every connected client."""
        log.info("Broadcaster ready - waiting for audio frames…")
        try:
            while True:
                blob: bytes = await self._queue.get()

                if not self._clients:
                    continue

                # Snapshot the set before awaiting so mid-send disconnects are safe
                results = await asyncio.gather(
                    *[client.send(blob) for client in list(self._clients)],
                    return_exceptions=True,
                )
                for res in results:
                    if isinstance(res, Exception):
                        log.debug("Send error (client likely disconnected): %s", res)
        except asyncio.CancelledError:
            log.info("Broadcaster task cancelled - shutting down")
            raise

    # Entry point
    async def run(self) -> None:
        """Opens the PipeWire/JACK stream and starts the WebSocket server."""
        self._loop  = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=QUEUE_MAX_FRAMES)

        device_idx = find_suitable_device(NUM_CHANNELS)
        if device_idx is None:
            log.error(
                "No audio device found with >= %d input channels. "
                "Make sure pw-jack is wrapping this script and that your "
                "DAW / source is running.",
                NUM_CHANNELS,
            )
            sys.exit(1)

        dev_info = sd.query_devices(device_idx)
        max_ch = dev_info['max_input_channels']
        if max_ch > NUM_CHANNELS:
            log.warning(
                "Device '%s' offers %d input channels, but the bridge "
                "will only read %d.",
                dev_info['name'], max_ch, NUM_CHANNELS,
            )

        log.info(
            "Opening stream  [device=%d '%s', %d ch, %d Hz | "
            "JACK block=%d | WebSocket frame=%d]",
            device_idx, dev_info["name"], NUM_CHANNELS, SAMPLE_RATE,
            JACK_BLOCK, OUTPUT_FRAME,
        )

        # Use a generous latency to give the JACK ↔ asyncio pipeline enough headroom.
        try:
            self._stream = sd.InputStream(
                device=device_idx,
                channels=NUM_CHANNELS,
                samplerate=SAMPLE_RATE,
                blocksize=JACK_BLOCK,   # JACK period
                dtype="float32",
                latency="low",
                callback=self._audio_callback,
            )
        except Exception as e:
            log.error("Failed to open stream on device %d: %s", device_idx, e)
            sys.exit(1)

        with self._stream:
            log.info("JACK stream active (JACK_BLOCK=%d, OUTPUT_FRAME=%d)", JACK_BLOCK, OUTPUT_FRAME)
            async with websockets.serve(
                self._ws_handler,
                WS_HOST,
                WS_PORT,
                compression=None,       # disabled: lower CPU and latency
                max_size=256 * 1024,
            ):
                log.info(
                    "WebSocket server listening on ws://%s:%d", WS_HOST, WS_PORT
                )
                await self._broadcaster()   # blocks until cancelled


def _parse_args() -> int:
    """
    Parse the optional [frame_size] positional argument.

    Usage: pw-jack python pipewire_ws_bridge.py [FRAME_SIZE]

    Valid Opus frame sizes at 48 kHz: 120, 240, 480, 960, 1920, 2880 samples.
    Must match the MCU's -frame-size.
    """
    parser = argparse.ArgumentParser(
        description="Reaper -> PipeWire/JACK -> WebSocket bridge for 16-ch Ambisonics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Valid frame sizes (samples @ 48 kHz / ms):\n"
            "  120  ->  2.5 ms\n"
            "  240  ->    5 ms\n"
            "  480  ->   10 ms\n"
            "  960  ->   20 ms (default)\n"
            " 1920  ->   40 ms\n"
            " 2880  ->   60 ms\n"
        ),
    )
    parser.add_argument(
        "frame_size", type=int, nargs="?", default=960, metavar="FRAME_SIZE",
        help="Output frame size in samples (default: 960 = 20 ms at 48 kHz)",
    )
    args = parser.parse_args()
    if args.frame_size not in _VALID_FRAME_SIZES:
        parser.error(
            f"Invalid frame size {args.frame_size}. "
            f"Must be one of: {sorted(_VALID_FRAME_SIZES)}"
        )
    return args.frame_size


# Main
async def main() -> None:
    global OUTPUT_FRAME
    OUTPUT_FRAME = _parse_args()
    duration_ms = OUTPUT_FRAME * 1000 // SAMPLE_RATE
    log.info("Frame size: %d samples (%d ms @ %d Hz)", OUTPUT_FRAME, duration_ms, SAMPLE_RATE)

    list_audio_devices()
    bridge = AudioBridge()
    bridge_task = asyncio.create_task(bridge.run())

    loop = asyncio.get_running_loop()
    stop = loop.create_future()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set_result, None)
    await stop

    bridge_task.cancel()
    await asyncio.gather(bridge_task, return_exceptions=True)

    log.info("Bridge stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted (Ctrl+C) - shutting down")