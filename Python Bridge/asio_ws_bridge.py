"""
asio_ws_bridge.py

Local-Loop Bridge for Windows: Reaper -> ReaRoute ASIO -> Python -> WebSocket -> Browser

-- ARCHITECTURE OVERVIEW --
1. PortAudio/ASIO thread, high-priority
    - _audio_callback()     accumulates blocks into OUTPUT_FRAME-sized chunks,
                            then transfers them to the event loop via call_soon_threadsafe

2. Asyncio event loop, main thread
    - _broadcaster()        reads from the queue and broadcasts to all connected clients
    - _ws_handler()         manages client connections and disconnections

3. asyncio.Queue, thread-safe communication channel
    - implements drop-oldest policy to prevent latency buildup


-- BINARY PAYLOAD LAYOUT --
Each WebSocket message is a binary blob: an 8-byte little-endian uint64 sample
timestamp header, followed by one frame of interleaved audio samples:
    [ ts(uint64 LE) ][ s0 ch0, s0 ch1, ..., s0 ch15, s1 ch0, s1 ch1, ... ]

- Header       : 8 bytes, sample timestamp of the frame's first sample
- Frame size   : 960 samples (default)
- Channels     : 16 (3rd Order Ambisonics)
- Sample format: 32-bit float, little-endian, normalized to [-1.0, +1.0]
- Total size   : 8 + 960 * 16 * 4 = 61,448 bytes per frame

The timestamp advances by OUTPUT_FRAME for every frame PRODUCED, including frames
dropped on queue saturation. This lets the browser jitter buffer detect real gaps
(it inserts silence) instead of replaying dropped frames as if contiguous, which
would corrupt the waveform and sound metallic.


-- ASIO BLOCK vs OUTPUT FRAME --
The ASIO block size (ASIO_BLOCK) is the period REAPER negotiates with the hardware
driver and does not need to divide OUTPUT_FRAME evenly. The accumulation buffer in
_audio_callback() absorbs any block-size mismatch and always produces OUTPUT_FRAME-sized
payloads for the WebSocket layer.


-- AVOIDING DEVICE ENUMERATION --
sd.query_devices() on Windows probes every host API (WASAPI, WDM-KS, DirectSound, …)
and can cause glitches on microphones held open by other applications (e.g. video-call
clients). To skip enumeration entirely, set ASIO_DEVICE_INDEX to the integer index of
the ReaRoute ASIO device. Leave it as None to let find_asio_device_index() search by
name instead.

To discover the index once without running the bridge, execute:
    python -c "import os; os.environ['SD_ENABLE_ASIO']='1'; import sounddevice as sd; print(sd.query_devices())"

"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import os
import queue
from typing import Optional

# Must be set before importing sounddevice to enable ASIO support
os.environ["SD_ENABLE_ASIO"] = "1"

import numpy as np
import sounddevice as sd
import websockets
from websockets.legacy.server import WebSocketServerProtocol


# Configuration
ASIO_DEVICE_NAME:  str       = "ReaRoute ASIO"  # ASIO device name (used when ASIO_DEVICE_INDEX is None)
ASIO_DEVICE_INDEX: Optional[int] = None         # Set to an int to skip device enumeration entirely

SAMPLE_RATE:      int = 48_000  # Hz
NUM_CHANNELS:     int = 16      # 3rd Order Ambisonics
OUTPUT_FRAME:     int = 960     # frame size sent to WebSocket, overridden by CLI arg
ASIO_BLOCK:       int = 512     # ASIO block size, must match REAPER's buffer setting
WS_HOST:          str = "127.0.0.1"
WS_PORT:          int = 9090
QUEUE_MAX_FRAMES: int = 8       # excess frames are dropped (drop-oldest)

# Throttle repeated PortAudio status warnings: log the first 3, then every Nth
_PA_STATUS_LOG_INTERVAL: int = 50

# Valid Opus frame durations in samples at 48 kHz
_VALID_FRAME_SIZES = {120, 240, 480, 960, 1920, 2880}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("asio_bridge")


def find_asio_device_index(target_name: str) -> Optional[int]:
    """
    Returns the global sounddevice index of the named ASIO device.

    sounddevice requires the global device index, not the index within the
    ASIO host API. Returns None if the ASIO host API or the device is not found.
    """
    asio_api_idx: Optional[int] = None
    for idx, api in enumerate(sd.query_hostapis()):
        if "ASIO" in api["name"].upper():
            asio_api_idx = idx
            break

    if asio_api_idx is None:
        log.error(
            "ASIO host API not found. "
            "Check that an ASIO driver is installed and SD_ENABLE_ASIO=1 is set."
        )
        return None

    for idx, dev in enumerate(sd.query_devices()):
        if dev["hostapi"] == asio_api_idx and target_name in dev["name"]:
            return idx

    log.error("Device '%s' not found within the ASIO host API.", target_name)
    return None


class AudioBridge:
    """
    Bridges the PortAudio/ASIO real-time thread and the asyncio event loop.

    Thread safety is guaranteed by funneling all audio frames through an
    asyncio.Queue via call_soon_threadsafe. The callback thread never
    directly interacts with WebSocket state.
    """

    def __init__(self, device_name: str) -> None:
        self._device_name = device_name
        self._loop:           Optional[asyncio.AbstractEventLoop] = None
        self._queue:          Optional[asyncio.Queue]             = None
        self._clients:        set[WebSocketServerProtocol]        = set()
        self._stream:         Optional[sd.InputStream]            = None
        self._dropped_frames: int = 0
        self._pa_status_count: int = 0  # throttle repeated PortAudio warnings

        # Accumulation buffer: absorbs ASIO_BLOCK ≠ OUTPUT_FRAME mismatches
        self._accum:      np.ndarray = np.zeros((OUTPUT_FRAME, NUM_CHANNELS), dtype=np.float32)
        self._accum_fill: int        = 0

        # Clock for the sample timestamp header
        self._sample_clock: int = 0

    # ASIO callback
    def _audio_callback(
        self,
        indata:    np.ndarray,  # shape: (ASIO_BLOCK, NUM_CHANNELS), dtype=float32
        frames:    int,
        time_info,
        status:    sd.CallbackFlags,
    ) -> None:
        """
        Real-time audio callback invoked by PortAudio on the ASIO thread.

        Accumulates incoming blocks into OUTPUT_FRAME-sized chunks. When a
        frame is complete it is serialised to bytes and scheduled on the
        asyncio event loop via call_soon_threadsafe — the only asyncio-safe
        call permitted from the real-time thread.
        """
        if status:
            self._pa_status_count += 1
            if self._pa_status_count <= 3 or self._pa_status_count % _PA_STATUS_LOG_INTERVAL == 0:
                log.warning(
                    "ASIO status: %s (occurrence #%d)",
                    status, self._pa_status_count,
                )

        if self._loop is None or self._queue is None:
            return

        # Defensive cast: no-op when already float32, avoids mismatches otherwise
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
                                    "Queue saturated: %d frames dropped total",
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
        """Registers a client on connect and removes it on disconnect."""
        remote = websocket.remote_address
        log.info("Client connected:    %s:%s", *remote)

        # Send frame-size config as the first message
        config_msg = json.dumps({"type": "config", "frameSize": OUTPUT_FRAME})
        await websocket.send(config_msg)
        log.debug("Sent config to %s:%s  frameSize=%d", *remote, OUTPUT_FRAME)

        # Reset the sample clock and accumulator so this session starts at ts=0.
        self._sample_clock = 0
        self._accum_fill   = 0

        self._clients.add(websocket)
        try:
            await websocket.wait_closed()
        finally:
            self._clients.discard(websocket)
            log.info("Client disconnected: %s:%s", *remote)

    async def _broadcaster(self) -> None:
        """Dequeues audio blobs and broadcasts them out to every connected client."""
        log.info("Broadcaster started - waiting for audio frames...")
        try:
            while True:
                blob: bytes = await self._queue.get()

                if not self._clients:
                    continue

                # Snapshot before awaiting so mid-send disconnects are safe
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
        """Opens the ASIO stream and starts the WebSocket server."""
        self._loop  = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=QUEUE_MAX_FRAMES)

        # Resolve device index — skip enumeration if hardcoded
        if ASIO_DEVICE_INDEX is not None:
            device_idx = ASIO_DEVICE_INDEX
            log.info("Using hardcoded ASIO device index: %d", device_idx)
        else:
            device_idx = find_asio_device_index(self._device_name)
            if device_idx is None:
                log.error(
                    "ASIO device '%s' not found. Install an ASIO driver (e.g. ASIO4ALL) "
                    "and ensure SD_ENABLE_ASIO=1 is set before importing sounddevice. "
                    "Alternatively, set ASIO_DEVICE_INDEX to skip enumeration.",
                    self._device_name,
                )
                sys.exit(1)

        dev_info = sd.query_devices(device_idx)
        max_ch   = dev_info["max_input_channels"]
        if max_ch < NUM_CHANNELS:
            log.error(
                "Device '%s' has only %d input channels (%d required). "
                "Ensure the Zylia Converter plugin is active in REAPER.",
                dev_info["name"], max_ch, NUM_CHANNELS,
            )
            sys.exit(1)
        if max_ch > NUM_CHANNELS:
            log.warning(
                "Device '%s' exposes %d channels; bridge will read only %d (0-%d).",
                dev_info["name"], max_ch, NUM_CHANNELS, NUM_CHANNELS - 1,
            )

        log.info(
            "Opening ASIO stream on '%s' [%d ch @ %d Hz | ASIO block=%d | WS frame=%d]",
            dev_info["name"], NUM_CHANNELS, SAMPLE_RATE, ASIO_BLOCK, OUTPUT_FRAME,
        )

        try:
            self._stream = sd.InputStream(
                device=device_idx,
                channels=NUM_CHANNELS,
                samplerate=SAMPLE_RATE,
                blocksize=ASIO_BLOCK,
                dtype="float32",
                latency="low",
                extra_settings=sd.AsioSettings(
                    channel_selectors=list(range(NUM_CHANNELS))  # explicit mapping 0-15
                ),
                callback=self._audio_callback,
            )
        except Exception as e:
            log.error("Failed to open ASIO device '%s': %s", dev_info["name"], e)
            sys.exit(1)

        with self._stream:
            log.info("ASIO stream active")
            async with websockets.serve(
                self._ws_handler,
                WS_HOST,
                WS_PORT,
                compression=None,       # disabled: reduces CPU overhead and latency
                max_size=256 * 1024,
            ):
                log.info("WebSocket server listening on ws://%s:%d", WS_HOST, WS_PORT)
                await self._broadcaster()  # blocks until cancelled


def _parse_args() -> int:
    """
    Parse optional [frame_size] positional argument.
 
    Usage: python asio_ws_bridge.py [FRAME_SIZE]
 
    Valid Opus frame sizes at 48 kHz: 120, 240, 480, 960, 1920, 2880 samples.
    """
    parser = argparse.ArgumentParser(
        description="Reaper -> ASIO -> WebSocket bridge for 16-ch Ambisonics",
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
        "frame_size",
        type=int,
        nargs="?",
        default=960,
        metavar="FRAME_SIZE",
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

    bridge = AudioBridge(ASIO_DEVICE_NAME)
    bridge_task = asyncio.create_task(bridge.run())

    try:
        await bridge_task
    finally:
        if not bridge_task.done():
            bridge_task.cancel()
            await asyncio.gather(bridge_task, return_exceptions=True)
            
        log.info("Bridge stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted (Ctrl+C) - shutting down")