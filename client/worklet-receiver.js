/**
 * @fileoverview worklet-receiver.js
 * Plays back 16ch PCM frames posted by the main thread via a timestamp-based
 * ring buffer. Starts playback after a configurable prefill (default 100ms).
 * Outputs silence on underrun (packet loss or late arrival).
 */

'use strict';

const CHANNELS    = 16;
const BUF_SECONDS = 2; // ring buffer capacity, must exceed max network jitter

/**
 * Custom AudioWorklet processor that acts as a Jitter Buffer for playback.
 * @extends AudioWorkletProcessor
 */
class ReceiverProcessor extends AudioWorkletProcessor {
  constructor() {
    super();

    // Ring buffer for interleaved samples
    const bufLen     = Math.ceil(sampleRate * BUF_SECONDS);
    this._buf        = new Float32Array(bufLen * CHANNELS); // pre-zeroed
    this._bufLen     = bufLen;

    this._writeTs    = 0;     // Highest sample timestamp written + 1
    this._readTs     = 0;     // Next sample timestamp to read for playback
    this._initTs     = -1;    // Timestamp of the first frame received

    this._started    = false; // True once prebuffer threshold is met
    this._frameSize  = 960;   // Default, updated by config messages

    this._prebuffer  = Math.ceil(0.1 * sampleRate);
    this._isUnderrunning = false;

    // Handle incoming messages from the main thread
    this.port.onmessage = ({ data }) => {
      if (data.type === 'frame')  { this._writeFrame(data); return; }

      // Re-anchor: drop the current buffer and treat the next frame as fresh origin
      if (data.type === 'reanchor') {
        this._started = false;
        this._initTs  = -1;
        this._writeTs = 0;
        this._readTs  = 0;
        this._isUnderrunning = false;
        this._buf.fill(0);
        return;
      }

      if (data.type === 'config') {
        let needsReset = false;

        // A frame-size change requires a playback reset to avoid timestamp discontinuities
        if (data.frameSize != null && data.frameSize !== this._frameSize) {
          this._frameSize = data.frameSize;
          needsReset = true;
        }

        // Prebuffer depth is authoritative from the main thread.
        if (data.prebuffer != null) {
          this._prebuffer = data.prebuffer;
        }

        // Reset playback state on a frame-size change
        if (needsReset) {
          this._started = false;
          this._initTs  = -1;
          this._writeTs = 0;
          this._readTs  = 0;
          this._buf.fill(0);
        }
      }
    };
  }

  /**
   * Writes a complete 16-channel frame into the circular ring buffer.
   * Data is placed based on its absolute timestamp, guaranteeing perfect 
   * phase alignment even if the main thread dispatches frames slightly out-of-order.
   * @param {Object} payload 
   * @param {number} payload.timestamp - The absolute starting sample of this frame.
   * @param {Float32Array[]} payload.channels - 16 planar mono arrays.
   */
  _writeFrame({ timestamp, channels }) {
    const frameSize = channels[0].length;

    // Anchor the ring buffer origin to the first frame's timestamp
    if (this._initTs < 0) {
      this._initTs  = timestamp;
      this._readTs  = timestamp;
      this._writeTs = timestamp;
    }

    // Write each sample at its absolute interleaved ring-buffer position
    for (let s = 0; s < frameSize; s++) {
      const pos = ((timestamp + s) % this._bufLen) * CHANNELS;
      for (let c = 0; c < CHANNELS; c++) {
        this._buf[pos + c] = channels[c][s];
      }
    }

    // Advance write timestamp
    const newWriteTs = timestamp + frameSize;
    if (newWriteTs > this._writeTs) this._writeTs = newWriteTs;

    // Overflow protection
    const maxSpan = this._bufLen - frameSize;
    if (this._writeTs - this._readTs > maxSpan) {
      this._readTs = this._writeTs - maxSpan;
    }

    // Begin playback only once the prebuffer threshold is reached
    if (!this._started && (this._writeTs - this._readTs) >= this._prebuffer) {
      this._started = true;
    }

    const buffers   = new Array(CHANNELS);
    const transfers = new Array(CHANNELS);
    for (let c = 0; c < CHANNELS; c++) {
      buffers[c]   = channels[c].buffer;
      transfers[c] = channels[c].buffer;
    }
    this.port.postMessage({ type: 'recycle', buffers }, transfers);

  }

  /**
   * The core playback loop. Called by the WebAudio engine every 128 samples.
   * @param {Float32Array[][]} inputs - Unused (this is an endpoint node).
   * @param {Float32Array[][]} outputs - Hardware output destination buffers.
   * @returns {boolean} True to keep the processor alive.
   */
  process(inputs, outputs) {
    const out       = outputs[0];
    const blockSize = out[0].length; // Strictly 128 samples per render quantum

    if (!this._started) return true; // still pre-buffering

    const available = this._writeTs - this._readTs;

    // Underrun protection
    if (available < blockSize) {
      // Hold read position, emit silence, wait for more data
      if (!this._isUnderrunning) {
        this._isUnderrunning = true;
        this.port.postMessage({ type: 'underrun' });
      }
      return true;
    }

    // Underrun resolved
    this._isUnderrunning = false;

    // Playback
    for (let s = 0; s < blockSize; s++) {
      const pos = ((this._readTs + s) % this._bufLen) * CHANNELS;

      // Copy interleaved data to output and zero out the ring buffer position
      for (let c = 0; c < CHANNELS; c++) {
        // If the frame containing this sample has arrived, copy it to output
        if (c < out.length) {
          out[c][s] = this._buf[pos + c];
        }
        // Clear the buffer slot
        this._buf[pos + c] = 0;
      }
    }

    // Advance the read pointer by 128 samples
    this._readTs += blockSize;
    return true;
  }
}

registerProcessor('receiver-processor', ReceiverProcessor);