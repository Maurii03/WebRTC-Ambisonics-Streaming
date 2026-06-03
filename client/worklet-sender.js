/**
 * @fileoverview worklet-sender.js
 * Accumulates 16-channel audio from the WebAudio graph into N-sample frames 
 * (e.g., 960 samples for 20ms) and posts 16 independent planar (mono) 
 * Float32Array buffers back to the main thread for WebCodecs encoding.
 */

'use strict';

const CHANNELS = 16;

/**
 * Custom AudioWorklet node processor.
 * Runs on a dedicated audio thread to guarantee low-latency processing without
 * being affected by main-thread UI blocking.
 * @extends AudioWorkletProcessor
 */
class SenderProcessor extends AudioWorkletProcessor {
  /**
   * Initializes the processor, allocates initial planar buffers, and sets up
   * the message port to receive updates from the main thread.
   */
  constructor() {
    super();
    this._frameSize = 960; // Default (20ms @ 48kHz), overridable via config
    // Allocate 16 planar buffers for the initial frame size
    this._buf  = Array.from({ length: CHANNELS }, () => new Float32Array(this._frameSize));
    this._fill = 0;  // Samples accumulated in current frame
    this._ts   = 0;  // Running timestamp in samples

    // Handle incoming messages from the main thread
    this.port.onmessage = ({ data }) => {
      if (data.type === 'config' && data.frameSize != null) { // new frame size config
        this._frameSize = data.frameSize;
        // Reallocate buffers for the new frame size
        this._buf  = Array.from({ length: CHANNELS }, () => new Float32Array(this._frameSize));
        this._fill = 0;
        this._ts   = 0;
      }
    };
  }

  /**
   * The core audio processing loop. Called by the browser's audio engine every
   * 128 samples (render quantum).
   * @param {Float32Array[][]} inputs - Multi-dimensional array representing 
   * sample blocks for each input and channel.
   * @returns {boolean} True to keep the processor alive and running in the audio graph.
   */
  process(inputs) {
    const input = inputs[0];
    if (!input?.length) return true; // No input connected, keep alive

    const blockSize = input[0].length;
    let offset = 0;

    // Accumulate samples into the current frame
    while (offset < blockSize) {
      // Calculate how many samples we can copy in this iteration
      const toCopy = Math.min(blockSize - offset, this._frameSize - this._fill);

      for (let c = 0; c < CHANNELS; c++) {
        // Fallback to channel 0 if the device delivered fewer channels
        const ch = input[c] ?? input[0];
        // Copy samples into the planar buffer for this channel
        this._buf[c].set(ch.subarray(offset, offset + toCopy), this._fill);
      }

      // Update fill level and offset for the next iteration
      this._fill += toCopy;
      offset     += toCopy;

      // If we've filled a complete frame, post it to the main thread
      if (this._fill === this._frameSize) {
        const channels = [];
        const xfer     = [];

        for (let c = 0; c < CHANNELS; c++) {
          const ch = this._buf[c];
          channels.push(ch);
          xfer.push(ch.buffer);
          // New buffer for the next frame
          this._buf[c] = new Float32Array(this._frameSize);
        }

        // Post the payload to the main thread.
        // Zero-copy transfer of all 16 channel buffers
        this.port.postMessage({ timestamp: this._ts, channels }, xfer);

        this._ts  += this._frameSize;
        this._fill = 0;
      }
    }

    return true; // keep processor alive
  }
}

registerProcessor('sender-processor', SenderProcessor);