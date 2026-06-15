// multiuser/bundle.js
//
// JS mirror of the Go bundled wire format v2 (mcu/internal/proto/bundle.go). It
// is MCU-only: imported by app-mcu.js and INJECTED into the AmbisonicsNode
// engine, so the shared bidirectional path never carries any bundled code.
//
// One network packet carries multiple channels of the SAME audio frame, plus
// the R=2 base-layer redundancy, instead of one 9-byte-header packet per channel.
//
// Header (little-endian), then the Opus payloads concatenated, ascending chIdx:
//   byte 0    : version(high nibble)=1 | flags(low nibble, reserved 0)
//   bytes 1-4 : frameTs uint32 LE      (samples; shared by every channel)
//   bytes 5-6 : chMask  uint16 LE      (bit c set => channel c present)
//   bytes 7.. : (k-1) payload lengths as unsigned LEB128 varints, ascending
//               chIdx; the LAST channel's length is implicit (rest of packet).
'use strict';

export const BUNDLE_VERSION = 1;
export const CHANNELS = 16;
// Byte 0 low-nibble flag: this frame belongs to a synchronized recording window
// (mirrors the Go proto.BundleFlagIsRec and the legacy header's 0x80 isRec bit).
export const BUNDLE_FLAG_ISREC = 0x01;

const MTU_CHANNEL_BUDGET = 1130; // bytes of Opus payload per packet (see Go)
const SAMPLE_RATE = 48000;
const BITRATE = 64000;

// Per-channel Opus bitrate (bits/s) for the multiuser path — mirror of Go
// proto.ChannelBitRates. Order-tapered (ACN ordering): the perceptually dominant
// low orders get more bits, the higher orders fewer, cutting the uplink from
// 16×64=1024 kbps to ~520 kbps while preserving spatial fidelity where it counts.
//   order 0-1 (ch 0-3): 48 kbps · order 2 (ch 4-8): 32 kbps · order 3 (ch 9-15): 24 kbps
export function channelBitrates() {
  const b = new Array(CHANNELS);
  for (let c = 0; c < CHANNELS; c++) b[c] = c <= 3 ? 48000 : c <= 8 ? 32000 : 24000;
  return b;
}

// marshalBundle(frameTs, chans) -> Uint8Array.
// chans: [{ chIdx, payload: Uint8Array }], any order, unique chIdx in [0,15].
export function marshalBundle(frameTs, chans, isRec = false) {
  if (!chans || chans.length === 0) throw new Error('bundle: empty');
  if (chans.length > CHANNELS) throw new Error('bundle: too many channels');
  const sorted = [...chans].sort((a, b) => a.chIdx - b.chIdx);

  let mask = 0;
  let total = 0;
  for (let i = 0; i < sorted.length; i++) {
    const c = sorted[i];
    if (c.chIdx < 0 || c.chIdx >= CHANNELS) throw new Error('bundle: chIdx range');
    const bit = 1 << c.chIdx;
    if (mask & bit) throw new Error('bundle: duplicate channel');
    mask |= bit;
    total += c.payload.length;
  }

  // Length varints for the first k-1 channels (last is implicit).
  const lenBytes = [];
  for (let i = 0; i < sorted.length - 1; i++) lenBytes.push(uvarint(sorted[i].payload.length));
  let lenTotal = 0;
  for (const v of lenBytes) lenTotal += v.length;

  const buf = new Uint8Array(7 + lenTotal + total);
  const dv = new DataView(buf.buffer);
  buf[0] = (BUNDLE_VERSION << 4) | (isRec ? BUNDLE_FLAG_ISREC : 0);
  dv.setUint32(1, frameTs >>> 0, true);
  dv.setUint16(5, mask, true);
  let off = 7;
  for (const v of lenBytes) { buf.set(v, off); off += v.length; }
  for (const c of sorted) { buf.set(c.payload, off); off += c.payload.length; }
  return buf;
}

// parseBundle(input) -> { frameTs, chans: [{ chIdx, payload }] }.
// Payloads are subarrays of the input (no copy) — copy if the buffer is reused.
export function parseBundle(input) {
  const buf = input instanceof Uint8Array ? input : new Uint8Array(input);
  if (buf.length < 7) throw new Error('bundle: short');
  if (buf[0] >> 4 !== BUNDLE_VERSION) throw new Error('bundle: version');
  const isRec = (buf[0] & BUNDLE_FLAG_ISREC) !== 0;
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const frameTs = dv.getUint32(1, true);
  const mask = dv.getUint16(5, true);

  const idxs = [];
  for (let c = 0; c < CHANNELS; c++) if (mask & (1 << c)) idxs.push(c);
  const k = idxs.length;
  if (k === 0) throw new Error('bundle: empty');

  let off = 7;
  let sum = 0;
  const lengths = new Array(k);
  for (let i = 0; i < k - 1; i++) {
    const [v, n] = readUvarint(buf, off);
    off += n;
    lengths[i] = v;
    sum += v;
  }
  const regionLen = buf.length - off;
  if (sum > regionLen) throw new Error('bundle: length overrun');
  lengths[k - 1] = regionLen - sum;

  const chans = new Array(k);
  let p = off;
  for (let i = 0; i < k; i++) {
    chans[i] = { chIdx: idxs[i], payload: buf.subarray(p, p + lengths[i]) };
    p += lengths[i];
  }
  return { frameTs, chans, isRec };
}

// maxChannelsPerPacket / planLayout mirror Go's PlanLayout: R=2 base (ch0-3 in
// two packets), order-aligned enhancement (order2=ch4-8, order3=ch9-15), MTU-
// bounded. P=2 up to 10 ms; dedicated base copies + per-order groups above.
export function maxChannelsPerPacket(frameSize) {
  let ref = Math.floor((BITRATE * frameSize) / (SAMPLE_RATE * 8));
  if (ref < 1) ref = 1;
  let m = Math.floor(MTU_CHANNEL_BUDGET / ref);
  if (m < 1) m = 1;
  if (m > CHANNELS) m = CHANNELS;
  return m;
}

export function planLayout(frameSize) {
  const base = [0, 1, 2, 3];
  const order2 = [4, 5, 6, 7, 8];
  const order3 = [9, 10, 11, 12, 13, 14, 15];
  const m = maxChannelsPerPacket(frameSize);

  if (base.length + order2.length <= m && base.length + order3.length <= m) {
    return [[...base, ...order2], [...base, ...order3]];
  }
  const out = [];
  const chunk = (s) => { for (let i = 0; i < s.length; i += m) out.push(s.slice(i, i + m)); };
  chunk(base);   // base copy 1
  chunk(order2);
  chunk(order3);
  chunk(base);   // base copy 2 (R=2)
  return out;
}

// --- unsigned LEB128 (matches Go encoding/binary Uvarint) ---
function uvarint(value) {
  const out = [];
  let v = value >>> 0;
  while (v >= 0x80) { out.push((v & 0x7f) | 0x80); v >>>= 7; }
  out.push(v);
  return Uint8Array.from(out);
}

function readUvarint(buf, off) {
  let x = 0, s = 0, n = 0;
  for (;;) {
    const b = buf[off + n];
    if (b === undefined) throw new Error('bundle: varint truncated');
    n++;
    if (b < 0x80) { x |= b << s; break; }
    x |= (b & 0x7f) << s;
    s += 7;
    if (n > 5) throw new Error('bundle: varint too long');
  }
  return [x >>> 0, n];
}
