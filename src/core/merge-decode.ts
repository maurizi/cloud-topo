// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Low-level decode primitives for the merge pipeline: per-arc endpoint
 * lookups (used as Map keys by the stitcher), the growable Float64
 * coordinate buffer, ring decoding from per-arc bytes, and the shoelace
 * area used to order rings. Self-contained — depends only on the varint
 * reader and the quantization transform type. `merge-stitch` and
 * `merge-prep` build on it.
 */

import { readVarintZigzagInto, type VarintCursor } from "./format";
import { type TransformDef } from "./util";

// The un-quantized arc format stores coords as little-endian f64 pairs.
// On a little-endian host (every mainstream JS runtime) a Float64Array
// view shares that layout, so we can bulk-copy instead of reading each
// coordinate through DataView.getFloat64. Computed once.
const HOST_LITTLE_ENDIAN = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;

// --- Arc endpoint lookup (per-arc bytes from the fetcher) ---

export interface EndpointLookup<K> {
  start(signedArcId: number): K;
  end(signedArcId: number): K;
}

// Pack two int32 quantized coords into one BigInt key (32 bits per
// axis). A single JS Number cannot injectively hold two int32 coords:
// fine grids exceed 2^25 per axis (e.g. NATIONAL_QUANTIZATION = 1e8 ≈
// 2^26.6), and no Number multiplier both separates the axes (> max y)
// and keeps the product under 2^53 — so a packed-Number key collides
// for coords ≥ 2^25. BigInt has no such ceiling.
//
// Why pack instead of `${x},${y}` strings: stitchArcs reads two
// endpoint keys per boundary arc and uses them as Map<K, Fragment>
// keys. At national-CD scale (~1000 boundary arcs/merge × 435 merges
// × 2 endpoints = ~870K key materializations) the string form was the
// dominant allocator inside merge — 47% of sampled bytes in the heap
// profile. A microbench at 1e8-grid scale put BigInt keys ~2.4× faster
// than strings (string keys also force a 4× larger per-key alloc),
// while the whole-national-stitch delta vs an (unsafe) packed-Number
// key was sub-second — correctness, not speed, drives the choice.
function packCoord(x: number, y: number): bigint {
  return (BigInt(x) << 32n) | BigInt(y >>> 0);
}

// Section-backed numeric endpoint lookup. `endpoints` carries per-arc
// length-4 Int32Array views (see client.fetchArcEndpoints) — start at
// [0..2), end at [2..4). Signed arc ids reverse direction: a negative
// signed id ~i reads i's end as its "start" and vice versa.
export function makeNumericEndpointLookup(
  endpoints: ReadonlyMap<number, Int32Array>,
): EndpointLookup<bigint> {
  function readForward(arcId: number): Int32Array {
    const v = endpoints.get(arcId);
    if (v === undefined) {
      throw new Error(`ctopo: missing fetched endpoints for arc ${arcId}`);
    }
    return v;
  }
  return {
    start(signedArcId: number): bigint {
      if (signedArcId >= 0) {
        const v = readForward(signedArcId);
        return packCoord(v[0], v[1]);
      }
      const v = readForward(~signedArcId);
      return packCoord(v[2], v[3]);
    },
    end(signedArcId: number): bigint {
      if (signedArcId >= 0) {
        const v = readForward(signedArcId);
        return packCoord(v[2], v[3]);
      }
      const v = readForward(~signedArcId);
      return packCoord(v[0], v[1]);
    },
  };
}

// Byte-based numeric endpoint lookup for quantized files where we
// already hold arc_coords (the merge path always fetches them for
// ring decode). Decodes the first and last (dx, dy) points from each
// arc's varint stream and packs into the same BigInt key
// makeNumericEndpointLookup uses — so stitchArcs is key-type-
// agnostic.
//
// No per-call memoization: stitchArcs visits each unsigned arc id
// exactly once in its boundary list (forward or reverse, never both
// directions in the same call), so a per-call start/end cache has a
// measured 0% hit rate (417,703 lookups, 0 hits on the national CD
// merge). Adding the Maps was pure overhead — removed.
export function makeNumericEndpointLookupFromBytes(
  arcBytes: ReadonlyMap<number, Uint8Array>,
): EndpointLookup<bigint> {
  // One cursor reused across every start/end read in this lookup.
  // Sync hot path; no concurrency to worry about.
  const cur: VarintCursor = { value: 0, off: 0 };
  function readStartPacked(arcId: number): bigint {
    const bytes = requireArcBytes(arcBytes, arcId);
    cur.off = 0;
    readVarintZigzagInto(bytes, cur);
    const dx = cur.value;
    readVarintZigzagInto(bytes, cur);
    return packCoord(dx, cur.value);
  }
  function readEndPacked(arcId: number): bigint {
    const bytes = requireArcBytes(arcBytes, arcId);
    let x = 0;
    let y = 0;
    cur.off = 0;
    const end = bytes.byteLength;
    while (cur.off < end) {
      readVarintZigzagInto(bytes, cur);
      const dx = cur.value;
      readVarintZigzagInto(bytes, cur);
      x += dx;
      y += cur.value;
    }
    return packCoord(x, y);
  }
  return {
    start(signedArcId: number): bigint {
      return signedArcId >= 0
        ? readStartPacked(signedArcId)
        : readEndPacked(~signedArcId);
    },
    end(signedArcId: number): bigint {
      return signedArcId >= 0
        ? readEndPacked(signedArcId)
        : readStartPacked(~signedArcId);
    },
  };
}

export function makeEndpointLookup(
  arcBytes: ReadonlyMap<number, Uint8Array>,
  isQuantized: boolean,
): EndpointLookup<string> {
  const cur: VarintCursor = { value: 0, off: 0 };

  function readStart(arcId: number): [number, number] {
    const bytes = requireArcBytes(arcBytes, arcId);
    if (isQuantized) {
      cur.off = 0;
      readVarintZigzagInto(bytes, cur);
      const dx = cur.value;
      readVarintZigzagInto(bytes, cur);
      return [dx, cur.value];
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return [view.getFloat64(0, true), view.getFloat64(8, true)];
  }

  function readEnd(arcId: number): [number, number] {
    const bytes = requireArcBytes(arcBytes, arcId);
    if (isQuantized) {
      let x = 0;
      let y = 0;
      cur.off = 0;
      const end = bytes.byteLength;
      while (cur.off < end) {
        readVarintZigzagInto(bytes, cur);
        const dx = cur.value;
        readVarintZigzagInto(bytes, cur);
        x += dx;
        y += cur.value;
      }
      return [x, y];
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const numPoints = bytes.byteLength / 16;
    const lastOff = (numPoints - 1) * 16;
    return [view.getFloat64(lastOff, true), view.getFloat64(lastOff + 8, true)];
  }

  return {
    start(signedArcId: number): string {
      const [p0, p1] =
        signedArcId >= 0 ? readStart(signedArcId) : readEnd(~signedArcId);
      return `${p0},${p1}`;
    },
    end(signedArcId: number): string {
      const [p0, p1] =
        signedArcId >= 0 ? readEnd(signedArcId) : readStart(~signedArcId);
      return `${p0},${p1}`;
    },
  };
}

function requireArcBytes(
  arcBytes: ReadonlyMap<number, Uint8Array>,
  arcId: number,
): Uint8Array {
  const bytes = arcBytes.get(arcId);
  if (bytes === undefined) {
    throw new Error(`ctopo: missing fetched bytes for arc ${arcId}`);
  }
  return bytes;
}

// Growable Float64 buffer — the per-merge scratch into which
// decodeRingFlat appends ring points. Doubling capacity keeps the
// amortized cost linear; `finalize()` returns a tightly-sized slice
// that owns its own ArrayBuffer (so the worker can postMessage with
// transfer semantics without orphaning pooled storage).
export class Float64Growable {
  buf: Float64Array;
  // Number of *floats* written so far (= 2 * point count).
  len: number;
  constructor(initialFloats: number) {
    this.buf = new Float64Array(initialFloats);
    this.len = 0;
  }
  ensure(needFloats: number): void {
    if (this.len + needFloats <= this.buf.length) return;
    let cap = this.buf.length;
    while (cap < this.len + needFloats) cap *= 2;
    const grown = new Float64Array(cap);
    grown.set(this.buf.subarray(0, this.len));
    this.buf = grown;
  }
  pushPoint(x: number, y: number): void {
    this.ensure(2);
    this.buf[this.len++] = x;
    this.buf[this.len++] = y;
  }
  popPoint(): void {
    this.len -= 2;
  }
  // Reverse points in place from `startFloats` to current end. Operates
  // on whole (x, y) pairs — same semantics as the in-place reverse in
  // the legacy decodeRing's number[][] path.
  reverseRange(startFloats: number): void {
    let lo = startFloats;
    let hi = this.len - 2;
    while (lo < hi) {
      const x = this.buf[lo];
      const y = this.buf[lo + 1];
      this.buf[lo] = this.buf[hi];
      this.buf[lo + 1] = this.buf[hi + 1];
      this.buf[hi] = x;
      this.buf[hi + 1] = y;
      lo += 2;
      hi -= 2;
    }
  }
  finalize(): Float64Array {
    return this.buf.slice(0, this.len);
  }
}

// Growable Int32 buffer — the arcs-variant analogue of
// Float64Growable. stitchBatchArcs appends signed arc ids here ring by
// ring; doubling keeps amortized appends linear and `finalize()` hands
// back a tightly-sized, transfer-owning Int32Array. Avoids the
// intermediate `number[]` + `new Int32Array(arr)` element-by-element
// conversion the batch used to pay over every boundary arc.
export class Int32Growable {
  buf: Int32Array;
  len: number;
  constructor(initial: number) {
    this.buf = new Int32Array(initial);
    this.len = 0;
  }
  push(v: number): void {
    if (this.len === this.buf.length) {
      const grown = new Int32Array(this.buf.length * 2);
      grown.set(this.buf);
      this.buf = grown;
    }
    this.buf[this.len++] = v;
  }
  finalize(): Int32Array {
    return this.buf.slice(0, this.len);
  }
}

// --- decodeRingFlat (per-arc bytes → packed Float64) ---
//
// Walks each arc's varint stream (or, for un-quantized, its raw f64
// pairs) and pushes points into `out` in ring order. Mirrors the old
// number[][] decodeRing semantics exactly: shared join points dropped,
// reverse-direction arcs reversed in place, the closing duplicate
// pushed at the end.
//
// The returned (start, end) is in *point indices* — multiply by 2 to
// get float positions inside out.buf.
export function decodeRingFlat(
  arcIds: ReadonlyArray<number>,
  arcBytes: ReadonlyMap<number, Uint8Array>,
  transform: TransformDef,
  out: Float64Growable,
): { start: number; end: number } {
  const t = transform;
  const isQuantized = t !== null;
  const kx = isQuantized ? t.scale[0] : 0;
  const ky = isQuantized ? t.scale[1] : 0;
  const tx = isQuantized ? t.translate[0] : 0;
  const ty = isQuantized ? t.translate[1] : 0;
  const ringStartFloats = out.len;
  const cur: VarintCursor = { value: 0, off: 0 };

  for (const signed of arcIds) {
    const arcId = signed >= 0 ? signed : ~signed;
    const forward = signed >= 0;
    const bytes = requireArcBytes(arcBytes, arcId);
    const segStartFloats = out.len;

    if (isQuantized) {
      let x = 0;
      let y = 0;
      cur.off = 0;
      const end = bytes.byteLength;
      while (cur.off < end) {
        readVarintZigzagInto(bytes, cur);
        const dx = cur.value;
        readVarintZigzagInto(bytes, cur);
        x += dx;
        y += cur.value;
        out.pushPoint(x * kx + tx, y * ky + ty);
      }
    } else {
      const numFloats = bytes.byteLength / 8;
      out.ensure(numFloats);
      if (HOST_LITTLE_ENDIAN && bytes.byteOffset % 8 === 0) {
        // 8-byte aligned on a little-endian host: read the f64 pairs as
        // one typed-array view and bulk-copy, skipping the per-point
        // DataView.getFloat64 calls.
        const src = new Float64Array(bytes.buffer, bytes.byteOffset, numFloats);
        out.buf.set(src, out.len);
        out.len += numFloats;
      } else {
        // Misaligned view: Float64Array can't wrap it, so fall back to
        // DataView's unaligned reads.
        const view = new DataView(
          bytes.buffer,
          bytes.byteOffset,
          bytes.byteLength,
        );
        for (let i = 0; i < numFloats; i++) {
          out.buf[out.len++] = view.getFloat64(i * 8, true);
        }
      }
    }

    if (out.len === segStartFloats) continue;

    if (forward) {
      // Drop the last point — shared join with the next arc's start.
      out.popPoint();
    } else {
      // Reverse the arc's segment in place, then drop the new tail
      // (originally the first decoded point — the shared join in the
      // backward walk).
      out.reverseRange(segStartFloats);
      out.popPoint();
    }
  }

  if (out.len > ringStartFloats) {
    // Close the ring with the first point.
    const sx = out.buf[ringStartFloats];
    const sy = out.buf[ringStartFloats + 1];
    out.pushPoint(sx, sy);
  }
  return { start: ringStartFloats / 2, end: out.len / 2 };
}

// --- ringAreaFlat (shoelace, on packed Float64) ---

export function ringAreaFlat(
  coords: Float64Array,
  startPoint: number,
  endPoint: number,
): number {
  const n = endPoint - startPoint;
  if (n < 3) return 0;
  let area = 0;
  let bx = coords[(endPoint - 1) * 2];
  let by = coords[(endPoint - 1) * 2 + 1];
  for (let i = startPoint; i < endPoint; i++) {
    const ax = bx;
    const ay = by;
    const off = i * 2;
    bx = coords[off];
    by = coords[off + 1];
    area += ax * by - ay * bx;
  }
  return Math.abs(area);
}
