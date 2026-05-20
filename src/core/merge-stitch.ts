// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Arc stitching and per-batch assembly. `stitchArcsPure` is the
 * topojson-client stitch algorithm over fetched per-arc endpoints;
 * `stitchBatch{Arcs,Coords}` run a batch of connectivity groups into
 * flat typed-array results, and `assemble{Arcs,Coords}Result` wrap a
 * batch result as the transferable Flat* shape. Builds on merge-decode.
 */

import {
  decodeRingFlat,
  Float64Growable,
  Int32Growable,
  ringAreaFlat,
  type EndpointLookup,
} from "./merge-decode";
import { type TransformDef } from "./util";

// --- Flat (transferable) result shapes ---
//
// The merge primitives' "real" outputs are typed-array CSR structures
// (FlatMultiPolygon / FlatMultiPolygonArcs) — these are what crosses
// the worker boundary, transferred zero-copy.
//
// Ring storage uses parallel ringStarts / ringEnds (rather than a
// monotonic CSR offset array) so `merge`'s area-sorted ring order
// inside a polygon doesn't force the encoder to reorder coord bytes
// after decode.
export interface FlatMultiPolygon {
  readonly type: "MultiPolygon";
  // Packed [x0,y0,x1,y1,...]; already unquantized (cheap fused
  // multiply-add inside decodeRingFlat) so the main-thread rebuild
  // walks the buffer with no further math.
  readonly coords: Float64Array;
  // Per-ring start/end point indices into `coords`. Length = numRings.
  readonly ringStarts: Uint32Array;
  readonly ringEnds: Uint32Array;
  // CSR over rings — polygon p's rings are
  // ringStarts/ringEnds[polyRingStarts[p] .. polyRingStarts[p+1]).
  // Length = numPolys + 1.
  readonly polyRingStarts: Uint32Array;
}

export interface FlatMultiPolygonArcs {
  readonly type: "MultiPolygon";
  readonly arcs: Int32Array;
  readonly ringStarts: Uint32Array;
  readonly ringEnds: Uint32Array;
  readonly polyRingStarts: Uint32Array;
}

// --- stitch scratch ---

// Per-client scratch space for stitchArcs. The three structures (two
// Maps + one Set) are allocated fresh on each stitchArcs call in the
// naive version — across the national merge that's ~thousands of
// short-lived Map/Set instances, each carrying its own hash-table
// backing. Holding one instance per client and `.clear()`ing between
// uses keeps the underlying hash storage alive across calls, so Map
// growth happens once (during the first/biggest call) rather than
// every time. Safe to share because stitchArcs is fully synchronous —
// no merge can interleave another stitchArcs on the same client
// between clear() and the end of the function.
export interface StitchScratch {
  fragmentByStart: Map<unknown, Fragment>;
  fragmentByEnd: Map<unknown, Fragment>;
  stitched: Set<number>;
}

// Allocate a fresh StitchScratch. Compute-worker callers maintain
// their own instance for the worker's lifetime; the coordinator
// pulls scratch from the per-client WeakMap (`getStitchScratch`).
export function makeStitchScratch(): StitchScratch {
  return {
    fragmentByStart: new Map(),
    fragmentByEnd: new Map(),
    stitched: new Set(),
  };
}

// --- Batch primitives (per-merge unit of work, can run off-thread) ---

// Context shared across all batches in one merge. Carries the
// endpoint lookup, arc bytes (for decoded rings), and transform.
// Compute workers reconstruct this from per-batch postMessage payloads;
// the coordinator inline path builds it once per merge.
export interface StitchBatchContext {
  readonly transform: TransformDef;
  readonly endpoints: EndpointLookup<unknown>;
  // Required for the coords path (mergeFlat). Undefined for the arcs
  // path (mergeArcsFlat) which never decodes ring coords.
  readonly arcBytes: ReadonlyMap<number, Uint8Array> | undefined;
}

// Per-batch result for the arcs path (mergeArcsFlat).
export interface StitchBatchArcsResult {
  // Concatenated signed arc ids across all rings emitted by this batch.
  readonly arcs: Int32Array;
  // Per-ring start indices into `arcs`. Length = total rings in batch.
  readonly ringStarts: Uint32Array;
  readonly ringEnds: Uint32Array;
  // numRingsPerGroup[g] = number of rings emitted by groups[g].
  // Length = groups.length. Empty groups have count 0.
  readonly numRingsPerGroup: Uint32Array;
}

// Per-batch result for the coords path (mergeFlat).
export interface StitchBatchCoordsResult {
  readonly coords: Float64Array;
  // Per-ring point indices into `coords`. Length = total rings in batch.
  readonly ringStarts: Uint32Array;
  readonly ringEnds: Uint32Array;
  readonly numRingsPerGroup: Uint32Array;
}

// Stitch a batch of groups into a flat arcs result. Pure: no `client`,
// no I/O, no async. Scratch is cleared per group inside `stitchArcsPure`.
// The result types are postMessage-transferable; ArrayBuffers move
// zero-copy.
export function stitchBatchArcs(
  ctx: StitchBatchContext,
  groups: ReadonlyArray<ReadonlyArray<number>>,
  scratch: StitchScratch,
): StitchBatchArcsResult {
  const arcsOut = new Int32Growable(1024);
  const ringStarts: number[] = [];
  const ringEnds: number[] = [];
  const numRingsPerGroup = new Uint32Array(groups.length);
  for (let g = 0; g < groups.length; g++) {
    const ext = groups[g];
    if (ext.length === 0) continue;
    const rings = stitchArcsPure(scratch, ext, ctx.endpoints);
    if (rings.length === 0) continue;
    let count = 0;
    for (const ring of rings) {
      const start = arcsOut.len;
      for (let k = 0; k < ring.length; k++) arcsOut.push(ring[k]);
      ringStarts.push(start);
      ringEnds.push(arcsOut.len);
      count++;
    }
    numRingsPerGroup[g] = count;
  }
  return {
    arcs: arcsOut.finalize(),
    ringStarts: new Uint32Array(ringStarts),
    ringEnds: new Uint32Array(ringEnds),
    numRingsPerGroup,
  };
}

// Stitch a batch of groups into a flat coords result. Pure: requires
// ctx.arcBytes (throws if absent). Per-group rings are area-sorted
// (largest first) to match the mergeFlat contract.
export function stitchBatchCoords(
  ctx: StitchBatchContext,
  groups: ReadonlyArray<ReadonlyArray<number>>,
  scratch: StitchScratch,
): StitchBatchCoordsResult {
  if (ctx.arcBytes === undefined) {
    throw new Error("ctopo: stitchBatchCoords requires arcBytes in context");
  }
  const arcBytes = ctx.arcBytes;
  const transform = ctx.transform;
  const out = new Float64Growable(2048);
  const ringStarts: number[] = [];
  const ringEnds: number[] = [];
  const numRingsPerGroup = new Uint32Array(groups.length);
  for (let g = 0; g < groups.length; g++) {
    const ext = groups[g];
    if (ext.length === 0) continue;
    const rings = stitchArcsPure(scratch, ext, ctx.endpoints);
    interface RingInfo {
      start: number;
      end: number;
      area: number;
    }
    const groupRings: RingInfo[] = [];
    for (const ring of rings) {
      const before = out.len;
      const range = decodeRingFlat(ring, arcBytes, transform, out);
      if (range.end - range.start < 4) {
        out.len = before;
        continue;
      }
      groupRings.push({
        start: range.start,
        end: range.end,
        area: ringAreaFlat(out.buf, range.start, range.end),
      });
    }
    if (groupRings.length === 0) continue;
    if (groupRings.length > 1) {
      groupRings.sort((a, b) => b.area - a.area);
    }
    for (const ri of groupRings) {
      ringStarts.push(ri.start);
      ringEnds.push(ri.end);
    }
    numRingsPerGroup[g] = groupRings.length;
  }
  return {
    coords: out.finalize(),
    ringStarts: new Uint32Array(ringStarts),
    ringEnds: new Uint32Array(ringEnds),
    numRingsPerGroup,
  };
}

// polyRingStarts: cumulative ring count at each non-empty group
// boundary. Groups that emitted zero rings are skipped, matching the
// inline path. Length = (number of non-empty groups) + 1.
function polyRingStartsFrom(numRingsPerGroup: Uint32Array): Uint32Array {
  let nonEmptyGroups = 0;
  for (let i = 0; i < numRingsPerGroup.length; i++) {
    if (numRingsPerGroup[i] > 0) nonEmptyGroups++;
  }
  const polyRingStarts = new Uint32Array(nonEmptyGroups + 1);
  let ringCursor = 0;
  let polyCursor = 0;
  for (let g = 0; g < numRingsPerGroup.length; g++) {
    const count = numRingsPerGroup[g];
    if (count === 0) continue;
    ringCursor += count;
    polyCursor++;
    polyRingStarts[polyCursor] = ringCursor;
  }
  return polyRingStarts;
}

// Wrap a batch's arcs stitch result as a FlatMultiPolygonArcs. The
// batch's ring offsets already index its own arcs buffer, so they pass
// through unchanged; only polyRingStarts is derived.
export function assembleArcsResult(
  result: StitchBatchArcsResult,
): FlatMultiPolygonArcs {
  return {
    type: "MultiPolygon",
    arcs: result.arcs,
    ringStarts: result.ringStarts,
    ringEnds: result.ringEnds,
    polyRingStarts: polyRingStartsFrom(result.numRingsPerGroup),
  };
}

// Wrap a batch's coords stitch result as a FlatMultiPolygon.
export function assembleCoordsResult(
  result: StitchBatchCoordsResult,
): FlatMultiPolygon {
  return {
    type: "MultiPolygon",
    coords: result.coords,
    ringStarts: result.ringStarts,
    ringEnds: result.ringEnds,
    polyRingStarts: polyRingStartsFrom(result.numRingsPerGroup),
  };
}

// --- stitchArcs ---

// Fragment key type is the same as the EndpointLookup's key type
// (bigint for the section path, string for the legacy un-quantized
// path). The scratch Maps are typed as Map<unknown, Fragment> so they
// can carry either at runtime without conversion.
//
// A fragment's logical arc sequence is `prefix` reversed followed by
// `body`: appends push onto `body` (O(1)); prepends push onto `prefix`
// (also O(1), avoiding the O(n) array shift that `unshift` would
// incur). `prefix` is allocated lazily — append-only fragments (the
// common case) keep just `body`, so the allocation profile matches the
// pre-split single-array form. The logical order is materialized into a
// plain number[] once, at drain (`materializeFragment`).
interface Fragment {
  body: number[];
  prefix: number[] | null;
  start: unknown;
  end: unknown;
}

// Append `src`'s logical sequence (prefix reversed, then body) onto
// `destBody`.
function appendFragmentLogical(destBody: number[], src: Fragment): void {
  const { prefix, body } = src;
  if (prefix !== null) {
    for (let k = prefix.length - 1; k >= 0; k--) destBody.push(prefix[k]);
  }
  for (let k = 0; k < body.length; k++) destBody.push(body[k]);
}

// Flatten a fragment to its logical arc order. Append-only fragments
// (no prefix) return `body` directly with no copy.
function materializeFragment(f: Fragment): number[] {
  if (f.prefix === null) return f.body;
  const out: number[] = [];
  appendFragmentLogical(out, f);
  return out;
}

// Stitches a flat list of signed boundary arc ids into closed rings —
// the topojson-client/src/stitch.js algorithm, taking its endpoint
// lookup from per-arc fetched bytes rather than a single in-memory
// ArrayBuffer so individual arcs can be Range-fetched on demand.
//
// Pure form: scratch is passed in explicitly. The coordinator pulls
// scratch from its per-client WeakMap; compute workers maintain their
// own per-worker instance. Scratch is cleared on entry, so the caller
// only has to allocate it once and reuse.
export function stitchArcsPure<K>(
  scratch: StitchScratch,
  arcs: ReadonlyArray<number>,
  endpoints: EndpointLookup<K>,
): number[][] {
  scratch.fragmentByStart.clear();
  scratch.fragmentByEnd.clear();
  scratch.stitched.clear();
  const { fragmentByStart, fragmentByEnd, stitched } = scratch;
  const fragments: number[][] = [];

  for (const i of arcs) {
    const startKey = endpoints.start(i);
    const endKey = endpoints.end(i);

    let f = fragmentByEnd.get(startKey);
    if (f !== undefined) {
      fragmentByEnd.delete(f.end);
      f.body.push(i);
      f.end = endKey;
      const g = fragmentByStart.get(endKey);
      if (g !== undefined) {
        fragmentByStart.delete(g.start);
        // Merge g into f in place. f.concat(g) was the dominant
        // allocator in the merge phase — at national scale it pushed
        // ~8 MB / 46% of merge-only sampled allocations through GC.
        if (g !== f) {
          appendFragmentLogical(f.body, g);
        }
        f.end = g.end;
        fragmentByStart.set(f.start, f);
        fragmentByEnd.set(f.end, f);
      } else {
        fragmentByStart.set(f.start, f);
        fragmentByEnd.set(f.end, f);
      }
    } else {
      f = fragmentByStart.get(endKey);
      if (f !== undefined) {
        fragmentByStart.delete(f.start);
        // Prepend i to f's front. O(1) push onto the lazily-allocated
        // `prefix` instead of an O(n) Array.unshift shift.
        (f.prefix ??= []).push(i);
        f.start = startKey;
        const g = fragmentByEnd.get(startKey);
        if (g !== undefined) {
          fragmentByEnd.delete(g.end);
          // Merge — final order is [...g, ...f]. Append f to g in
          // place using g as the surviving fragment (its handle is
          // the one the outer for-loop iteration drops). f is unused
          // after this iteration of the outer for-loop.
          if (g === f) {
            fragmentByStart.set(f.start, f);
            fragmentByEnd.set(f.end, f);
          } else {
            appendFragmentLogical(g.body, f);
            g.end = f.end;
            fragmentByStart.set(g.start, g);
            fragmentByEnd.set(g.end, g);
          }
        } else {
          fragmentByStart.set(f.start, f);
          fragmentByEnd.set(f.end, f);
        }
      } else {
        const fresh: Fragment = {
          body: [i],
          prefix: null,
          start: startKey,
          end: endKey,
        };
        fragmentByStart.set(startKey, fresh);
        fragmentByEnd.set(endKey, fresh);
      }
    }
  }

  // Drain — closed rings end up in either map; collect them and mark
  // each constituent arc as stitched.
  function drain(
    byEnd: Map<unknown, Fragment>,
    byStart: Map<unknown, Fragment>,
  ): void {
    for (const f of byEnd.values()) {
      byStart.delete(f.start);
      const ring = materializeFragment(f);
      for (const i of ring) stitched.add(i < 0 ? ~i : i);
      fragments.push(ring);
    }
    byEnd.clear();
  }
  drain(fragmentByEnd, fragmentByStart);
  drain(fragmentByStart, fragmentByEnd);

  // Anything not stitched is a degenerate single-arc ring — preserve
  // it so the caller can decide what to do (matches topojson-client).
  for (const i of arcs) {
    if (!stitched.has(i < 0 ? ~i : i)) fragments.push([i]);
  }
  return fragments;
}
