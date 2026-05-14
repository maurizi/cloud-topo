// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Primitives over a `.ctopo` container — mirroring the topojson-client
 * API (`merge`, `mergeArcs`, `neighbors`) but range-aware: each call
 * fetches only the arc coord slices it needs.
 *
 * The unit-level semantics (which units to merge, how to group results,
 * how to interpret per-feature properties) belong to the caller; this
 * file knows about layers, units, and arcs.
 */

import { type MultiPolygon } from "geojson";

import { type CtopoCore } from "./client";
import { readVarintZigzagInto, type VarintCursor } from "./format";
import { type LayerGeometry, type LayerSelection } from "./types";

// Topojson arcs are global across layers, so cancellation works
// uniformly even for multi-layer inputs.

// --- Int32Array pool (per-client, encapsulated here) ---

// Reuses the per-merge Int32Arrays (signedArcIds, polyOffsets,
// polyIndices, rowByArc, offsets) across mergeArcs/merge calls on the
// same client. Without pooling each merge allocates ~1 MiB of
// external memory; on the national bench (~435 merges) that's
// hundreds of MiB of external-buffer churn that drives V8
// Mark-Compact pauses. WeakMap-keyed by client so:
//   - pools don't leak between clients running in the same process
//   - pool lifetime is tied to client lifetime (no manual cleanup)
//   - no API surface added to CtopoCore
// Concurrent merges interleave on the event loop but only one runs
// synchronously at a time; each acquires buffers during its sync
// block and releases before the next await — peak in-flight is ~one
// merge's worth.
class Int32ArrayPool {
  private free: Int32Array[] = [];
  private static readonly MAX_FREE = 32;

  // Acquire an Int32Array of exactly `size` elements. Returns a
  // subarray view of a pooled larger buffer when available, or a
  // fresh allocation otherwise. Contents are NOT zeroed — caller
  // must initialize any slot it reads (current callers write every
  // slot before reading, except polyOffsets[0] / offsets[0] which
  // are set explicitly).
  acquire(size: number): Int32Array {
    let bestIdx = -1;
    let bestLen = Infinity;
    for (let i = 0; i < this.free.length; i++) {
      const len = this.free[i].length;
      if (len >= size && len < bestLen) {
        bestIdx = i;
        bestLen = len;
      }
    }
    if (bestIdx >= 0) {
      const buf = this.free[bestIdx];
      this.free[bestIdx] = this.free[this.free.length - 1];
      this.free.pop();
      return buf.subarray(0, size);
    }
    return new Int32Array(size);
  }

  release(buf: Int32Array): void {
    if (this.free.length >= Int32ArrayPool.MAX_FREE) return;
    // Recover a full-length view of the underlying ArrayBuffer so a
    // subsequent acquire(larger) can use the entire capacity.
    const full =
      buf.byteOffset === 0 && buf.length * 4 === buf.buffer.byteLength
        ? buf
        : new Int32Array(buf.buffer, 0, buf.buffer.byteLength / 4);
    this.free.push(full);
  }
}

const poolByClient = new WeakMap<CtopoCore, Int32ArrayPool>();
function getPool(client: CtopoCore): Int32ArrayPool {
  let pool = poolByClient.get(client);
  if (pool === undefined) {
    pool = new Int32ArrayPool();
    poolByClient.set(client, pool);
  }
  return pool;
}

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
interface StitchScratch {
  fragmentByStart: Map<unknown, Fragment>;
  fragmentByEnd: Map<unknown, Fragment>;
  stitched: Set<number>;
}
// Direct-indexed arc-id → CSR-row table for buildPolygonsByArc. One
// Int32Array per client, sized to client.meta.numArcs, kept alive
// for the client's lifetime. The packing layout (8 bits generation,
// 24 bits row) plus the per-merge generation bump are explained at
// the use site in buildPolygonsByArc; the WeakMap here keeps the
// buffer scoped to the client without growing the public API.
interface ArcGenState {
  index: Int32Array;
  gen: number;
}
const arcGenStateByClient = new WeakMap<CtopoCore, ArcGenState>();
function getArcGenIndex(client: CtopoCore, numArcs: number): Int32Array {
  let s = arcGenStateByClient.get(client);
  if (s === undefined) {
    s = { index: new Int32Array(numArcs), gen: 0 };
    arcGenStateByClient.set(client, s);
  }
  return s.index;
}
function bumpArcGen(client: CtopoCore): number {
  // Caller is responsible for the per-client WeakMap entry existing
  // (we created it in getArcGenIndex earlier in the same call). Wrap
  // at 0xff so the high byte of the packed slot can never collide
  // with a stale generation; caller handles the rare wrap-and-reset.
  const s = arcGenStateByClient.get(client) as ArcGenState;
  s.gen = (s.gen + 1) & 0xff;
  return s.gen;
}

const stitchScratchByClient = new WeakMap<CtopoCore, StitchScratch>();
function getStitchScratch(client: CtopoCore): StitchScratch {
  let s = stitchScratchByClient.get(client);
  if (s === undefined) {
    s = {
      fragmentByStart: new Map(),
      fragmentByEnd: new Map(),
      stitched: new Set(),
    };
    stitchScratchByClient.set(client, s);
  }
  return s;
}

// --- merge / mergeArcs ---

export interface MultiPolygonArcs {
  readonly type: "MultiPolygon";
  // Three-level array of arc id rings (matches topojson-client's
  // mergeArcs return shape). Outer level = polygons; middle = rings of
  // each polygon; inner = signed arc ids of each ring.
  readonly arcs: number[][][];
}

// Shared preamble for merge / mergeArcs: build polygons, group by
// connectivity, collect each group's exterior arcs, fetch what each
// caller needs, build an endpoint lookup.
//
// `needCoords` lets mergeArcs skip the arc_coords fetch when the file
// ships a dedicated arc_endpoints section — stitching only needs
// endpoints, not full per-point coords. When the section is absent,
// or `needCoords` is true (merge — has to decode rings), the call
// fetches arc bytes too. When both are needed, the two fetches run
// in parallel so wall-clock is bounded by the slower one.
async function prepareMergeGroups(
  client: CtopoCore,
  inputs: ReadonlyArray<LayerSelection>,
  needCoords: boolean,
  signal: AbortSignal | undefined,
): Promise<{
  groupExteriorArcs: number[][];
  arcBytes: Map<number, Uint8Array>;
  endpoints: EndpointLookup<unknown>;
}> {
  const polygons = await buildInputPolygons(client, inputs, signal);
  if (polygons.numPolygons === 0) {
    // Empty path doesn't touch the pool — the empty-input FlatPolygons
    // is a fresh tiny allocation, not pooled.
    return emptyPrepareResult(client);
  }

  const polygonsByArc = buildPolygonsByArc(client, polygons);
  const groups = groupPolygonsByConnectivity(polygons, polygonsByArc);

  // Collect every group's exterior arcs in one go so we can fetch
  // arc bytes in a single batched call rather than one fetch per
  // group.
  const allExteriorArcs: number[] = [];
  const groupExteriorArcs: number[][] = groups.map((group) => {
    const ext = exteriorArcsForGroup(group, polygons, polygonsByArc);
    for (const a of ext) allExteriorArcs.push(a);
    return ext;
  });

  // The per-merge Int32Arrays inside FlatPolygons and PolygonsByArc
  // aren't referenced past this point — groupExteriorArcs is a plain
  // number[][] that survives. Release the pooled buffers now, BEFORE
  // the async fetchArcs await, so the next merge's sync block (which
  // the event loop may interleave during the network wait) can reuse
  // them. Without this release the pool would balloon to peak-
  // concurrency size rather than ~one merge's worth.
  const pool = getPool(client);
  pool.release(polygons.signedArcIds);
  pool.release(polygons.polyOffsets);
  pool.release(polygonsByArc.rowByArc);
  pool.release(polygonsByArc.offsets);
  pool.release(polygonsByArc.polyIndices);

  if (allExteriorArcs.length === 0) {
    return emptyPrepareResult(client);
  }

  const ids = absArcIds(allExteriorArcs);
  const isQuantized = client.transform !== null;

  // Path selection:
  //
  // mergeArcs (needCoords=false) + arc_endpoints present + quantized
  //   → ONLY fetch endpoints. This is the killer use case for the
  //   section: zero arc_coords download. Bench (CA-cd, broadband):
  //   −22% wall, −31% bytes downloaded vs the legacy path. Coalesce
  //   gap=0 doesn't help further — decompress cost (not wire bytes)
  //   is what costs us.
  //
  // merge (needCoords=true) → fetch arc_coords for ring decode and
  //   recover endpoints from those bytes inline. The arc_endpoints
  //   section would be net-negative here — it adds a parallel fetch
  //   for partitions we'd otherwise skip, plus per-partition decode
  //   overhead, for a CPU win (skip varint walk for end-points) that
  //   doesn't break even. Quantized merges still get the numeric
  //   stitchArcs-key win via makeNumericEndpointLookupFromBytes.
  //
  // No-section / un-quantized → bytes-based lookup with string keys
  //   (un-quantized float64 doesn't pack to a Number safely).
  if (!needCoords && client.hasArcEndpointsSection() && isQuantized) {
    const eps = await client.fetchArcEndpoints(ids, signal);
    return {
      groupExteriorArcs,
      arcBytes: new Map<number, Uint8Array>(),
      endpoints: makeNumericEndpointLookup(eps, client),
    };
  }

  const arcBytes = await client.fetchArcs(ids, signal);
  const endpoints: EndpointLookup<unknown> = isQuantized
    ? makeNumericEndpointLookupFromBytes(arcBytes, client)
    : makeEndpointLookup(arcBytes, client);
  return { groupExteriorArcs, arcBytes, endpoints };
}

function emptyPrepareResult(client: CtopoCore): {
  groupExteriorArcs: number[][];
  arcBytes: Map<number, Uint8Array>;
  endpoints: EndpointLookup<unknown>;
} {
  const arcBytes = new Map<number, Uint8Array>();
  return {
    groupExteriorArcs: [],
    arcBytes,
    endpoints: makeEndpointLookup(arcBytes, client),
  };
}

// --- Flat (transferable) result shapes ---
//
// The merge primitives' "real" outputs are typed-array CSR structures
// (FlatMultiPolygon / FlatMultiPolygonArcs / FlatNeighbors) — these are
// what crosses the worker boundary, transferred zero-copy. The
// GeoJSON-shaped exports below (`merge`, `mergeArcs`, `neighbors`) are
// thin wrappers that rebuild nested-array forms from the flat outputs;
// the worker side never calls them, but in-process consumers and the
// existing test suite still do.
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

export interface FlatNeighbors {
  // Per-geometry adjacency in CSR form. Geometry g's neighbors are
  // values[offsets[g] .. offsets[g+1]). offsets length = numGeoms + 1.
  readonly offsets: Uint32Array;
  readonly values: Uint32Array;
}

// Growable Float64 buffer — the per-merge scratch into which
// decodeRingFlat appends ring points. Doubling capacity keeps the
// amortized cost linear; `finalize()` returns a tightly-sized slice
// that owns its own ArrayBuffer (so the worker can postMessage with
// transfer semantics without orphaning pooled storage).
class Float64Growable {
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

// `mergeArcsFlat` — flat-typed-array form of mergeArcs. Returned arcs
// follow the same stitch-order semantics (no area-based reordering;
// see mergeArcs for the contract).
export async function mergeArcsFlat(
  client: CtopoCore,
  inputs: ReadonlyArray<LayerSelection>,
  signal?: AbortSignal,
): Promise<FlatMultiPolygonArcs> {
  const { groupExteriorArcs, endpoints } = await prepareMergeGroups(
    client,
    inputs,
    false,
    signal,
  );
  const arcsOut: number[] = [];
  const ringStarts: number[] = [];
  const ringEnds: number[] = [];
  const polyRingStarts: number[] = [0];
  for (const ext of groupExteriorArcs) {
    if (ext.length === 0) continue;
    const rings = stitchArcs(client, ext, endpoints);
    if (rings.length === 0) continue;
    for (const ring of rings) {
      const start = arcsOut.length;
      for (let k = 0; k < ring.length; k++) arcsOut.push(ring[k]);
      ringStarts.push(start);
      ringEnds.push(arcsOut.length);
    }
    polyRingStarts.push(ringStarts.length);
  }
  return {
    type: "MultiPolygon",
    arcs: new Int32Array(arcsOut),
    ringStarts: new Uint32Array(ringStarts),
    ringEnds: new Uint32Array(ringEnds),
    polyRingStarts: new Uint32Array(polyRingStarts),
  };
}

// `mergeFlat` — flat-typed-array form of merge. Largest-area ring per
// connected component lands first inside its polygon's [polyRingStarts]
// slice; the rest follow as holes (same semantics as merge).
export async function mergeFlat(
  client: CtopoCore,
  inputs: ReadonlyArray<LayerSelection>,
  signal?: AbortSignal,
): Promise<FlatMultiPolygon> {
  const { groupExteriorArcs, arcBytes, endpoints } = await prepareMergeGroups(
    client,
    inputs,
    true,
    signal,
  );
  // 2048 floats = 1024 points. Grows as needed.
  const out = new Float64Growable(2048);
  const ringStarts: number[] = [];
  const ringEnds: number[] = [];
  const polyRingStarts: number[] = [0];

  for (const ext of groupExteriorArcs) {
    if (ext.length === 0) continue;
    const rings = stitchArcs(client, ext, endpoints);
    // Per-group ring info — points already live in `out`, so sorting
    // by area only reorders the (start, end) entries pushed below; no
    // coord copy needed.
    interface RingInfo {
      start: number;
      end: number;
      area: number;
    }
    const groupRings: RingInfo[] = [];
    for (const ring of rings) {
      const before = out.len;
      const range = decodeRingFlat(ring, arcBytes, client, out);
      // Same <4-points filter as the legacy path. Rewind so the
      // discarded ring's bytes don't bloat coords.
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
    for (let i = 0; i < groupRings.length; i++) {
      ringStarts.push(groupRings[i].start);
      ringEnds.push(groupRings[i].end);
    }
    polyRingStarts.push(ringStarts.length);
  }

  return {
    type: "MultiPolygon",
    coords: out.finalize(),
    ringStarts: new Uint32Array(ringStarts),
    ringEnds: new Uint32Array(ringEnds),
    polyRingStarts: new Uint32Array(polyRingStarts),
  };
}

// --- GeoJSON-shaped wrappers ---
//
// Build the nested-array forms from the flat outputs. The worker never
// calls these; in-process callers (and existing tests) get the same
// observable shape they did before the flat split.

// `mergeArcs` — union of inputs as topology-style geometry. Cheap: only
// arc endpoints are fetched (for stitching), not full coords.
export async function mergeArcs(
  client: CtopoCore,
  inputs: ReadonlyArray<LayerSelection>,
  signal?: AbortSignal,
): Promise<MultiPolygonArcs> {
  const flat = await mergeArcsFlat(client, inputs, signal);
  const out: number[][][] = [];
  const numPolys = flat.polyRingStarts.length - 1;
  for (let p = 0; p < numPolys; p++) {
    const polygon: number[][] = [];
    const rStart = flat.polyRingStarts[p];
    const rEnd = flat.polyRingStarts[p + 1];
    for (let r = rStart; r < rEnd; r++) {
      const aStart = flat.ringStarts[r];
      const aEnd = flat.ringEnds[r];
      const ring = new Array<number>(aEnd - aStart);
      for (let i = 0; i < ring.length; i++) ring[i] = flat.arcs[aStart + i];
      polygon.push(ring);
    }
    out.push(polygon);
  }
  return { type: "MultiPolygon", arcs: out };
}

// `merge` — union of inputs as decoded GeoJSON MultiPolygon. Fetches
// boundary arc coord bytes, stitches into rings, decodes, and assembles
// one output polygon per connected component of input polygons (matches
// topojson-client's `merge` semantics). Within each component the
// largest-area stitched ring becomes the exterior; the rest are holes.
export async function merge(
  client: CtopoCore,
  inputs: ReadonlyArray<LayerSelection>,
  signal?: AbortSignal,
): Promise<MultiPolygon> {
  const flat = await mergeFlat(client, inputs, signal);
  const coordinates: number[][][][] = [];
  const numPolys = flat.polyRingStarts.length - 1;
  for (let p = 0; p < numPolys; p++) {
    const polygon: number[][][] = [];
    const rStart = flat.polyRingStarts[p];
    const rEnd = flat.polyRingStarts[p + 1];
    for (let r = rStart; r < rEnd; r++) {
      const pStart = flat.ringStarts[r];
      const pEnd = flat.ringEnds[r];
      const ring = new Array<number[]>(pEnd - pStart);
      for (let i = 0; i < ring.length; i++) {
        const off = (pStart + i) * 2;
        ring[i] = [flat.coords[off], flat.coords[off + 1]];
      }
      polygon.push(ring);
    }
    coordinates.push(polygon);
  }
  return { type: "MultiPolygon", coordinates };
}

// --- neighbors ---

// `neighborsFlat` — CSR adjacency for the layer (one Uint32Array of
// neighbor ids + one Uint32Array of per-geometry offsets). Pure
// in-memory once the layer's CSR triple is loaded — no arc coord
// fetch needed.
export async function neighborsFlat(
  client: CtopoCore,
  layer: string,
  signal?: AbortSignal,
): Promise<FlatNeighbors> {
  const csr = await client.layerGeometry(layer, signal);
  const numGeoms = csr.polyOffsets.length - 1;
  const numArcs = client.meta.numArcs;

  // Build arc → [forward geom, reverse geom]. -1 = no geom on that
  // side (exterior arc).
  const fwd = new Int32Array(numArcs).fill(-1);
  const rev = new Int32Array(numArcs).fill(-1);
  for (let g = 0; g < numGeoms; g++) {
    const ringStart = csr.polyOffsets[g];
    const ringEnd = csr.polyOffsets[g + 1];
    for (let r = ringStart; r < ringEnd; r++) {
      const arcStart = csr.ringOffsets[r];
      const arcEnd = csr.ringOffsets[r + 1];
      for (let a = arcStart; a < arcEnd; a++) {
        const signed = csr.arcRefs[a];
        const id = signed >= 0 ? signed : ~signed;
        if (signed >= 0) {
          if (fwd[id] === -1) fwd[id] = g;
        } else {
          if (rev[id] === -1) rev[id] = g;
        }
      }
    }
  }

  // For each geom, walk its arcs and collect the OTHER geom on each
  // shared arc; flatten into the CSR result.
  const seen = new Set<number>();
  const offsets = new Uint32Array(numGeoms + 1);
  const valuesBuf: number[] = [];
  for (let g = 0; g < numGeoms; g++) {
    seen.clear();
    const ringStart = csr.polyOffsets[g];
    const ringEnd = csr.polyOffsets[g + 1];
    for (let r = ringStart; r < ringEnd; r++) {
      const arcStart = csr.ringOffsets[r];
      const arcEnd = csr.ringOffsets[r + 1];
      for (let a = arcStart; a < arcEnd; a++) {
        const signed = csr.arcRefs[a];
        const id = signed >= 0 ? signed : ~signed;
        const other = fwd[id] === g ? rev[id] : fwd[id];
        if (other >= 0 && other !== g) seen.add(other);
      }
    }
    const list = Array.from(seen);
    list.sort((a, b) => a - b);
    for (let i = 0; i < list.length; i++) valuesBuf.push(list[i]);
    offsets[g + 1] = valuesBuf.length;
  }
  return { offsets, values: new Uint32Array(valuesBuf) };
}

// `neighbors` — GeoJSON-shaped wrapper around `neighborsFlat`. Returns
// per-geometry sorted neighbor lists.
export async function neighbors(
  client: CtopoCore,
  layer: string,
  signal?: AbortSignal,
): Promise<readonly (readonly number[])[]> {
  const flat = await neighborsFlat(client, layer, signal);
  const numGeoms = flat.offsets.length - 1;
  const out = new Array<number[]>(numGeoms);
  for (let g = 0; g < numGeoms; g++) {
    const start = flat.offsets[g];
    const end = flat.offsets[g + 1];
    const list = new Array<number>(end - start);
    for (let i = 0; i < list.length; i++) list[i] = flat.values[start + i];
    out[g] = list;
  }
  return out;
}

// --- bbox / transform helpers (mirror topojson-client signatures) ---

export function bbox(
  client: CtopoCore,
): readonly [number, number, number, number] {
  return client.meta.bbox;
}

type TransformDef = {
  readonly scale: readonly [number, number];
  readonly translate: readonly [number, number];
} | null;

export function transform(
  t: TransformDef,
): (point: readonly [number, number]) => [number, number] {
  if (t === null) return (p) => [p[0], p[1]];
  return (p) => [
    p[0] * t.scale[0] + t.translate[0],
    p[1] * t.scale[1] + t.translate[1],
  ];
}

export function untransform(
  t: TransformDef,
): (point: readonly [number, number]) => [number, number] {
  if (t === null) return (p) => [p[0], p[1]];
  return (p) => [
    (p[0] - t.translate[0]) / t.scale[0],
    (p[1] - t.translate[1]) / t.scale[1],
  ];
}

// --- Boundary-arc collection (interior cancellation) ---

// Walk every requested unit's signed arc refs into a single bag.
// Forward usage of arc i increments fwdCount[i]; reverse usage
// (negative ref ~i) increments revCount[i]. An arc that ends up with
// fwdCount === revCount is interior (cancels). Otherwise it's a
// boundary arc, signed by its dominant direction in the input.
// One TopoJSON polygon entry as a list of arc rings. Distinct entries
// of the same MultiPolygon (e.g. island-in-lake) are kept separate so
// connectivity grouping can recover the original polygon structure.
// Stored as a flat (poly-major) Int32Array of signed arc ids — ring
// boundaries are dropped here because none of the merge-core consumers
// (buildPolygonsByArc / groupPolygonsByConnectivity / exteriorArcsForGroup)
// care which ring of a polygon a given arc came from, only which
// polygon. The flat layout is materially cheaper to iterate than the
// triple-nested polygon[].rings[].arc[] structure it replaces — fewer
// property loads per arc and (more importantly) no per-ring `number[]`
// allocation in `makeInputPolygon`, which was a steady source of
// merge-call survivor pressure during Scavenges.
interface FlatPolygons {
  // Number of polygons.
  readonly numPolygons: number;
  // Flat signed arc ids across all polygons, in polygon-major order.
  readonly signedArcIds: Int32Array;
  // CSR row pointers — polygon p owns signedArcIds[polyOffsets[p]..polyOffsets[p+1]).
  // Length == numPolygons + 1.
  readonly polyOffsets: Int32Array;
}

async function buildInputPolygons(
  client: CtopoCore,
  inputs: ReadonlyArray<LayerSelection>,
  signal: AbortSignal | undefined,
): Promise<FlatPolygons> {
  if (inputs.length === 0) {
    return {
      numPolygons: 0,
      signedArcIds: new Int32Array(0),
      polyOffsets: new Int32Array(1),
    };
  }
  const layerCsr = await Promise.all(
    inputs.map((input) => client.layerGeometry(input.layer, signal)),
  );
  // Expand each input layer separately. For the single-input case
  // (the common one — mergeArcs of a single district selection) we
  // skip the concat copy entirely; the per-layer FlatPolygons is the
  // result.
  if (inputs.length === 1) {
    return expandLayerPolygons(
      client,
      inputs[0].layer,
      layerCsr[0],
      inputs[0].indices,
    );
  }
  const perLayer = new Array<FlatPolygons>(inputs.length);
  for (let i = 0; i < inputs.length; i++) {
    perLayer[i] = expandLayerPolygons(
      client,
      inputs[i].layer,
      layerCsr[i],
      inputs[i].indices,
    );
  }
  let totalArcs = 0;
  let totalPolys = 0;
  for (const fp of perLayer) {
    totalArcs += fp.signedArcIds.length;
    totalPolys += fp.numPolygons;
  }
  const pool = getPool(client);
  const signedArcIds = pool.acquire(totalArcs);
  const polyOffsets = pool.acquire(totalPolys + 1);
  polyOffsets[0] = 0;
  let arcCursor = 0;
  let polyCursor = 0;
  for (const fp of perLayer) {
    signedArcIds.set(fp.signedArcIds, arcCursor);
    for (let p = 0; p < fp.numPolygons; p++) {
      polyOffsets[polyCursor + p + 1] = fp.polyOffsets[p + 1] + arcCursor;
    }
    arcCursor += fp.signedArcIds.length;
    polyCursor += fp.numPolygons;
  }
  // Per-layer arrays are no longer referenced — return them to the
  // pool so the concatenated result is the only live allocation.
  for (const fp of perLayer) {
    pool.release(fp.signedArcIds);
    pool.release(fp.polyOffsets);
  }
  return { numPolygons: totalPolys, signedArcIds, polyOffsets };
}

function expandLayerPolygons(
  client: CtopoCore,
  layer: string,
  csr: LayerGeometry,
  geomIndices: Iterable<number>,
): FlatPolygons {
  // Index the sparse multi_poly_breaks table by geom for O(1) lookup
  // per geom we visit. Empty in the typical single-Polygon-only case.
  // We touch the whole table once; attribute its full byteLength to
  // the useful-bytes tally for this layer.
  client.tallyUseful(
    `${layer}/multi_poly_breaks`,
    csr.multiPolyBreaks.byteLength,
  );
  const breaksByGeom = new Map<number, number[]>();
  for (let i = 0; i < csr.multiPolyBreaks.length; i += 2) {
    const g = csr.multiPolyBreaks[i];
    const r = csr.multiPolyBreaks[i + 1];
    let list = breaksByGeom.get(g);
    if (list === undefined) {
      list = [];
      breaksByGeom.set(g, list);
    }
    list.push(r);
  }

  // Pass 1: count. Walks selected geoms to tally:
  //   - poly count after multi_poly_breaks splits
  //   - ring count (for useful-bytes tally)
  //   - total arcs (for exact-sized signedArcIds allocation)
  // The geomIndices iterable is materialized into a number[] here
  // because we need to walk it twice (pass 1, pass 2) and Iterable
  // can be single-use.
  const geomList: number[] = [];
  for (const g of geomIndices) geomList.push(g);
  let polyOffsetsCount = 0;
  let ringOffsetsCount = 0;
  let totalArcs = 0;
  let numPolygons = 0;
  for (let i = 0; i < geomList.length; i++) {
    const g = geomList[i];
    const ringStart = csr.polyOffsets[g];
    const ringEnd = csr.polyOffsets[g + 1];
    polyOffsetsCount++;
    if (ringStart === ringEnd) continue;
    ringOffsetsCount += ringEnd - ringStart;
    totalArcs += csr.ringOffsets[ringEnd] - csr.ringOffsets[ringStart];
    const breaks = breaksByGeom.get(g);
    numPolygons += breaks === undefined ? 1 : breaks.length + 1;
  }

  const pool = getPool(client);
  const signedArcIds = pool.acquire(totalArcs);
  const polyOffsets = pool.acquire(numPolygons + 1);
  polyOffsets[0] = 0;

  // Pass 2: fill. For each emitted polygon, copy its slice of
  // csr.arcRefs into signedArcIds using typed-array set (memcpy-fast).
  // multi_poly_breaks splits a geom's ring range into multiple
  // polygons that share contiguous arc ranges.
  let polyIdx = 0;
  let arcCursor = 0;
  for (let i = 0; i < geomList.length; i++) {
    const g = geomList[i];
    const ringStart = csr.polyOffsets[g];
    const ringEnd = csr.polyOffsets[g + 1];
    if (ringStart === ringEnd) continue;
    const breaks = breaksByGeom.get(g);
    if (breaks === undefined) {
      const arcStart = csr.ringOffsets[ringStart];
      const arcEnd = csr.ringOffsets[ringEnd];
      signedArcIds.set(csr.arcRefs.subarray(arcStart, arcEnd), arcCursor);
      arcCursor += arcEnd - arcStart;
      polyOffsets[polyIdx + 1] = arcCursor;
      polyIdx++;
      continue;
    }
    let prev = ringStart;
    for (let bi = 0; bi < breaks.length; bi++) {
      const b = breaks[bi];
      const arcStart = csr.ringOffsets[prev];
      const arcEnd = csr.ringOffsets[b];
      signedArcIds.set(csr.arcRefs.subarray(arcStart, arcEnd), arcCursor);
      arcCursor += arcEnd - arcStart;
      polyOffsets[polyIdx + 1] = arcCursor;
      polyIdx++;
      prev = b;
    }
    const arcStart = csr.ringOffsets[prev];
    const arcEnd = csr.ringOffsets[ringEnd];
    signedArcIds.set(csr.arcRefs.subarray(arcStart, arcEnd), arcCursor);
    arcCursor += arcEnd - arcStart;
    polyOffsets[polyIdx + 1] = arcCursor;
    polyIdx++;
  }

  client.tallyUseful(`${layer}/poly_offsets`, 8 * polyOffsetsCount);
  client.tallyUseful(`${layer}/ring_offsets`, 8 * ringOffsetsCount);
  client.tallyUseful(`${layer}/arc_refs`, 4 * totalArcs);

  return { numPolygons, signedArcIds, polyOffsets };
}

// Map unsigned arc id → indices of the InputPolygons that reference it.
// An arc with a single membership is on the exterior of its group; an
// arc with ≥2 memberships is interior (cancels during merge).
// Compressed-sparse-row layout for "which input polygons reference
// each arc id". Replaces a `Map<number, number[]>` with one Map for
// the id-to-row lookup plus two flat Int32Arrays — the small per-arc
// `number[]` arrays in the old version were a big chunk of merge-phase
// Scavenge survivor pressure (one short-lived heap object per unique
// arc, multiplied across hundreds of districts). With CSR, the only
// merge-call-lifetime allocations are the index Map plus two typed
// arrays whose storage is tracked as external memory rather than
// young-gen heap.
interface PolygonsByArc {
  // Row pointers. Row i covers polyIndices[offsets[i]..offsets[i+1]).
  readonly offsets: Int32Array;
  // Flat polygon-index storage. Each entry is an index into the
  // `polygons` array passed to buildPolygonsByArc — i.e. "this arc
  // is a member of input polygon P". Length == sum of per-arc
  // memberships across all input polygons.
  readonly polyIndices: Int32Array;
  // Denormalized: for each arc occurrence at FlatPolygons.signedArcIds[k],
  // rowByArc[k] is the CSR row in offsets/polyIndices. Lets the
  // consumers (groupPolygonsByConnectivity / exteriorArcsForGroup)
  // skip a Map<arcId, row>.get() per arc in their inner loops —
  // measurable on the national merge.
  readonly rowByArc: Int32Array;
}

function buildPolygonsByArc(
  client: CtopoCore,
  polygons: FlatPolygons,
): PolygonsByArc {
  const { numPolygons, signedArcIds, polyOffsets } = polygons;
  const totalArcs = signedArcIds.length;
  const pool = getPool(client);
  // Pass 1: count memberships per arc id while assigning row indices.
  //
  // Replaced `new Map<number, number>()` with a direct-indexed
  // Int32Array. V8 Map<number, number>'s .get/.set sit on the
  // general hash path with HeapNumber boxing per access; merge bench
  // (national CD) showed buildPolygonsByArc at 3.9s self / 5.7% of
  // profile dominated by those calls.
  //
  // Layout: one Int32Array of length `meta.numArcs`, held for the
  // client's lifetime. Each slot packs a generation marker in the
  // high 8 bits and the per-merge row index in the low 24 bits. A
  // slot is "present in this merge" iff its high byte matches the
  // current generation. No per-merge zero-fill: bumping the
  // generation counter logically invalidates every slot in O(1).
  // When the counter would overflow (every 256 merges), we reset the
  // array — a single .fill(0) amortized to ~free.
  //
  // Memory cost: numArcs × 4 bytes resident, kept alive on the
  // client. ~80 MiB at national (20M arcs). Per-merge cost is one
  // typed-array read per arc reference; no hashing, no probing, no
  // allocation.
  //
  // Row capacity per merge: 2^24 = 16M. National merges peak in the
  // 100K-row range; well under the cap. We throw if a merge ever
  // approaches it so the regression is loud rather than silent.
  const ARC_GEN_SHIFT = 24;
  const ARC_ROW_MASK = (1 << ARC_GEN_SHIFT) - 1;
  const arcGenIndex = getArcGenIndex(client, client.meta.numArcs);
  let curGen = bumpArcGen(client);
  if (curGen === 0) {
    // Wrapped past 0xff — reset stale generations so the next lookup
    // can't false-match.
    arcGenIndex.fill(0);
    curGen = bumpArcGen(client);
  }
  const curGenShifted = curGen << ARC_GEN_SHIFT;

  // counts stays as number[]: V8 holds it as a packed SMI elements
  // array and Scavenge handles its growth cheaply; pooling it as
  // Int32Array measured as a net wash here (the extra pool entry's
  // external-memory footprint cost as much Mark-Compact as the alloc
  // churn it saved).
  const counts: number[] = [];
  const rowByArc = pool.acquire(totalArcs);
  let numUnique = 0;
  for (let k = 0; k < totalArcs; k++) {
    const signed = signedArcIds[k];
    const id = signed < 0 ? ~signed : signed;
    const slot = arcGenIndex[id];
    let idx: number;
    if ((slot & ~ARC_ROW_MASK) === curGenShifted) {
      idx = slot & ARC_ROW_MASK;
      counts[idx]++;
    } else {
      idx = numUnique++;
      arcGenIndex[id] = curGenShifted | idx;
      counts.push(1);
    }
    rowByArc[k] = idx;
  }
  if (numUnique > ARC_ROW_MASK) {
    throw new Error(
      `ctopo: buildPolygonsByArc unique-arc count ${numUnique} exceeds packed-row cap ${ARC_ROW_MASK}; widen ARC_GEN_SHIFT`,
    );
  }

  // Build CSR offsets from counts via prefix sum. Pool buffers may
  // carry stale data, so set offsets[0] explicitly.
  const numArcs = numUnique;
  const offsets = pool.acquire(numArcs + 1);
  offsets[0] = 0;
  for (let i = 0; i < numArcs; i++) offsets[i + 1] = offsets[i] + counts[i];
  const polyIndices = pool.acquire(offsets[numArcs]);
  // Pass 2: fill polyIndices using cached rowByArc. Decrement `counts`
  // as a cursor so the final layout is each arc's row at
  // [offsets[idx], offsets[idx+1]).
  for (let p = 0; p < numPolygons; p++) {
    const start = polyOffsets[p];
    const end = polyOffsets[p + 1];
    for (let k = start; k < end; k++) {
      const idx = rowByArc[k];
      const slot = offsets[idx + 1] - counts[idx];
      counts[idx]--;
      polyIndices[slot] = p;
    }
  }
  return { offsets, polyIndices, rowByArc };
}

// BFS: two input polygons are connected iff they share any arc.
// Each connected component becomes one polygon in the merged output —
// preserving the original topology's polygon grouping (an island sitting
// in a lake remains its own polygon, separate from the surrounding mass).
// Returns one number[] per connected component, each entry being a
// polygon index into FlatPolygons. Consumers (exteriorArcsForGroup)
// take this + FlatPolygons to walk the group's arcs.
function groupPolygonsByConnectivity(
  polygons: FlatPolygons,
  polygonsByArc: PolygonsByArc,
): number[][] {
  const { numPolygons, polyOffsets } = polygons;
  const { offsets, polyIndices, rowByArc } = polygonsByArc;
  // visited is a per-polygon bit. Fresh Uint8Array per call —
  // pooling it traded Scavenge for Mark-Compact on the national
  // bench, net wash. V8 zero-fills typed arrays cheaply.
  const visited = new Uint8Array(numPolygons);
  const groups: number[][] = [];
  for (let seed = 0; seed < numPolygons; seed++) {
    if (visited[seed] !== 0) continue;
    visited[seed] = 1;
    const group: number[] = [seed];
    const stack: number[] = [seed];
    while (stack.length > 0) {
      const idx = stack.pop() as number;
      const arcStart = polyOffsets[idx];
      const arcEnd = polyOffsets[idx + 1];
      for (let k = arcStart; k < arcEnd; k++) {
        const row = rowByArc[k];
        const rowStart = offsets[row];
        const rowEnd = offsets[row + 1];
        for (let r = rowStart; r < rowEnd; r++) {
          const other = polyIndices[r];
          if (visited[other] === 0) {
            visited[other] = 1;
            stack.push(other);
            group.push(other);
          }
        }
      }
    }
    groups.push(group);
  }
  return groups;
}

// Within a connected group, an arc is on the exterior boundary iff it
// belongs to exactly one input polygon. Arcs with ≥2 memberships are
// interior (shared between two polygons in the group) and cancel.
function exteriorArcsForGroup(
  group: ReadonlyArray<number>,
  polygons: FlatPolygons,
  polygonsByArc: PolygonsByArc,
): number[] {
  const { signedArcIds, polyOffsets } = polygons;
  const { offsets, rowByArc } = polygonsByArc;
  const out: number[] = [];
  for (let g = 0; g < group.length; g++) {
    const p = group[g];
    const arcStart = polyOffsets[p];
    const arcEnd = polyOffsets[p + 1];
    for (let k = arcStart; k < arcEnd; k++) {
      const row = rowByArc[k];
      // exterior iff exactly one membership
      if (offsets[row + 1] - offsets[row] < 2) {
        out.push(signedArcIds[k]);
      }
    }
  }
  return out;
}

function absArcIds(signed: ReadonlyArray<number>): number[] {
  const out = new Array<number>(signed.length);
  for (let i = 0; i < signed.length; i++) {
    const s = signed[i];
    out[i] = s >= 0 ? s : ~s;
  }
  return out;
}

// --- Arc endpoint lookup (per-arc bytes from the fetcher) ---

interface EndpointLookup<K> {
  start(signedArcId: number): K;
  end(signedArcId: number): K;
}

// Pack two int32 quantized coords into one Number key. Safe-integer
// math: with the +SHIFT bias each component lives in [0, 2*SHIFT), and
// total bits used is 2 * (log2(SHIFT) + 1) — choose SHIFT = 2^25 →
// 52 bits, comfortably within Number.MAX_SAFE_INTEGER. Practical
// quantized topojson grids (typical N = 1e5 — 17 bits per coord) are
// way under 2^25, so the bias never clips. The encoder validates the
// int32 range when reading delta values; same range applies here.
//
// Why numeric instead of `${x},${y}` strings: stitchArcs reads two
// endpoint keys per boundary arc and uses them as Map<K, Fragment>
// keys. At national-CD scale (~1000 boundary arcs/merge × 435 merges
// × 2 endpoints = ~870K key materializations) the string form was
// the dominant allocator inside merge — 47% of sampled bytes in the
// heap profile. Numeric keys carry zero allocation per key.
const COORD_KEY_SHIFT = 1 << 25;
function packCoord(x: number, y: number): number {
  return (x + COORD_KEY_SHIFT) * (2 * COORD_KEY_SHIFT) + (y + COORD_KEY_SHIFT);
}

// Section-backed numeric endpoint lookup. `endpoints` carries per-arc
// length-4 Int32Array views (see client.fetchArcEndpoints) — start at
// [0..2), end at [2..4). Signed arc ids reverse direction: a negative
// signed id ~i reads i's end as its "start" and vice versa.
function makeNumericEndpointLookup(
  endpoints: ReadonlyMap<number, Int32Array>,
  client: CtopoCore,
): EndpointLookup<number> {
  function readForward(arcId: number): Int32Array {
    const v = endpoints.get(arcId);
    if (v === undefined) {
      throw new Error(`ctopo: missing fetched endpoints for arc ${arcId}`);
    }
    return v;
  }
  // Useful-bytes tally: 4 i32 reads per arc lookup = 16 B (matches the
  // client's per-arc tally for the section path; double-count is fine
  // — the merge-side tally is for "what merge actually consumed", not
  // wire bytes).
  void client;
  return {
    start(signedArcId: number): number {
      if (signedArcId >= 0) {
        const v = readForward(signedArcId);
        return packCoord(v[0], v[1]);
      }
      const v = readForward(~signedArcId);
      return packCoord(v[2], v[3]);
    },
    end(signedArcId: number): number {
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
// arc's varint stream and packs into the same 52-bit Number key
// makeNumericEndpointLookup uses — so stitchArcs is key-type-
// agnostic.
//
// No per-call memoization: stitchArcs visits each unsigned arc id
// exactly once in its boundary list (forward or reverse, never both
// directions in the same call), so a per-call start/end cache has a
// measured 0% hit rate (417,703 lookups, 0 hits on the national CD
// merge). Adding the Maps was pure overhead — removed.
function makeNumericEndpointLookupFromBytes(
  arcBytes: ReadonlyMap<number, Uint8Array>,
  client: CtopoCore,
): EndpointLookup<number> {
  // One cursor reused across every start/end read in this lookup.
  // Sync hot path; no concurrency to worry about.
  const cur: VarintCursor = { value: 0, off: 0 };
  function readStartPacked(arcId: number): number {
    const bytes = requireArcBytes(arcBytes, arcId);
    cur.off = 0;
    readVarintZigzagInto(bytes, cur);
    const dx = cur.value;
    readVarintZigzagInto(bytes, cur);
    return packCoord(dx, cur.value);
  }
  function readEndPacked(arcId: number): number {
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
  void client;
  return {
    start(signedArcId: number): number {
      return signedArcId >= 0
        ? readStartPacked(signedArcId)
        : readEndPacked(~signedArcId);
    },
    end(signedArcId: number): number {
      return signedArcId >= 0
        ? readEndPacked(signedArcId)
        : readStartPacked(~signedArcId);
    },
  };
}

function makeEndpointLookup(
  arcBytes: ReadonlyMap<number, Uint8Array>,
  client: CtopoCore,
): EndpointLookup<string> {
  const isQuantized = client.transform !== null;
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

// --- stitchArcs ---

// Fragment key type is the same as the EndpointLookup's key type
// (number for the section path, string for the legacy un-quantized
// path). The scratch Maps are typed as Map<unknown, Fragment> so they
// can carry either at runtime without conversion.
interface Fragment extends Array<number> {
  start: unknown;
  end: unknown;
}

// Stitches a flat list of signed boundary arc ids into closed rings —
// the topojson-client/src/stitch.js algorithm, taking its endpoint
// lookup from per-arc fetched bytes rather than a single in-memory
// ArrayBuffer so individual arcs can be Range-fetched on demand.
function stitchArcs<K>(
  client: CtopoCore,
  arcs: ReadonlyArray<number>,
  endpoints: EndpointLookup<K>,
): number[][] {
  const scratch = getStitchScratch(client);
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
      f.push(i);
      f.end = endKey;
      const g = fragmentByStart.get(endKey);
      if (g !== undefined) {
        fragmentByStart.delete(g.start);
        // Merge g into f in place. f.concat(g) was the dominant
        // allocator in the merge phase — at national scale it pushed
        // ~8 MB / 46% of merge-only sampled allocations through GC.
        if (g !== f) {
          for (let k = 0; k < g.length; k++) f.push(g[k]);
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
        f.unshift(i);
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
            for (let k = 0; k < f.length; k++) g.push(f[k]);
            g.end = f.end;
            fragmentByStart.set(g.start, g);
            fragmentByEnd.set(g.end, g);
          }
        } else {
          fragmentByStart.set(f.start, f);
          fragmentByEnd.set(f.end, f);
        }
      } else {
        const fresh: Fragment = [i] as Fragment;
        fresh.start = startKey;
        fresh.end = endKey;
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
      for (const i of f) stitched.add(i < 0 ? ~i : i);
      fragments.push(f);
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
function decodeRingFlat(
  arcIds: ReadonlyArray<number>,
  arcBytes: ReadonlyMap<number, Uint8Array>,
  client: CtopoCore,
  out: Float64Growable,
): { start: number; end: number } {
  const t = client.transform;
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
      const view = new DataView(
        bytes.buffer,
        bytes.byteOffset,
        bytes.byteLength,
      );
      const numPoints = bytes.byteLength / 16;
      out.ensure(numPoints * 2);
      for (let i = 0; i < numPoints; i++) {
        const off = i * 16;
        out.buf[out.len++] = view.getFloat64(off, true);
        out.buf[out.len++] = view.getFloat64(off + 8, true);
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

function ringAreaFlat(
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
