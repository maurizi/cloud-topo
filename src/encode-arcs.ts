// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Arc building and visit-order-based reordering for the `.ctopo` encoder.
 *
 * Orders arcs by walking each layer's geometries top → base, assigning
 * each arc a monotonically-increasing visit index in the chosen spatial
 * sort order (Hilbert, STR, greedy-path, or hierarchical variants).
 * Top-layer boundary arcs naturally land at the front of arc_coords
 * because they're visited first; base-only arcs come last. The encoder's
 * front-prefetch model depends on this layout: top-layer skeleton arcs
 * land at the front of arc_coords, so a single Range GET covers them.
 *
 * Single-layer topologies keep identity order (no reordering needed
 * when there's only one layer).
 */

import { type Topology } from "topojson-specification";

import { VARINT_MAX_BYTES, writeVarintZigzag } from "./core/format";

// --- Types ---

export interface LayerCsrForOrder {
  readonly polyOffsets: Uint32Array;
  readonly ringOffsets: Uint32Array;
  readonly arcRefs: Int32Array;
}

// Topology arc shape — list of points; for quantized topologies
// each point is a delta from the previous (with [0] being the delta
// from the origin = the absolute first coordinate). The geojson
// Position type is variable-length so we read by index.
type TopoArcs = ReadonlyArray<ReadonlyArray<ReadonlyArray<number>>>;

export type SpatialSort =
  // Plain Hilbert: one global Hilbert pass over every geom across
  // every layer. Geoms are mapped to a 16-bit Hilbert key from a
  // representative point; arcs inherit the order via the geom walk.
  // Cheap and consistent. Hilbert keys ignore layer order; tie-
  // breaks are deterministic on (layerIdx, geomIdx).
  | "hilbert"
  // Hierarchical family: layer-agnostic refCount-based tiering with a
  // configurable within-tier walk. Operates on arcs directly — no
  // geom walk, no "base layer" concept.
  //   1. refCount[arc] = total appearances across every ring of every
  //      geom in every layer.
  //   2. Group arcs by descending refCount tier.
  //   3. Within each tier, walk arcs in the order picked by the suffix:
  //      - "-hilbert"      : sort by 16-bit Hilbert key of arc rep point.
  //      - "-str"          : sort by x; split into ⌈√N⌉ strips; sort
  //                          each by y. Tightest packing on most
  //                          fixtures by pure bytes-fetched.
  //      - "-greedy-path"  : nearest-unvisited walk by rep-point
  //                          distance. Snake-path through the tier.
  // Arcs shared by many rings (boundaries common to multiple tiers of nesting)
  // land at the front; base-only arcs (refCount = 2) land at the tail.
  | "hierarchical-hilbert"
  | "hierarchical-str"
  | "hierarchical-greedy-path";

// --- Public API ---

// Per-arc absolute endpoints, computed during the same arc walk that
// emits arc_coords. Layout is interleaved [startX, startY, endX, endY]
// per arc — i.e. arc id `k` reads from indices [4k, 4k+4). Int32Array
// for the quantized path; the un-quantized path skips endpoint capture
// (a 4×f64 section would be enormous and we'd need a different format).
export interface ArcEndpoints {
  readonly bytes: Int32Array;
}

export function buildArcSections(
  topology: Topology,
  arcOrder: Uint32Array,
): {
  arcOffsetsBytes: Uint8Array;
  arcCoordsBytes: Uint8Array;
  bytesPerPoint: 8 | 16;
  // Per-arc absolute (startX, startY, endX, endY) — quantized path
  // only. Undefined when the input is un-quantized. Caller decides
  // whether to encode this into a dedicated section.
  arcEndpoints: ArcEndpoints | undefined;
} {
  const numArcs = topology.arcs.length;
  const isQuantized = topology.transform !== undefined;
  const bytesPerPoint: 8 | 16 = isQuantized ? 8 : 16;

  let totalPoints = 0;
  for (const arc of topology.arcs) totalPoints += arc.length;

  const offsets = new Uint32Array(numArcs + 1);

  if (!isQuantized) {
    // Float64 absolutes — no transform to delta + varint, store raw.
    const coords = Buffer.alloc(totalPoints * 16);
    let byteOffset = 0;
    for (let newId = 0; newId < numArcs; newId++) {
      offsets[newId] = byteOffset;
      const arc = topology.arcs[arcOrder[newId]];
      for (const point of arc) {
        coords.writeDoubleLE(point[0], byteOffset);
        coords.writeDoubleLE(point[1], byteOffset + 8);
        byteOffset += 16;
      }
    }
    offsets[numArcs] = byteOffset;
    return {
      arcOffsetsBytes: typedArrayBytes(offsets),
      arcCoordsBytes: coords,
      bytesPerPoint,
      arcEndpoints: undefined,
    };
  }

  // Quantized path: each point is a (dx, dy) int32 delta. Encode as
  // varint(zigzag(dx)) varint(zigzag(dy)) — typical deltas (< 256
  // quantization units) shrink from 8 bytes to 2-3 bytes per point.
  // Allocate the worst case (10 bytes per point) up front, slice at end.
  //
  // We also capture per-arc absolute (start, end) endpoints in the
  // same walk — they're a free byproduct of running the dx/dy
  // accumulator that the merge-side stitcher would otherwise have to
  // rebuild via a full varint walk per boundary arc.
  const scratch = new Uint8Array(totalPoints * 2 * VARINT_MAX_BYTES);
  const endpointsBuf = new Int32Array(numArcs * 4);
  let byteOffset = 0;
  for (let newId = 0; newId < numArcs; newId++) {
    offsets[newId] = byteOffset;
    const arc = topology.arcs[arcOrder[newId]];
    let absX = 0;
    let absY = 0;
    let pIdx = 0;
    for (const point of arc) {
      const dx = point[0];
      const dy = point[1];
      byteOffset = writeVarintZigzag(dx, scratch, byteOffset);
      byteOffset = writeVarintZigzag(dy, scratch, byteOffset);
      absX += dx;
      absY += dy;
      if (pIdx === 0) {
        endpointsBuf[newId * 4] = absX;
        endpointsBuf[newId * 4 + 1] = absY;
      }
      pIdx++;
    }
    // arc.length === 0 leaves all four entries at 0, matching the
    // "no points" arc the varint stream encodes (a zero-byte slice).
    endpointsBuf[newId * 4 + 2] = absX;
    endpointsBuf[newId * 4 + 3] = absY;
  }
  offsets[numArcs] = byteOffset;
  const coords = scratch.subarray(0, byteOffset);

  return {
    arcOffsetsBytes: typedArrayBytes(offsets),
    arcCoordsBytes: coords,
    bytesPerPoint,
    arcEndpoints: { bytes: endpointsBuf },
  };
}

export function computeArcOrder(
  numArcs: number,
  layerCsrs: ReadonlyArray<{ name: string; csr: LayerCsrForOrder }>,
  arcs: TopoArcs,
  spatialSort: SpatialSort,
  isQuantized: boolean,
): Uint32Array {
  // Plain "hilbert" sorts geometries; with only one layer the geom
  // walk degenerates to "geoms in input order," so identity is fine.
  // The hierarchical-* family sorts arcs directly and its load-bearing
  // property is byte-identical output regardless of layer separation
  // — must run even with one layer.
  const isHierarchical =
    spatialSort === "hierarchical-hilbert" ||
    spatialSort === "hierarchical-str" ||
    spatialSort === "hierarchical-greedy-path";
  if (layerCsrs.length <= 1 && !isHierarchical) {
    return identityOrder(numArcs);
  }
  if (layerCsrs.length === 0) {
    return identityOrder(numArcs);
  }

  // Walk every signed arc reference in every layer ONCE: the hierarchical
  // path needs total refCount per arc, and the outline-front-loading pass
  // needs forward/reverse counts. refCount = fwd + rev, so we derive it
  // from the same scan instead of re-walking arcRefs inside the visit
  // pass. For million-arc topologies with many cross-layer references,
  // this is the second-largest pass after the comparator-driven sort.
  //
  // Front-load the topology's outer-boundary arcs. Computed via the same
  // signed-reference cancellation that topojson's `mergeArcs` uses: an arc
  // is on the boundary of the union of all geoms iff its forward
  // references don't equal its reverse references when summed across
  // every geom in every layer. Interior arcs (shared by two adjacent
  // polygons running in opposite directions) cancel; outer-boundary arcs
  // don't.
  //
  // This is layer-organization-agnostic — collapsing nested layers into
  // one or splitting one layer into many doesn't change the outline as
  // long as the geoms themselves cover the same area. Pulled by the
  // relatively rare merges that touch the outer boundary; grouping them
  // up front keeps those merges contiguous and out of the way of common
  // interior fetches.
  const outlineFwd = new Int32Array(numArcs);
  const outlineRev = new Int32Array(numArcs);
  for (const { csr } of layerCsrs) {
    const arcRefs = csr.arcRefs;
    for (let i = 0; i < arcRefs.length; i++) {
      const r = arcRefs[i];
      if (r >= 0) outlineFwd[r]++;
      else outlineRev[~r]++;
    }
  }
  const isOutline = new Uint8Array(numArcs);
  for (let id = 0; id < numArcs; id++) {
    if (outlineFwd[id] !== outlineRev[id]) isOutline[id] = 1;
  }

  // Each spatial sort produces a single layer-agnostic arc visit
  // pass: it walks all arcs across all layers and assigns each one
  // a monotonically-increasing visit index. Plain "hilbert" sorts
  // geometries by Hilbert key; the hierarchical-* family sorts arcs
  // directly by refCount tier with a configurable within-tier walk.
  const visit = new Int32Array(numArcs).fill(-1);
  if (
    spatialSort === "hierarchical-hilbert" ||
    spatialSort === "hierarchical-str" ||
    spatialSort === "hierarchical-greedy-path"
  ) {
    const within: "hilbert" | "str" | "greedy-path" =
      spatialSort === "hierarchical-str"
        ? "str"
        : spatialSort === "hierarchical-greedy-path"
          ? "greedy-path"
          : "hilbert";
    // Derive refCount from outlineFwd + outlineRev — every signed
    // reference contributed to one or the other, so their sum is the
    // total refCount per arc. Saves a second walk over all arcRefs.
    const refCount = new Int32Array(numArcs);
    for (let id = 0; id < numArcs; id++) {
      refCount[id] = outlineFwd[id] + outlineRev[id];
    }
    hierarchicalAssignVisit(arcs, visit, 0, within, refCount);
  } else {
    globalHilbertAssignVisit(
      layerCsrs.map((l) => l.csr),
      arcs,
      visit,
      0,
      isQuantized,
    );
  }

  // Final order: outline arcs first (in visit order), then non-outline
  // (in visit order). Counting-sort it in O(n) instead of an O(n log n)
  // comparator-driven sort. Orphaned arcs (visit === -1, only possible
  // on the global hilbert path) sort to the front of their group, matching
  // the previous comparator's `visit[a] - visit[b]` behavior on -1.
  let outlineCount = 0;
  for (let id = 0; id < numArcs; id++) if (isOutline[id]) outlineCount++;
  const result = new Uint32Array(numArcs);
  let outCur = 0;
  let nonOutCur = outlineCount;
  for (let id = 0; id < numArcs; id++) {
    if (visit[id] !== -1) continue;
    if (isOutline[id]) result[outCur++] = id;
    else result[nonOutCur++] = id;
  }
  const invVisit = new Int32Array(numArcs).fill(-1);
  for (let id = 0; id < numArcs; id++) {
    if (visit[id] !== -1) invVisit[visit[id]] = id;
  }
  for (let v = 0; v < numArcs; v++) {
    const id = invVisit[v];
    if (id === -1) continue;
    if (isOutline[id]) result[outCur++] = id;
    else result[nonOutCur++] = id;
  }
  return result;
}

export function invertPermutation(perm: Uint32Array): Int32Array {
  const inv = new Int32Array(perm.length);
  for (let i = 0; i < perm.length; i++) inv[perm[i]] = i;
  return inv;
}

// Rewrite signed arc refs in place from old → new ids (sign preserved).
export function remapArcRefs(arcRefs: Int32Array, newIdOf: Int32Array): void {
  for (let i = 0; i < arcRefs.length; i++) {
    const old = arcRefs[i];
    if (old >= 0) arcRefs[i] = newIdOf[old];
    else arcRefs[i] = ~newIdOf[~old];
  }
}

// --- Internal helpers ---

// Per-arc visit-order passes (hierarchicalAssignVisit + globalHilbertAssignVisit
// below) take all layers' CSRs together so refCount and rep-point
// computations are layer-agnostic. The running visit Int32Array is
// filled with -1 before the first pass; each pass assigns
// monotonically-increasing visit indices to unclaimed arcs and
// returns the updated cursor.

// Global Hilbert pass over every geometry across every layer.
// One bbox, one sort, one walk — the layer the geometry came from
// is not load-bearing. Geometries with smaller Hilbert keys land
// first in arc_coords; ties break deterministically by (layerIdx,
// geomIdx) so output is stable across runs.
//
// For inputs where the caller separated nested geometries into
// multiple layers (e.g. county / precinct / block), the result is
// byte-identical to encoding the same geometries flattened into a
// single layer.
function globalHilbertAssignVisit(
  layerCsrs: ReadonlyArray<LayerCsrForOrder>,
  arcs: TopoArcs,
  visit: Int32Array,
  startCursor: number,
  _isQuantized: boolean,
): number {
  // Total geom count across all layers.
  let totalGeoms = 0;
  for (const csr of layerCsrs) totalGeoms += csr.polyOffsets.length - 1;
  if (totalGeoms === 0) return startCursor;

  // Representative point per (layer, geom). For quantized topologies
  // the topology's first arc point is the delta from the origin =
  // the absolute coordinate, so the same code works uniformly for
  // both quantized and unquantized inputs.
  const repX = new Float64Array(totalGeoms);
  const repY = new Float64Array(totalGeoms);
  // Parallel arrays mapping flat index → (layer, geom).
  const flatLayer = new Int32Array(totalGeoms);
  const flatGeom = new Int32Array(totalGeoms);
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  let flat = 0;
  for (let l = 0; l < layerCsrs.length; l++) {
    const csr = layerCsrs[l];
    const numGeoms = csr.polyOffsets.length - 1;
    for (let g = 0; g < numGeoms; g++) {
      flatLayer[flat] = l;
      flatGeom[flat] = g;
      const ringStart = csr.polyOffsets[g];
      if (csr.polyOffsets[g + 1] === ringStart) {
        // No rings — geometry has no arcs. Place at origin so the
        // sort is stable; rare in practice.
        repX[flat] = 0;
        repY[flat] = 0;
        flat++;
        continue;
      }
      const arcStart = csr.ringOffsets[ringStart];
      const signed = csr.arcRefs[arcStart];
      const arcId = signed >= 0 ? signed : ~signed;
      const arc = arcs[arcId];
      if (arc.length === 0) {
        repX[flat] = 0;
        repY[flat] = 0;
        flat++;
        continue;
      }
      const x = arc[0][0];
      const y = arc[0][1];
      repX[flat] = x;
      repY[flat] = y;
      if (x < minX) minX = x;
      if (x > maxX) maxX = x;
      if (y < minY) minY = y;
      if (y > maxY) maxY = y;
      flat++;
    }
  }

  // 16-bit Hilbert grid → keys fit in u32.
  const HSIZE = 1 << 16;
  const HSIZE_M1 = HSIZE - 1;
  const xScale = HSIZE_M1 / (maxX - minX || 1);
  const yScale = HSIZE_M1 / (maxY - minY || 1);
  const keys = new Uint32Array(totalGeoms);
  for (let i = 0; i < totalGeoms; i++) {
    let nx = (repX[i] - minX) * xScale;
    let ny = (repY[i] - minY) * yScale;
    if (nx < 0) nx = 0;
    else if (nx > HSIZE_M1) nx = HSIZE_M1;
    if (ny < 0) ny = 0;
    else if (ny > HSIZE_M1) ny = HSIZE_M1;
    keys[i] = hilbertXYToKey(nx | 0, ny | 0, HSIZE);
  }

  // Sort flat indices by key, tiebreaking on (layer, geom) for
  // deterministic output across collisions. Uint32Array.sort with a
  // comparator avoids the boxing/unboxing overhead of Array<number>.
  const sorted = new Uint32Array(totalGeoms);
  for (let i = 0; i < totalGeoms; i++) sorted[i] = i;
  sorted.sort(
    (a, b) =>
      keys[a] - keys[b] ||
      flatLayer[a] - flatLayer[b] ||
      flatGeom[a] - flatGeom[b],
  );

  let cursor = startCursor;
  for (let s = 0; s < totalGeoms; s++) {
    const i = sorted[s];
    const csr = layerCsrs[flatLayer[i]];
    const g = flatGeom[i];
    const ringEnd = csr.polyOffsets[g + 1];
    for (let r = csr.polyOffsets[g]; r < ringEnd; r++) {
      const arcEnd = csr.ringOffsets[r + 1];
      const arcRefs = csr.arcRefs;
      for (let a = csr.ringOffsets[r]; a < arcEnd; a++) {
        const ref = arcRefs[a];
        const id = ref >= 0 ? ref : ~ref;
        if (visit[id] === -1) visit[id] = cursor++;
      }
    }
  }
  return cursor;
}

// Layer-agnostic arc-level sort: refCount-based tiering with a
// pluggable within-tier walk. Operates directly on arcs, not on
// geometries — there's no geom walk, no "base layer", no per-layer
// iteration. Same arc order whether the caller passed three layers
// (county / precinct / block) or one concatenated layer holding the
// same geometries.
//
// Algorithm:
//   refCount[arc] = total appearances across every ring of every
//                   geom across every layer.
//   tier(arc)     = refCount[arc]; arcs are bucketed by tier and
//                   tiers are walked in DESCENDING order so arcs
//                   shared by many rings (boundaries common to
//                   several layers of nesting) land at the front.
//   within-tier   = "hilbert" / "str" / "greedy-path" — picks the
//                   walk order among arcs that share a tier.
//
// Rep point per arc is the arc's first point — absolute for non-
// quantized topologies; for quantized topologies the first point
// is the delta-from-origin which equals the absolute first
// coordinate, so the same code works uniformly.
function hierarchicalAssignVisit(
  arcs: TopoArcs,
  visit: Int32Array,
  startCursor: number,
  withinStrategy: "hilbert" | "str" | "greedy-path",
  refCount: Int32Array,
): number {
  const numArcs = visit.length;
  if (numArcs === 0) return startCursor;

  // Rep point per arc (= arc's first point) and global bbox.
  const repX = new Float64Array(numArcs);
  const repY = new Float64Array(numArcs);
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  for (let id = 0; id < numArcs; id++) {
    const arc = arcs[id];
    if (arc.length === 0) continue;
    const x = arc[0][0];
    const y = arc[0][1];
    repX[id] = x;
    repY[id] = y;
    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }

  // Hilbert is a pure ordering — sorting by (refCount desc, hilbertKey
  // asc, arcId asc) is byte-equivalent to bucketing by refCount and
  // sorting within each bucket. Skip the tier-bucket scaffolding, the
  // per-tier allocations, and the per-tier walkTier dispatch with a
  // single sort.
  if (withinStrategy === "hilbert") {
    const HSIZE = 1 << 16;
    const HSIZE_M1 = HSIZE - 1;
    const xScale = HSIZE_M1 / (maxX - minX || 1);
    const yScale = HSIZE_M1 / (maxY - minY || 1);
    const keys = new Uint32Array(numArcs);
    for (let id = 0; id < numArcs; id++) {
      let nx = (repX[id] - minX) * xScale;
      let ny = (repY[id] - minY) * yScale;
      if (nx < 0) nx = 0;
      else if (nx > HSIZE_M1) nx = HSIZE_M1;
      if (ny < 0) ny = 0;
      else if (ny > HSIZE_M1) ny = HSIZE_M1;
      keys[id] = hilbertXYToKey(nx | 0, ny | 0, HSIZE);
    }
    const order = new Uint32Array(numArcs);
    for (let i = 0; i < numArcs; i++) order[i] = i;
    order.sort(
      (a, b) => refCount[b] - refCount[a] || keys[a] - keys[b] || a - b,
    );
    let cursor = startCursor;
    for (let i = 0; i < numArcs; i++) visit[order[i]] = cursor++;
    return cursor;
  }

  // STR and greedy have a within-tier walk that isn't expressible as a
  // single comparator, so they keep the bucket-by-refCount scaffolding.
  // Counting sort is O(n) — refCount is small-bounded so the histogram
  // fits comfortably.
  let maxRC = 0;
  for (let id = 0; id < numArcs; id++) {
    if (refCount[id] > maxRC) maxRC = refCount[id];
  }
  const tierCount = new Uint32Array(maxRC + 1);
  for (let id = 0; id < numArcs; id++) tierCount[refCount[id]]++;
  // tierStart[rc] = beginning offset in `bucketed` for arcs with that
  // refCount, with tiers laid out in DESCENDING refCount order.
  const tierStart = new Uint32Array(maxRC + 1);
  let acc = 0;
  for (let rc = maxRC; rc >= 0; rc--) {
    tierStart[rc] = acc;
    acc += tierCount[rc];
  }
  const tierFill = new Uint32Array(maxRC + 1);
  const bucketed = new Uint32Array(numArcs);
  // Walk arcs in ascending id order so each tier's bucket ends up in
  // ascending id order — matches the prior `|| a - b` tiebreak.
  for (let id = 0; id < numArcs; id++) {
    const rc = refCount[id];
    bucketed[tierStart[rc] + tierFill[rc]++] = id;
  }

  // Reusable scratch sized for the largest possible tier (= numArcs).
  const scratch = new Uint32Array(numArcs);

  let cursor = startCursor;
  let tierBegin = 0;
  for (let rc = maxRC; rc >= 0; rc--) {
    const size = tierCount[rc];
    if (size === 0) continue;
    const tier = bucketed.subarray(tierBegin, tierBegin + size);
    cursor = walkTier(
      tier,
      withinStrategy,
      repX,
      repY,
      scratch.subarray(0, size),
      visit,
      cursor,
    );
    tierBegin += size;
  }
  return cursor;
}

// Walk a single refCount tier in the chosen within-tier order, emitting
// visit indices directly into `visit`. Each arc is in exactly one tier,
// so we don't need a `visit[id] === -1` guard. `scratch` is a writable
// Uint32Array the caller has sized to tier.length (reused across tiers
// to keep allocation traffic out of the inner loop). `tier` is a
// subarray view and is never mutated.
function walkTier(
  tier: Uint32Array,
  strategy: "str" | "greedy-path",
  repX: Float64Array,
  repY: Float64Array,
  scratch: Uint32Array,
  visit: Int32Array,
  startCursor: number,
): number {
  const n = tier.length;
  let cursor = startCursor;
  if (n === 1) {
    visit[tier[0]] = cursor++;
    return cursor;
  }

  if (strategy === "str") {
    // Sort-Tile-Recursive: sort by x, split into ⌈√N⌉ vertical strips,
    // sort each strip by y. Tighter packing than Hilbert for elongated
    // tiers; pure bytes-fetched winner on most fixtures.
    scratch.set(tier);
    scratch.sort((a, b) => repX[a] - repX[b] || a - b);
    const numStrips = Math.max(1, Math.ceil(Math.sqrt(n)));
    const stripSize = Math.ceil(n / numStrips);
    for (let s = 0; s < numStrips; s++) {
      const lo = s * stripSize;
      const hi = Math.min(lo + stripSize, n);
      if (lo >= hi) continue;
      // subarray shares the underlying buffer; sorting it sorts in place.
      scratch.subarray(lo, hi).sort((a, b) => repY[a] - repY[b] || a - b);
    }
    for (let k = 0; k < n; k++) visit[scratch[k]] = cursor++;
    return cursor;
  }

  // "greedy-path": nearest-unvisited walk by rep-point distance.
  // Starts at the corner-most arc (smallest x+y) and at each step
  // picks the nearest unvisited tier member. Uses a uniform spatial
  // grid (~√n × √n cells, ≈1 point per cell at startup) so each NN
  // query expands outward from the current cell until it can prove
  // any unchecked cell is strictly farther than the best found so
  // far. Brings worst-case work from O(n²) to roughly O(n log n) for
  // typical (non-pathological) point distributions.
  const tx = new Float64Array(n);
  const ty = new Float64Array(n);
  let txMin = Infinity;
  let txMax = -Infinity;
  let tyMin = Infinity;
  let tyMax = -Infinity;
  for (let k = 0; k < n; k++) {
    const x = repX[tier[k]];
    const y = repY[tier[k]];
    tx[k] = x;
    ty[k] = y;
    if (x < txMin) txMin = x;
    if (x > txMax) txMax = x;
    if (y < tyMin) tyMin = y;
    if (y > tyMax) tyMax = y;
  }

  const gridSide = Math.max(1, Math.ceil(Math.sqrt(n)));
  const cellSizeX = (txMax - txMin || 1) / gridSide;
  const cellSizeY = (tyMax - tyMin || 1) / gridSide;
  const xInvScale = 1 / cellSizeX;
  const yInvScale = 1 / cellSizeY;
  const minCellSize = cellSizeX < cellSizeY ? cellSizeX : cellSizeY;
  const minCellSizeSq = minCellSize * minCellSize;
  const numCells = gridSide * gridSide;

  // Bucket points into cells (CSR layout). cellPoints lists tier
  // indices in cell-major order; within each cell they stay in
  // ascending tier-index order — that ordering is what preserves the
  // original tiebreak (lowest tier-index wins on equal distance).
  const cellOf = new Uint32Array(n);
  const cellCounts = new Uint32Array(numCells);
  for (let k = 0; k < n; k++) {
    let cxi = ((tx[k] - txMin) * xInvScale) | 0;
    let cyi = ((ty[k] - tyMin) * yInvScale) | 0;
    if (cxi >= gridSide) cxi = gridSide - 1;
    if (cyi >= gridSide) cyi = gridSide - 1;
    if (cxi < 0) cxi = 0;
    if (cyi < 0) cyi = 0;
    const c = cyi * gridSide + cxi;
    cellOf[k] = c;
    cellCounts[c]++;
  }
  const cellStart = new Uint32Array(numCells + 1);
  for (let c = 0; c < numCells; c++) {
    cellStart[c + 1] = cellStart[c] + cellCounts[c];
  }
  // Reuse the caller's scratch buffer for the cell-points list.
  const cellPoints = scratch;
  const cellFill = new Uint32Array(numCells);
  for (let k = 0; k < n; k++) {
    const c = cellOf[k];
    cellPoints[cellStart[c] + cellFill[c]++] = k;
  }

  const visited = new Uint8Array(n);
  let startIdx = 0;
  let bestStart = tx[0] + ty[0];
  for (let k = 1; k < n; k++) {
    const s = tx[k] + ty[k];
    if (s < bestStart) {
      bestStart = s;
      startIdx = k;
    }
  }
  visited[startIdx] = 1;
  visit[tier[startIdx]] = cursor++;
  let currentIdx = startIdx;
  let placed = 1;

  while (placed < n) {
    const cx = tx[currentIdx];
    const cy = ty[currentIdx];
    let qcx = ((cx - txMin) * xInvScale) | 0;
    let qcy = ((cy - tyMin) * yInvScale) | 0;
    if (qcx >= gridSide) qcx = gridSide - 1;
    if (qcy >= gridSide) qcy = gridSide - 1;
    if (qcx < 0) qcx = 0;
    if (qcy < 0) qcy = 0;
    const maxR =
      Math.max(qcx, qcy, gridSide - 1 - qcx, gridSide - 1 - qcy) || 0;

    let nextIdx = -1;
    let bestDistSq = Infinity;
    let r = 0;
    while (true) {
      const xLo = qcx - r < 0 ? 0 : qcx - r;
      const xHi = qcx + r > gridSide - 1 ? gridSide - 1 : qcx + r;
      const yLo = qcy - r < 0 ? 0 : qcy - r;
      const yHi = qcy + r > gridSide - 1 ? gridSide - 1 : qcy + r;
      for (let yy = yLo; yy <= yHi; yy++) {
        const dyCells = yy - qcy;
        const adyCells = dyCells < 0 ? -dyCells : dyCells;
        for (let xx = xLo; xx <= xHi; xx++) {
          const dxCells = xx - qcx;
          const adxCells = dxCells < 0 ? -dxCells : dxCells;
          // Skip cells covered by earlier (smaller-r) rings.
          if ((adxCells > adyCells ? adxCells : adyCells) !== r) continue;
          const c = yy * gridSide + xx;
          const start = cellStart[c];
          const end = cellStart[c + 1];
          for (let p = start; p < end; p++) {
            const k = cellPoints[p];
            if (visited[k]) continue;
            const dx = tx[k] - cx;
            const dy = ty[k] - cy;
            const d = dx * dx + dy * dy;
            if (d < bestDistSq || (d === bestDistSq && k < nextIdx)) {
              bestDistSq = d;
              nextIdx = k;
            }
          }
        }
      }
      // Unchecked cells (Chebyshev ≥ r+1) have min euclidean distance
      // > r * minCellSize — strict, derived from the half-open cell
      // boundary. So once bestDist² ≤ r² * minCellSize² any unchecked
      // point is strictly farther and can't tie-break either.
      if (bestDistSq <= r * r * minCellSizeSq) break;
      if (r >= maxR) break;
      r++;
    }

    if (nextIdx === -1) break;
    visited[nextIdx] = 1;
    visit[tier[nextIdx]] = cursor++;
    currentIdx = nextIdx;
    placed++;
  }
  return cursor;
}

// Map (x, y) on an n × n integer grid (n a power of 2) to its
// Hilbert distance — the position along the order-log2(n) Hilbert
// space-filling curve. Standard iterative implementation; n=2^16
// here so the result fits in u32.
function hilbertXYToKey(x: number, y: number, n: number): number {
  let rx;
  let ry;
  let d = 0;
  for (let s = n >> 1; s > 0; s >>= 1) {
    rx = (x & s) > 0 ? 1 : 0;
    ry = (y & s) > 0 ? 1 : 0;
    d += s * s * ((3 * rx) ^ ry);
    // Rotate quadrant.
    if (ry === 0) {
      if (rx === 1) {
        x = s - 1 - x;
        y = s - 1 - y;
      }
      const t = x;
      x = y;
      y = t;
    }
  }

  return d;
}

function identityOrder(n: number): Uint32Array {
  const out = new Uint32Array(n);
  for (let i = 0; i < n; i++) out[i] = i;
  return out;
}

export function typedArrayBytes(arr: ArrayBufferView): Uint8Array {
  return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
}
