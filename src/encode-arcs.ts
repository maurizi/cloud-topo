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

import { VARINT_MAX_BYTES, writeVarintZigzag } from "./format";

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
  // Plain Hilbert: each geom is mapped to a 16-bit Hilbert key from
  // its representative point; geoms (and their arcs by walk order)
  // are emitted in key order. Default. Cheap and consistent.
  | "hilbert"
  // Hierarchical: arc-ref-count-driven partition. An arc with refs
  // ≤ 2 is "base-only" (just bounds 2 base-layer geoms); refs > 2
  // means it also bounds a parent-layer geom and is a boundary
  // arc. Boundary arcs are emitted at the file front (in Hilbert
  // order); base-only arcs are bucketed by parent component
  // (computed via union-find on base-only arcs) and emitted by
  // component, ordered Hilbert within component.
  //
  // Layer-agnostic — partition detected purely from arc-sharing
  // patterns, works for any nesting depth. The current production
  // winner: -1 to -6% wall-clock on small/medium topologies vs
  // plain Hilbert + the default coalesce gap, neutral on the
  // largest fixtures.
  | "hierarchical-hilbert"
  // Same hierarchical partition as hierarchical-hilbert, but the
  // within-component walk is greedy-path (least-deflection) over
  // the component's geoms. Bytes-fetched is lower than Hilbert-
  // within on most fixtures (per the bench's packing index), but
  // the walk produces more "splits" inside the perimeter — under
  // the current single-range HTTP fetcher the extra request count
  // costs more than the byte savings buy. Promoted to a future
  // candidate: with multi-range HTTP requests (one GET, multiple
  // disjoint byte ranges), the per-request RTT amortizes and this
  // becomes the right default. Same code, gate on client.
  | "hierarchical-greedy-path"
  // Same hierarchical partition, within-component walk is STR
  // (Sort-Tile-Recursive: sort geoms by x-rep, split into ⌈√N⌉
  // strips, sort each strip by y-rep). Best by pure bytes-fetched
  // (beat both Hilbert-within and greedy-path-within on most
  // fixtures in the packing-index bench). Same caveat as
  // hierarchical-greedy-path: produces more requests, so wall-
  // clock loses under the current single-range fetcher. Future
  // candidate for the multi-range world.
  | "hierarchical-str";

// --- Public API ---

export function buildArcSections(
  topology: Topology,
  arcOrder: Uint32Array,
): {
  arcOffsetsBytes: Uint8Array;
  arcCoordsBytes: Uint8Array;
  bytesPerPoint: 8 | 16;
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
    };
  }

  // Quantized path: each point is a (dx, dy) int32 delta. Encode as
  // varint(zigzag(dx)) varint(zigzag(dy)) — typical deltas (< 256
  // quantization units) shrink from 8 bytes to 2-3 bytes per point.
  // Allocate the worst case (10 bytes per point) up front, slice at end.
  const scratch = new Uint8Array(totalPoints * 2 * VARINT_MAX_BYTES);
  let byteOffset = 0;
  for (let newId = 0; newId < numArcs; newId++) {
    offsets[newId] = byteOffset;
    const arc = topology.arcs[arcOrder[newId]];
    for (const point of arc) {
      byteOffset = writeVarintZigzag(point[0], scratch, byteOffset);
      byteOffset = writeVarintZigzag(point[1], scratch, byteOffset);
    }
  }
  offsets[numArcs] = byteOffset;
  const coords = scratch.subarray(0, byteOffset);

  return {
    arcOffsetsBytes: typedArrayBytes(offsets),
    arcCoordsBytes: coords,
    bytesPerPoint,
  };
}

export function computeArcOrder(
  numArcs: number,
  layerCsrs: ReadonlyArray<{ name: string; csr: LayerCsrForOrder }>,
  arcs: TopoArcs,
  spatialSort: SpatialSort,
  isQuantized: boolean,
): Uint32Array {
  if (layerCsrs.length <= 1) {
    return identityOrder(numArcs);
  }

  // Visit indices: each layer top → base contributes a sort pass.
  // The chosen `spatialSort` decides geometry visit order within
  // each pass; arcs inherit that order via the geometry walk.
  // Top-layer boundary arcs (outer perimeter + parent-layer
  // interior boundaries) naturally land at the front of arc_coords
  // because they're assigned visit numbers first. Base-only arcs
  // come last.
  const numLayers = layerCsrs.length;
  const visit = new Int32Array(numArcs).fill(-1);
  let cursor = 0;
  if (
    spatialSort === "hierarchical-hilbert" ||
    spatialSort === "hierarchical-greedy-path" ||
    spatialSort === "hierarchical-str"
  ) {
    const within: "hilbert" | "greedy-path" | "str" =
      spatialSort === "hierarchical-greedy-path"
        ? "greedy-path"
        : spatialSort === "hierarchical-str"
          ? "str"
          : "hilbert";
    // Use the same `within` strategy at non-base layers (no parent
    // grouping needed — top-tier arcs are between top-level
    // features whose parent is the whole topology). Then hand the
    // base layer to the hierarchical visit which subgroups
    // base-only arcs by parent component computed from arc-ref
    // counts across all layers.
    const flatVisitFn =
      within === "greedy-path"
        ? greedyPathAssignVisit
        : within === "str"
          ? strAssignVisit
          : hilbertAssignVisit;
    for (let level = numLayers - 1; level >= 1; level--) {
      cursor = flatVisitFn(
        layerCsrs[level].csr,
        arcs,
        visit,
        cursor,
        isQuantized,
      );
    }
    cursor = hierarchicalAssignVisit(
      layerCsrs.map((l) => l.csr),
      arcs,
      visit,
      cursor,
      isQuantized,
      within,
    );
  } else {
    const visitFn = pickVisitFn(spatialSort);
    for (let level = numLayers - 1; level >= 0; level--) {
      cursor = visitFn(layerCsrs[level].csr, arcs, visit, cursor, isQuantized);
    }
  }

  // Front-load the topology's outer-boundary arcs. Computed via
  // the same signed-reference cancellation that topojson's
  // `mergeArcs` uses: an arc is on the boundary of the union of
  // all geoms iff its forward references don't equal its reverse
  // references when summed across every geom in every layer.
  // Interior arcs (shared by two adjacent polygons running in
  // opposite directions) cancel; outer-boundary arcs don't.
  //
  // This is layer-organization-agnostic — collapsing nested layers
  // into one or splitting one layer into many doesn't change the
  // outline as long as the geoms themselves cover the same area.
  // Pulled by the relatively rare merges that touch the outer
  // boundary; grouping them up front keeps those merges contiguous
  // and out of the way of common interior fetches.
  const outlineFwd = new Int32Array(numArcs);
  const outlineRev = new Int32Array(numArcs);
  for (const { csr } of layerCsrs) {
    for (let i = 0; i < csr.arcRefs.length; i++) {
      const r = csr.arcRefs[i];
      if (r >= 0) outlineFwd[r]++;
      else outlineRev[~r]++;
    }
  }
  const isOutline = new Uint8Array(numArcs);
  for (let id = 0; id < numArcs; id++) {
    if (outlineFwd[id] !== outlineRev[id]) isOutline[id] = 1;
  }

  // Array.sort with a comparator is fine for ~millions of arcs
  // (~hundreds of ms one-time at producer side at the high end).
  const sorted = Array.from(identityOrder(numArcs)).sort(
    (a, b) => isOutline[b] - isOutline[a] || visit[a] - visit[b],
  );
  return new Uint32Array(sorted);
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

// Visit-order pass: takes a layer's CSR + the global arcs array and
// the running per-arc visit index Int32Array (filled with -1 before
// the first pass). Walks geometries (or arcs directly, for arc-graph
// variants) in some algorithm-specific order, assigning each
// unclaimed arc a monotonically-increasing visit index. Returns the
// cursor so subsequent layer passes continue numbering from where
// this one left off.
//
// `isQuantized` is threaded through for variants that may need to
// walk delta-encoded arcs to recover absolute endpoint coordinates;
// the current set of variants only need the "first point"
// representative (absolute in both quantized and unquantized
// topologies) and ignore it.
type VisitFn = (
  csr: LayerCsrForOrder,
  arcs: TopoArcs,
  visit: Int32Array,
  startCursor: number,
  isQuantized: boolean,
) => number;

function pickVisitFn(sort: SpatialSort): VisitFn {
  switch (sort) {
    case "hilbert":
      return hilbertAssignVisit;
    case "hierarchical-hilbert":
    case "hierarchical-greedy-path":
    case "hierarchical-str":
      // Special-cased in computeArcOrder — these multi-strategy
      // sorts go through hierarchicalAssignVisit, which pickVisitFn
      // never sees. Return Hilbert as a defensive fallback if
      // something ever calls in.
      return hilbertAssignVisit;
  }
}

// Hilbert-curve visit-order pass for one layer. For each geometry
// take its first arc's first absolute point as a representative
// position, normalize against the layer's bbox onto a 16-bit grid,
// compute the Hilbert key, sort geometries by key, and walk in
// order emitting any not-yet-claimed arcs. Returns the updated
// cursor so subsequent layer passes continue numbering from where
// this one left off.
function hilbertAssignVisit(
  csr: LayerCsrForOrder,
  arcs: TopoArcs,
  visit: Int32Array,
  startCursor: number,
  _isQuantized: boolean,
): number {
  const numGeoms = csr.polyOffsets.length - 1;
  if (numGeoms === 0) return startCursor;

  // Representative point per geometry (absolute coords). For
  // quantized topologies the topology's first arc point is the
  // delta from the origin = the absolute coordinate, so this works
  // uniformly without checking for `transform`.
  const repX = new Float64Array(numGeoms);
  const repY = new Float64Array(numGeoms);
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  for (let g = 0; g < numGeoms; g++) {
    const ringStart = csr.polyOffsets[g];
    if (csr.polyOffsets[g + 1] === ringStart) {
      // No rings — geometry has no arcs. Place at origin so the
      // sort is stable; rare in practice.
      repX[g] = 0;
      repY[g] = 0;
      continue;
    }
    const arcStart = csr.ringOffsets[ringStart];
    const signed = csr.arcRefs[arcStart];
    const arcId = signed >= 0 ? signed : ~signed;
    const arc = arcs[arcId];
    if (arc.length === 0) {
      repX[g] = 0;
      repY[g] = 0;
      continue;
    }
    const x = arc[0][0];
    const y = arc[0][1];
    repX[g] = x;
    repY[g] = y;
    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }

  // 16-bit Hilbert grid → keys fit in u32 (2^32 = 16-bit × 16-bit).
  const HSIZE = 1 << 16;
  const xRange = maxX - minX || 1;
  const yRange = maxY - minY || 1;
  const keys = new Uint32Array(numGeoms);
  for (let g = 0; g < numGeoms; g++) {
    let nx = Math.floor(((repX[g] - minX) / xRange) * (HSIZE - 1));
    let ny = Math.floor(((repY[g] - minY) / yRange) * (HSIZE - 1));
    if (nx < 0) nx = 0;
    if (ny < 0) ny = 0;
    if (nx >= HSIZE) nx = HSIZE - 1;
    if (ny >= HSIZE) ny = HSIZE - 1;
    keys[g] = hilbertXYToKey(nx, ny, HSIZE);
  }

  // Sort geometry indices by key. Stable sort isn't required for
  // correctness — ties just get an arbitrary but deterministic
  // order — but we use a tiebreaker on geometry index so the
  // result is identical across runs for fixtures with collisions.
  const sorted = new Array<number>(numGeoms);
  for (let g = 0; g < numGeoms; g++) sorted[g] = g;
  sorted.sort((a, b) => keys[a] - keys[b] || a - b);

  let cursor = startCursor;
  for (const g of sorted) {
    const ringEnd = csr.polyOffsets[g + 1];
    for (let r = csr.polyOffsets[g]; r < ringEnd; r++) {
      const arcEnd = csr.ringOffsets[r + 1];
      for (let a = csr.ringOffsets[r]; a < arcEnd; a++) {
        const id = csr.arcRefs[a] >= 0 ? csr.arcRefs[a] : ~csr.arcRefs[a];
        if (visit[id] === -1) visit[id] = cursor++;
      }
    }
  }
  return cursor;
}

// Hierarchical visit pass for the BASE layer. Base-only arcs
// (= arcs whose total reference count across all layers' CSRs is
// ≤ 2 — i.e., they bound only the 2 base-layer geoms and don't
// also bound any parent-layer geom) are grouped by their parent
// component, computed via union-find: any base-only arc connects
// two base geoms in the same parent component by definition.
// Within each parent component, arcs follow the base layer's
// Hilbert visit order; across components, by component head's
// Hilbert.
//
// Layer-agnostic — the partition is detected purely from
// arc-sharing patterns (ref counts), no parent-layer CSR
// consulted directly. Generalizes to any nesting depth.
//
// Different signature from the standard VisitFn — needs all
// layer CSRs to count arc references. Called from computeArcOrder
// via a special path.
function hierarchicalAssignVisit(
  layerCsrs: ReadonlyArray<LayerCsrForOrder>,
  arcs: TopoArcs,
  visit: Int32Array,
  startCursor: number,
  isQuantized: boolean,
  withinStrategy: "hilbert" | "greedy-path" | "str" = "hilbert",
): number {
  if (layerCsrs.length === 0) return startCursor;
  const baseCsr = layerCsrs[0];
  const numArcs = visit.length;

  // Count each arc's references across all layers' CSRs. An arc
  // referenced exactly twice (= once per side at the base layer)
  // is "base-only"; higher counts mean it also bounds a parent-
  // layer geom and is a boundary arc handled by an earlier pass.
  const refCount = new Int32Array(numArcs);
  for (const csr of layerCsrs) {
    for (let i = 0; i < csr.arcRefs.length; i++) {
      const id = csr.arcRefs[i] >= 0 ? csr.arcRefs[i] : ~csr.arcRefs[i];
      refCount[id]++;
    }
  }
  // parentArcs = arcs that ALSO appear in some non-base layer
  // (refCount > 2). Equivalent to "boundary between parent geoms"
  // without naming the parent layer.
  const parentArcs = new Set<number>();
  for (let i = 0; i < numArcs; i++) {
    if (refCount[i] > 2) parentArcs.add(i);
  }

  const numBaseGeoms = baseCsr.polyOffsets.length - 1;
  if (numBaseGeoms === 0) return startCursor;

  // Build arc → base-geom map. arcToBaseGeom{0,1}[id] hold up to 2
  // base-layer geom indices that bound this arc. Use sentinel -1
  // for "no second geom" so we don't allocate per-arc arrays.
  // Visit array length is the global arc count.
  const arcToBaseGeom0 = new Int32Array(numArcs);
  const arcToBaseGeom1 = new Int32Array(numArcs);
  arcToBaseGeom0.fill(-1);
  arcToBaseGeom1.fill(-1);
  for (let g = 0; g < numBaseGeoms; g++) {
    const ringEnd = baseCsr.polyOffsets[g + 1];
    for (let r = baseCsr.polyOffsets[g]; r < ringEnd; r++) {
      const arcEnd = baseCsr.ringOffsets[r + 1];
      for (let a = baseCsr.ringOffsets[r]; a < arcEnd; a++) {
        const id =
          baseCsr.arcRefs[a] >= 0 ? baseCsr.arcRefs[a] : ~baseCsr.arcRefs[a];
        if (arcToBaseGeom0[id] === -1) arcToBaseGeom0[id] = g;
        else if (arcToBaseGeom0[id] !== g && arcToBaseGeom1[id] === -1)
          arcToBaseGeom1[id] = g;
      }
    }
  }

  // Union-find on base geoms via base-only arcs (arcs not in the parent set).
  const parent = new Int32Array(numBaseGeoms);
  for (let i = 0; i < numBaseGeoms; i++) parent[i] = i;
  const find = (x: number): number => {
    while (parent[x] !== x) {
      parent[x] = parent[parent[x]];
      x = parent[x];
    }
    return x;
  };
  for (let arcId = 0; arcId < numArcs; arcId++) {
    if (parentArcs.has(arcId)) continue;
    const b0 = arcToBaseGeom0[arcId];
    const b1 = arcToBaseGeom1[arcId];
    if (b0 < 0 || b1 < 0) continue;
    const ra = find(b0);
    const rb = find(b1);
    if (ra !== rb) parent[ra] = rb;
  }
  // baseGeomToComponent[geom] = root component id (used as bucket key).
  const baseGeomToComponent = new Int32Array(numBaseGeoms);
  for (let i = 0; i < numBaseGeoms; i++) baseGeomToComponent[i] = find(i);

  // Run a Hilbert pass over the base layer to get a within-component
  // visit order (and a deterministic geom ordering across
  // components). We don't use the visit numbers from this pass
  // directly — we use the geom VISIT ORDER produced.
  //
  // Easier: compute Hilbert keys per geom (same as hilbertAssignVisit
  // first phase), then for each component collect its base-only arcs
  // in geom-Hilbert order; emit components in the Hilbert order of
  // their head geom.
  const repX = new Float64Array(numBaseGeoms);
  const repY = new Float64Array(numBaseGeoms);
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  for (let g = 0; g < numBaseGeoms; g++) {
    const ringStart = baseCsr.polyOffsets[g];
    if (baseCsr.polyOffsets[g + 1] === ringStart) continue;
    const arcStart = baseCsr.ringOffsets[ringStart];
    const signed = baseCsr.arcRefs[arcStart];
    const arcId = signed >= 0 ? signed : ~signed;
    const arc = arcs[arcId];
    if (arc.length === 0) continue;
    const x = arc[0][0];
    const y = arc[0][1];
    repX[g] = x;
    repY[g] = y;
    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }
  const HSIZE = 1 << 16;
  const xRange = maxX - minX || 1;
  const yRange = maxY - minY || 1;
  const baseGeomHilbert = new Uint32Array(numBaseGeoms);
  for (let g = 0; g < numBaseGeoms; g++) {
    let nx = Math.floor(((repX[g] - minX) / xRange) * (HSIZE - 1));
    let ny = Math.floor(((repY[g] - minY) / yRange) * (HSIZE - 1));
    if (nx < 0) nx = 0;
    if (ny < 0) ny = 0;
    if (nx >= HSIZE) nx = HSIZE - 1;
    if (ny >= HSIZE) ny = HSIZE - 1;
    baseGeomHilbert[g] = hilbertXYToKey(nx, ny, HSIZE);
  }

  // Group base geoms by parent component.
  const baseGeomsByComponent = new Map<number, number[]>();
  for (let g = 0; g < numBaseGeoms; g++) {
    const p = baseGeomToComponent[g];
    const list = baseGeomsByComponent.get(p);
    if (list === undefined) baseGeomsByComponent.set(p, [g]);
    else list.push(g);
  }

  // Per-component geom walk:
  //   - "hilbert": sort the component's geoms by Hilbert key.
  //   - "greedy-path": start at corner-most geom, walk to the
  //     unvisited intra-component neighbor with smallest deflection
  //     from the previous step.
  // Both produce a 1D ordering of the component's geoms; arcs of
  // each geom are emitted in walk order (deduped).
  let cursor = startCursor;

  // Build intra-component geom adjacency for greedy-path.
  // (Hilbert path doesn't need it.)
  type NbrEdge = { nbr: number; arcId: number };
  const baseGeomNeighbors = new Map<number, NbrEdge[]>();
  if (withinStrategy === "greedy-path") {
    for (let arcId = 0; arcId < numArcs; arcId++) {
      if (parentArcs.has(arcId)) continue;
      const a = arcToBaseGeom0[arcId];
      const b = arcToBaseGeom1[arcId];
      if (a < 0 || b < 0) continue;
      if (baseGeomToComponent[a] !== baseGeomToComponent[b]) continue;
      const la = baseGeomNeighbors.get(a) ?? [];
      la.push({ nbr: b, arcId });
      baseGeomNeighbors.set(a, la);
      const lb = baseGeomNeighbors.get(b) ?? [];
      lb.push({ nbr: a, arcId });
      baseGeomNeighbors.set(b, lb);
    }
  }

  // Order components by their lowest-Hilbert geom (deterministic +
  // approximate Hilbert order for the inter-component sequence).
  const componentOrder = Array.from(baseGeomsByComponent.entries())
    .map(([cid, baseGeoms]) => {
      let head = baseGeomHilbert[baseGeoms[0]];
      let headGeom = baseGeoms[0];
      for (const g of baseGeoms) {
        if (baseGeomHilbert[g] < head) {
          head = baseGeomHilbert[g];
          headGeom = g;
        }
      }
      return { cid, baseGeoms, head, headGeom };
    })
    .sort((a, b) => a.head - b.head);

  for (const { baseGeoms, headGeom } of componentOrder) {
    let walk: readonly number[];
    if (withinStrategy === "hilbert") {
      walk = [...baseGeoms].sort(
        (a, b) => baseGeomHilbert[a] - baseGeomHilbert[b] || a - b,
      );
    } else if (withinStrategy === "str") {
      // STR sort within the component's geoms: sort by x-rep, split
      // into ⌈√N⌉ vertical strips, sort each strip by y-rep.
      const numGeoms = baseGeoms.length;
      const numStrips = Math.max(1, Math.ceil(Math.sqrt(numGeoms)));
      const stripSize = Math.ceil(numGeoms / numStrips);
      const byX = [...baseGeoms].sort((a, b) => repX[a] - repX[b] || a - b);
      const order: number[] = [];
      for (let s = 0; s < numStrips; s++) {
        const lo = s * stripSize;
        const hi = Math.min(lo + stripSize, numGeoms);
        if (lo >= hi) continue;
        const strip = byX.slice(lo, hi);
        strip.sort((a, b) => repY[a] - repY[b] || a - b);
        for (const g of strip) order.push(g);
      }
      walk = order;
    } else {
      // Greedy-path walk through the component's geoms.
      const visited = new Uint8Array(numBaseGeoms);
      const order: number[] = [];
      // Start at corner-most by repX+repY (matches the existing
      // greedy-path-only sort in this file).
      let start = baseGeoms[0];
      let bestStartScore = repX[start] + repY[start];
      for (const g of baseGeoms) {
        const s = repX[g] + repY[g];
        if (s < bestStartScore) {
          bestStartScore = s;
          start = g;
        }
      }
      visited[start] = 1;
      order.push(start);
      let current = start;
      let prevDx = 0;
      let prevDy = 0;
      let havePrev = false;
      while (order.length < baseGeoms.length) {
        const nbrs = (baseGeomNeighbors.get(current) ?? []).filter(
          ({ nbr }) =>
            !visited[nbr] &&
            baseGeomToComponent[nbr] === baseGeomToComponent[current],
        );
        let next = -1;
        if (nbrs.length > 0) {
          if (havePrev) {
            let bestScore = -Infinity;
            for (const { nbr } of nbrs) {
              const dx = repX[nbr] - repX[current];
              const dy = repY[nbr] - repY[current];
              const norm = Math.hypot(dx, dy) || 1;
              const cosAngle = (dx * prevDx + dy * prevDy) / norm;
              if (cosAngle > bestScore) {
                bestScore = cosAngle;
                next = nbr;
              }
            }
          } else {
            next = nbrs[0].nbr;
          }
        } else {
          // Disconnected within component (shouldn't happen — every
          // base-only arc connects two geoms of the same parent
          // component, so a component's geoms are connected by
          // definition). Fall back to closest-by-Euclidean.
          let bestD = Infinity;
          for (const g of baseGeoms) {
            if (visited[g]) continue;
            const d = Math.hypot(
              repX[g] - repX[current],
              repY[g] - repY[current],
            );
            if (d < bestD) {
              bestD = d;
              next = g;
            }
          }
        }
        if (next < 0) break;
        prevDx = repX[next] - repX[current];
        prevDy = repY[next] - repY[current];
        const norm = Math.hypot(prevDx, prevDy) || 1;
        prevDx /= norm;
        prevDy /= norm;
        havePrev = true;
        visited[next] = 1;
        order.push(next);
        current = next;
      }
      void headGeom; // (unused; kept for tabular structure)
      walk = order;
    }
    // Emit: each geom's unclaimed arcs in walk order, deduped.
    const seen = new Set<number>();
    for (const g of walk) {
      const ringEnd = baseCsr.polyOffsets[g + 1];
      for (let r = baseCsr.polyOffsets[g]; r < ringEnd; r++) {
        const arcEnd = baseCsr.ringOffsets[r + 1];
        for (let a = baseCsr.ringOffsets[r]; a < arcEnd; a++) {
          const id =
            baseCsr.arcRefs[a] >= 0 ? baseCsr.arcRefs[a] : ~baseCsr.arcRefs[a];
          if (visit[id] !== -1 || parentArcs.has(id) || seen.has(id)) continue;
          seen.add(id);
          visit[id] = cursor++;
        }
      }
    }
  }
  return cursor;
}

// Sort-Tile-Recursive packing on per-geometry bboxes. Sort by bbox
// x-center, split into ⌈√N⌉ vertical strips of ⌈N/⌈√N⌉⌉ items each;
// within each strip sort by bbox y-center. Walk in that order
// emitting unclaimed arcs. Better than Hilbert for elongated
// geometries that don't curl with the curve.
//
// Bboxes are approximated from each arc's first point (arc[0]) —
// same "representative point" trick that hilbertAssignVisit uses,
// just aggregated across all arcs per geometry. For quantized
// topologies arc[0] is the absolute origin coordinate (per the
// topojson convention used elsewhere in this file), so this works
// without threading a quantization flag through. For polygons with
// many small arcs (the common case for fine-grained polygon meshes)
// the bbox of arc start points tightly approximates the true geom
// bbox.
function strAssignVisit(
  csr: LayerCsrForOrder,
  arcs: TopoArcs,
  visit: Int32Array,
  startCursor: number,
  _isQuantized: boolean,
): number {
  const numGeoms = csr.polyOffsets.length - 1;
  if (numGeoms === 0) return startCursor;

  // Per-geom bbox center. Float64Array pair so we can sort indices
  // by the values without re-walking the CSR.
  const cx = new Float64Array(numGeoms);
  const cy = new Float64Array(numGeoms);
  for (let g = 0; g < numGeoms; g++) {
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    let any = false;
    const ringEnd = csr.polyOffsets[g + 1];
    for (let r = csr.polyOffsets[g]; r < ringEnd; r++) {
      const arcEnd = csr.ringOffsets[r + 1];
      for (let a = csr.ringOffsets[r]; a < arcEnd; a++) {
        const signed = csr.arcRefs[a];
        const arcId = signed >= 0 ? signed : ~signed;
        const arc = arcs[arcId];
        if (arc.length === 0) continue;
        const x = arc[0][0];
        const y = arc[0][1];
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
        any = true;
      }
    }
    if (any) {
      cx[g] = (minX + maxX) / 2;
      cy[g] = (minY + maxY) / 2;
    } // else cx=cy=0; sort puts these together in some arbitrary corner
  }

  // Pass 1: sort all geometries by x-center.
  const byX = new Array<number>(numGeoms);
  for (let g = 0; g < numGeoms; g++) byX[g] = g;
  byX.sort((a, b) => cx[a] - cx[b] || a - b);

  // Pass 2: split into ⌈√N⌉ vertical strips of ⌈N/⌈√N⌉⌉ items each.
  // Within each strip sort by y-center. Tied centers fall back to
  // index order so the result is deterministic.
  const numStrips = Math.max(1, Math.ceil(Math.sqrt(numGeoms)));
  const stripSize = Math.ceil(numGeoms / numStrips);
  const sorted = new Array<number>(numGeoms);
  let outIdx = 0;
  for (let s = 0; s < numStrips; s++) {
    const lo = s * stripSize;
    const hi = Math.min(lo + stripSize, numGeoms);
    if (lo >= hi) continue;
    const strip = byX.slice(lo, hi);
    strip.sort((a, b) => cy[a] - cy[b] || a - b);
    for (const g of strip) sorted[outIdx++] = g;
  }

  let cursor = startCursor;
  for (const g of sorted) {
    const ringEnd = csr.polyOffsets[g + 1];
    for (let r = csr.polyOffsets[g]; r < ringEnd; r++) {
      const arcEnd = csr.ringOffsets[r + 1];
      for (let a = csr.ringOffsets[r]; a < arcEnd; a++) {
        const id = csr.arcRefs[a] >= 0 ? csr.arcRefs[a] : ~csr.arcRefs[a];
        if (visit[id] === -1) visit[id] = cursor++;
      }
    }
  }
  return cursor;
}

// Greedy least-deflection path. Walks the geom-geom adjacency graph
// (edges = shared arcs), at each step picking the unvisited neighbor
// most aligned with the previous direction-of-travel vector. Falls
// back to the spatially-nearest unvisited geom when no unvisited
// neighbor is available (rare on real connected geographies; happens
// at dead-end vertices and across disconnected components).
//
// Produces a snake-path through the geom set; arcs of consecutive
// geoms tend to land in neighboring file positions which is the
// goal — selections that form connected components in the geom
// graph get tighter file ranges.
function greedyPathAssignVisit(
  csr: LayerCsrForOrder,
  arcs: TopoArcs,
  visit: Int32Array,
  startCursor: number,
  _isQuantized: boolean,
): number {
  const numGeoms = csr.polyOffsets.length - 1;
  if (numGeoms === 0) return startCursor;

  // Representative point per geom (arc[0] of the first arc — same
  // as hilbertAssignVisit). Used for direction vectors and the
  // nearest-unvisited fallback.
  const repX = new Float64Array(numGeoms);
  const repY = new Float64Array(numGeoms);
  for (let g = 0; g < numGeoms; g++) {
    const ringStart = csr.polyOffsets[g];
    if (csr.polyOffsets[g + 1] === ringStart) continue;
    const arcStart = csr.ringOffsets[ringStart];
    const signed = csr.arcRefs[arcStart];
    const arcId = signed >= 0 ? signed : ~signed;
    const arc = arcs[arcId];
    if (arc.length === 0) continue;
    repX[g] = arc[0][0];
    repY[g] = arc[0][1];
  }

  // Build geom-geom adjacency by walking CSR once.
  // Pass 1: arc → first geom that references it (or -1 if not yet seen).
  // Pass 2: when an arc is referenced a second time, the two geoms are
  // mutual neighbors. Topojson arcs are shared by at most 2 polygons,
  // so the second-reference assumption holds.
  const adj = buildGeomAdjacency(csr);

  // Start at the corner-most geom (minimum repX + repY) for
  // determinism.
  let start = 0;
  let bestStartScore = repX[0] + repY[0];
  for (let g = 1; g < numGeoms; g++) {
    const s = repX[g] + repY[g];
    if (s < bestStartScore) {
      bestStartScore = s;
      start = g;
    }
  }

  const visited = new Uint8Array(numGeoms);
  const order = new Array<number>(numGeoms);
  let orderLen = 0;
  let current = start;
  let prevDx = 0;
  let prevDy = 0;
  let havePrev = false;
  visited[current] = 1;
  order[orderLen++] = current;

  while (orderLen < numGeoms) {
    const neighbors = adj[current];
    let next = -1;
    let bestScore = -Infinity;
    for (const n of neighbors) {
      if (visited[n]) continue;
      const dx = repX[n] - repX[current];
      const dy = repY[n] - repY[current];
      const score = scoreCandidate(prevDx, prevDy, dx, dy, havePrev);
      if (score > bestScore) {
        bestScore = score;
        next = n;
      }
    }
    if (next === -1) {
      // No unvisited neighbor: jump to spatially-nearest unvisited.
      // Linear scan is O(N) per jump but we rarely jump on a
      // connected geography (most base-layer graphs are single
      // connected components).
      let bestDist = Infinity;
      for (let g = 0; g < numGeoms; g++) {
        if (visited[g]) continue;
        const dx = repX[g] - repX[current];
        const dy = repY[g] - repY[current];
        const d = dx * dx + dy * dy;
        if (d < bestDist) {
          bestDist = d;
          next = g;
        }
      }
      // After a jump there's no continuous direction.
      havePrev = false;
    } else {
      prevDx = repX[next] - repX[current];
      prevDy = repY[next] - repY[current];
      havePrev = true;
    }
    if (next === -1) break; // all visited
    visited[next] = 1;
    order[orderLen++] = next;
    current = next;
  }

  let cursor = startCursor;
  for (let i = 0; i < orderLen; i++) {
    const g = order[i];
    const ringEnd = csr.polyOffsets[g + 1];
    for (let r = csr.polyOffsets[g]; r < ringEnd; r++) {
      const arcEnd = csr.ringOffsets[r + 1];
      for (let a = csr.ringOffsets[r]; a < arcEnd; a++) {
        const id = csr.arcRefs[a] >= 0 ? csr.arcRefs[a] : ~csr.arcRefs[a];
        if (visit[id] === -1) visit[id] = cursor++;
      }
    }
  }
  return cursor;
}
// Walk CSR once, return adjacency lists per geometry. Two geoms are
// adjacent iff they share at least one arc (regardless of orientation).
// Used by greedy-path.
function buildGeomAdjacency(
  csr: LayerCsrForOrder,
): ReadonlyArray<ReadonlyArray<number>> {
  const numGeoms = csr.polyOffsets.length - 1;
  // Pass 1: arc → first geom that referenced it (or -1).
  let maxArcId = -1;
  for (let i = 0; i < csr.arcRefs.length; i++) {
    const id = csr.arcRefs[i] >= 0 ? csr.arcRefs[i] : ~csr.arcRefs[i];
    if (id > maxArcId) maxArcId = id;
  }
  const arcOwner = new Int32Array(maxArcId + 1).fill(-1);
  const adj: number[][] = new Array(numGeoms);
  for (let g = 0; g < numGeoms; g++) adj[g] = [];
  for (let g = 0; g < numGeoms; g++) {
    const ringEnd = csr.polyOffsets[g + 1];
    for (let r = csr.polyOffsets[g]; r < ringEnd; r++) {
      const arcEnd = csr.ringOffsets[r + 1];
      for (let a = csr.ringOffsets[r]; a < arcEnd; a++) {
        const id = csr.arcRefs[a] >= 0 ? csr.arcRefs[a] : ~csr.arcRefs[a];
        const owner = arcOwner[id];
        if (owner === -1) {
          arcOwner[id] = g;
        } else if (owner !== g) {
          adj[g].push(owner);
          adj[owner].push(g);
          // No third polygon will reference a topojson arc, so we
          // could break out — but cheap to leave the loop running.
        }
      }
    }
  }
  // Dedupe in case the same arc was referenced multiple times by the
  // same polygon (happens with rings that revisit an arc — rare but
  // observed on real-world inputs).
  for (let g = 0; g < numGeoms; g++) {
    if (adj[g].length > 1) {
      const seen = new Set<number>();
      const out: number[] = [];
      for (const n of adj[g]) {
        if (!seen.has(n)) {
          seen.add(n);
          out.push(n);
        }
      }
      adj[g] = out;
    }
  }
  return adj;
}

// Greedy-path candidate scoring. Returns a higher value for a more
// "preferred" neighbor. With no prior direction, prefer the nearest
// neighbor (ties broken arbitrarily). With a prior direction, prefer
// the neighbor whose direction most closely matches it (cosine
// similarity), tiebreak on nearness so straight-but-far loses to
// straight-and-close.
function scoreCandidate(
  prevDx: number,
  prevDy: number,
  dx: number,
  dy: number,
  havePrev: boolean,
): number {
  const len2 = dx * dx + dy * dy;
  if (!havePrev) {
    // Negate so smaller distance = higher score.
    return -len2;
  }
  const prevLen = Math.sqrt(prevDx * prevDx + prevDy * prevDy);
  const len = Math.sqrt(len2);
  if (prevLen === 0 || len === 0) return -len2;
  const cosine = (prevDx * dx + prevDy * dy) / (prevLen * len);
  // Cosine in [-1, 1]; near +1 = aligned (preferred). Add a small
  // penalty for distance to break ties in favor of near neighbors.
  return cosine - 1e-9 * len;
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
