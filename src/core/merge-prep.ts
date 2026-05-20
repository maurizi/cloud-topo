// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Merge prep: the CSR-only CPU work that turns a layer selection into
 * connectivity groups and each group's exterior (boundary) arcs —
 * expandLayerPolygons → buildPolygonsByArc → groupPolygonsByConnectivity
 * → exteriorArcsForGroup. `prepareMergeGroups` also fetches the arc
 * bytes/endpoints and builds the stitch endpoint lookup;
 * `prepareMergeGroupsForPipeline` stops after collection so the
 * MergePool coordinator can do one coalesced bulk fetch. Pooled
 * per-merge Int32Arrays live here.
 */

import { type CtopoCore } from "./client";
import {
  makeEndpointLookup,
  makeNumericEndpointLookup,
  makeNumericEndpointLookupFromBytes,
  type EndpointLookup,
} from "./merge-decode";
import { type LayerGeometry, type LayerSelection } from "./types";

// Invert a layer's multiPolyBreaks (flat [geomIndex, ringIndex, …]
// pairs) into geomIndex → ringIndex[]. Callers cache the result
// per-layer; this is the shared body behind CtopoCore.breaksByGeom and
// the merge-worker PrepShim's equivalent.
export function breaksFromCsr(csr: LayerGeometry): Map<number, number[]> {
  const byGeom = new Map<number, number[]>();
  const breaks = csr.multiPolyBreaks;
  for (let i = 0; i < breaks.length; i += 2) {
    const g = breaks[i];
    const r = breaks[i + 1];
    let list = byGeom.get(g);
    if (list === undefined) {
      list = [];
      byGeom.set(g, list);
    }
    list.push(r);
  }
  return byGeom;
}

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

// --- merge prep entry points ---

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
export async function prepareMergeGroups(
  client: CtopoCore,
  inputs: ReadonlyArray<LayerSelection>,
  needCoords: boolean,
  signal: AbortSignal | undefined,
): Promise<{
  groupExteriorArcs: number[][];
  arcBytes: Map<number, Uint8Array>;
  endpoints: EndpointLookup<unknown>;
}> {
  const { groupExteriorArcs, allExteriorArcs } = await collectMergeGroups(
    client,
    inputs,
    signal,
  );
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
      endpoints: makeNumericEndpointLookup(eps),
    };
  }

  const arcBytes = await client.fetchArcs(ids, signal);
  const endpoints: EndpointLookup<unknown> = isQuantized
    ? makeNumericEndpointLookupFromBytes(arcBytes)
    : makeEndpointLookup(arcBytes, isQuantized);
  return { groupExteriorArcs, arcBytes, endpoints };
}

// Prep-only variant: runs steps 1-4 (build polygons, CSR-to-arc map,
// connectivity groups, exterior-arc collection) and stops there. The
// caller is responsible for fetching arc bytes / endpoints for the
// returned `uniqueArcIds` and then driving stitching. Used by the
// MergePool's two-phase pipelined dispatch: compute workers run prep
// in parallel (CSR-only, no I/O), surrender the boundary arc set to
// the coordinator for one coalesced bulk fetch, then resume with the
// post phase once their bytes are ready.
export async function prepareMergeGroupsForPipeline(
  client: CtopoCore,
  inputs: ReadonlyArray<LayerSelection>,
  signal: AbortSignal | undefined,
): Promise<{
  readonly groupExteriorArcs: number[][];
  readonly uniqueArcIds: number[];
}> {
  const { groupExteriorArcs, allExteriorArcs } = await collectMergeGroups(
    client,
    inputs,
    signal,
  );
  if (allExteriorArcs.length === 0) {
    return { groupExteriorArcs: [], uniqueArcIds: [] };
  }
  return { groupExteriorArcs, uniqueArcIds: absArcIds(allExteriorArcs) };
}

// Steps 1-4 shared by both prep entry points: build polygons, the
// CSR-to-arc map, connectivity groups, and each group's exterior arc
// list. Returns the per-group exterior arcs (plain number[][] that
// survives) plus the flattened bag used to derive the unique fetch
// set. The pooled per-merge Int32Arrays are released here, BEFORE any
// caller's async fetch await, so a merge interleaved during the
// network wait can reuse them rather than growing the pool to peak
// concurrency. An empty result (no polygons or no exterior arcs)
// comes back as empty arrays without touching the pool.
async function collectMergeGroups(
  client: CtopoCore,
  inputs: ReadonlyArray<LayerSelection>,
  signal: AbortSignal | undefined,
): Promise<{ groupExteriorArcs: number[][]; allExteriorArcs: number[] }> {
  const polygons = await buildInputPolygons(client, inputs, signal);
  if (polygons.numPolygons === 0) {
    return { groupExteriorArcs: [], allExteriorArcs: [] };
  }
  const polygonsByArc = buildPolygonsByArc(client, polygons);
  const groups = groupPolygonsByConnectivity(polygons, polygonsByArc);
  const allExteriorArcs: number[] = [];
  const groupExteriorArcs: number[][] = groups.map((group) => {
    const ext = exteriorArcsForGroup(group, polygons, polygonsByArc);
    for (const a of ext) allExteriorArcs.push(a);
    return ext;
  });
  const pool = getPool(client);
  pool.release(polygons.signedArcIds);
  pool.release(polygons.polyOffsets);
  pool.release(polygonsByArc.rowByArc);
  pool.release(polygonsByArc.offsets);
  pool.release(polygonsByArc.polyIndices);
  return { groupExteriorArcs, allExteriorArcs };
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
    endpoints: makeEndpointLookup(arcBytes, client.transform !== null),
  };
}

// --- Boundary-arc collection (interior cancellation) ---

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
  // O(1) lookup per geom we visit. Index built once per layer on
  // the client (`breaksByGeom`); empty Map in the typical single-
  // Polygon-only case. We attribute the whole table's byteLength to
  // the useful-bytes tally for this layer.
  client.tallyUseful(
    `${layer}/multi_poly_breaks`,
    csr.multiPolyBreaks.byteLength,
  );
  const breaksByGeom = client.breaksByGeom(layer, csr);

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
