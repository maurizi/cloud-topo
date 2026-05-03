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

import { type CtopoClient } from "./client";
import { readVarintZigzag } from "./format";
import { type LayerSelection } from "./types";

// Topojson arcs are global across layers, so cancellation works
// uniformly even for multi-layer inputs.

// --- merge / mergeArcs ---

export interface MultiPolygonArcs {
  readonly type: "MultiPolygon";
  // Three-level array of arc id rings (matches topojson-client's
  // mergeArcs return shape). Outer level = polygons; middle = rings of
  // each polygon; inner = signed arc ids of each ring.
  readonly arcs: number[][][];
}

// `mergeArcs` — union of inputs as topology-style geometry. Cheap: only
// arc endpoints are fetched (for stitching), not full coords.
export async function mergeArcs(
  client: CtopoClient,
  inputs: ReadonlyArray<LayerSelection>,
  signal?: AbortSignal,
): Promise<MultiPolygonArcs> {
  const boundaryArcs = await collectBoundaryArcs(client, inputs, signal);
  if (boundaryArcs.length === 0) {
    return { type: "MultiPolygon", arcs: [] };
  }
  const arcBytes = await client.fetchArcs(absArcIds(boundaryArcs), signal);
  const endpoints = makeEndpointLookup(arcBytes, client);
  const rings = stitchArcs(boundaryArcs, endpoints);
  // No coord decode here — arc-id rings only. Group into one polygon
  // per ring (callers can re-group by area if they care about
  // exterior/hole classification at the topology level).
  return {
    type: "MultiPolygon",
    arcs: rings.map((ring) => [ring]),
  };
}

// `merge` — union of inputs as decoded GeoJSON MultiPolygon. Fetches
// boundary arc coord bytes, stitches into rings, decodes, and groups
// rings into polygons by area (largest = exterior).
export async function merge(
  client: CtopoClient,
  inputs: ReadonlyArray<LayerSelection>,
  signal?: AbortSignal,
): Promise<MultiPolygon> {
  const boundaryArcs = await collectBoundaryArcs(client, inputs, signal);
  if (boundaryArcs.length === 0) {
    return { type: "MultiPolygon", coordinates: [] };
  }
  const arcBytes = await client.fetchArcs(absArcIds(boundaryArcs), signal);
  const endpoints = makeEndpointLookup(arcBytes, client);
  const rings = stitchArcs(boundaryArcs, endpoints);
  const decoded = rings
    .map((ring) => decodeRing(ring, arcBytes, client))
    .filter((r) => r.length >= 4);
  if (decoded.length === 0) return { type: "MultiPolygon", coordinates: [] };

  // Group rings into polygons by area: largest ring is the exterior;
  // smaller rings nested inside it are holes. For inputs that resolve
  // to multiple disconnected components this collapses to a single
  // polygon with all smaller rings as holes — callers that need true
  // polygon decomposition (point-in-polygon containment) should run
  // the rings through a downstream classifier, or call merge once per
  // component instead.
  if (decoded.length === 1) {
    return { type: "MultiPolygon", coordinates: [decoded] };
  }
  const indexed = decoded.map((r, i) => ({ area: ringArea(r), idx: i }));
  indexed.sort((a, b) => b.area - a.area);
  return {
    type: "MultiPolygon",
    coordinates: [indexed.map((x) => decoded[x.idx])],
  };
}

// --- neighbors ---

// For the given layer, return per-geometry adjacent geometry indices
// (sorted, deduped). Pure in-memory once the layer's CSR triple is
// loaded — no arc coord fetch needed.
export async function neighbors(
  client: CtopoClient,
  layer: string,
  signal?: AbortSignal,
): Promise<readonly (readonly number[])[]> {
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
  // shared arc.
  const out: number[][] = [];
  for (let g = 0; g < numGeoms; g++) {
    const seen = new Set<number>();
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
    out.push(list);
  }
  return out;
}

// --- bbox / transform helpers (mirror topojson-client signatures) ---

export function bbox(
  client: CtopoClient,
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
async function collectBoundaryArcs(
  client: CtopoClient,
  inputs: ReadonlyArray<LayerSelection>,
  signal: AbortSignal | undefined,
): Promise<number[]> {
  if (inputs.length === 0) return [];
  const numArcs = client.meta.numArcs;
  const fwdCount = new Int32Array(numArcs);
  const revCount = new Int32Array(numArcs);

  // Load every needed layer's CSR in parallel.
  const layerCsr = await Promise.all(
    inputs.map((input) => client.layerGeometry(input.layer, signal)),
  );

  for (let i = 0; i < inputs.length; i++) {
    const csr = layerCsr[i];
    for (const geomIdx of inputs[i].indices) {
      const ringStart = csr.polyOffsets[geomIdx];
      const ringEnd = csr.polyOffsets[geomIdx + 1];
      for (let r = ringStart; r < ringEnd; r++) {
        const arcStart = csr.ringOffsets[r];
        const arcEnd = csr.ringOffsets[r + 1];
        for (let a = arcStart; a < arcEnd; a++) {
          const signed = csr.arcRefs[a];
          if (signed >= 0) fwdCount[signed]++;
          else revCount[~signed]++;
        }
      }
    }
  }

  const boundary: number[] = [];
  for (let i = 0; i < numArcs; i++) {
    const fwd = fwdCount[i];
    const rev = revCount[i];
    if (fwd === rev) continue;
    if (fwd > rev) boundary.push(i);
    else boundary.push(~i);
  }
  return boundary;
}

function absArcIds(signed: ReadonlyArray<number>): number[] {
  const out: number[] = new Array(signed.length);
  for (let i = 0; i < signed.length; i++) {
    const s = signed[i];
    out[i] = s >= 0 ? s : ~s;
  }
  return out;
}

// --- Arc endpoint lookup (per-arc bytes from the fetcher) ---

interface EndpointLookup {
  start(signedArcId: number): string;
  end(signedArcId: number): string;
}

function makeEndpointLookup(
  arcBytes: ReadonlyMap<number, Uint8Array>,
  client: CtopoClient,
): EndpointLookup {
  const isQuantized = client.transform !== null;

  function readStart(arcId: number): [number, number] {
    const bytes = requireArcBytes(arcBytes, arcId);
    if (isQuantized) {
      // First (dx, dy) varint pair = absolute first point (delta from origin).
      const dx = readVarintZigzag(bytes, 0);
      const dy = readVarintZigzag(bytes, dx.consumed);
      return [dx.value, dy.value];
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return [view.getFloat64(0, true), view.getFloat64(8, true)];
  }

  function readEnd(arcId: number): [number, number] {
    const bytes = requireArcBytes(arcBytes, arcId);
    if (isQuantized) {
      // Quantized arc is varint-encoded deltas — walk every pair to
      // accumulate onto the absolute end point.
      let x = 0;
      let y = 0;
      let off = 0;
      while (off < bytes.byteLength) {
        const dx = readVarintZigzag(bytes, off);
        off += dx.consumed;
        const dy = readVarintZigzag(bytes, off);
        off += dy.consumed;
        x += dx.value;
        y += dy.value;
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

interface Fragment extends Array<number> {
  start: string;
  end: string;
}

// Stitches a flat list of signed boundary arc ids into closed rings —
// the topojson-client/src/stitch.js algorithm, taking its endpoint
// lookup from per-arc fetched bytes rather than a single in-memory
// ArrayBuffer so individual arcs can be Range-fetched on demand.
function stitchArcs(
  arcs: ReadonlyArray<number>,
  endpoints: EndpointLookup,
): number[][] {
  const stitched = new Set<number>();
  const fragmentByStart = new Map<string, Fragment>();
  const fragmentByEnd = new Map<string, Fragment>();
  const fragments: number[][] = [];
  const remaining = arcs.slice();

  for (const i of remaining) {
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
        const fg: Fragment = g === f ? f : (f.concat(g) as Fragment);
        fg.start = f.start;
        fg.end = g.end;
        fragmentByStart.set(fg.start, fg);
        fragmentByEnd.set(fg.end, fg);
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
          const gf: Fragment = g === f ? f : (g.concat(f) as Fragment);
          gf.start = g.start;
          gf.end = f.end;
          fragmentByStart.set(gf.start, gf);
          fragmentByEnd.set(gf.end, gf);
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
    byEnd: Map<string, Fragment>,
    byStart: Map<string, Fragment>,
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
  for (const i of remaining) {
    if (!stitched.has(i < 0 ? ~i : i)) fragments.push([i]);
  }
  return fragments;
}

// --- decodeRing (per-arc bytes) ---

function decodeRing(
  arcIds: ReadonlyArray<number>,
  arcBytes: ReadonlyMap<number, Uint8Array>,
  client: CtopoClient,
): number[][] {
  const isQuantized = client.transform !== null;
  const t = client.transform;
  const ring: number[][] = [];

  for (const signed of arcIds) {
    const arcId = signed >= 0 ? signed : ~signed;
    const forward = signed >= 0;
    const bytes = requireArcBytes(arcBytes, arcId);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const points = decodeArcPoints(view, isQuantized, t);
    if (!forward) points.reverse();
    // Drop last point of each arc — it's shared with the next arc's
    // start; the final ring closure point is appended explicitly.
    for (let i = 0; i < points.length - 1; i++) ring.push(points[i]);
  }

  if (ring.length > 0) ring.push(ring[0]);
  return ring;
}

function decodeArcPoints(
  view: DataView,
  isQuantized: boolean,
  t: TransformDef,
): number[][] {
  if (isQuantized) {
    if (t === null) throw new Error("ctopo: quantized arc with null transform");
    const bytes = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
    const points: number[][] = [];
    let x = 0;
    let y = 0;
    let off = 0;
    while (off < bytes.byteLength) {
      const dx = readVarintZigzag(bytes, off);
      off += dx.consumed;
      const dy = readVarintZigzag(bytes, off);
      off += dy.consumed;
      x += dx.value;
      y += dy.value;
      points.push([
        x * t.scale[0] + t.translate[0],
        y * t.scale[1] + t.translate[1],
      ]);
    }
    return points;
  }
  const numPoints = view.byteLength / 16;
  const points: number[][] = new Array(numPoints);
  for (let i = 0; i < numPoints; i++) {
    const off = i * 16;
    points[i] = [view.getFloat64(off, true), view.getFloat64(off + 8, true)];
  }
  return points;
}

// --- ringArea (shoelace) ---

function ringArea(ring: ReadonlyArray<ReadonlyArray<number>>): number {
  let area = 0;
  const n = ring.length;
  if (n < 3) return 0;
  let b = ring[n - 1];
  for (let i = 0; i < n; i++) {
    const a = b;
    b = ring[i];
    area += a[0] * b[1] - a[1] * b[0];
  }
  return Math.abs(area);
}
