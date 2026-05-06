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
import { type LayerGeometry, type LayerSelection } from "./types";

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
  const polygons = await buildInputPolygons(client, inputs, signal);
  if (polygons.length === 0) return { type: "MultiPolygon", arcs: [] };

  const polygonsByArc = buildPolygonsByArc(polygons);
  const groups = groupPolygonsByConnectivity(polygons, polygonsByArc);

  // Collect every group's exterior arcs in one go so we can fetch
  // arc-endpoint bytes in a single batched call rather than one
  // fetch per group.
  const allExteriorArcs: number[] = [];
  const groupExteriorArcs: number[][] = groups.map((group) => {
    const ext = exteriorArcsForGroup(group, polygonsByArc);
    for (const a of ext) allExteriorArcs.push(a);
    return ext;
  });
  if (allExteriorArcs.length === 0) return { type: "MultiPolygon", arcs: [] };

  const arcBytes = await client.fetchArcs(absArcIds(allExteriorArcs), signal);
  const endpoints = makeEndpointLookup(arcBytes, client);

  const out: number[][][] = [];
  for (const ext of groupExteriorArcs) {
    if (ext.length === 0) continue;
    const rings = stitchArcs(ext, endpoints);
    if (rings.length === 0) continue;
    // At most one ring in a connected group can be the exterior;
    // pick the largest by area. Rest become holes of the same
    // polygon. Area is computed from the arc-id stitching using
    // arc endpoints — but to avoid decoding coords here we defer
    // ordering to the callers of mergeArcs that actually care.
    // Topojson-client's mergeArcs returns arcs in stitch order;
    // we match that contract — `merge` does the area-ordered
    // exterior selection.
    out.push(rings);
  }
  return { type: "MultiPolygon", arcs: out };
}

// `merge` — union of inputs as decoded GeoJSON MultiPolygon. Fetches
// boundary arc coord bytes, stitches into rings, decodes, and assembles
// one output polygon per connected component of input polygons (matches
// topojson-client's `merge` semantics). Within each component the
// largest-area stitched ring becomes the exterior; the rest are holes.
export async function merge(
  client: CtopoClient,
  inputs: ReadonlyArray<LayerSelection>,
  signal?: AbortSignal,
): Promise<MultiPolygon> {
  const polygons = await buildInputPolygons(client, inputs, signal);
  if (polygons.length === 0) return { type: "MultiPolygon", coordinates: [] };

  const polygonsByArc = buildPolygonsByArc(polygons);
  const groups = groupPolygonsByConnectivity(polygons, polygonsByArc);

  const allExteriorArcs: number[] = [];
  const groupExteriorArcs: number[][] = groups.map((group) => {
    const ext = exteriorArcsForGroup(group, polygonsByArc);
    for (const a of ext) allExteriorArcs.push(a);
    return ext;
  });
  if (allExteriorArcs.length === 0)
    return { type: "MultiPolygon", coordinates: [] };

  const arcBytes = await client.fetchArcs(absArcIds(allExteriorArcs), signal);
  const endpoints = makeEndpointLookup(arcBytes, client);

  const coordinates: number[][][][] = [];
  for (const ext of groupExteriorArcs) {
    if (ext.length === 0) continue;
    const rings = stitchArcs(ext, endpoints);
    const decoded = rings
      .map((ring) => decodeRing(ring, arcBytes, client))
      .filter((r) => r.length >= 4);
    if (decoded.length === 0) continue;
    if (decoded.length === 1) {
      coordinates.push(decoded);
      continue;
    }
    // Largest area = exterior; others are holes inside it. In planar
    // topology this is exact — a hole strictly contained in the
    // exterior is necessarily smaller in area.
    const indexed = decoded.map((r, i) => ({ area: ringArea(r), idx: i }));
    indexed.sort((a, b) => b.area - a.area);
    coordinates.push(indexed.map((x) => decoded[x.idx]));
  }
  return { type: "MultiPolygon", coordinates };
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
// One TopoJSON polygon entry as a list of arc rings. Distinct entries
// of the same MultiPolygon (e.g. island-in-lake) are kept separate so
// connectivity grouping can recover the original polygon structure.
interface InputPolygon {
  readonly rings: ReadonlyArray<ReadonlyArray<number>>;
}

async function buildInputPolygons(
  client: CtopoClient,
  inputs: ReadonlyArray<LayerSelection>,
  signal: AbortSignal | undefined,
): Promise<InputPolygon[]> {
  if (inputs.length === 0) return [];
  const layerCsr = await Promise.all(
    inputs.map((input) => client.layerGeometry(input.layer, signal)),
  );
  const polygons: InputPolygon[] = [];
  for (let i = 0; i < inputs.length; i++) {
    expandLayerPolygons(layerCsr[i], inputs[i].indices, polygons);
  }
  return polygons;
}

function expandLayerPolygons(
  csr: LayerGeometry,
  geomIndices: Iterable<number>,
  out: InputPolygon[],
): void {
  // Index the sparse multi_poly_breaks table by geom for O(1) lookup
  // per geom we visit. Empty in the typical single-Polygon-only case.
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

  for (const g of geomIndices) {
    const ringStart = csr.polyOffsets[g];
    const ringEnd = csr.polyOffsets[g + 1];
    if (ringStart === ringEnd) continue; // non-polygon geom
    const breaks = breaksByGeom.get(g);
    if (breaks === undefined) {
      out.push(makeInputPolygon(csr, ringStart, ringEnd));
      continue;
    }
    // Polygon entry boundaries within geom g: ringStart, *breaks, ringEnd.
    let prev = ringStart;
    for (const b of breaks) {
      out.push(makeInputPolygon(csr, prev, b));
      prev = b;
    }
    out.push(makeInputPolygon(csr, prev, ringEnd));
  }
}

function makeInputPolygon(
  csr: LayerGeometry,
  ringStart: number,
  ringEnd: number,
): InputPolygon {
  const rings: number[][] = [];
  for (let r = ringStart; r < ringEnd; r++) {
    const arcStart = csr.ringOffsets[r];
    const arcEnd = csr.ringOffsets[r + 1];
    const ring = new Array<number>(arcEnd - arcStart);
    for (let a = arcStart; a < arcEnd; a++) ring[a - arcStart] = csr.arcRefs[a];
    rings.push(ring);
  }
  return { rings };
}

// Map unsigned arc id → indices of the InputPolygons that reference it.
// An arc with a single membership is on the exterior of its group; an
// arc with ≥2 memberships is interior (cancels during merge).
function buildPolygonsByArc(
  polygons: ReadonlyArray<InputPolygon>,
): Map<number, number[]> {
  const m = new Map<number, number[]>();
  for (let p = 0; p < polygons.length; p++) {
    for (const ring of polygons[p].rings) {
      for (const signed of ring) {
        const id = signed < 0 ? ~signed : signed;
        let arr = m.get(id);
        if (arr === undefined) {
          arr = [];
          m.set(id, arr);
        }
        arr.push(p);
      }
    }
  }
  return m;
}

// BFS: two input polygons are connected iff they share any arc.
// Each connected component becomes one polygon in the merged output —
// preserving the original topology's polygon grouping (an island sitting
// in a lake remains its own polygon, separate from the surrounding mass).
function groupPolygonsByConnectivity(
  polygons: ReadonlyArray<InputPolygon>,
  polygonsByArc: ReadonlyMap<number, ReadonlyArray<number>>,
): InputPolygon[][] {
  const visited = new Uint8Array(polygons.length);
  const groups: InputPolygon[][] = [];
  for (let start = 0; start < polygons.length; start++) {
    if (visited[start] !== 0) continue;
    visited[start] = 1;
    const group: InputPolygon[] = [];
    const stack: number[] = [start];
    while (stack.length > 0) {
      const idx = stack.pop() as number;
      group.push(polygons[idx]);
      for (const ring of polygons[idx].rings) {
        for (const signed of ring) {
          const id = signed < 0 ? ~signed : signed;
          const neighbors = polygonsByArc.get(id);
          if (neighbors === undefined) continue;
          for (const other of neighbors) {
            if (visited[other] === 0) {
              visited[other] = 1;
              stack.push(other);
            }
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
  group: ReadonlyArray<InputPolygon>,
  polygonsByArc: ReadonlyMap<number, ReadonlyArray<number>>,
): number[] {
  const out: number[] = [];
  for (const poly of group) {
    for (const ring of poly.rings) {
      for (const signed of ring) {
        const id = signed < 0 ? ~signed : signed;
        const memberships = polygonsByArc.get(id);
        if (memberships !== undefined && memberships.length < 2) {
          out.push(signed);
        }
      }
    }
  }
  return out;
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
