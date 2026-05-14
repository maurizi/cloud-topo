// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Main-thread helpers that materialize GeoJSON / nested-array forms
 * from the worker's transferable flat shapes. The worker hands these
 * routines an `ArrayBuffer` per typed-array field; the proxy here
 * wraps each in the right view, then walks the CSR to produce the
 * shape consumers expect.
 *
 * Kept separate from `proxy/merge.ts` so the rebuild loops can be
 * unit-tested without spawning a worker, and so the proxy `merge.ts`
 * stays a one-liner per method.
 */

import { type MultiPolygon } from "geojson";

import { type MultiPolygonArcs } from "../core/merge";
import type {
  WireFlatMultiPolygon,
  WireFlatMultiPolygonArcs,
  WireFlatNeighbors,
} from "../core/wire";

export function rebuildMultiPolygon(flat: WireFlatMultiPolygon): MultiPolygon {
  const coords = new Float64Array(flat.coords);
  const ringStarts = new Uint32Array(flat.ringStarts);
  const ringEnds = new Uint32Array(flat.ringEnds);
  const polyRingStarts = new Uint32Array(flat.polyRingStarts);
  const numPolys = polyRingStarts.length - 1;
  const coordinates = new Array<number[][][]>(numPolys);
  for (let p = 0; p < numPolys; p++) {
    const rStart = polyRingStarts[p];
    const rEnd = polyRingStarts[p + 1];
    const polygon = new Array<number[][]>(rEnd - rStart);
    for (let r = rStart; r < rEnd; r++) {
      const pStart = ringStarts[r];
      const pEnd = ringEnds[r];
      const ring = new Array<number[]>(pEnd - pStart);
      for (let i = 0; i < ring.length; i++) {
        const off = (pStart + i) * 2;
        ring[i] = [coords[off], coords[off + 1]];
      }
      polygon[r - rStart] = ring;
    }
    coordinates[p] = polygon;
  }
  return { type: "MultiPolygon", coordinates };
}

export function rebuildMultiPolygonArcs(
  flat: WireFlatMultiPolygonArcs,
): MultiPolygonArcs {
  const arcs = new Int32Array(flat.arcs);
  const ringStarts = new Uint32Array(flat.ringStarts);
  const ringEnds = new Uint32Array(flat.ringEnds);
  const polyRingStarts = new Uint32Array(flat.polyRingStarts);
  const numPolys = polyRingStarts.length - 1;
  const out = new Array<number[][]>(numPolys);
  for (let p = 0; p < numPolys; p++) {
    const rStart = polyRingStarts[p];
    const rEnd = polyRingStarts[p + 1];
    const polygon = new Array<number[]>(rEnd - rStart);
    for (let r = rStart; r < rEnd; r++) {
      const aStart = ringStarts[r];
      const aEnd = ringEnds[r];
      const ring = new Array<number>(aEnd - aStart);
      for (let i = 0; i < ring.length; i++) ring[i] = arcs[aStart + i];
      polygon[r - rStart] = ring;
    }
    out[p] = polygon;
  }
  return { type: "MultiPolygon", arcs: out };
}

export function rebuildNeighbors(
  flat: WireFlatNeighbors,
): readonly (readonly number[])[] {
  const offsets = new Uint32Array(flat.offsets);
  const values = new Uint32Array(flat.values);
  const numGeoms = offsets.length - 1;
  const out = new Array<number[]>(numGeoms);
  for (let g = 0; g < numGeoms; g++) {
    const start = offsets[g];
    const end = offsets[g + 1];
    const list = new Array<number>(end - start);
    for (let i = 0; i < list.length; i++) list[i] = values[start + i];
    out[g] = list;
  }
  return out;
}
