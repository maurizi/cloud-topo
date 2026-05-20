// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Pure flat-CSR → nested-array decoders shared by the in-process merge
 * wrappers (core/merge.ts) and the main-thread proxy rebuild
 * (proxy/rebuild.ts). The two entry points differ only in how they
 * obtain the typed arrays — already-typed from an in-process merge vs
 * wrapped from transferred ArrayBuffers — but the CSR walk that
 * materializes the nested GeoJSON / arc-id shape is identical, so it
 * lives here once.
 */

import { type MultiPolygon } from "geojson";

import { type MultiPolygonArcs } from "./merge";

export function multiPolygonFromFlat(
  coords: Float64Array,
  ringStarts: Uint32Array,
  ringEnds: Uint32Array,
  polyRingStarts: Uint32Array,
): MultiPolygon {
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

export function multiPolygonArcsFromFlat(
  arcs: Int32Array,
  ringStarts: Uint32Array,
  ringEnds: Uint32Array,
  polyRingStarts: Uint32Array,
): MultiPolygonArcs {
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

export function neighborsFromFlat(
  offsets: Uint32Array,
  values: Uint32Array,
): readonly (readonly number[])[] {
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
