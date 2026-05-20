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

import {
  multiPolygonArcsFromFlat,
  multiPolygonFromFlat,
  neighborsFromFlat,
} from "../core/flat-rebuild";
import { type MultiPolygonArcs } from "../core/merge";
import type {
  WireFlatMultiPolygon,
  WireFlatMultiPolygonArcs,
  WireFlatNeighbors,
} from "../core/wire";

export function rebuildMultiPolygon(flat: WireFlatMultiPolygon): MultiPolygon {
  return multiPolygonFromFlat(
    new Float64Array(flat.coords),
    new Uint32Array(flat.ringStarts),
    new Uint32Array(flat.ringEnds),
    new Uint32Array(flat.polyRingStarts),
  );
}

export function rebuildMultiPolygonArcs(
  flat: WireFlatMultiPolygonArcs,
): MultiPolygonArcs {
  return multiPolygonArcsFromFlat(
    new Int32Array(flat.arcs),
    new Uint32Array(flat.ringStarts),
    new Uint32Array(flat.ringEnds),
    new Uint32Array(flat.polyRingStarts),
  );
}

export function rebuildNeighbors(
  flat: WireFlatNeighbors,
): readonly (readonly number[])[] {
  return neighborsFromFlat(
    new Uint32Array(flat.offsets),
    new Uint32Array(flat.values),
  );
}
