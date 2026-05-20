// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Primitives over a `.ctopo` container — mirroring the topojson-client
 * API (`merge`, `mergeArcs`, `neighbors`) but range-aware: each call
 * fetches only the arc coord slices it needs.
 *
 * This module is the orchestrator: it wires prep → stitch → assemble
 * into the public merge primitives and the GeoJSON-shaped wrappers. The
 * pieces live in sibling modules and are re-exported here so existing
 * importers of "./merge" keep resolving:
 *   - ./merge-prep   — CSR/connectivity/boundary-arc collection
 *   - ./merge-decode — endpoint lookups, Float64Growable, ring decode
 *   - ./merge-stitch — stitch state machine + per-batch assembly
 */

import { type MultiPolygon } from "geojson";

import { type CtopoCore } from "./client";
import {
  multiPolygonArcsFromFlat,
  multiPolygonFromFlat,
  neighborsFromFlat,
} from "./flat-rebuild";
import { prepareMergeGroups } from "./merge-prep";
import {
  assembleArcsResult,
  assembleCoordsResult,
  makeStitchScratch,
  stitchBatchArcs,
  stitchBatchCoords,
  type FlatMultiPolygon,
  type FlatMultiPolygonArcs,
  type StitchBatchContext,
  type StitchScratch,
} from "./merge-stitch";
import { type LayerSelection } from "./types";
import { transform, untransform, type TransformDef } from "./util";

// Re-export the building blocks so "./merge" stays the single import
// surface for the worker, pool, rebuild, and tests.
export * from "./merge-decode";
export * from "./merge-stitch";
export {
  prepareMergeGroups,
  prepareMergeGroupsForPipeline,
} from "./merge-prep";
// Quantization transform helpers live in ./util (one copy shared with
// the proxy layer); re-export so merge-wire's TransformDef etc. resolve
// through "./merge".
export { transform, untransform };
export type { TransformDef };

// --- merge / mergeArcs ---

export interface MultiPolygonArcs {
  readonly type: "MultiPolygon";
  // Three-level array of arc id rings (matches topojson-client's
  // mergeArcs return shape). Outer level = polygons; middle = rings of
  // each polygon; inner = signed arc ids of each ring.
  readonly arcs: number[][][];
}

export interface FlatNeighbors {
  // Per-geometry adjacency in CSR form. Geometry g's neighbors are
  // values[offsets[g] .. offsets[g+1]). offsets length = numGeoms + 1.
  readonly offsets: Uint32Array;
  readonly values: Uint32Array;
}

// Per-client stitch scratch. Compute workers keep their own instance;
// the coordinator inline path pulls one per client and clears it
// between merges so the hash-table backing stays warm.
const stitchScratchByClient = new WeakMap<CtopoCore, StitchScratch>();
function getStitchScratch(client: CtopoCore): StitchScratch {
  let s = stitchScratchByClient.get(client);
  if (s === undefined) {
    s = makeStitchScratch();
    stitchScratchByClient.set(client, s);
  }
  return s;
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
  const ctx: StitchBatchContext = {
    transform: client.transform,
    endpoints,
    arcBytes: undefined,
  };
  const scratch = getStitchScratch(client);
  const result = stitchBatchArcs(ctx, groupExteriorArcs, scratch);
  return assembleArcsResult(result);
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
  const ctx: StitchBatchContext = {
    transform: client.transform,
    endpoints,
    arcBytes,
  };
  const scratch = getStitchScratch(client);
  const result = stitchBatchCoords(ctx, groupExteriorArcs, scratch);
  return assembleCoordsResult(result);
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
  return multiPolygonArcsFromFlat(
    flat.arcs,
    flat.ringStarts,
    flat.ringEnds,
    flat.polyRingStarts,
  );
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
  return multiPolygonFromFlat(
    flat.coords,
    flat.ringStarts,
    flat.ringEnds,
    flat.polyRingStarts,
  );
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
  return neighborsFromFlat(flat.offsets, flat.values);
}
