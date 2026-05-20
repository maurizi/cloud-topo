// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Main-thread merge / mergeArcs / neighbors / bbox / transform /
 * untransform — all delegate the heavy lifting to the worker via the
 * proxy client, then rebuild the public GeoJSON / nested-array shape.
 *
 * Sync getters (`bbox`, `transform`, `untransform`) read the proxy's
 * cached metadata and don't cross the worker boundary.
 */

import { type MultiPolygon } from "geojson";

import { type MultiPolygonArcs } from "../core/merge";
import { type LayerSelection } from "../core/types";

import { type CtopoClient } from "./client";
import {
  rebuildMultiPolygon,
  rebuildMultiPolygonArcs,
  rebuildNeighbors,
} from "./rebuild";

export { type MultiPolygonArcs } from "../core/merge";
// transform / untransform are pure point-mappers shared with the core
// layer — re-export the single copy from ./core/util.
export { transform, untransform } from "../core/util";

export async function merge(
  client: CtopoClient,
  inputs: ReadonlyArray<LayerSelection>,
  signal?: AbortSignal,
): Promise<MultiPolygon> {
  const flat = await client.mergeFlatWire(inputs, signal);
  return rebuildMultiPolygon(flat);
}

export async function mergeArcs(
  client: CtopoClient,
  inputs: ReadonlyArray<LayerSelection>,
  signal?: AbortSignal,
): Promise<MultiPolygonArcs> {
  const flat = await client.mergeArcsFlatWire(inputs, signal);
  return rebuildMultiPolygonArcs(flat);
}

export async function neighbors(
  client: CtopoClient,
  layer: string,
  signal?: AbortSignal,
): Promise<readonly (readonly number[])[]> {
  const flat = await client.neighborsFlatWire(layer, signal);
  return rebuildNeighbors(flat);
}

export function bbox(
  client: CtopoClient,
): readonly [number, number, number, number] {
  return client.meta.bbox;
}
