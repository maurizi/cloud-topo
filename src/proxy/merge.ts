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
import { type ContainerMeta, type LayerSelection } from "../core/types";

import { type CtopoClient } from "./client";
import {
  rebuildMultiPolygon,
  rebuildMultiPolygonArcs,
  rebuildNeighbors,
} from "./rebuild";

export { type MultiPolygonArcs } from "../core/merge";

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

type TransformDef = ContainerMeta["transform"];

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
