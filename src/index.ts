// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * cloud-topo — cloud-optimized container + topojson-style primitives.
 * Holds a quantized topology, per-layer geometry, and per-feature
 * properties in one HTTP-Range-friendly file; primitives mirror
 * topojson-client (`merge`, `mergeArcs`, `neighbors`, `bbox`, …).
 *
 * The library is worker-backed end-to-end: `CtopoClient` proxies to a
 * dedicated Worker that owns the network, decompression, and merge
 * math; results cross the boundary as transferable typed arrays and
 * the proxy rebuilds GeoJSON / nested-array forms on the main thread.
 *
 * Encoder lives in the `cloud-topo/encode` subpath and is Node-only.
 */

// Public types from the core layer (re-export only — no in-process
// CtopoCore class on the public surface).
export type {
  ContainerMeta,
  DType,
  LayerGeometry,
  LayerSelection,
  PropertyOverride,
  SectionEntry,
} from "./core/types";

export { StringArray } from "./core/types";

// Reader helpers for in-memory containers (no network involved, no
// worker needed).
export {
  parseContainer,
  parseFooter,
  parseFrontHeader,
  viewSection,
} from "./core/reader";

// FetcherSpec replaces the function-based RangeFetcher slot on the
// public surface. The internal `RangeFetcher` interface is no longer
// exported — function-shaped fetchers can't cross the worker boundary,
// so the only way to feed bytes to the worker is via a spec.
export type { FetcherSpec } from "./core/fetcher";

// Worker-backed client + open helper.
export {
  CtopoClient,
  openContainer,
  type OpenContainerOptions,
} from "./proxy/client";

// `CtopoClientStats` lives in core (the worker computes it) but the
// type itself is part of the public surface.
export type { CtopoClientStats } from "./core/client";

// Top-level merge primitives — proxy wrappers that round-trip through
// the worker.
export {
  bbox,
  merge,
  mergeArcs,
  neighbors,
  transform,
  untransform,
  type MultiPolygonArcs,
} from "./proxy/merge";
