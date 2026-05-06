// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * cloud-topo — cloud-optimized container + topojson-style primitives.
 * Holds a quantized topology, per-layer geometry, and per-feature properties
 * in one HTTP-Range-friendly file; primitives mirror topojson-client
 * (`merge`, `mergeArcs`, `neighbors`, `feature`, `bbox`, …) but fetch
 * only the arc coord slices they need.
 *
 * Public surface only — implementation lives in sibling modules.
 */

export type {
  ContainerMeta,
  DType,
  LayerGeometry,
  LayerSelection,
  PropertyOverride,
  SectionEntry,
} from "./types";

export { StringArray } from "./types";

// Encoder lives in `./encode` and uses Node `fs`; import that subpath
// directly from build-time tools (process-geojson, update-voting-data,
// etc.) so client / worker bundles don't pull `fs` in transitively.
export {
  parseContainer,
  parseFooter,
  parseFrontHeader,
  viewSection,
} from "./reader";
export {
  CtopoClient,
  openContainer,
  type CtopoClientStats,
  type OpenContainerOptions,
} from "./client";
export {
  type FetchPriority,
  type MultiRangeResult,
  type RangeFetcher,
  makeBufferFetcher,
  makeHttpFetcher,
  makeRangeFetcher,
} from "./fetcher";
export {
  bbox,
  merge,
  mergeArcs,
  neighbors,
  transform,
  untransform,
  type MultiPolygonArcs,
} from "./merge";
