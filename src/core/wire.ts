// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Wire-format types shared by the worker entrypoint and the main-thread
 * proxy. Everything in this file must be structured-cloneable (no
 * functions, no AbortSignals, no class instances) so it can cross
 * postMessage cleanly.
 */

import type { FetcherSpec } from "./fetcher";
import type { DType } from "./types";

// LayerSelection over the wire. The runtime LayerSelection accepts an
// Iterable<number>, but iterables don't serialize — the proxy
// materializes to a plain array before posting.
export interface WireLayerSelection {
  readonly layer: string;
  readonly indices: ReadonlyArray<number>;
}

// OpenContainerOptions stripped of the in-process-only fields
// (`signal`, `fetcher`) — those don't cross the boundary. `fetcher` is
// passed separately as a FetcherSpec; abort signals are proxied via
// per-request `{ id, abort: true }` messages.
export interface WireOpenContainerOptions {
  readonly frontPrefetchBytes?: number;
  readonly backPrefetchBytes?: number;
  readonly coalesceGapBytes?: number;
  readonly coalesceGapByFamily?: Readonly<Record<string, number>>;
  readonly maxChunkBytes?: number;
  readonly maxParallelRanges?: number;
  readonly byteRangeCacheBytes?: number;
  readonly arcCoordsPrefetchBytes?: number;
  readonly arcOffsetsPrefetch?: boolean;
  readonly maxRangesPerRequest?: number;
  readonly multiRangeEnabled?: boolean;
}

// Per-property wire result. The worker hands the underlying
// `ArrayBuffer` to the proxy with a `dtype` tag so the proxy can
// reconstruct the right typed-array view. `byteOffset` / `byteLength`
// are 0 / buffer.byteLength when the worker copied the slice into a
// fresh buffer — and we always copy out of the cache, so the proxy
// can wrap with a single `new TypedArray(buf)` call.
export interface WirePropertyResult {
  readonly buffer: ArrayBufferLike;
  readonly dtype: DType;
}

export interface WireStringsResult {
  readonly buffer: ArrayBufferLike;
}

export interface WireLayerGeometryResult {
  readonly polyOffsets: ArrayBufferLike;
  readonly ringOffsets: ArrayBufferLike;
  readonly arcRefs: ArrayBufferLike;
  readonly multiPolyBreaks: ArrayBufferLike;
}

export interface WireFlatMultiPolygon {
  readonly type: "MultiPolygon";
  readonly coords: ArrayBufferLike;
  readonly ringStarts: ArrayBufferLike;
  readonly ringEnds: ArrayBufferLike;
  readonly polyRingStarts: ArrayBufferLike;
}

export interface WireFlatMultiPolygonArcs {
  readonly type: "MultiPolygon";
  readonly arcs: ArrayBufferLike;
  readonly ringStarts: ArrayBufferLike;
  readonly ringEnds: ArrayBufferLike;
  readonly polyRingStarts: ArrayBufferLike;
}

export interface WireFlatNeighbors {
  readonly offsets: ArrayBufferLike;
  readonly values: ArrayBufferLike;
}

// Discriminated request union — each variant declares its own payload
// shape so the worker dispatcher and the proxy stay typed end-to-end.
export type WorkerRequest =
  | {
      readonly id: number;
      readonly method: "open";
      readonly args: {
        readonly fetcher: FetcherSpec;
        readonly url: string;
        readonly opts: WireOpenContainerOptions;
      };
    }
  | {
      readonly id: number;
      readonly method: "property";
      readonly args: { readonly name: string };
    }
  | {
      readonly id: number;
      readonly method: "strings";
      readonly args: { readonly name: string };
    }
  | {
      readonly id: number;
      readonly method: "layerGeometry";
      readonly args: { readonly layer: string };
    }
  | {
      readonly id: number;
      readonly method: "mergeFlat";
      readonly args: { readonly selections: ReadonlyArray<WireLayerSelection> };
    }
  | {
      readonly id: number;
      readonly method: "mergeArcsFlat";
      readonly args: { readonly selections: ReadonlyArray<WireLayerSelection> };
    }
  | {
      readonly id: number;
      readonly method: "neighborsFlat";
      readonly args: { readonly layer: string };
    }
  | {
      readonly id: number;
      readonly method: "fetchArcs";
      readonly args: { readonly ids: ReadonlyArray<number> };
    }
  | {
      readonly id: number;
      readonly method: "fetchArcEndpoints";
      readonly args: { readonly ids: ReadonlyArray<number> };
    }
  | {
      readonly id: number;
      readonly method: "getStats";
      readonly args: Record<string, never>;
    }
  | {
      readonly id: number;
      readonly method: "resetStats";
      readonly args: Record<string, never>;
    }
  | {
      readonly id: number;
      readonly method: "tallyUseful";
      readonly args: { readonly name: string; readonly bytes: number };
    }
  | {
      readonly id: number;
      readonly method: "hasArcEndpointsSection";
      readonly args: Record<string, never>;
    }
  | {
      readonly id: number;
      readonly method: "close";
      readonly args: Record<string, never>;
    };

// Sent on its own (no `method`) so the dispatcher can short-circuit
// without paying the per-method overhead.
export interface WorkerAbort {
  readonly id: number;
  readonly abort: true;
}

export type WorkerResponse =
  | { readonly id: number; readonly ok: true; readonly result: unknown }
  | {
      readonly id: number;
      readonly ok: false;
      readonly error: { readonly name: string; readonly message: string };
    };
