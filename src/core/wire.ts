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
  // Merge-phase worker pool. Compute workers run whole merges in
  // parallel; the coordinator dispatches per-merge with SAB-backed
  // CSR views (post zstd-into-shared-memory). `null` (or omitted) =
  // no pool, run merges inline on the coordinator.
  readonly pool?: { readonly size: number } | null;
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

// `clientId` namespaces every request/response to one CtopoClient
// proxy. The proxy generates a random clientId per instance; the worker
// tracks state in a `Map<clientId, ...>`. With this in place a single
// worker can serve multiple proxies — including proxies on different
// threads attached via `addPort` — and their `id` counters never have
// to coordinate (each proxy's per-instance `nextId` is filtered by
// `clientId` on the response side, so ID collisions across clients are
// harmless).
//
// Discriminated request union — each variant declares its own payload
// shape so the worker dispatcher and the proxy stay typed end-to-end.
export type WorkerRequest =
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "open";
      readonly args: {
        readonly fetcher: FetcherSpec;
        readonly url: string;
        readonly opts: WireOpenContainerOptions;
      };
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "property";
      readonly args: { readonly name: string };
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "strings";
      readonly args: { readonly name: string };
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "layerGeometry";
      readonly args: { readonly layer: string };
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "mergeFlat";
      readonly args: { readonly selections: ReadonlyArray<WireLayerSelection> };
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "mergeArcsFlat";
      readonly args: { readonly selections: ReadonlyArray<WireLayerSelection> };
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "neighborsFlat";
      readonly args: { readonly layer: string };
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "fetchArcs";
      readonly args: { readonly ids: ReadonlyArray<number> };
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "fetchArcEndpoints";
      readonly args: { readonly ids: ReadonlyArray<number> };
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "getStats";
      readonly args: Record<string, never>;
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "resetStats";
      readonly args: Record<string, never>;
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "tallyUseful";
      readonly args: { readonly name: string; readonly bytes: number };
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "hasArcEndpointsSection";
      readonly args: Record<string, never>;
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly method: "close";
      readonly args: Record<string, never>;
    };

// Sent on its own (no `method`) so the dispatcher can short-circuit
// without paying the per-method overhead.
export interface WorkerAbort {
  readonly clientId: number;
  readonly id: number;
  readonly abort: true;
}

// Control message — used to attach an additional MessagePort to an
// already-running worker so another thread can talk to the same
// container without spawning a second worker. The worker hooks
// `port.onmessage` and routes its requests by `clientId` just like the
// primary connection.
//
// The port is embedded in the message body (not as a separate
// `event.ports` slot) so this works uniformly in browsers and in
// Node's `worker_threads` shim: Node exposes transferred MessagePorts
// only through structured deserialization of the cloned data,
// whereas browsers expose them via both `event.data` and
// `event.ports`. The port must also appear in the postMessage
// transfer list for both runtimes.
export interface WorkerAddPort {
  readonly kind: "addPort";
  readonly port: MessagePort;
}

export type WorkerResponse =
  | {
      readonly clientId: number;
      readonly id: number;
      readonly ok: true;
      readonly result: unknown;
    }
  | {
      readonly clientId: number;
      readonly id: number;
      readonly ok: false;
      readonly error: { readonly name: string; readonly message: string };
    };
