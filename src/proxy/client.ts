// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Main-thread `CtopoClient` — proxy around the worker that holds the
 * in-process `CtopoCore`. All container reads (`property`, `strings`,
 * `layerGeometry`, `fetchArcs`, …) and the merge primitives execute
 * in the worker; results come back as transferable typed arrays.
 *
 * Sync getters (`meta`, `sections`, `transform`) read from a snapshot
 * captured at open time so common in-render reads don't pay an RPC.
 */

import Worker from "web-worker";

import { type CtopoClientStats } from "../core/client";
import { type FetcherSpec } from "../core/fetcher";
import {
  StringArray,
  type ContainerMeta,
  type DType,
  type LayerGeometry,
  type LayerSelection,
  type SectionEntry,
} from "../core/types";
import type {
  WireFlatMultiPolygon,
  WireFlatMultiPolygonArcs,
  WireFlatNeighbors,
  WireLayerGeometryResult,
  WireLayerSelection,
  WireOpenContainerOptions,
  WirePropertyResult,
  WireStringsResult,
  WorkerRequest,
  WorkerResponse,
} from "../core/wire";

// --- Public options ---

export interface OpenContainerOptions extends WireOpenContainerOptions {
  // Cancel the open call. Method-level signals are accepted on each
  // method.
  readonly signal?: AbortSignal;
  // Where to load the worker bundle from. Default: `new URL(
  // "./worker.js", import.meta.url)`, which resolves to the
  // `dist/worker.js` chunk the build emits alongside `dist/index.js`.
  // Override when the worker file is hosted at a non-default path
  // (CDN, custom dev server, etc.) or when running against TypeScript
  // sources at test time.
  readonly workerUrl?: string | URL;
  // Caller-supplied Worker. Takes precedence over `workerUrl`. Useful
  // for test rigs that pre-spawn a Worker with a non-standard loader
  // (e.g. vite-node, tsx) or for consumers that want to share one
  // worker across multiple opens.
  readonly worker?: Worker;
  // FetcherSpec — how the worker should obtain container bytes.
  // Defaults to `{ kind: "http", url }` using the URL passed to
  // `open(url, opts)`. Use `{ kind: "buffer", bytes }` for in-memory
  // containers (the worker reads the bytes directly — no HTTP).
  readonly fetcher?: FetcherSpec;
}

// --- Public CtopoClient ---

interface OpenSnapshot {
  meta: ContainerMeta;
  sections: ReadonlyArray<SectionEntry>;
  transform: ContainerMeta["transform"];
}

interface PendingCall {
  resolve: (result: unknown) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

export class CtopoClient {
  readonly meta: ContainerMeta;
  readonly sections: ReadonlyArray<SectionEntry>;
  readonly transform: ContainerMeta["transform"];

  private readonly worker: Worker;
  // Strict ownership flag: when true the proxy created the worker and
  // calls .terminate() on close; when false the caller passed a
  // shared worker via `OpenContainerOptions.worker` and is
  // responsible for its lifetime.
  private readonly ownsWorker: boolean;
  private readonly pending = new Map<number, PendingCall>();
  private nextId = 0;
  // `closing` blocks new calls from `call()` while still letting the
  // close RPC itself fire. `closed` flips once the worker has acked
  // the close (or the round-trip failed) and is used to short-circuit
  // double-close.
  private closing = false;
  private closed = false;

  private constructor(args: {
    worker: Worker;
    ownsWorker: boolean;
    snapshot: OpenSnapshot;
  }) {
    this.worker = args.worker;
    this.ownsWorker = args.ownsWorker;
    this.meta = args.snapshot.meta;
    this.sections = args.snapshot.sections;
    this.transform = args.snapshot.transform;
  }

  // --- Factory ---

  static async open(
    url: string,
    opts: OpenContainerOptions = {},
  ): Promise<CtopoClient> {
    const worker =
      opts.worker ??
      new Worker(
        opts.workerUrl !== undefined
          ? typeof opts.workerUrl === "string"
            ? opts.workerUrl
            : opts.workerUrl.href
          : new URL("./worker.js", import.meta.url).href,
        { type: "module" },
      );
    const ownsWorker = opts.worker === undefined;

    // Strip proxy-only options before posting to the worker.
    const wireOpts: WireOpenContainerOptions = {
      frontPrefetchBytes: opts.frontPrefetchBytes,
      backPrefetchBytes: opts.backPrefetchBytes,
      coalesceGapBytes: opts.coalesceGapBytes,
      coalesceGapByFamily: opts.coalesceGapByFamily,
      maxChunkBytes: opts.maxChunkBytes,
      maxParallelRanges: opts.maxParallelRanges,
      byteRangeCacheBytes: opts.byteRangeCacheBytes,
      arcCoordsPrefetchBytes: opts.arcCoordsPrefetchBytes,
      arcOffsetsPrefetch: opts.arcOffsetsPrefetch,
      maxRangesPerRequest: opts.maxRangesPerRequest,
      multiRangeEnabled: opts.multiRangeEnabled,
    };
    const fetcher: FetcherSpec = opts.fetcher ?? { kind: "http", url };

    // Open is its own RPC — we need the meta/sections snapshot back
    // before the proxy can be constructed, so we run a one-shot
    // listener bound to the open request id rather than installing
    // the steady-state dispatcher first. Steady-state hookup happens
    // after open resolves.
    const openId = 1;
    const transfer: ArrayBufferLike[] =
      fetcher.kind === "buffer" ? [fetcher.bytes.buffer] : [];
    const req: WorkerRequest = {
      id: openId,
      method: "open",
      args: { fetcher, url, opts: wireOpts },
    };

    const snapshot = await new Promise<OpenSnapshot>((resolve, reject) => {
      const onMessage = (e: { data: WorkerResponse }): void => {
        const r = e.data;
        if (r.id !== openId) return;
        worker.removeEventListener("message", onMessage);
        if (r.ok) {
          resolve(r.result as OpenSnapshot);
        } else {
          reject(new Error(r.error.message));
        }
      };
      worker.addEventListener("message", onMessage);
      if (opts.signal !== undefined) {
        if (opts.signal.aborted) {
          worker.removeEventListener("message", onMessage);
          reject(new DOMException("Aborted", "AbortError"));
          return;
        }
        opts.signal.addEventListener(
          "abort",
          () => {
            worker.removeEventListener("message", onMessage);
            worker.postMessage({ id: openId, abort: true });
            reject(new DOMException("Aborted", "AbortError"));
          },
          { once: true },
        );
      }
      worker.postMessage(req, transfer);
    });

    const proxy = new CtopoClient({ worker, ownsWorker, snapshot });
    proxy.installMessageHandler();
    return proxy;
  }

  // --- RPC plumbing ---

  private installMessageHandler(): void {
    this.worker.addEventListener("message", (e: { data: WorkerResponse }) => {
      const r = e.data;
      const pending = this.pending.get(r.id);
      if (pending === undefined) return; // already aborted / mismatched id
      this.pending.delete(r.id);
      if (pending.onAbort !== undefined && pending.signal !== undefined) {
        pending.signal.removeEventListener("abort", pending.onAbort);
      }
      if (r.ok) {
        pending.resolve(r.result);
      } else {
        pending.reject(new Error(r.error.message));
      }
    });
  }

  private call<T>(
    method: WorkerRequest["method"],
    args: unknown,
    signal: AbortSignal | undefined,
    transfer: ArrayBufferLike[] = [],
  ): Promise<T> {
    // The close RPC needs to fire even while `closing` is true; every
    // other method is rejected once close has begun.
    if (this.closed || (this.closing && method !== "close")) {
      return Promise.reject(new Error("ctopo: client is closed"));
    }
    const id = ++this.nextId;
    return new Promise<T>((resolve, reject) => {
      if (signal !== undefined && signal.aborted) {
        reject(new DOMException("Aborted", "AbortError"));
        return;
      }
      const pending: PendingCall = {
        resolve: resolve as (r: unknown) => void,
        reject,
        signal,
      };
      if (signal !== undefined) {
        pending.onAbort = (): void => {
          this.pending.delete(id);
          this.worker.postMessage({ id, abort: true });
          reject(new DOMException("Aborted", "AbortError"));
        };
        signal.addEventListener("abort", pending.onAbort, { once: true });
      }
      this.pending.set(id, pending);
      this.worker.postMessage({ id, method, args }, transfer);
    });
  }

  // --- Public methods (mirror CtopoCore) ---

  async property(name: string, signal?: AbortSignal): Promise<ArrayBufferView> {
    const r = await this.call<WirePropertyResult>("property", { name }, signal);
    return viewForDtype(r.buffer, r.dtype);
  }

  async strings(name: string, signal?: AbortSignal): Promise<StringArray> {
    const r = await this.call<WireStringsResult>("strings", { name }, signal);
    return new StringArray(new Uint8Array(r.buffer));
  }

  async layerGeometry(
    layer: string,
    signal?: AbortSignal,
  ): Promise<LayerGeometry> {
    const r = await this.call<WireLayerGeometryResult>(
      "layerGeometry",
      { layer },
      signal,
    );
    return {
      polyOffsets: new Uint32Array(r.polyOffsets),
      ringOffsets: new Uint32Array(r.ringOffsets),
      arcRefs: new Int32Array(r.arcRefs),
      multiPolyBreaks: new Uint32Array(r.multiPolyBreaks),
    };
  }

  // Worker-internal — exposed for advanced callers that want raw arc
  // bytes. Cross-boundary cost is non-trivial (structured clone of a
  // Map of Uint8Arrays); most callers should route through
  // `merge` / `mergeArcs` instead.
  fetchArcs(
    ids: Iterable<number>,
    signal?: AbortSignal,
  ): Promise<Map<number, Uint8Array>> {
    return this.call("fetchArcs", { ids: Array.from(ids) }, signal);
  }

  fetchArcEndpoints(
    ids: Iterable<number>,
    signal?: AbortSignal,
  ): Promise<Map<number, Int32Array>> {
    return this.call("fetchArcEndpoints", { ids: Array.from(ids) }, signal);
  }

  hasArcEndpointsSection(): Promise<boolean> {
    return this.call("hasArcEndpointsSection", {}, undefined);
  }

  getStats(): Promise<CtopoClientStats> {
    return this.call("getStats", {}, undefined);
  }

  resetStats(): Promise<void> {
    return this.call<null>("resetStats", {}, undefined).then(() => undefined);
  }

  tallyUseful(name: string, bytes: number): Promise<void> {
    return this.call<null>("tallyUseful", { name, bytes }, undefined).then(
      () => undefined,
    );
  }

  // --- Internal: merge primitives use these to get flat results ---

  mergeFlatWire(
    selections: ReadonlyArray<LayerSelection>,
    signal?: AbortSignal,
  ): Promise<WireFlatMultiPolygon> {
    return this.call<WireFlatMultiPolygon>(
      "mergeFlat",
      { selections: toWireSelections(selections) },
      signal,
    );
  }

  mergeArcsFlatWire(
    selections: ReadonlyArray<LayerSelection>,
    signal?: AbortSignal,
  ): Promise<WireFlatMultiPolygonArcs> {
    return this.call<WireFlatMultiPolygonArcs>(
      "mergeArcsFlat",
      { selections: toWireSelections(selections) },
      signal,
    );
  }

  neighborsFlatWire(
    layer: string,
    signal?: AbortSignal,
  ): Promise<WireFlatNeighbors> {
    return this.call<WireFlatNeighbors>("neighborsFlat", { layer }, signal);
  }

  // --- Lifecycle ---

  async close(): Promise<void> {
    if (this.closed || this.closing) return;
    this.closing = true;
    try {
      await this.call<null>("close", {}, undefined);
    } catch {
      // If the close round-trip itself fails (worker died, message
      // lost), still proceed with local cleanup — the proxy is dead
      // either way.
    } finally {
      this.closed = true;
      // The worker terminates itself after replying to "close"; the
      // proxy still .terminate()s defensively.
      if (this.ownsWorker) this.worker.terminate();
      const stale = this.pending;
      this.pending.clear();
      const err = new Error("ctopo: client closed");
      for (const p of stale.values()) p.reject(err);
    }
  }
}

export async function openContainer(
  url: string,
  opts?: OpenContainerOptions,
): Promise<CtopoClient> {
  return CtopoClient.open(url, opts);
}

// --- Helpers ---

function toWireSelections(
  selections: ReadonlyArray<LayerSelection>,
): WireLayerSelection[] {
  return selections.map((s) => ({
    layer: s.layer,
    indices: Array.isArray(s.indices)
      ? (s.indices as ReadonlyArray<number>)
      : Array.from(s.indices),
  }));
}

function viewForDtype(buffer: ArrayBufferLike, dtype: DType): ArrayBufferView {
  switch (dtype) {
    case "i8":
      return new Int8Array(buffer);
    case "u8":
      return new Uint8Array(buffer);
    case "i16":
      return new Int16Array(buffer);
    case "u16":
      return new Uint16Array(buffer);
    case "i32":
      return new Int32Array(buffer);
    case "u32":
      return new Uint32Array(buffer);
    case "f64":
      return new Float64Array(buffer);
    case "blob":
      return new Uint8Array(buffer);
    case "strings":
      // `strings` sections are handled by `client.strings(name)` —
      // calling `client.property(...)` on a strings section is an
      // API misuse, but we still return something coherent.
      return new Uint8Array(buffer);
    default: {
      const _exhaustive: never = dtype;
      void _exhaustive;
      throw new Error(`ctopo: unknown dtype ${dtype as string}`);
    }
  }
}
