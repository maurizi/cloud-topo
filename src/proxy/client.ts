// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Main-thread `CtopoClient` — proxy around the worker that holds the
 * `CtopoCore`. All container reads (`property`, `strings`,
 * `layerGeometry`, `fetchArcs`, …) and the merge primitives execute
 * in the worker; results come back as transferable typed arrays.
 *
 * Sync getters (`meta`, `sections`, `transform`) read from a snapshot
 * captured at open time so common in-render reads don't pay an RPC.
 *
 * One worker can host multiple `CtopoClient` instances (each gets its
 * own random `clientId` and the worker keeps a `Map<clientId, …>` of
 * cores). To attach a client running in a different thread, call
 * `attachPort()` on an existing client and transfer the returned
 * `MessagePort` to the other thread; that thread passes it as
 * `openContainer(url, { port })` and skips spawning its own worker.
 * Two opens against the same URL share the underlying core (and its
 * byte-range cache) inside the worker.
 */

import { type CtopoClientStats } from "../core/client";
import { type FetcherSpec } from "../core/fetcher";
import { spawnWorker } from "../core/worker-host";
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
  WorkerAddPort,
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
  // sources at test time. Ignored when `worker` or `port` is set.
  readonly workerUrl?: string | URL;
  // Caller-supplied Worker. Takes precedence over `workerUrl`. Useful
  // for test rigs that pre-spawn a Worker with a non-standard loader
  // (e.g. vite-node, tsx) or for consumers that want to share one
  // worker across multiple opens on the same thread.
  readonly worker?: Worker;
  // Caller-supplied MessagePort connected to an existing cloud-topo
  // worker (typically obtained via `attachPort()` on another client
  // and transferred across a `postMessage` boundary). When set, the
  // proxy talks to the worker over this port instead of spawning one.
  // Lets a second thread attach to an already-running cloud-topo
  // worker; if both clients open the same URL, the worker dedupes the
  // core so byte-range caches are shared.
  readonly port?: MessagePort;
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

// Minimal shape shared by `Worker` and `MessagePort` — the proxy needs
// `postMessage` + `addEventListener("message", …)` + `start?()`, plus
// best-effort `"error"`/`"exit"` so a dying spawned worker can fail
// in-flight calls instead of hanging them forever. MessagePort requires
// `start()`; Worker doesn't. The error/exit events only fire for the
// spawn/Worker paths — registering them on a plain MessagePort is a
// harmless no-op (it never emits them).
interface PortLike {
  postMessage: (msg: unknown, transfer?: ReadonlyArray<Transferable>) => void;
  addEventListener: {
    (type: "message", listener: (e: { data: unknown }) => void): void;
    (
      type: "error" | "exit",
      listener: (e: { data?: unknown; message?: string }) => void,
    ): void;
  };
  removeEventListener: {
    (type: "message", listener: (e: { data: unknown }) => void): void;
    (
      type: "error" | "exit",
      listener: (e: { data?: unknown; message?: string }) => void,
    ): void;
  };
  start?: () => void;
}

// Generate a clientId that's statistically unique across all proxies
// the worker is likely to see in one session. `Math.random()` over the
// 53-bit safe-integer range is fine — birthday collisions are far below
// any realistic client count.
function makeClientId(): number {
  return Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);
}

export class CtopoClient {
  readonly meta: ContainerMeta;
  readonly sections: ReadonlyArray<SectionEntry>;
  readonly transform: ContainerMeta["transform"];

  // The underlying message channel. May be a Worker (spawn path) or a
  // MessagePort (attach path). The worker self-terminates when its
  // last client closes — we don't need to track ownership here.
  private readonly port: PortLike;
  // Unique per-instance identifier. Stamped on every request so the
  // worker can route to the right `CtopoCore` and the proxy can
  // ignore responses meant for other clients sharing the same port.
  private readonly clientId: number;
  private readonly pending = new Map<number, PendingCall>();
  private nextId = 0;
  // Bound port listeners, retained so `close()` can detach them. The
  // spawned-worker path self-terminates so detaching is moot there, but
  // when a single Worker is shared across clients (`opts.worker`) a
  // closed client must unhook its listeners or it leaks one set per
  // close onto the still-live Worker (and its message handler keeps
  // firing for every other client's replies).
  private onMessageListener?: (e: { data: unknown }) => void;
  private onErrorListener?: (e: { message?: string }) => void;
  private onExitListener?: (e: { data?: unknown }) => void;
  // `closing` blocks new calls from `call()` while still letting the
  // close RPC itself fire. `closed` flips once the worker has acked
  // the close (or the round-trip failed) and is used to short-circuit
  // double-close.
  private closing = false;
  private closed = false;

  private constructor(args: {
    port: PortLike;
    clientId: number;
    snapshot: OpenSnapshot;
  }) {
    this.port = args.port;
    this.clientId = args.clientId;
    this.meta = args.snapshot.meta;
    this.sections = args.snapshot.sections;
    this.transform = args.snapshot.transform;
  }

  // --- Factory ---

  // `source` selects how the worker obtains the container bytes:
  //   - string  → HTTP Range fetches against that URL. The URL also
  //     keys the worker's core dedup, so two clients opening the same
  //     URL (e.g. via `attachPort`) share one cache.
  //   - Uint8Array → the bytes are handed to the worker directly (no
  //     HTTP). The buffer is transferred at open, so the caller's view
  //     is detached afterward. Each such open gets its own core.
  static async open(
    source: string | Uint8Array,
    opts: OpenContainerOptions = {},
  ): Promise<CtopoClient> {
    // Resolve the underlying port. Precedence: caller-supplied port >
    // caller-supplied worker > spawn a fresh worker from `workerUrl`.
    let port: PortLike;
    if (opts.port !== undefined) {
      port = opts.port as unknown as PortLike;
      // MessagePort needs an explicit start() before it'll deliver
      // messages; calling it on an already-started port is a no-op.
      port.start?.();
    } else if (opts.worker !== undefined) {
      port = opts.worker as unknown as PortLike;
    } else {
      const workerUrl =
        opts.workerUrl !== undefined
          ? typeof opts.workerUrl === "string"
            ? opts.workerUrl
            : opts.workerUrl.href
          : new URL("./worker.js", import.meta.url).href;
      port = (await spawnWorker(workerUrl)) as unknown as PortLike;
    }

    const clientId = makeClientId();
    const openId = 1;

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
      pool: opts.pool,
    };
    // String source → HTTP fetcher keyed on the URL (the URL doubles
    // as the worker's core-dedup key). Bytes source → buffer fetcher
    // with a per-client synthetic URL so each open gets its own core.
    const fetcher: FetcherSpec =
      typeof source === "string"
        ? { kind: "http", url: source }
        : { kind: "buffer", bytes: source };
    const url = typeof source === "string" ? source : `buffer://${clientId}`;

    // Open is its own RPC — we need the meta/sections snapshot back
    // before the proxy can be constructed, so we run a one-shot
    // listener bound to (clientId, openId) rather than installing the
    // steady-state dispatcher first. Steady-state hookup happens after
    // open resolves.
    const transfer: Transferable[] =
      fetcher.kind === "buffer" ? [fetcher.bytes.buffer as Transferable] : [];
    const req: WorkerRequest = {
      clientId,
      id: openId,
      method: "open",
      args: { fetcher, url, opts: wireOpts },
    };

    const snapshot = await new Promise<OpenSnapshot>((resolve, reject) => {
      const onMessage = (e: { data: unknown }): void => {
        const r = e.data as WorkerResponse;
        if (r.clientId !== clientId || r.id !== openId) return;
        port.removeEventListener("message", onMessage);
        if (r.ok) {
          resolve(r.result as OpenSnapshot);
        } else {
          reject(new Error(r.error.message));
        }
      };
      port.addEventListener("message", onMessage);
      if (opts.signal !== undefined) {
        if (opts.signal.aborted) {
          port.removeEventListener("message", onMessage);
          reject(new DOMException("Aborted", "AbortError"));
          return;
        }
        opts.signal.addEventListener(
          "abort",
          () => {
            port.removeEventListener("message", onMessage);
            port.postMessage({ clientId, id: openId, abort: true });
            reject(new DOMException("Aborted", "AbortError"));
          },
          { once: true },
        );
      }
      port.postMessage(req, transfer);
    });

    const proxy = new CtopoClient({ port, clientId, snapshot });
    proxy.installMessageHandler();
    return proxy;
  }

  // --- RPC plumbing ---

  private installMessageHandler(): void {
    this.onMessageListener = (e: { data: unknown }): void => {
      const r = e.data as WorkerResponse;
      // When two CtopoClient instances share one underlying Worker
      // (e.g. both opened with the same `opts.worker`) their
      // `nextId`s overlap; the clientId check keeps each proxy from
      // resolving the other's pending entries. Port-attached clients
      // are naturally isolated (each port has its own message
      // queue), but the check is cheap and protects the worker-shared
      // case too.
      if (r.clientId !== this.clientId) return;
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
    };
    this.port.addEventListener("message", this.onMessageListener);

    // A spawned worker that crashes (OOM on a large merge, an uncaught
    // async rejection outside the dispatched call) would otherwise leave
    // every in-flight call's promise unsettled forever. Fail them loudly
    // instead. Only the spawn/Worker paths emit these; a shared
    // MessagePort never does, so this is a no-op there.
    this.onErrorListener = (e: { message?: string }): void => {
      const detail = e.message ?? "unknown";
      this.failAll(new Error(`ctopo: worker error: ${detail}`));
    };
    this.port.addEventListener("error", this.onErrorListener);
    this.onExitListener = (e: { data?: unknown }): void => {
      const code = (e as { data?: number }).data;
      if (code !== 0) {
        this.failAll(
          new Error(`ctopo: worker exited with code ${code ?? "unknown"}`),
        );
      }
    };
    this.port.addEventListener("exit", this.onExitListener);
  }

  // Detach the steady-state port listeners. Critical for the shared-
  // Worker case (`opts.worker`) where the Worker outlives this client;
  // a no-op for the spawn path (worker self-terminates) and harmless
  // for a MessagePort (it never emitted error/exit anyway).
  private removePortListeners(): void {
    if (this.onMessageListener !== undefined) {
      this.port.removeEventListener("message", this.onMessageListener);
      this.onMessageListener = undefined;
    }
    if (this.onErrorListener !== undefined) {
      this.port.removeEventListener("error", this.onErrorListener);
      this.onErrorListener = undefined;
    }
    if (this.onExitListener !== undefined) {
      this.port.removeEventListener("exit", this.onExitListener);
      this.onExitListener = undefined;
    }
  }

  // Reject every in-flight call with `err` and mark the client unusable.
  // Used when the worker dies — there will be no further replies, so the
  // pending promises must be settled here or they hang.
  private failAll(err: Error): void {
    if (this.closed) return;
    this.closed = true;
    this.removePortListeners();
    for (const pending of this.pending.values()) {
      if (pending.onAbort !== undefined && pending.signal !== undefined) {
        pending.signal.removeEventListener("abort", pending.onAbort);
      }
      pending.reject(err);
    }
    this.pending.clear();
  }

  private call<T>(
    method: WorkerRequest["method"],
    args: unknown,
    signal: AbortSignal | undefined,
    transfer: Transferable[] = [],
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
          this.port.postMessage({ clientId: this.clientId, id, abort: true });
          reject(new DOMException("Aborted", "AbortError"));
        };
        signal.addEventListener("abort", pending.onAbort, { once: true });
      }
      this.pending.set(id, pending);
      this.port.postMessage(
        { clientId: this.clientId, id, method, args },
        transfer,
      );
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

  // Derived from the open-time snapshot — same as the other sync-known
  // facts (meta/sections/transform). No RPC: the core method reads the
  // same META field (`arcEndpointsBlocks`), which is already in `meta`.
  hasArcEndpointsSection(): Promise<boolean> {
    return Promise.resolve(this.meta.arcEndpointsBlocks !== undefined);
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

  // --- Cross-thread sharing ---

  // Returns a `MessagePort` that another thread can pass to
  // `openContainer(url, { port })` to talk to the same underlying
  // worker. Posts the worker-side port via the existing connection as
  // a control message; the worker hooks it and routes its requests
  // through the same dispatcher. When the other thread opens against
  // the same URL, the worker reuses one `CtopoCore` so the byte-range
  // cache is shared.
  attachPort(): MessagePort {
    if (this.closed || this.closing) {
      throw new Error("ctopo: client is closed");
    }
    const channel = new MessageChannel();
    // The worker-side port is embedded in the message AND added to
    // the transfer list — `event.ports` isn't populated under
    // Node's `worker_threads` shim, so receivers read the port out
    // of the cloned data.
    const ctrl: WorkerAddPort = { kind: "addPort", port: channel.port2 };
    this.port.postMessage(ctrl, [channel.port2]);
    return channel.port1;
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
      this.removePortListeners();
      // The worker self-terminates after its last client closes —
      // calling `worker.terminate()` here would kill any port-attached
      // clients on other threads.
      const stale = this.pending;
      this.pending.clear();
      const err = new Error("ctopo: client closed");
      for (const p of stale.values()) p.reject(err);
    }
  }
}

export async function openContainer(
  source: string | Uint8Array,
  opts?: OpenContainerOptions,
): Promise<CtopoClient> {
  return CtopoClient.open(source, opts);
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
