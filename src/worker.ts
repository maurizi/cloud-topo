// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Worker entrypoint — hosts a `Map<clientId, CtopoCore>` so a single
 * worker can serve any number of `CtopoClient` proxies, including
 * proxies running on other threads that connected via a transferred
 * `MessagePort` (see `addPort` below). Result types declared in
 * `core/wire.ts` are what cross postMessage; coord-heavy returns
 * (merge / mergeArcs / neighbors flat shapes) are transferred zero-copy,
 * while cached-section returns (property / strings / layerGeometry)
 * ship a slice-copy so the core's cache stays valid.
 *
 * When two clients open the same URL, the worker shares one
 * `CtopoCore` between them (refcounted): they see the same meta,
 * the same byte-range cache, and the same arc dictionary. The core is
 * torn down when the last client closes.
 *
 * In browsers this file runs inside a real `Worker`. In Node it runs
 * via the `web-worker` shim (a `node:worker_threads.Worker` with a
 * compatibility layer that emulates `self`, `postMessage`, etc.).
 */

import { CtopoCore, type OpenContainerOptions } from "./core/client";
import { closeZstdDecoderClient } from "./core/zstd-decoder-client";
import { reconstructFetcher } from "./core/fetcher";
import {
  getWorkerHost,
  toWireError,
  type WorkerHost,
} from "./core/worker-host";
import {
  mergeArcsFlat,
  mergeFlat,
  neighborsFlat,
  type FlatMultiPolygon,
  type FlatMultiPolygonArcs,
  type FlatNeighbors,
} from "./core/merge";
import { MergePool } from "./core/merge-pool";
import { copyView, sabUsable } from "./core/util";
import { type LayerSelection } from "./core/types";
import type {
  WireFlatMultiPolygon,
  WireFlatMultiPolygonArcs,
  WireFlatNeighbors,
  WireLayerGeometryResult,
  WireLayerSelection,
  WirePropertyResult,
  WireStringsResult,
  WorkerAbort,
  WorkerAddPort,
  WorkerRequest,
  WorkerResponse,
} from "./core/wire";

interface PortLike {
  postMessage: (msg: unknown, transfer?: ReadonlyArray<unknown>) => void;
  onmessage?: ((e: MessageLikeEvent) => void) | null;
  start?: () => void;
}

interface MessageLikeEvent {
  data: unknown;
  ports?: ReadonlyArray<PortLike>;
}

// One entry per open container URL. `refcount` is the number of
// connected clientIds; the core is closed once it hits zero.
interface CoreEntry {
  core: CtopoCore;
  url: string;
  refcount: number;
  // Pool size carried over from this client's open. The first client
  // to ask for a pool (size > 1) is the one that actually creates
  // the singleton; later clients with different sizes share the
  // existing pool.
  poolSize: number | null;
}

// Warn once per worker when we drop to the no-SharedArrayBuffer
// fallback, so a misconfigured (non-cross-origin-isolated) deployment
// is diagnosable without spamming every merge.
let warnedNoSab = false;
function warnNoSabOnce(): void {
  if (warnedNoSab) return;
  warnedNoSab = true;
  // eslint-disable-next-line no-console
  console.warn(
    "ctopo: SharedArrayBuffer is unavailable (page is not cross-origin " +
      "isolated), so cloud-topo is running its single-threaded fallback: " +
      "zstd decodes in-process and merges run unparallelized. Serve with " +
      "COOP: same-origin + COEP: require-corp headers to enable the fast " +
      "path. See the README 'Cross-origin isolation' section.",
  );
}

// Clamp a requested pool size to what the runtime supports. The merge
// pool ships CSR geometry to compute workers via SharedArrayBuffer; if
// SAB can't cross the worker boundary, force the in-process merge path
// (size 1) instead and warn.
function effectivePoolSize(requested: number | null): number | null {
  if (requested === null || requested <= 1 || sabUsable()) return requested;
  return 1;
}

// Pool size when the caller doesn't specify one (`pool` undefined). Default to
// parallelizing merges across available cores (capped — diminishing returns +
// per-worker spawn/CSR-ship overhead past ~8). effectivePoolSize clamps this
// back to inline when SAB isn't usable, so returning >1 here is safe; callers
// who want to force the inline path pass `pool: null`.
function defaultPoolSize(): number {
  const hc = (globalThis as { navigator?: { hardwareConcurrency?: number } })
    .navigator?.hardwareConcurrency;
  return typeof hc === "number" && hc > 1 ? Math.min(hc, 8) : 4;
}

// One pool per coordinator. Lazily created on first merge call from
// a client that opened with `pool.size > 1`.
let mergePool: MergePool | null = null;
function getMergePool(size: number): MergePool {
  if (mergePool === null) {
    mergePool = new MergePool({ size });
  }
  return mergePool;
}

// Active clients, keyed by clientId. Multiple clients can point at the
// same `CoreEntry` (URL dedup).
const clients = new Map<number, CoreEntry>();
// URL → entry. Lets a second `open` against the same URL share the
// already-loaded core, including its byte-range cache and arc dictionary.
const byUrl = new Map<string, CoreEntry>();
// In-flight calls, keyed by `${clientId}:${id}` so two clients' `id`
// counters never collide.
const inflight = new Map<string, AbortController>();

function inflightKey(clientId: number, id: number): string {
  return `${clientId}:${id}`;
}

function getEntry(clientId: number): CoreEntry {
  const entry = clients.get(clientId);
  if (entry === undefined) {
    throw new Error(
      `ctopo: worker received request for unknown client ${clientId} (open it first)`,
    );
  }
  return entry;
}

function getCore(clientId: number): CtopoCore {
  return getEntry(clientId).core;
}

// Map { layer, indices: number[] } → runtime LayerSelection (the
// runtime accepts Iterable<number>; a plain array satisfies it).
function toLayerSelection(
  wire: ReadonlyArray<WireLayerSelection>,
): LayerSelection[] {
  return wire.map((w) => ({ layer: w.layer, indices: w.indices }));
}

interface Reply {
  result: unknown;
  transfer: ReadonlyArray<ArrayBufferLike>;
}

function flatMultiPolygonReply(flat: FlatMultiPolygon): Reply {
  const result: WireFlatMultiPolygon = {
    type: "MultiPolygon",
    coords: flat.coords.buffer,
    ringStarts: flat.ringStarts.buffer,
    ringEnds: flat.ringEnds.buffer,
    polyRingStarts: flat.polyRingStarts.buffer,
  };
  return {
    result,
    transfer: [
      result.coords,
      result.ringStarts,
      result.ringEnds,
      result.polyRingStarts,
    ],
  };
}

function flatMultiPolygonArcsReply(flat: FlatMultiPolygonArcs): Reply {
  const result: WireFlatMultiPolygonArcs = {
    type: "MultiPolygon",
    arcs: flat.arcs.buffer,
    ringStarts: flat.ringStarts.buffer,
    ringEnds: flat.ringEnds.buffer,
    polyRingStarts: flat.polyRingStarts.buffer,
  };
  return {
    result,
    transfer: [
      result.arcs,
      result.ringStarts,
      result.ringEnds,
      result.polyRingStarts,
    ],
  };
}

function flatNeighborsReply(flat: FlatNeighbors): Reply {
  const result: WireFlatNeighbors = {
    offsets: flat.offsets.buffer,
    values: flat.values.buffer,
  };
  return { result, transfer: [result.offsets, result.values] };
}

async function dispatch(
  msg: WorkerRequest,
  signal: AbortSignal,
): Promise<Reply> {
  switch (msg.method) {
    case "open": {
      if (clients.has(msg.clientId)) {
        throw new Error(
          `ctopo: worker received \`open\` twice for client ${msg.clientId}`,
        );
      }
      if (!sabUsable()) warnNoSabOnce();
      let entry = byUrl.get(msg.args.url);
      if (entry === undefined) {
        const fetcher = reconstructFetcher(msg.args.fetcher);
        const opts: OpenContainerOptions = { ...msg.args.opts, signal };
        const core = await CtopoCore.openWith(fetcher, opts);
        // Pool selection: unspecified → smart default (auto-pool under SAB);
        // explicit null → inline; explicit { size } → that size. All clamped
        // to inline by effectivePoolSize when SAB isn't usable.
        const requestedPoolSize =
          msg.args.opts.pool === undefined
            ? defaultPoolSize()
            : msg.args.opts.pool === null
              ? 1
              : msg.args.opts.pool.size;
        const poolSize = effectivePoolSize(requestedPoolSize);
        entry = { core, url: msg.args.url, refcount: 0, poolSize };
        byUrl.set(msg.args.url, entry);
      }
      entry.refcount++;
      clients.set(msg.clientId, entry);
      return {
        result: {
          meta: entry.core.meta,
          sections: entry.core.sections,
          transform: entry.core.transform,
        },
        transfer: [],
      };
    }
    case "property": {
      const core = getCore(msg.clientId);
      const view = await core.property(msg.args.name, signal);
      // Find the dtype by scanning the section table — the worker
      // doesn't need to be fast here; this is a rare hot path.
      const section = core.sections.find((s) => s.name === msg.args.name);
      if (section === undefined) {
        throw new Error(`ctopo: unknown property section "${msg.args.name}"`);
      }
      const result: WirePropertyResult = {
        buffer: copyView(view),
        dtype: section.dtype,
      };
      return { result, transfer: [result.buffer] };
    }
    case "strings": {
      const strings = await getCore(msg.clientId).strings(
        msg.args.name,
        signal,
      );
      const result: WireStringsResult = { buffer: copyView(strings.bytes) };
      return { result, transfer: [result.buffer] };
    }
    case "layerGeometry": {
      const csr = await getCore(msg.clientId).layerGeometry(
        msg.args.layer,
        signal,
      );
      const result: WireLayerGeometryResult = {
        polyOffsets: copyView(csr.polyOffsets),
        ringOffsets: copyView(csr.ringOffsets),
        arcRefs: copyView(csr.arcRefs),
        multiPolyBreaks: copyView(csr.multiPolyBreaks),
      };
      return {
        result,
        transfer: [
          result.polyOffsets,
          result.ringOffsets,
          result.arcRefs,
          result.multiPolyBreaks,
        ],
      };
    }
    case "mergeFlat": {
      const entry = getEntry(msg.clientId);
      const selections = toLayerSelection(msg.args.selections);
      const flat: FlatMultiPolygon =
        entry.poolSize !== null && entry.poolSize > 1
          ? await getMergePool(entry.poolSize).runCoords(
              entry.core,
              selections,
              signal,
            )
          : await mergeFlat(entry.core, selections, signal);
      return flatMultiPolygonReply(flat);
    }
    case "mergeArcsFlat": {
      const entry = getEntry(msg.clientId);
      const selections = toLayerSelection(msg.args.selections);
      const flat: FlatMultiPolygonArcs =
        entry.poolSize !== null && entry.poolSize > 1
          ? await getMergePool(entry.poolSize).runArcs(
              entry.core,
              selections,
              signal,
            )
          : await mergeArcsFlat(entry.core, selections, signal);
      return flatMultiPolygonArcsReply(flat);
    }
    case "neighborsFlat": {
      const flat = await neighborsFlat(
        getCore(msg.clientId),
        msg.args.layer,
        signal,
      );
      return flatNeighborsReply(flat);
    }
    case "fetchArcs": {
      // Maps cross via structured clone — Uint8Array clones are O(N)
      // on byteLength but this method is rarely on the hot path
      // (advanced API).
      const result = await getCore(msg.clientId).fetchArcs(
        msg.args.ids,
        signal,
      );
      return { result, transfer: [] };
    }
    case "fetchArcEndpoints": {
      const result = await getCore(msg.clientId).fetchArcEndpoints(
        msg.args.ids,
        signal,
      );
      return { result, transfer: [] };
    }
    case "getStats":
      return { result: getCore(msg.clientId).getStats(), transfer: [] };
    case "resetStats":
      getCore(msg.clientId).resetStats();
      return { result: null, transfer: [] };
    case "tallyUseful":
      getCore(msg.clientId).tallyUseful(msg.args.name, msg.args.bytes);
      return { result: null, transfer: [] };
    case "hasArcEndpointsSection":
      return {
        result: getCore(msg.clientId).hasArcEndpointsSection(),
        transfer: [],
      };
    case "close": {
      const entry = clients.get(msg.clientId);
      clients.delete(msg.clientId);
      if (entry !== undefined) {
        entry.refcount--;
        if (entry.refcount <= 0) {
          entry.core.close();
          byUrl.delete(entry.url);
          // Drop this core's cached layer CSRs from the shared pool so
          // its SharedArrayBuffers are released with the core.
          mergePool?.evictCore(entry.core);
        }
      }
      // If this is the last client, flush the CPU profile (if any)
      // before replying. The reply is what unblocks the proxy's
      // `await client.close()`; doing it here means the bench can
      // safely `process.exit(0)` after that resolves.
      if (clients.size === 0) {
        if (mergePool !== null) {
          try {
            await mergePool.close();
          } catch {
            // ignore
          }
          mergePool = null;
        }
        if (cpuProfStop !== null) {
          try {
            await cpuProfStop();
          } catch {
            // ignore — best-effort
          }
          cpuProfStop = null;
        }
      }
      return { result: null, transfer: [] };
    }
    default: {
      const _exhaustive: never = msg;
      void _exhaustive;
      throw new Error(
        `ctopo: unknown worker method "${(msg as { method: string }).method}"`,
      );
    }
  }
}

function handleMessage(
  host: WorkerHost,
  data: unknown,
  source: PortLike,
): void {
  const maybeAbort = data as Partial<WorkerAbort>;
  if (maybeAbort.abort === true && typeof maybeAbort.clientId === "number") {
    const key = inflightKey(maybeAbort.clientId, maybeAbort.id as number);
    inflight.get(key)?.abort();
    inflight.delete(key);
    return;
  }

  const req = data as WorkerRequest;
  const key = inflightKey(req.clientId, req.id);
  const ac = new AbortController();
  inflight.set(key, ac);
  // Promise chained so multiple in-flight requests can proceed
  // concurrently; the (clientId, id) pair keeps replies straight on
  // the main side. Errors are serialized to a postMessage-friendly
  // shape (Error objects can't structured-clone in older environments).
  dispatch(req, ac.signal).then(
    ({ result, transfer }) => {
      inflight.delete(key);
      const reply: WorkerResponse = {
        clientId: req.clientId,
        id: req.id,
        ok: true,
        result,
      };
      source.postMessage(reply, transfer);
      // Last client gone — tear down the zstd sub-worker first so it
      // doesn't outlive its parent, then terminate this worker so the
      // holding thread's `new Worker(...)` can be GC'd.
      if (req.method === "close" && clients.size === 0) {
        closeZstdDecoderClient();
        // Flush the CPU profile (if active) BEFORE host.close(); the
        // process exits inside host.close() so any pending stop() +
        // file write must complete first.
        void (async (): Promise<void> => {
          if (cpuProfStop !== null) {
            try {
              await cpuProfStop();
            } catch {
              // ignore — best-effort
            }
          }
          host.close();
        })();
      }
    },
    (err: unknown) => {
      inflight.delete(key);
      const reply: WorkerResponse = {
        clientId: req.clientId,
        id: req.id,
        ok: false,
        error: toWireError(err),
      };
      source.postMessage(reply);
    },
  );
}

// CPU profiling (env: CTOPO_WORKER_CPU_PROF=<path>). Starts a CPU
// profile via the inspector API at worker boot and writes it to the
// given path when the last client closes. Must run BEFORE host.close()
// otherwise the worker process exits and the profile never lands.
let cpuProfStop: (() => Promise<void>) | null = null;
async function maybeStartCpuProfile(): Promise<void> {
  if (typeof process === "undefined") return;
  const path = process.env?.CTOPO_WORKER_CPU_PROF;
  if (path === undefined || path === "") return;
  const inspector = await import("node:inspector/promises");
  const session = new inspector.Session();
  session.connect();
  await session.post("Profiler.enable");
  await session.post("Profiler.start");
  cpuProfStop = async (): Promise<void> => {
    const { profile } = await session.post("Profiler.stop");
    const fs = await import("node:fs/promises");
    await fs.writeFile(path, JSON.stringify(profile));
    session.disconnect();
  };
}
void maybeStartCpuProfile();

void getWorkerHost().then((host) => {
  // Adapter so `handleMessage` can call `source.postMessage(reply,
  // transfer)` against the host (which has the same shape).
  const hostAsPort: PortLike = {
    postMessage: (msg, transfer) =>
      host.postMessage(
        msg,
        transfer as ReadonlyArray<ArrayBufferLike> | undefined,
      ),
  };
  host.onMessage((data) => {
    if ((data as WorkerAddPort).kind === "addPort") {
      // `addPort` carries one MessagePort, embedded in the message
      // body for cross-runtime compatibility (Node's worker_threads
      // delivers transferred MessagePorts inside the message
      // payload, not on event.ports). Hook the port and route its
      // messages through the same dispatcher; replies go back over
      // that same port so the connecting thread receives them
      // directly.
      const port = (data as WorkerAddPort).port as unknown as
        | PortLike
        | undefined;
      if (port === undefined) {
        throw new Error("ctopo: `addPort` arrived without a transferred port");
      }
      port.onmessage = (ev: MessageLikeEvent): void =>
        handleMessage(host, ev.data, port);
      port.start?.();
      return;
    }
    handleMessage(host, data, hostAsPort);
  });
});
