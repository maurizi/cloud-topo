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
import { getWorkerHost, type WorkerHost } from "./core/worker-host";
import {
  mergeArcsFlat,
  mergeFlat,
  neighborsFlat,
  type FlatMultiPolygon,
  type FlatMultiPolygonArcs,
  type FlatNeighbors,
} from "./core/merge";
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

function copyBuffer(view: ArrayBufferView): ArrayBufferLike {
  // Slice the live bytes into a fresh, transferable ArrayBuffer.
  // Used for the cached-section returns (property / strings /
  // layerGeometry) so the worker keeps its cache and the proxy gets
  // an independent copy it can transfer (and ultimately wrap as a
  // typed array on the main thread without coupling lifetimes).
  return view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength);
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
      let entry = byUrl.get(msg.args.url);
      if (entry === undefined) {
        const fetcher = reconstructFetcher(msg.args.fetcher);
        const opts: OpenContainerOptions = { ...msg.args.opts, signal };
        const core = await CtopoCore.openWith(fetcher, opts);
        entry = { core, url: msg.args.url, refcount: 0 };
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
        buffer: copyBuffer(view),
        dtype: section.dtype,
      };
      return { result, transfer: [result.buffer] };
    }
    case "strings": {
      const strings = await getCore(msg.clientId).strings(msg.args.name, signal);
      const result: WireStringsResult = { buffer: copyBuffer(strings.bytes) };
      return { result, transfer: [result.buffer] };
    }
    case "layerGeometry": {
      const csr = await getCore(msg.clientId).layerGeometry(
        msg.args.layer,
        signal,
      );
      const result: WireLayerGeometryResult = {
        polyOffsets: copyBuffer(csr.polyOffsets),
        ringOffsets: copyBuffer(csr.ringOffsets),
        arcRefs: copyBuffer(csr.arcRefs),
        multiPolyBreaks: copyBuffer(csr.multiPolyBreaks),
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
      const flat = await mergeFlat(
        getCore(msg.clientId),
        toLayerSelection(msg.args.selections),
        signal,
      );
      return flatMultiPolygonReply(flat);
    }
    case "mergeArcsFlat": {
      const flat = await mergeArcsFlat(
        getCore(msg.clientId),
        toLayerSelection(msg.args.selections),
        signal,
      );
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
      const result = await getCore(msg.clientId).fetchArcs(msg.args.ids, signal);
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

function handleMessage(host: WorkerHost, data: unknown, source: PortLike): void {
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
        host.close();
      }
    },
    (err: unknown) => {
      inflight.delete(key);
      const message = err instanceof Error ? err.message : String(err);
      const name = err instanceof Error ? err.name : "Error";
      const reply: WorkerResponse = {
        clientId: req.clientId,
        id: req.id,
        ok: false,
        error: { name, message },
      };
      source.postMessage(reply);
    },
  );
}

void getWorkerHost().then((host) => {
  // Adapter so `handleMessage` can call `source.postMessage(reply,
  // transfer)` against the host (which has the same shape).
  const hostAsPort: PortLike = {
    postMessage: (msg, transfer) =>
      host.postMessage(msg, transfer as ReadonlyArray<ArrayBufferLike> | undefined),
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
