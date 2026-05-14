// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Worker entrypoint — owns one `CtopoCore` and dispatches RPC calls
 * from the main-thread proxy. The result types declared in
 * `core/wire.ts` are what cross postMessage; coord-heavy returns
 * (merge / mergeArcs / neighbors flat shapes) are transferred
 * zero-copy, while cached-section returns (property / strings /
 * layerGeometry) ship a slice-copy so the core's cache stays valid.
 *
 * In browsers this file runs inside a real `Worker`. In Node it runs
 * via the `web-worker` shim (a `node:worker_threads.Worker` with a
 * compatibility layer that emulates `self`, `postMessage`, etc.).
 */

import { CtopoCore, type OpenContainerOptions } from "./core/client";
import { reconstructFetcher } from "./core/fetcher";
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
  WorkerRequest,
  WorkerResponse,
} from "./core/wire";

// The `self` global is a `DedicatedWorkerGlobalScope` in real workers
// and a polyfill in the web-worker Node shim. Typed loose so this
// file builds with the default lib (no "WebWorker" libs added).
declare const self: {
  onmessage: ((e: { data: unknown }) => void) | null;
  postMessage: (msg: unknown, transfer?: ReadonlyArray<unknown>) => void;
  close: () => void;
};

// One container per worker. Spawned by the proxy at open time; reused
// for every subsequent call. `close` releases it and terminates the
// worker.
let core: CtopoCore | null = null;
const inflight = new Map<number, AbortController>();

function requireCore(): CtopoCore {
  if (core === null) {
    throw new Error("ctopo: worker received a request before `open`");
  }
  return core;
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
      if (core !== null) {
        throw new Error(
          "ctopo: worker received `open` twice on the same container",
        );
      }
      const fetcher = reconstructFetcher(msg.args.fetcher);
      const opts: OpenContainerOptions = { ...msg.args.opts, signal };
      core = await CtopoCore.openWith(fetcher, opts);
      // The proxy keeps an immutable snapshot of meta / sections /
      // transform after open so all sync getters work without an RPC.
      return {
        result: {
          meta: core.meta,
          sections: core.sections,
          transform: core.transform,
        },
        transfer: [],
      };
    }
    case "property": {
      const view = await requireCore().property(msg.args.name, signal);
      // Find the dtype by scanning the section table — the worker
      // doesn't need to be fast here; this is a rare hot path.
      const section = requireCore().sections.find(
        (s) => s.name === msg.args.name,
      );
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
      const strings = await requireCore().strings(msg.args.name, signal);
      const result: WireStringsResult = { buffer: copyBuffer(strings.bytes) };
      return { result, transfer: [result.buffer] };
    }
    case "layerGeometry": {
      const csr = await requireCore().layerGeometry(msg.args.layer, signal);
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
        requireCore(),
        toLayerSelection(msg.args.selections),
        signal,
      );
      return flatMultiPolygonReply(flat);
    }
    case "mergeArcsFlat": {
      const flat = await mergeArcsFlat(
        requireCore(),
        toLayerSelection(msg.args.selections),
        signal,
      );
      return flatMultiPolygonArcsReply(flat);
    }
    case "neighborsFlat": {
      const flat = await neighborsFlat(requireCore(), msg.args.layer, signal);
      return flatNeighborsReply(flat);
    }
    case "fetchArcs": {
      // Maps cross via structured clone — Uint8Array clones are O(N)
      // on byteLength but this method is rarely on the hot path
      // (advanced API).
      const result = await requireCore().fetchArcs(msg.args.ids, signal);
      return { result, transfer: [] };
    }
    case "fetchArcEndpoints": {
      const result = await requireCore().fetchArcEndpoints(
        msg.args.ids,
        signal,
      );
      return { result, transfer: [] };
    }
    case "getStats":
      return { result: requireCore().getStats(), transfer: [] };
    case "resetStats":
      requireCore().resetStats();
      return { result: null, transfer: [] };
    case "tallyUseful":
      requireCore().tallyUseful(msg.args.name, msg.args.bytes);
      return { result: null, transfer: [] };
    case "hasArcEndpointsSection":
      return {
        result: requireCore().hasArcEndpointsSection(),
        transfer: [],
      };
    case "close":
      requireCore().close();
      core = null;
      return { result: null, transfer: [] };
    default: {
      const _exhaustive: never = msg;
      void _exhaustive;
      throw new Error(
        `ctopo: unknown worker method "${(msg as { method: string }).method}"`,
      );
    }
  }
}

self.onmessage = (e: { data: unknown }): void => {
  const msg = e.data as WorkerRequest | WorkerAbort;
  if ((msg as WorkerAbort).abort === true) {
    inflight.get(msg.id)?.abort();
    inflight.delete(msg.id);
    return;
  }
  const req = msg as WorkerRequest;
  const ac = new AbortController();
  inflight.set(req.id, ac);
  // Promise chained so multiple in-flight requests can proceed
  // concurrently; the per-request id keeps replies straight on the
  // main side. Errors are serialized to a postMessage-friendly shape
  // (Error objects can't structured-clone in older environments).
  dispatch(req, ac.signal).then(
    ({ result, transfer }) => {
      inflight.delete(req.id);
      const reply: WorkerResponse = { id: req.id, ok: true, result };
      self.postMessage(reply, transfer);
      // For `close`, terminate the worker after the reply lands so
      // the proxy can confirm the close before the worker is gone.
      if (req.method === "close") {
        self.close();
      }
    },
    (err: unknown) => {
      inflight.delete(req.id);
      const message = err instanceof Error ? err.message : String(err);
      const name = err instanceof Error ? err.name : "Error";
      const reply: WorkerResponse = {
        id: req.id,
        ok: false,
        error: { name, message },
      };
      self.postMessage(reply);
    },
  );
};
