// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Merge compute worker — two-phase pipelined.
 *
 * Phase 1 (prep, CPU-only over CSR):
 *   - Receive `prepMerge` with SAB-backed CSR views.
 *   - Run `prepareMergeGroupsForPipeline` (expandLayerPolygons +
 *     buildPolygonsByArc + groupPolygonsByConnectivity +
 *     exteriorArcsForGroup).
 *   - Stash `groupExteriorArcs` locally keyed by merge id.
 *   - Return `uniqueArcIds` (boundary arcs only — interior arcs
 *     already canceled).
 *
 * Phase 2 (post, CPU + the bytes):
 *   - Receive `postMerge` carrying this merge's arc bytes or
 *     endpoints as SAB-backed ViewSpec lists.
 *   - Build the endpoint lookup, run stitchBatch + decode, assemble
 *     flat result, transfer back.
 *
 * Same-worker affinity: the post phase ALWAYS lands on the worker
 * that ran prep, because that worker holds the stashed
 * `groupExteriorArcs`. The pool tracks (mergeId → worker) and routes
 * accordingly.
 *
 * Within one worker, prep + post can interleave on the event loop —
 * while one merge is awaiting its arc bytes (sitting in the
 * coordinator's queue between prep and post), the worker handles a
 * new merge's prep. Per-client scratch (Int32ArrayPool, ArcGenIndex)
 * is touched only synchronously inside prep, before the prep reply,
 * so it doesn't conflict with a concurrently running post (which
 * uses StitchScratch + Float64Growable, disjoint scratch sets).
 */

import type { CtopoCore } from "./client";
import {
  assembleArcsResult,
  assembleCoordsResult,
  makeEndpointLookup,
  makeNumericEndpointLookup,
  makeNumericEndpointLookupFromBytes,
  makeStitchScratch,
  prepareMergeGroupsForPipeline,
  stitchBatchArcs,
  stitchBatchCoords,
  type EndpointLookup,
  type StitchScratch,
  type TransformDef,
} from "./merge";
import { getWorkerHost, toWireError, type WorkerHost } from "./worker-host";
import { breaksFromCsr } from "./merge-prep";
import type { LayerGeometry } from "./types";
import type {
  ComputePostReplyError,
  ComputePostReplyOk,
  ComputePostRequest,
  ComputePrepReplyError,
  ComputePrepReplyOk,
  ComputePrepRequest,
  ComputeReadyReply,
  ComputeRequest,
  CsrSpec,
} from "./merge-wire";

function csrFromSpec(spec: CsrSpec): LayerGeometry {
  return {
    polyOffsets: new Uint32Array(
      spec.polyOffsets.buffer,
      spec.polyOffsets.byteOffset,
      spec.polyOffsets.byteLength / 4,
    ),
    ringOffsets: new Uint32Array(
      spec.ringOffsets.buffer,
      spec.ringOffsets.byteOffset,
      spec.ringOffsets.byteLength / 4,
    ),
    arcRefs: new Int32Array(
      spec.arcRefs.buffer,
      spec.arcRefs.byteOffset,
      spec.arcRefs.byteLength / 4,
    ),
    multiPolyBreaks: new Uint32Array(
      spec.multiPolyBreaks.buffer,
      spec.multiPolyBreaks.byteOffset,
      spec.multiPolyBreaks.byteLength / 4,
    ),
  };
}

// CtopoCore-shaped shim used during prep. Only carries what
// `prepareMergeGroupsForPipeline` (and its helpers buildInputPolygons,
// buildPolygonsByArc, groupPolygonsByConnectivity, exteriorArcsForGroup)
// actually read: meta.numArcs, transform, breaksByGeom, layerGeometry,
// tallyUseful. Post phase uses primitive merge.ts entries that take
// the transform/scratch explicitly — no shim needed.
class PrepShim {
  readonly meta: { readonly numArcs: number };
  readonly transform: TransformDef;
  private layerCsrs = new Map<string, LayerGeometry>();
  private readonly breaksByGeomCache = new Map<string, Map<number, number[]>>();

  constructor(numArcs: number, transform: TransformDef) {
    this.meta = { numArcs };
    this.transform = transform;
  }

  setLayerCsrs(csrs: ReadonlyMap<string, LayerGeometry>): void {
    this.layerCsrs = new Map(csrs);
  }

  layerGeometry(layer: string): Promise<LayerGeometry> {
    const csr = this.layerCsrs.get(layer);
    if (csr === undefined) {
      return Promise.reject(
        new Error(`ctopo: merge-worker missing CSR for layer "${layer}"`),
      );
    }
    return Promise.resolve(csr);
  }

  tallyUseful(_name: string, _bytes: number): void {
    // Stats live on the coordinator.
  }

  breaksByGeom(layer: string, csr: LayerGeometry): Map<number, number[]> {
    let cached = this.breaksByGeomCache.get(layer);
    if (cached !== undefined) return cached;
    cached = breaksFromCsr(csr);
    this.breaksByGeomCache.set(layer, cached);
    return cached;
  }
}

// Per-merge intermediates stashed between prep and post. Keyed by
// merge id. The post phase needs:
//   - variant (arcs/coords) to pick the assembler.
//   - transform + isQuantized to drive decodeRingFlat + endpoint
//     lookup selection.
//   - groupExteriorArcs (the prep output that survives).
//   - whether the coordinator will deliver bytes or endpoints (so
//     the post handler picks the right lookup builder).
interface PrepIntermediates {
  readonly variant: "arcs" | "coords";
  readonly transform: TransformDef;
  readonly isQuantized: boolean;
  readonly groupExteriorArcs: number[][];
}

const stash = new Map<number, PrepIntermediates>();

// Ids the coordinator discarded (abort / coordinator-side error)
// before this worker stashed their prep result. A discard message can
// race ahead of the prep result it cancels — prep awaits CPU work, so
// a queued discard runs during that await — so we remember the id and
// skip the upcoming stash instead. Holds only the few ids whose
// discard out-raced their stash.
const discarded = new Set<number>();

// Ids whose prep is currently awaiting CPU work (between the prepMerge
// message and its reply). A discard is only meaningful while prep is in
// this window; one that arrives after post already consumed the stash
// must NOT be recorded in `discarded`, or the id would leak there
// forever (no later prep will ever clear it).
const inFlightPrep = new Set<number>();

// Persistent shim + scratch across merges. The WeakMap-keyed scratch
// inside merge.ts (Int32ArrayPool, ArcGenIndex, StitchScratch) stays
// warm for the bench's 435 sequential merges as long as the shim
// instance is the same.
let prepShim: PrepShim | undefined;
const postScratch: StitchScratch = makeStitchScratch();

async function handlePrep(
  host: WorkerHost,
  req: ComputePrepRequest,
): Promise<void> {
  if (prepShim === undefined) {
    prepShim = new PrepShim(req.numArcs, req.transform);
  }
  const csrs = new Map<string, LayerGeometry>();
  for (let i = 0; i < req.inputs.length; i++) {
    csrs.set(req.inputs[i].layer, csrFromSpec(req.csrs[i]));
  }
  prepShim.setLayerCsrs(csrs);

  const core = prepShim as unknown as CtopoCore;
  const inputs = req.inputs.map((s) => ({
    layer: s.layer,
    indices: s.indices,
  }));

  inFlightPrep.add(req.id);
  const { groupExteriorArcs, uniqueArcIds } =
    await prepareMergeGroupsForPipeline(core, inputs, undefined);
  inFlightPrep.delete(req.id);

  // Discarded while prep was running — drop the result, no reply.
  if (discarded.delete(req.id)) return;

  // Empty merge: the coordinator synthesizes the empty result without
  // a post phase, so stashing would leak. Still reply so its prep
  // await resolves.
  if (uniqueArcIds.length > 0) {
    stash.set(req.id, {
      variant: req.variant,
      transform: req.transform,
      isQuantized: req.isQuantized,
      groupExteriorArcs,
    });
  }

  const reply: ComputePrepReplyOk = {
    kind: "prepResult",
    id: req.id,
    uniqueArcIds,
  };
  host.postMessage(reply);
}

function buildEndpointLookupForPost(
  req: ComputePostRequest,
  isQuantized: boolean,
): {
  endpoints: EndpointLookup<unknown>;
  arcBytes?: ReadonlyMap<number, Uint8Array>;
} {
  if (req.endpoints !== undefined) {
    const map = new Map<number, Int32Array>();
    for (let i = 0; i < req.endpoints.ids.length; i++) {
      const v = req.endpoints.views[i];
      map.set(
        req.endpoints.ids[i],
        new Int32Array(v.buffer, v.byteOffset, v.byteLength / 4),
      );
    }
    return { endpoints: makeNumericEndpointLookup(map) };
  }
  if (req.arcBytes === undefined) {
    throw new Error(
      "ctopo: merge-worker postMerge missing both arcBytes and endpoints",
    );
  }
  const map = new Map<number, Uint8Array>();
  for (let i = 0; i < req.arcBytes.ids.length; i++) {
    const v = req.arcBytes.views[i];
    map.set(
      req.arcBytes.ids[i],
      new Uint8Array(v.buffer, v.byteOffset, v.byteLength),
    );
  }
  // mergeArcs without arc_endpoints section, or any merge with
  // arc_coords: build the lookup from bytes. Quantized goes through
  // the numeric (Number-keyed) shape; un-quantized falls back to
  // string keys.
  const endpoints: EndpointLookup<unknown> = isQuantized
    ? makeNumericEndpointLookupFromBytes(map)
    : makeEndpointLookup(map, isQuantized);
  return { endpoints, arcBytes: map };
}

function handlePost(host: WorkerHost, req: ComputePostRequest): void {
  const prep = stash.get(req.id);
  if (prep === undefined) {
    throw new Error(
      `ctopo: merge-worker postMerge for unknown id ${req.id} (prep wasn't received or already discarded)`,
    );
  }
  stash.delete(req.id);
  const { endpoints, arcBytes } = buildEndpointLookupForPost(
    req,
    prep.isQuantized,
  );
  const ctx = {
    transform: prep.transform,
    endpoints,
    arcBytes,
  };

  if (prep.variant === "arcs") {
    const result = stitchBatchArcs(ctx, prep.groupExteriorArcs, postScratch);
    const flat = assembleArcsResult(result);
    const reply: ComputePostReplyOk = {
      kind: "postResult",
      id: req.id,
      variant: "arcs",
      payloadBuffer: flat.arcs.buffer,
      ringStartsBuffer: flat.ringStarts.buffer,
      ringEndsBuffer: flat.ringEnds.buffer,
      polyRingStartsBuffer: flat.polyRingStarts.buffer,
    };
    host.postMessage(reply, [
      reply.payloadBuffer,
      reply.ringStartsBuffer,
      reply.ringEndsBuffer,
      reply.polyRingStartsBuffer,
    ]);
    return;
  }
  // coords
  const result = stitchBatchCoords(ctx, prep.groupExteriorArcs, postScratch);
  const flat = assembleCoordsResult(result);
  const reply: ComputePostReplyOk = {
    kind: "postResult",
    id: req.id,
    variant: "coords",
    payloadBuffer: flat.coords.buffer,
    ringStartsBuffer: flat.ringStarts.buffer,
    ringEndsBuffer: flat.ringEnds.buffer,
    polyRingStartsBuffer: flat.polyRingStarts.buffer,
  };
  host.postMessage(reply, [
    reply.payloadBuffer,
    reply.ringStartsBuffer,
    reply.ringEndsBuffer,
    reply.polyRingStartsBuffer,
  ]);
}

void getWorkerHost().then((host) => {
  const ready: ComputeReadyReply = { kind: "ready" };
  host.postMessage(ready);
  host.onMessage((data) => {
    const req = data as ComputeRequest;
    switch (req.kind) {
      case "shutdown": {
        host.close();
        return;
      }
      case "discardMerge": {
        // Drop the stash if prep already ran. Otherwise, only mark the
        // id to skip its upcoming stash if prep is genuinely still in
        // flight — a discard that arrives after post already consumed
        // the stash matches neither and is a no-op (recording it would
        // leak the id in `discarded` permanently).
        if (!stash.delete(req.id) && inFlightPrep.has(req.id)) {
          discarded.add(req.id);
        }
        return;
      }
      case "prepMerge": {
        handlePrep(host, req).catch((err: unknown) => {
          // Prep threw before its own cleanup ran: clear both the
          // in-flight marker and any discard that raced in while prep
          // was running, otherwise the id leaks in these sets forever.
          inFlightPrep.delete(req.id);
          discarded.delete(req.id);
          const errReply: ComputePrepReplyError = {
            kind: "prepError",
            id: req.id,
            error: toWireError(err),
          };
          host.postMessage(errReply);
        });
        return;
      }
      case "postMerge": {
        try {
          handlePost(host, req);
        } catch (err) {
          const errReply: ComputePostReplyError = {
            kind: "postError",
            id: req.id,
            error: toWireError(err),
          };
          host.postMessage(errReply);
        }
        return;
      }
      default: {
        const _exhaustive: never = req;
        void _exhaustive;
      }
    }
  });
});
