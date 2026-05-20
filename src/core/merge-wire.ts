// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Wire-format types for cross-thread messaging between the coordinator
 * worker (src/worker.ts) and the merge compute workers (src/core/merge-
 * worker.ts). Everything here must be structured-cloneable.
 *
 * The pool runs each merge as a TWO-PHASE pipeline:
 *   1. prepMerge: worker runs the CSR-only CPU work (steps 1-4 of
 *      prepareMergeGroups — expandLayerPolygons, buildPolygonsByArc,
 *      groupPolygonsByConnectivity, exteriorArcsForGroup). It stashes
 *      the per-group exterior arc lists locally keyed by `id` and
 *      returns just the unique unsigned arc ids the merge will need.
 *   2. coordinator-side fetch: calls `core.fetchArcs(uniqueIds)` (or
 *      fetchArcEndpoints) for THIS merge. Many merges' fetchArcs
 *      calls arriving in the same event-loop cohort enqueue into a
 *      single coalescer drain — matching the baseline's single
 *      multi-thousand-range microtask drain.
 *   3. postMerge: coordinator sends the arc bytes / endpoints back to
 *      the SAME worker (it has the stashed groupExteriorArcs). Worker
 *      runs stitching + decoding and returns the final flat result.
 *
 * Same-worker affinity: the post phase MUST land on the worker that
 * ran prep, because that worker holds the prep intermediates. Pool
 * tracks (mergeId → slotIdx).
 *
 * Shared-memory model: ViewSpec carries an ArrayBufferLike + byte-
 * Offset + byteLength. The layer CSR specs are coerced to SAB-backed
 * buffers (see `toSabViewSpec` in merge-pool) so they cross to the
 * compute worker with no copy and are shared read-only across merges.
 * The per-merge postMerge payloads (arc bytes / endpoints) go through
 * plain `toViewSpec` and are NOT coerced — they originate from
 * core.fetchArcs / fetchArcEndpoints and may be normal ArrayBuffers,
 * in which case structured clone copies them on send.
 */

import type { TransformDef } from "./merge";

export interface ViewSpec {
  readonly buffer: ArrayBufferLike;
  readonly byteOffset: number;
  readonly byteLength: number;
}

export interface CsrSpec {
  readonly polyOffsets: ViewSpec;
  readonly ringOffsets: ViewSpec;
  readonly arcRefs: ViewSpec;
  readonly multiPolyBreaks: ViewSpec;
}

// Phase 1 dispatch: worker runs prep, stashes intermediates keyed
// by `id`, returns the boundary arc id set.
export interface ComputePrepRequest {
  readonly kind: "prepMerge";
  readonly id: number;
  readonly variant: "arcs" | "coords";
  readonly inputs: ReadonlyArray<{
    readonly layer: string;
    readonly indices: ReadonlyArray<number>;
  }>;
  readonly csrs: ReadonlyArray<CsrSpec>;
  readonly numArcs: number;
  readonly transform: TransformDef;
  readonly isQuantized: boolean;
  readonly hasArcEndpointsSection: boolean;
}

export interface ComputePrepReplyOk {
  readonly kind: "prepResult";
  readonly id: number;
  // Unsigned unique arc ids the post phase will need. Plain array
  // (not typed array) so the coordinator can pass it straight to
  // core.fetchArcs / fetchArcEndpoints without re-conversion.
  readonly uniqueArcIds: ReadonlyArray<number>;
}

export interface ComputePrepReplyError {
  readonly kind: "prepError";
  readonly id: number;
  readonly error: { readonly name: string; readonly message: string };
}

// Phase 3 dispatch: coordinator sends arc bytes (or endpoints) for
// THIS merge's stashed prep. Worker resumes with stitching + decode.
export interface ComputePostRequest {
  readonly kind: "postMerge";
  readonly id: number;
  // One of these will be populated depending on which path the
  // coordinator took during the bulk fetch — bytes for the
  // arc_coords-driven path, endpoints for the arc_endpoints-section
  // shortcut on quantized mergeArcs.
  readonly arcBytes?: {
    readonly ids: ReadonlyArray<number>;
    readonly views: ReadonlyArray<ViewSpec>;
  };
  readonly endpoints?: {
    readonly ids: ReadonlyArray<number>;
    readonly views: ReadonlyArray<ViewSpec>;
  };
}

export interface ComputePostReplyOk {
  readonly kind: "postResult";
  readonly id: number;
  readonly variant: "arcs" | "coords";
  readonly payloadBuffer: ArrayBufferLike;
  readonly ringStartsBuffer: ArrayBufferLike;
  readonly ringEndsBuffer: ArrayBufferLike;
  readonly polyRingStartsBuffer: ArrayBufferLike;
}

export interface ComputePostReplyError {
  readonly kind: "postError";
  readonly id: number;
  readonly error: { readonly name: string; readonly message: string };
}

// Drop a stashed prep result without running post. Sent when a
// merge is aborted after prep but before (or during) post, so the
// worker doesn't hold `groupExteriorArcs` for the lifetime of the
// pool. Carries no reply. If the worker hasn't stashed yet (prep
// still in flight), it records the id and skips the upcoming stash.
export interface ComputeDiscardRequest {
  readonly kind: "discardMerge";
  readonly id: number;
}

export interface ComputeShutdownRequest {
  readonly kind: "shutdown";
}

export interface ComputeReadyReply {
  readonly kind: "ready";
}

export type ComputeRequest =
  | ComputePrepRequest
  | ComputePostRequest
  | ComputeDiscardRequest
  | ComputeShutdownRequest;

export type ComputeReply =
  | ComputeReadyReply
  | ComputePrepReplyOk
  | ComputePrepReplyError
  | ComputePostReplyOk
  | ComputePostReplyError;
