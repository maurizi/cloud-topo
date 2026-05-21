// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Worker pool that runs merges as a two-phase pipeline:
 *   1. prepMerge (worker): CSR-only CPU work — expandLayerPolygons,
 *      buildPolygonsByArc, groupPolygonsByConnectivity,
 *      exteriorArcsForGroup. Returns the boundary arc id set.
 *   2. core.fetchArcs (coordinator): bulk-fetches the boundary arcs.
 *      Multiple concurrent prep results' fetchArcs calls land in
 *      the same coalescer microtask drain, matching the baseline's
 *      single multi-thousand-range fetch shape (~110 HTTP reqs vs
 *      the previous per-worker ~700 reqs).
 *   3. postMerge (same worker): stitch + decode + assemble. The
 *      post phase must land on the worker that ran prep, because
 *      that worker holds the stashed `groupExteriorArcs`.
 *
 * Streaming: each merge progresses independently. The moment merge
 * X's fetchArcs resolves, post is dispatched to its prep worker —
 * other merges are still in prep or still awaiting their own bytes.
 *
 * CPU saturation: workers can interleave prep + post on the event
 * loop. While merge A is awaiting its bytes (between prep and post),
 * the same worker picks up merge B's prep. The per-client scratch
 * inside merge.ts (Int32ArrayPool / ArcGenIndex) is only touched
 * synchronously inside prep — disjoint from the post phase's
 * StitchScratch — so the interleaving is race-free.
 *
 * Same-worker affinity for prep→post: tracked via `slotByMergeId`.
 */

import {
  mergeArcsFlat,
  mergeFlat,
  type FlatMultiPolygon,
  type FlatMultiPolygonArcs,
} from "./merge";
import { spawnWorker, type WorkerHandle } from "./worker-host";
import { copyView } from "./util";
import type { CtopoCore } from "./client";
import type { LayerGeometry, LayerSelection } from "./types";
import type {
  ComputeDiscardRequest,
  ComputePostReplyOk,
  ComputePostRequest,
  ComputePrepRequest,
  ComputeReply,
  ComputeShutdownRequest,
  CsrSpec,
  ViewSpec,
} from "./merge-wire";

// Variable (not an inline literal) so bundlers don't emit merge-worker.js
// as a dead static asset; the literal lives in the `new Worker(new URL(…))`
// thunk at the spawn site, which is what bundlers detect and bundle.
const MERGE_WORKER_ENTRY = "./merge-worker.js";

function resolveComputeWorkerUrl(): URL {
  return new URL(MERGE_WORKER_ENTRY, import.meta.url);
}

function abortError(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  return reason instanceof Error ? reason : new Error("aborted");
}

// Defer the bulk-fetch flush to the next "check" phase boundary
// (setImmediate in Node, falling back to setTimeout(0) elsewhere).
// Every prep reply that arrives during the current poll iteration
// adds to the same batch; the union is fetched in one core.fetchArcs
// call, so the section coalescer sees one drain instead of one per
// merge.
const scheduleBulkFlush: (fn: () => void) => void =
  typeof setImmediate === "function"
    ? (fn) => {
        setImmediate(fn);
      }
    : (fn) => {
        setTimeout(fn, 0);
      };

interface PoolSlot {
  readonly handle: WorkerHandle;
  // # of merges currently routed to this slot in EITHER phase. The
  // dispatcher uses this only as a load-balancing hint for new prep
  // dispatches — post phase always lands on the slot that ran prep.
  inflight: number;
  readonly ready: Promise<void>;
}

// One pending caller in a bulk-fetch batch. Each has its own arc id
// set; the batch unions them, issues one core.fetchArcs, and slices
// per caller on the way back.
interface BulkBatchEntry<T> {
  readonly ids: ReadonlyArray<number>;
  readonly resolve: (out: Map<number, T>) => void;
  readonly reject: (err: unknown) => void;
}

interface BulkBatchState<T> {
  readonly core: CtopoCore;
  readonly entries: BulkBatchEntry<T>[];
}

interface PendingMerge {
  readonly variant: "arcs" | "coords";
  readonly core: CtopoCore;
  readonly signal: AbortSignal | undefined;
  // The slot that ran prep — post MUST go to the same one because
  // it holds the per-merge intermediates.
  slotIdx: number;
  // Set when prep returns. Used to drive the bulk fetch.
  uniqueArcIds?: ReadonlyArray<number>;
  resolveFinal: (result: FlatMultiPolygon | FlatMultiPolygonArcs) => void;
  rejectFinal: (err: unknown) => void;
  resolvePrep: (ids: ReadonlyArray<number>) => void;
  rejectPrep: (err: unknown) => void;
}

export interface MergePoolOptions {
  readonly size: number;
}

export class MergePool {
  private readonly size: number;
  private readonly slots: PoolSlot[] = [];
  // mergeId → pending entry. Survives across prep, fetch, post.
  private readonly pending = new Map<number, PendingMerge>();
  private nextId = 0;
  private closed = false;
  private spawnPromise: Promise<void> | null = null;

  // Per-core, per-layer cached CsrSpec — same SAB-shared layer CSR
  // reused across all merges on the same layer. Stored as
  // Promise<CsrSpec> so concurrent first-call merges share one
  // in-flight copy. (435 concurrent merges in the bench previously
  // re-copied 163 MiB each — 70 GB of memcpy total — because the sync
  // `get(...)` returned undefined for every caller before the first
  // resolved.)
  //
  // Keyed by core (not by layer name alone): the pool is a process-wide
  // singleton shared across every client/container, so two containers
  // that both have a layer named e.g. "blocks" must NOT share a CSR —
  // their arc ids index different containers. The inner map is dropped
  // by `evictCore` when a core is torn down so its (potentially tens of
  // MiB) CSR SharedArrayBuffers don't live for the whole process.
  private readonly csrCache = new Map<
    CtopoCore,
    Map<string, Promise<CsrSpec>>
  >();

  // Coordinator-side bulk fetch accumulator. Prep replies stream in
  // as separate `message` events (one task each), so a per-merge
  // `core.fetchArcs(...)` would get its own coalescer drain — many
  // small drains instead of the baseline inline path's single huge
  // drain. Accumulate uniqueArcIds across all prep replies that
  // arrive within a batch window (setImmediate's "next check phase"
  // by default — captures everything from the current poll
  // iteration) and issue ONE `core.fetchArcs` with the union. Each
  // merge's await resolves with its own slice of the result.
  //
  // Keyed by core: the pool is a process-wide singleton shared across
  // every client/container, so two clients merging in the same poll
  // iteration must not be unioned into one fetch — their arc ids index
  // different containers. One batch (and one fetchArcs) per core.
  private readonly bulkBytesBatches = new Map<
    CtopoCore,
    BulkBatchState<Uint8Array>
  >();
  private readonly bulkEndpointsBatches = new Map<
    CtopoCore,
    BulkBatchState<Int32Array>
  >();

  constructor(opts: MergePoolOptions) {
    if (!Number.isInteger(opts.size) || opts.size < 1) {
      throw new Error(
        `ctopo: MergePool size must be a positive integer; got ${opts.size}`,
      );
    }
    this.size = opts.size;
  }

  async runArcs(
    core: CtopoCore,
    inputs: ReadonlyArray<LayerSelection>,
    signal: AbortSignal | undefined,
  ): Promise<FlatMultiPolygonArcs> {
    if (this.size === 1 || this.closed) {
      return mergeArcsFlat(core, inputs, signal);
    }
    return (await this.run(
      "arcs",
      core,
      inputs,
      signal,
    )) as FlatMultiPolygonArcs;
  }

  async runCoords(
    core: CtopoCore,
    inputs: ReadonlyArray<LayerSelection>,
    signal: AbortSignal | undefined,
  ): Promise<FlatMultiPolygon> {
    if (this.size === 1 || this.closed) {
      return mergeFlat(core, inputs, signal);
    }
    return (await this.run("coords", core, inputs, signal)) as FlatMultiPolygon;
  }

  private async run(
    variant: "arcs" | "coords",
    core: CtopoCore,
    inputs: ReadonlyArray<LayerSelection>,
    signal: AbortSignal | undefined,
  ): Promise<FlatMultiPolygon | FlatMultiPolygonArcs> {
    if (signal?.aborted) throw abortError(signal);
    await this.ensureSpawned();
    if (this.closed) throw new Error("ctopo: MergePool is closed");

    // Pre-load CSRs (one shared SAB copy per layer).
    const wireInputs: Array<{
      readonly layer: string;
      readonly indices: ReadonlyArray<number>;
    }> = [];
    const csrSpecPromises: Promise<CsrSpec>[] = [];
    let layerCache = this.csrCache.get(core);
    if (layerCache === undefined) {
      layerCache = new Map<string, Promise<CsrSpec>>();
      this.csrCache.set(core, layerCache);
    }
    for (const input of inputs) {
      let specP = layerCache.get(input.layer);
      if (specP === undefined) {
        // The CSR spec is cached per (core, layer) and shared across all
        // merges, so it must NOT be tied to one merge's abort signal —
        // otherwise aborting the first merge rejects this promise and poisons
        // the cache for every later merge on the layer. layerGeometry itself
        // is cached on the core, so the fetch happens once regardless; a
        // merge's own abort is enforced by withAbort on its prep/post phases.
        specP = (async (): Promise<CsrSpec> => {
          const csr = await core.layerGeometry(input.layer);
          return sabCsrSpecFrom(csr);
        })();
        // Don't let a transient failure stick in the cache — evict so the
        // next merge retries instead of inheriting the rejection.
        specP.catch(() => {
          if (layerCache.get(input.layer) === specP) {
            layerCache.delete(input.layer);
          }
        });
        layerCache.set(input.layer, specP);
      }
      csrSpecPromises.push(specP);
      wireInputs.push({
        layer: input.layer,
        indices: Array.isArray(input.indices)
          ? (input.indices as ReadonlyArray<number>)
          : Array.from(input.indices),
      });
    }
    const csrSpecs: CsrSpec[] = await Promise.all(csrSpecPromises);

    const id = ++this.nextId;
    const useEndpointsSection =
      variant === "arcs" &&
      core.hasArcEndpointsSection() &&
      core.transform !== null;

    // Pick a slot for prep. The slot is sticky for this merge's
    // entire lifecycle — post MUST land on the same worker because
    // that worker holds the prep intermediates.
    const slotIdx = this.pickSlot();
    const slot = this.slots[slotIdx];
    slot.inflight++;

    // From here on, an abort must both reject this call promptly and
    // tell the worker to drop its stash (prep may already have run).
    try {
      // Phase 1: prep.
      const uniqueArcIds = await this.withAbort(
        signal,
        new Promise<ReadonlyArray<number>>((resolve, reject) => {
          this.pending.set(id, {
            variant,
            core,
            signal,
            slotIdx,
            resolveFinal: () => {
              // Replaced below when post is dispatched.
            },
            rejectFinal: () => {
              // Replaced below.
            },
            resolvePrep: resolve,
            rejectPrep: reject,
          });
          const req: ComputePrepRequest = {
            kind: "prepMerge",
            id,
            variant,
            inputs: wireInputs,
            csrs: csrSpecs,
            numArcs: core.meta.numArcs,
            transform: core.transform,
            isQuantized: core.transform !== null,
            hasArcEndpointsSection: core.hasArcEndpointsSection(),
          };
          slot.handle.postMessage(req);
        }),
      );

      if (uniqueArcIds.length === 0) {
        // Empty merge — synthesize an empty result without involving
        // the post phase or any fetch.
        slot.inflight = Math.max(0, slot.inflight - 1);
        this.pending.delete(id);
        return emptyFlat(variant);
      }

      // Phase 2: bulk fetch on the coordinator. Multiple concurrent
      // merges hitting this point during the same poll iteration are
      // batched into ONE core.fetchArcs call so the section coalescer
      // sees them as a single drain (matching the inline path's
      // single multi-thousand-range microtask flush).
      let postRequest: ComputePostRequest;
      if (useEndpointsSection) {
        const endpointsMap = await this.withAbort(
          signal,
          this.bulkFetchEndpoints(core, uniqueArcIds),
        );
        const ids: number[] = [];
        const views: ViewSpec[] = [];
        for (const idVal of uniqueArcIds) {
          const v = endpointsMap.get(idVal);
          if (v === undefined) {
            throw new Error(
              `ctopo: fetchArcEndpoints returned no value for arc ${idVal}`,
            );
          }
          ids.push(idVal);
          views.push(toViewSpec(v));
        }
        postRequest = {
          kind: "postMerge",
          id,
          endpoints: { ids, views },
        };
      } else {
        const arcBytesMap = await this.withAbort(
          signal,
          this.bulkFetchBytes(core, uniqueArcIds),
        );
        const ids: number[] = [];
        const views: ViewSpec[] = [];
        for (const idVal of uniqueArcIds) {
          const v = arcBytesMap.get(idVal);
          if (v === undefined) {
            throw new Error(
              `ctopo: fetchArcs returned no bytes for arc ${idVal}`,
            );
          }
          ids.push(idVal);
          views.push(toViewSpec(v));
        }
        postRequest = {
          kind: "postMerge",
          id,
          arcBytes: { ids, views },
        };
      }

      // Phase 3: post — must go to the same slot.
      return await this.withAbort(
        signal,
        new Promise<FlatMultiPolygon | FlatMultiPolygonArcs>(
          (resolve, reject) => {
            const entry = this.pending.get(id);
            if (entry === undefined) {
              reject(new Error("ctopo: lost track of pending merge"));
              return;
            }
            entry.resolveFinal = resolve;
            entry.rejectFinal = reject;
            slot.handle.postMessage(postRequest);
          },
        ),
      );
    } catch (err) {
      this.discardMerge(id, slotIdx);
      throw err;
    }
  }

  // Drop this merge's coordinator state and tell its worker to discard
  // any stashed prep intermediates. Only messages the worker when the
  // merge was still pending here: a worker-originated error (prep/
  // postError) already cleaned its own stash and removed the pending
  // entry via handleReply, so there is nothing left to discard. The
  // remaining callers are aborts and coordinator-side throws, where
  // prep may have stashed but post never ran.
  private discardMerge(id: number, slotIdx: number): void {
    const wasPending = this.pending.delete(id);
    if (!wasPending) return;
    this.slots[slotIdx].inflight = Math.max(
      0,
      this.slots[slotIdx].inflight - 1,
    );
    const slot = this.slots[slotIdx];
    if (slot === undefined) return;
    const discard: ComputeDiscardRequest = { kind: "discardMerge", id };
    try {
      slot.handle.postMessage(discard);
    } catch {
      // Worker already gone (closed/terminated) — nothing to drop.
    }
  }

  // Race a phase promise against the caller's abort signal. On abort
  // the returned promise rejects immediately with the signal's reason;
  // the underlying worker work may still complete and is cleaned up by
  // the caller's `discardMerge`.
  private withAbort<T>(
    signal: AbortSignal | undefined,
    p: Promise<T>,
  ): Promise<T> {
    if (signal === undefined) return p;
    if (signal.aborted) return Promise.reject(abortError(signal));
    let onAbort!: () => void;
    const aborted = new Promise<never>((_, reject) => {
      onAbort = (): void => {
        reject(abortError(signal));
      };
      signal.addEventListener("abort", onAbort, { once: true });
    });
    return Promise.race([p, aborted]).finally(() => {
      signal.removeEventListener("abort", onAbort);
    });
  }

  // Batch concurrent fetchArcs requests across all in-flight merges.
  // Each caller's `ids` may overlap with others'; the batch issues a
  // single core.fetchArcs(union) and slices per caller. The flush is
  // deferred to the next "check" phase via `scheduleBulkFlush` so
  // every prep reply landing in the current poll iteration lands in
  // the same batch. Result: one coalescer drain per cohort instead
  // of one drain per merge.
  private bulkFetchBytes(
    core: CtopoCore,
    ids: ReadonlyArray<number>,
  ): Promise<Map<number, Uint8Array>> {
    return new Promise<Map<number, Uint8Array>>((resolve, reject) => {
      let batch = this.bulkBytesBatches.get(core);
      if (batch === undefined) {
        batch = { core, entries: [] };
        this.bulkBytesBatches.set(core, batch);
        scheduleBulkFlush(() => {
          void this.flushBulkBytes(core);
        });
      }
      batch.entries.push({ ids, resolve, reject });
    });
  }

  private bulkFetchEndpoints(
    core: CtopoCore,
    ids: ReadonlyArray<number>,
  ): Promise<Map<number, Int32Array>> {
    return new Promise<Map<number, Int32Array>>((resolve, reject) => {
      let batch = this.bulkEndpointsBatches.get(core);
      if (batch === undefined) {
        batch = { core, entries: [] };
        this.bulkEndpointsBatches.set(core, batch);
        scheduleBulkFlush(() => {
          void this.flushBulkEndpoints(core);
        });
      }
      batch.entries.push({ ids, resolve, reject });
    });
  }

  private async flushBulkBytes(core: CtopoCore): Promise<void> {
    const batch = this.bulkBytesBatches.get(core);
    if (batch === undefined) return;
    this.bulkBytesBatches.delete(core);
    const union = new Set<number>();
    for (const e of batch.entries) {
      for (const id of e.ids) union.add(id);
    }
    try {
      // No signal: this fetch unions arc ids from multiple callers
      // that may carry different signals. Results are cached, so a
      // caller aborting rejects its own merge (via the per-merge
      // abort race) without cancelling the shared fetch.
      const map = await batch.core.fetchArcs(Array.from(union), undefined);
      // Slice per-caller. The Map is shared (no per-caller copy);
      // each caller filters out just the ids it asked for.
      for (const entry of batch.entries) {
        const slice = new Map<number, Uint8Array>();
        for (const id of entry.ids) {
          const v = map.get(id);
          if (v !== undefined) slice.set(id, v);
        }
        entry.resolve(slice);
      }
    } catch (err) {
      for (const entry of batch.entries) entry.reject(err);
    }
  }

  private async flushBulkEndpoints(core: CtopoCore): Promise<void> {
    const batch = this.bulkEndpointsBatches.get(core);
    if (batch === undefined) return;
    this.bulkEndpointsBatches.delete(core);
    const union = new Set<number>();
    for (const e of batch.entries) {
      for (const id of e.ids) union.add(id);
    }
    try {
      const map = await batch.core.fetchArcEndpoints(
        Array.from(union),
        undefined,
      );
      for (const entry of batch.entries) {
        const slice = new Map<number, Int32Array>();
        for (const id of entry.ids) {
          const v = map.get(id);
          if (v !== undefined) slice.set(id, v);
        }
        entry.resolve(slice);
      }
    } catch (err) {
      for (const entry of batch.entries) entry.reject(err);
    }
  }

  // Drop a torn-down core's cached layer CSRs so their SharedArrayBuffer
  // copies (tens of MiB for a national layer) don't outlive the core.
  // Called by the coordinator when a core's last client closes.
  evictCore(core: CtopoCore): void {
    this.csrCache.delete(core);
    this.bulkBytesBatches.delete(core);
    this.bulkEndpointsBatches.delete(core);
  }

  // Least-loaded by inflight. Tie-break to lowest index.
  private pickSlot(): number {
    let bestIdx = 0;
    let bestLoad = this.slots[0].inflight;
    for (let i = 1; i < this.slots.length; i++) {
      if (this.slots[i].inflight < bestLoad) {
        bestLoad = this.slots[i].inflight;
        bestIdx = i;
      }
    }
    return bestIdx;
  }

  private async ensureSpawned(): Promise<void> {
    if (this.spawnPromise !== null) return this.spawnPromise;
    this.spawnPromise = this.spawn();
    return this.spawnPromise;
  }

  private async spawn(): Promise<void> {
    const nodeUrl = resolveComputeWorkerUrl();
    for (let i = 0; i < this.size; i++) {
      const slotIdx = i;
      const handle = await spawnWorker(
        () =>
          new Worker(new URL("./merge-worker.js", import.meta.url), {
            type: "module",
          }) as unknown as WorkerHandle,
        nodeUrl,
      );
      let resolveReady!: () => void;
      const ready = new Promise<void>((resolve, reject) => {
        resolveReady = resolve;
        handle.addEventListener("error", (e) => {
          reject(
            new Error(
              `ctopo: merge-worker spawn error: ${e.message ?? "unknown"}`,
            ),
          );
        });
      });
      handle.addEventListener("message", (e) => {
        const data = e.data as ComputeReply;
        if (data?.kind === "ready") {
          resolveReady();
          return;
        }
        this.handleReply(slotIdx, data);
      });
      // Post-spawn worker death (OOM on a large merge, an uncaught
      // async rejection in the worker) yields no further replies, so
      // every in-flight merge bound to a slot would hang. Fail them and
      // mark the pool closed — subsequent merges fall back to the inline
      // coordinator path (see the `this.closed` guard in `run`).
      handle.addEventListener("error", (e) => {
        this.failAll(
          new Error(`ctopo: merge-worker error: ${e.message ?? "unknown"}`),
        );
      });
      handle.addEventListener("exit", (e) => {
        const code = (e as { data?: number }).data;
        if (code !== 0) {
          this.failAll(
            new Error(
              `ctopo: merge-worker exited with code ${code ?? "unknown"}`,
            ),
          );
        }
      });
      this.slots.push({ handle, inflight: 0, ready });
    }
    await Promise.all(this.slots.map((s) => s.ready));
  }

  // Reject every in-flight merge (whichever phase is awaiting) and mark
  // the pool closed. Mirrors close()'s rejection, minus the graceful
  // worker shutdown — there's a dead worker, so there's nothing to ack.
  private failAll(err: Error): void {
    if (this.closed) return;
    this.closed = true;
    for (const p of this.pending.values()) {
      p.rejectPrep(err);
      p.rejectFinal(err);
    }
    this.pending.clear();
  }

  // A terminal reply (prep failed, or post resolved/failed) frees the
  // slot and drops the pending entry. prepResult is NOT terminal — the
  // merge stays in-flight through its post phase.
  private settle(slotIdx: number, id: number): void {
    this.slots[slotIdx].inflight = Math.max(
      0,
      this.slots[slotIdx].inflight - 1,
    );
    this.pending.delete(id);
  }

  private handleReply(slotIdx: number, data: ComputeReply): void {
    if (data.kind === "ready") return;
    if (data.kind === "prepResult") {
      const entry = this.pending.get(data.id);
      if (entry !== undefined) {
        entry.uniqueArcIds = data.uniqueArcIds;
        entry.resolvePrep(data.uniqueArcIds);
      }
      return;
    }
    if (data.kind === "prepError") {
      const entry = this.pending.get(data.id);
      if (entry !== undefined) {
        this.settle(slotIdx, data.id);
        entry.rejectPrep(new Error(data.error.message));
      }
      return;
    }
    if (data.kind === "postResult") {
      const entry = this.pending.get(data.id);
      if (entry !== undefined) {
        this.settle(slotIdx, data.id);
        try {
          const flat = reconstructFlat(data);
          entry.resolveFinal(flat);
        } catch (err) {
          entry.rejectFinal(err);
        }
      }
      return;
    }
    if (data.kind === "postError") {
      const entry = this.pending.get(data.id);
      if (entry !== undefined) {
        this.settle(slotIdx, data.id);
        entry.rejectFinal(new Error(data.error.message));
      }
      return;
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.spawnPromise !== null) {
      try {
        await this.spawnPromise;
      } catch {
        // ignore
      }
    }
    const err = new Error("ctopo: MergePool closed");
    for (const p of this.pending.values()) {
      // Reject whichever phase is currently awaiting.
      p.rejectPrep(err);
      p.rejectFinal(err);
    }
    this.pending.clear();
    this.csrCache.clear();
    const shutdown: ComputeShutdownRequest = { kind: "shutdown" };
    for (const slot of this.slots) {
      try {
        slot.handle.postMessage(shutdown);
      } catch {
        // ignore
      }
      try {
        await slot.handle.terminate?.();
      } catch {
        // ignore
      }
    }
    this.slots.length = 0;
  }
}

function toViewSpec(view: ArrayBufferView): ViewSpec {
  return {
    buffer: view.buffer,
    byteOffset: view.byteOffset,
    byteLength: view.byteLength,
  };
}

function toSabViewSpec(view: ArrayBufferView): ViewSpec {
  // The pool only runs when SharedArrayBuffer is usable (see
  // worker.ts effectivePoolSize), so the constructor here is safe.
  // Reuse the view in place when it's already SAB-backed; otherwise
  // copy into a fresh SAB so it can cross to the compute workers.
  if (view.buffer instanceof SharedArrayBuffer) return toViewSpec(view);
  const sab = copyView(view, true);
  return { buffer: sab, byteOffset: 0, byteLength: view.byteLength };
}

function sabCsrSpecFrom(csr: LayerGeometry): CsrSpec {
  return {
    polyOffsets: toSabViewSpec(csr.polyOffsets),
    ringOffsets: toSabViewSpec(csr.ringOffsets),
    arcRefs: toSabViewSpec(csr.arcRefs),
    multiPolyBreaks: toSabViewSpec(csr.multiPolyBreaks),
  };
}

function reconstructFlat(
  data: ComputePostReplyOk,
): FlatMultiPolygon | FlatMultiPolygonArcs {
  if (data.variant === "arcs") {
    return {
      type: "MultiPolygon",
      arcs: new Int32Array(data.payloadBuffer),
      ringStarts: new Uint32Array(data.ringStartsBuffer),
      ringEnds: new Uint32Array(data.ringEndsBuffer),
      polyRingStarts: new Uint32Array(data.polyRingStartsBuffer),
    };
  }
  return {
    type: "MultiPolygon",
    coords: new Float64Array(data.payloadBuffer),
    ringStarts: new Uint32Array(data.ringStartsBuffer),
    ringEnds: new Uint32Array(data.ringEndsBuffer),
    polyRingStarts: new Uint32Array(data.polyRingStartsBuffer),
  };
}

function emptyFlat(
  variant: "arcs" | "coords",
): FlatMultiPolygonArcs | FlatMultiPolygon {
  const empty32 = new Uint32Array(1);
  if (variant === "arcs") {
    return {
      type: "MultiPolygon",
      arcs: new Int32Array(0),
      ringStarts: new Uint32Array(0),
      ringEnds: new Uint32Array(0),
      polyRingStarts: empty32,
    };
  }
  return {
    type: "MultiPolygon",
    coords: new Float64Array(0),
    ringStarts: new Uint32Array(0),
    ringEnds: new Uint32Array(0),
    polyRingStarts: empty32,
  };
}
