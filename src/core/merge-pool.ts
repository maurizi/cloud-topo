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
import type { CtopoCore } from "./client";
import type { LayerGeometry, LayerSelection } from "./types";
import type {
  ComputePostReplyError,
  ComputePostReplyOk,
  ComputePostRequest,
  ComputePrepReplyError,
  ComputePrepReplyOk,
  ComputePrepRequest,
  ComputeReply,
  ComputeShutdownRequest,
  CsrSpec,
  ViewSpec,
} from "./merge-wire";

function resolveComputeWorkerUrl(): string {
  return new URL("./merge-worker.js", import.meta.url).href;
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
  // Driving core/signal — first caller's wins (the bench has one
  // core per coordinator so this is fine; multi-client coordinators
  // would queue separate batches per core if needed).
  readonly core: CtopoCore;
  readonly signal: AbortSignal | undefined;
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

  // Per-layer cached CsrSpec — same SAB-shared layer CSR reused
  // across all merges on the same layer. Stored as Promise<CsrSpec>
  // so concurrent first-call merges share one in-flight copy. (435
  // concurrent merges in the bench previously re-copied 163 MiB
  // each — 70 GB of memcpy total — because the sync `get(...)`
  // returned undefined for every caller before the first resolved.)
  private readonly csrCache = new Map<string, Promise<CsrSpec>>();

  // Coordinator-side bulk fetch accumulator. Prep replies stream in
  // as separate `message` events (one task each), so a per-merge
  // `core.fetchArcs(...)` would get its own coalescer drain — many
  // small drains instead of the baseline inline path's single huge
  // drain. Accumulate uniqueArcIds across all prep replies that
  // arrive within a batch window (setImmediate's "next check phase"
  // by default — captures everything from the current poll
  // iteration) and issue ONE `core.fetchArcs` with the union. Each
  // merge's await resolves with its own slice of the result.
  private bulkBytesBatch: BulkBatchState<Uint8Array> | null = null;
  private bulkEndpointsBatch: BulkBatchState<Int32Array> | null = null;

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
    return (await this.run("arcs", core, inputs, signal)) as FlatMultiPolygonArcs;
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
    await this.ensureSpawned();
    if (this.closed) throw new Error("ctopo: MergePool is closed");

    // Pre-load CSRs (one shared SAB copy per layer).
    const wireInputs: Array<{
      readonly layer: string;
      readonly indices: ReadonlyArray<number>;
    }> = [];
    const csrSpecPromises: Promise<CsrSpec>[] = [];
    for (const input of inputs) {
      let specP = this.csrCache.get(input.layer);
      if (specP === undefined) {
        specP = (async (): Promise<CsrSpec> => {
          const csr = await core.layerGeometry(input.layer, signal);
          return sabCsrSpecFrom(csr);
        })();
        this.csrCache.set(input.layer, specP);
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

    // Phase 1: prep.
    const uniqueArcIds = await new Promise<ReadonlyArray<number>>(
      (resolve, reject) => {
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
      },
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
      const endpointsMap = await this.bulkFetchEndpoints(
        core,
        uniqueArcIds,
        signal,
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
      const arcBytesMap = await this.bulkFetchBytes(core, uniqueArcIds, signal);
      const ids: number[] = [];
      const views: ViewSpec[] = [];
      for (const idVal of uniqueArcIds) {
        const v = arcBytesMap.get(idVal);
        if (v === undefined) {
          throw new Error(`ctopo: fetchArcs returned no bytes for arc ${idVal}`);
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
    return new Promise<FlatMultiPolygon | FlatMultiPolygonArcs>(
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
    );
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
    signal: AbortSignal | undefined,
  ): Promise<Map<number, Uint8Array>> {
    return new Promise<Map<number, Uint8Array>>((resolve, reject) => {
      if (this.bulkBytesBatch === null) {
        this.bulkBytesBatch = { core, signal, entries: [] };
        scheduleBulkFlush(() => this.flushBulkBytes());
      }
      this.bulkBytesBatch.entries.push({ ids, resolve, reject });
    });
  }

  private bulkFetchEndpoints(
    core: CtopoCore,
    ids: ReadonlyArray<number>,
    signal: AbortSignal | undefined,
  ): Promise<Map<number, Int32Array>> {
    return new Promise<Map<number, Int32Array>>((resolve, reject) => {
      if (this.bulkEndpointsBatch === null) {
        this.bulkEndpointsBatch = { core, signal, entries: [] };
        scheduleBulkFlush(() => this.flushBulkEndpoints());
      }
      this.bulkEndpointsBatch.entries.push({ ids, resolve, reject });
    });
  }

  private async flushBulkBytes(): Promise<void> {
    const batch = this.bulkBytesBatch;
    if (batch === null) return;
    this.bulkBytesBatch = null;
    const union = new Set<number>();
    for (const e of batch.entries) {
      for (const id of e.ids) union.add(id);
    }
    try {
      const map = await batch.core.fetchArcs(
        Array.from(union),
        batch.signal,
      );
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

  private async flushBulkEndpoints(): Promise<void> {
    const batch = this.bulkEndpointsBatch;
    if (batch === null) return;
    this.bulkEndpointsBatch = null;
    const union = new Set<number>();
    for (const e of batch.entries) {
      for (const id of e.ids) union.add(id);
    }
    try {
      const map = await batch.core.fetchArcEndpoints(
        Array.from(union),
        batch.signal,
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
    const url = resolveComputeWorkerUrl();
    for (let i = 0; i < this.size; i++) {
      const slotIdx = i;
      const handle = await spawnWorker(url);
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
      this.slots.push({ handle, inflight: 0, ready });
    }
    await Promise.all(this.slots.map((s) => s.ready));
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
        this.slots[slotIdx].inflight = Math.max(
          0,
          this.slots[slotIdx].inflight - 1,
        );
        this.pending.delete(data.id);
        entry.rejectPrep(new Error(data.error.message));
      }
      return;
    }
    if (data.kind === "postResult") {
      const entry = this.pending.get(data.id);
      if (entry !== undefined) {
        this.slots[slotIdx].inflight = Math.max(
          0,
          this.slots[slotIdx].inflight - 1,
        );
        this.pending.delete(data.id);
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
        this.slots[slotIdx].inflight = Math.max(
          0,
          this.slots[slotIdx].inflight - 1,
        );
        this.pending.delete(data.id);
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

function viewSpecToSab(view: ArrayBufferView): ViewSpec {
  const buf = view.buffer;
  if (
    typeof SharedArrayBuffer !== "undefined" &&
    buf instanceof SharedArrayBuffer
  ) {
    return toViewSpec(view);
  }
  const sab = new SharedArrayBuffer(view.byteLength);
  new Uint8Array(sab).set(
    new Uint8Array(buf as ArrayBuffer, view.byteOffset, view.byteLength),
  );
  return { buffer: sab, byteOffset: 0, byteLength: view.byteLength };
}

function sabCsrSpecFrom(csr: LayerGeometry): CsrSpec {
  return {
    polyOffsets: viewSpecToSab(csr.polyOffsets),
    ringOffsets: viewSpecToSab(csr.ringOffsets),
    arcRefs: viewSpecToSab(csr.arcRefs),
    multiPolyBreaks: viewSpecToSab(csr.multiPolyBreaks),
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
