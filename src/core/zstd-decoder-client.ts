// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Client API for the zstd sub-worker pool. Lives in the merge worker;
 * the sub-worker entry is `zstd-worker.ts`.
 *
 * Lazy pool — sub-workers come up on first decompress need (matches
 * the previous module-level `wasmZstdReady` semantics: zstd cost is
 * paid only when something is actually compressed). Pool size from
 * `CTOPO_ZSTD_POOL_SIZE` (Node env), default 2 — chosen from the
 * national-merge bench sweep, which showed n=2 strictly better than
 * n=1 and noticeably better than n=4 (extra workers contend with the
 * merge worker for cores + cache without enough decompress concurrency
 * to absorb them). Single-worker mode (`=1`) reproduces the pre-pool
 * behavior.
 *
 * Dict handling: dicts are large (~110 KiB for the national topo) and
 * the per-dict DDict digest is expensive to rebuild. On first use we
 * broadcast the dict to every worker — each pays the digest cost once
 * (~3 ms for a ~110 KiB dict) and then holds its own
 * `CtopoDecompressor`. After that any worker can decompress a frame
 * using that dict, so dict-aware decompresses fan out just like
 * non-dict ones. Memory cost is `pool_size × dict_bytes`; for the
 * national workload (~110 KiB × 4) that's negligible vs avoiding
 * single-worker serialization on the hot arc_coords dict.
 */

import { spawnWorker, type WorkerHandle } from "./worker-host";

// Worker file URL resolver — bundled dist/ keeps the worker as a
// sibling of this module; src/-loaded test runs fall through to the
// pretest-built dist/.
async function resolveZstdWorkerUrl(): Promise<string> {
  const sibling = new URL("./zstd-worker.js", import.meta.url);
  if (sibling.protocol !== "file:") return sibling.href;
  const fs = await import("node:fs");
  if (fs.existsSync(sibling)) return sibling.href;
  return new URL("../../dist/zstd-worker.js", import.meta.url).href;
}

interface ReplyOk {
  readonly id: number;
  readonly ok: true;
  readonly bytes?: ArrayBuffer;
}

interface ReplyErr {
  readonly id: number;
  readonly ok: false;
  readonly error: { readonly name: string; readonly message: string };
}

type Reply = ReplyOk | ReplyErr;

interface PendingCall {
  readonly resolve: (bytes: Uint8Array | undefined) => void;
  readonly reject: (err: Error) => void;
}

interface WorkerState {
  readonly handle: WorkerHandle;
  inflight: number;
  nextMsgId: number;
  readonly pending: Map<number, PendingCall>;
}

const DEFAULT_POOL_SIZE = 2;

function defaultPoolSize(): number {
  const env =
    typeof process !== "undefined"
      ? process.env?.CTOPO_ZSTD_POOL_SIZE
      : undefined;
  if (env !== undefined) {
    const n = parseInt(env, 10);
    if (Number.isFinite(n) && n >= 1) return n;
  }
  return DEFAULT_POOL_SIZE;
}

// Allocate a fresh ArrayBuffer carrying a copy of `src`. Uses
// `set()` rather than `Uint8Array.prototype.slice()` because Node's
// Buffer overrides slice to return a view (deprecated but still
// active) — slicing a Buffer and transferring the result's `.buffer`
// would detach the source.
function copyToFreshBuffer(src: Uint8Array): ArrayBuffer {
  const out = new ArrayBuffer(src.byteLength);
  new Uint8Array(out).set(src);
  return out;
}

class ZstdDecoderClient {
  private readonly poolSize: number;
  private readonly pool: (WorkerState | null)[];
  // Single promise covering "spawn every slot in parallel". Both
  // decompress and registerDict await this so the pool is always
  // fully up before any routing decision. Without it, many concurrent
  // first-call decompresses would all serialize on whichever slot
  // happened to start spawning first.
  private warmPromise: Promise<void> | null = null;
  // Sticky after any worker fails to spawn / dies; subsequent calls
  // reject immediately rather than hanging on a never-arriving reply.
  private terminalError: Error | null = null;

  // Each registered dict resolves to its dictId after the broadcast
  // register-dict round-trip has acked from every worker in the pool.
  // Subsequent dict-aware decompresses can route to ANY worker
  // because every worker holds its own CtopoDecompressor for that
  // dictId.
  private readonly dictRegistered = new WeakMap<
    Uint8Array,
    Promise<number>
  >();
  private nextDictId = 1;

  constructor(poolSize?: number) {
    this.poolSize = poolSize ?? defaultPoolSize();
    this.pool = new Array(this.poolSize).fill(null);
  }

  private failAll(err: Error): void {
    this.terminalError = err;
    for (const w of this.pool) {
      if (w === null) continue;
      for (const p of w.pending.values()) p.reject(err);
      w.pending.clear();
    }
    for (let i = 0; i < this.pool.length; i++) {
      this.pool[i] = null;
    }
    this.warmPromise = null;
  }

  private async spawnOne(slot: number): Promise<WorkerState> {
    const url = await resolveZstdWorkerUrl();
    const handle = await spawnWorker(url);
    const state: WorkerState = {
      handle,
      inflight: 0,
      nextMsgId: 1,
      pending: new Map(),
    };
    handle.addEventListener("message", (e) => {
      const r = e.data as Reply;
      const p = state.pending.get(r.id);
      if (p === undefined) return;
      state.pending.delete(r.id);
      state.inflight--;
      if (r.ok) {
        p.resolve(r.bytes !== undefined ? new Uint8Array(r.bytes) : undefined);
      } else {
        p.reject(new Error(r.error.message));
      }
    });
    handle.addEventListener("error", (e) => {
      const detail = (e as { message?: string }).message ?? String(e);
      this.failAll(new Error(`ctopo: zstd sub-worker error: ${detail}`));
    });
    handle.addEventListener("exit", (e) => {
      const code = (e as unknown as { data?: number }).data;
      if (code !== 0) {
        this.failAll(
          new Error(`ctopo: zstd sub-worker exited with code ${code}`),
        );
      }
    });
    this.pool[slot] = state;
    return state;
  }

  private warmAll(): Promise<void> {
    if (this.terminalError !== null) return Promise.reject(this.terminalError);
    if (this.warmPromise !== null) return this.warmPromise;
    this.warmPromise = Promise.all(
      Array.from({ length: this.poolSize }, (_, i) => this.spawnOne(i)),
    ).then(() => undefined);
    return this.warmPromise;
  }

  // Pool is fully spawned by the time this returns. Picks the worker
  // with the fewest inflight jobs.
  private async pickLeastLoaded(): Promise<WorkerState> {
    await this.warmAll();
    if (this.terminalError !== null) throw this.terminalError;
    let bestSlot = 0;
    let bestLoad = Infinity;
    for (let i = 0; i < this.poolSize; i++) {
      const w = this.pool[i]!;
      if (w.inflight < bestLoad) {
        bestLoad = w.inflight;
        bestSlot = i;
      }
    }
    return this.pool[bestSlot]!;
  }

  private send(
    target: WorkerState,
    msg: { type: string; id: number } & Record<string, unknown>,
    transfer: ArrayBuffer[],
  ): Promise<Uint8Array | undefined> {
    return new Promise<Uint8Array | undefined>((resolve, reject) => {
      target.pending.set(msg.id, { resolve, reject });
      target.inflight++;
      target.handle.postMessage(msg, transfer);
    });
  }

  private async registerDict(dict: Uint8Array): Promise<number> {
    const existing = this.dictRegistered.get(dict);
    if (existing !== undefined) return existing;
    const dictId = this.nextDictId++;
    const promise = (async (): Promise<number> => {
      // Broadcast to every slot. Each worker pays its own
      // ZSTD_createDDict (~3 ms for the national arc_coords dict);
      // doing them concurrently keeps the wall-clock cost ≈ one
      // single-worker register.
      await this.warmAll();
      const workers = this.pool as WorkerState[];
      await Promise.all(
        workers.map((worker) => {
          const copy = copyToFreshBuffer(dict);
          const id = worker.nextMsgId++;
          return this.send(
            worker,
            { type: "register-dict", id, dictId, dict: copy },
            [copy],
          );
        }),
      );
      return dictId;
    })();
    this.dictRegistered.set(dict, promise);
    return promise;
  }

  // Pre-spawn the whole pool (and start their WASM inits). Returns
  // when every slot is ready. Called from the open path so the pool
  // is up by the time the first decompress lands (overlap with the
  // header round-trip).
  warm(): Promise<void> {
    return this.warmAll();
  }

  async decompress(
    bytes: Uint8Array,
    capacity: number,
    dict?: Uint8Array,
  ): Promise<Uint8Array> {
    if (this.terminalError !== null) throw this.terminalError;
    let dictId: number | undefined;
    if (dict !== undefined) {
      dictId = await this.registerDict(dict);
    }
    const target = await this.pickLeastLoaded();
    // `bytes` is typically a subarray of a larger section/group buffer
    // that the caller still needs intact. Copy into a fresh
    // ArrayBuffer (`bytes.slice()` would alias the original when
    // `bytes` is a Node Buffer — Buffer overrides slice to return a
    // view, not a copy).
    const copy = copyToFreshBuffer(bytes);
    const id = target.nextMsgId++;
    const out = await this.send(
      target,
      { type: "decompress", id, bytes: copy, capacity, dictId },
      [copy],
    );
    if (out === undefined) {
      throw new Error("ctopo: zstd sub-worker returned no bytes");
    }
    return out;
  }

  close(): void {
    for (let i = 0; i < this.pool.length; i++) {
      const w = this.pool[i];
      if (w === null) continue;
      try {
        w.handle.postMessage({ type: "close", id: 0 });
      } catch {
        // ignore — worker may already be gone
      }
      void w.handle.terminate?.();
      this.pool[i] = null;
    }
    this.warmPromise = null;
  }
}

// Singleton — one zstd sub-worker pool per merge-worker process. The
// merge worker is itself singleton-per-process (or per-tab), so this
// matches the prior `wasmZstdReady` lifetime exactly.
let instance: ZstdDecoderClient | null = null;

export function getZstdDecoderClient(): ZstdDecoderClient {
  if (instance === null) instance = new ZstdDecoderClient();
  return instance;
}

export function closeZstdDecoderClient(): void {
  if (instance !== null) {
    instance.close();
    instance = null;
  }
}
