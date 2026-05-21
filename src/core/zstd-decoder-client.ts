// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Client API for the zstd sub-worker pool. Lives in the merge worker;
 * the sub-worker entry is `zstd-worker.ts`.
 *
 * Shared-memory output path: the recompiled zstd wasm declares a
 * shared linear memory (built with `+atomics --shared-memory
 * --import-memory`). The merge worker creates one
 * `WebAssembly.Memory({shared:true})` PER sub-worker at init and
 * keeps a reference to its `.buffer` (a SharedArrayBuffer). For each
 * decompress the sub-worker mallocs the output inside that shared
 * memory and replies with `{ptr, byteLength}` — the merge worker
 * wraps a `Uint8Array(sab, ptr, byteLength)` view directly without
 * any cross-thread memcpy. The view stays valid until the merge
 * worker posts a `free` message back releasing the wasm allocation.
 *
 * Why per-sub-worker memory rather than one Memory shared across
 * all sub-workers: zstd's allocator (dlmalloc) isn't lock-free.
 * Sharing one memory would force every sub-worker through one
 * malloc lock, contending exactly during the parallel work we're
 * trying to do. With one memory per sub-worker each one mallocs
 * independently; the merge worker just remembers which SAB each
 * pointer belongs to.
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
import { loadZstdWasmBytes } from "./zstd-wasm";
import { copyView } from "./util";

// Node/`worker_threads` worker file URL resolver — bundled dist/ keeps
// the worker as a sibling of this module; src/-loaded test runs fall
// through to the pretest-built dist/. The browser/bundler path doesn't
// come through here: it spawns via the literal `new Worker(new URL(…))`
// thunk at the call site (the only form bundlers detect and bundle).
// Entry paths are kept in variables so bundlers don't also emit
// zstd-worker.js as a dead static asset.
const ZSTD_WORKER_ENTRY = "./zstd-worker.js";
const ZSTD_WORKER_ENTRY_FROM_SRC = "../../dist/zstd-worker.js";
async function resolveZstdWorkerUrl(): Promise<string> {
  const sibling = new URL(ZSTD_WORKER_ENTRY, import.meta.url);
  // Browser / bundler: the worker is emitted next to this chunk.
  if (sibling.protocol !== "file:") return sibling.href;
  const fs = await import("node:fs");
  if (fs.existsSync(sibling)) return sibling.href;
  // Running from the TS source tree (vitest loads src/core/*.ts): the
  // pretest-built worker lives in the repo's dist/. Guard on /src/ so
  // we never rewrite a real dist/ sibling into a bogus dist/dist/ path
  // for an installed package.
  if (sibling.pathname.includes("/src/")) {
    return new URL(ZSTD_WORKER_ENTRY_FROM_SRC, import.meta.url).href;
  }
  return sibling.href;
}

// Memoize the wasm-bytes load — every sub-worker gets the same
// bytes, no need to re-fetch.
let wasmBytesPromise: Promise<ArrayBuffer> | undefined;
function loadWasmBytes(): Promise<ArrayBuffer> {
  if (wasmBytesPromise === undefined) wasmBytesPromise = loadZstdWasmBytes();
  return wasmBytesPromise;
}

interface SpawnReadyMsg {
  readonly kind: "spawn-ready";
}

interface DecompressReplyOk {
  readonly id: number;
  readonly ok: true;
  readonly ptr: number;
  readonly byteLength: number;
  readonly capacity: number;
}

interface AckReplyOk {
  readonly id: number;
  readonly ok: true;
}

interface ReplyErr {
  readonly id: number;
  readonly ok: false;
  readonly error: { readonly name: string; readonly message: string };
}

type Reply = DecompressReplyOk | AckReplyOk | ReplyErr | SpawnReadyMsg;

interface PendingDecompress {
  readonly kind: "decompress";
  readonly resolve: (bytes: Uint8Array) => void;
  readonly reject: (err: Error) => void;
  readonly slot: number;
}

interface PendingAck {
  readonly kind: "ack";
  readonly resolve: () => void;
  readonly reject: (err: Error) => void;
}

type Pending = PendingDecompress | PendingAck;

interface WorkerState {
  readonly handle: WorkerHandle;
  inflight: number;
  nextMsgId: number;
  readonly pending: Map<number, Pending>;
  // The shared wasm memory. We hold the WebAssembly.Memory and
  // re-read `.buffer` on each reply because Node may swap the
  // underlying SAB instance when wasm grows the memory; a cached
  // reference would point at the old (smaller) buffer and miss any
  // pointer past the original `initial` pages.
  memory?: WebAssembly.Memory;
  // Resolved once the worker has finished its init handshake.
  readonly ready: Promise<void>;
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
  // Clamp to the core count. This client is a singleton *per merge
  // worker*, so with a merge pool of M workers the process already runs
  // ~M zstd sub-workers per slot; defaulting to 2 each on a low-core box
  // oversubscribes the CPU. hardwareConcurrency is available in browsers
  // and Node ≥ 21.
  const hc =
    typeof navigator !== "undefined" &&
    typeof navigator.hardwareConcurrency === "number"
      ? navigator.hardwareConcurrency
      : undefined;
  return hc !== undefined && hc >= 1
    ? Math.max(1, Math.min(DEFAULT_POOL_SIZE, hc))
    : DEFAULT_POOL_SIZE;
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
  private readonly dictRegistered = new WeakMap<Uint8Array, Promise<number>>();
  private nextDictId = 1;

  // The output Uint8Array views we return reference SAB-backed wasm
  // memory and stay alive as long as the byteRangeCache holds them.
  // Once the cache evicts an entry we want to release the wasm
  // allocation — otherwise the wasm memory grows unbounded. The
  // FinalizationRegistry fires when the GC has collected the view
  // (so nothing references it anymore); we post a `free` to the
  // sub-worker that owns the allocation.
  private readonly freeRegistry: FinalizationRegistry<{
    slot: number;
    ptr: number;
    capacity: number;
  }>;

  constructor(poolSize?: number) {
    this.poolSize = poolSize ?? defaultPoolSize();
    this.pool = Array.from({ length: this.poolSize }, () => null);
    this.freeRegistry = new FinalizationRegistry((token) => {
      const slot = this.pool[token.slot];
      if (slot === null) return; // worker already gone
      try {
        slot.handle.postMessage({
          type: "free",
          id: 0,
          ptr: token.ptr,
          capacity: token.capacity,
        });
      } catch {
        // ignore — worker may have died
      }
    });
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
    const nodeUrl = await resolveZstdWorkerUrl();
    const handle = await spawnWorker(
      () =>
        new Worker(new URL("./zstd-worker.js", import.meta.url), {
          type: "module",
        }) as unknown as WorkerHandle,
      nodeUrl,
    );
    let resolveReady!: () => void;
    let rejectReady!: (err: Error) => void;
    const ready = new Promise<void>((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });
    const state: WorkerState = {
      handle,
      inflight: 0,
      nextMsgId: 1,
      pending: new Map(),
      ready,
    };
    handle.addEventListener("message", (e) => {
      const r = e.data as Reply;
      if ("kind" in r && r.kind === "spawn-ready") {
        // First message from the worker — send the init payload.
        // The merge worker's shared wasm memory is created here so
        // every sub-worker boot can be parallelized.
        void (async (): Promise<void> => {
          try {
            const wasmBytes = await loadWasmBytes();
            // Initial + maximum from the wasm-bindgen-generated JS
            // (the same values the wasm module declares). Initial
            // 18 pages = 1.1 MiB; we let it grow up to the cap.
            // The max-memory link flag is 4 GiB / 65536 pages.
            const memory = new WebAssembly.Memory({
              initial: 18,
              maximum: 65536,
              shared: true,
            });
            state.memory = memory;
            const id = state.nextMsgId++;
            await this.sendAck(state, {
              type: "init",
              id,
              memory,
              wasmBytes,
            });
            resolveReady();
          } catch (err) {
            rejectReady(err instanceof Error ? err : new Error(String(err)));
            this.failAll(err instanceof Error ? err : new Error(String(err)));
          }
        })();
        return;
      }
      const reply = r as DecompressReplyOk | AckReplyOk | ReplyErr;
      const p = state.pending.get(reply.id);
      if (p === undefined) return;
      state.pending.delete(reply.id);
      state.inflight--;
      if (!reply.ok) {
        p.reject(new Error(reply.error.message));
        return;
      }
      if (p.kind === "decompress") {
        const ok = reply as DecompressReplyOk;
        const memory = state.memory;
        if (memory === undefined) {
          p.reject(
            new Error(
              "ctopo: zstd worker replied with decompress result before init shared buffer",
            ),
          );
          return;
        }
        // Re-read .buffer freshly — after wasm.grow() Node may swap
        // the SAB instance, and a stale reference would miss the new
        // pages. Each Uint8Array view is constructed over whichever
        // buffer is current at reply time.
        const sab = memory.buffer as unknown as SharedArrayBuffer;
        const view = new Uint8Array(sab, ok.ptr, ok.byteLength);
        // Register the view with the freeRegistry so the wasm
        // allocation gets released when the caller's reference is
        // collected. Capacity is used (not byteLength) because that's
        // the size we malloc'd.
        this.freeRegistry.register(
          view,
          { slot: p.slot, ptr: ok.ptr, capacity: ok.capacity },
          view,
        );
        p.resolve(view);
      } else {
        p.resolve();
      }
    });
    handle.addEventListener("error", (e) => {
      const detail = (e as { message?: string }).message ?? "unknown";
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
    this.warmPromise = (async (): Promise<void> => {
      const states = await Promise.all(
        Array.from({ length: this.poolSize }, (_, i) => this.spawnOne(i)),
      );
      await Promise.all(states.map((s) => s.ready));
    })();
    return this.warmPromise;
  }

  // Pool is fully spawned by the time this returns. Picks the worker
  // with the fewest inflight jobs.
  private async pickLeastLoaded(): Promise<{
    state: WorkerState;
    slot: number;
  }> {
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
    return { state: this.pool[bestSlot]!, slot: bestSlot };
  }

  private sendDecompress(
    target: WorkerState,
    slot: number,
    msg: { type: string; id: number } & Record<string, unknown>,
    transfer: ArrayBuffer[],
  ): Promise<Uint8Array> {
    return new Promise<Uint8Array>((resolve, reject) => {
      target.pending.set(msg.id, { kind: "decompress", resolve, reject, slot });
      target.inflight++;
      target.handle.postMessage(msg, transfer);
    });
  }

  private sendAck(
    target: WorkerState,
    msg: { type: string; id: number } & Record<string, unknown>,
    transfer: ArrayBuffer[] = [],
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      target.pending.set(msg.id, { kind: "ack", resolve, reject });
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
          const copy = copyView(dict);
          const id = worker.nextMsgId++;
          return this.sendAck(
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
    const { state, slot } = await this.pickLeastLoaded();
    // `bytes` is typically a subarray of a larger section/group buffer
    // that the caller still needs intact. Copy into a fresh
    // ArrayBuffer (`bytes.slice()` would alias the original when
    // `bytes` is a Node Buffer — Buffer overrides slice to return a
    // view, not a copy).
    const copy = copyView(bytes);
    const id = state.nextMsgId++;
    return this.sendDecompress(
      state,
      slot,
      { type: "decompress", id, bytes: copy, capacity, dictId },
      [copy],
    );
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
