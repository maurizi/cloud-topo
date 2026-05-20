// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Sub-worker that owns the zstd WASM instance + DDict cache.
 * Spawned by the merge worker on first decompress; protocol lives in
 * `zstd-decoder-client.ts`.
 *
 * Shared-memory output path: this worker is built against a shared
 * wasm memory (the recompiled crates/zstd-decoder with `+atomics` +
 * `--shared-memory`). Decompressed bytes live in the wasm memory's
 * SharedArrayBuffer; we postMessage the buffer back to the merge
 * worker once at init so the caller can construct Uint8Array views
 * over a sub-worker's wasm memory without any cross-thread copy.
 *
 * Per decompress: we malloc `capacity` bytes inside wasm memory
 * (`__wbindgen_export` is the wasm-bindgen-emitted malloc), call
 * `decompress_into` to write the output directly there, and reply
 * with `{id, ok, ptr, byteLength}`. The caller wraps the SAB at
 * `ptr..ptr+byteLength`. We don't free immediately — the cache holds
 * the view; an explicit `free` message released it.
 */

import init, {
  CtopoDecompressor,
  decompress_no_dict_into,
} from "./zstd-wasm/ctopo_zstd_decoder.js";
import { getWorkerHost } from "./worker-host";

// Wire types for incoming messages.
interface RegisterDictMsg {
  readonly type: "register-dict";
  readonly id: number;
  readonly dictId: number;
  readonly dict: ArrayBuffer;
}

interface DecompressMsg {
  readonly type: "decompress";
  readonly id: number;
  readonly bytes: ArrayBuffer;
  readonly capacity: number;
  readonly dictId?: number;
}

interface FreeMsg {
  readonly type: "free";
  readonly id: number;
  readonly ptr: number;
  readonly capacity: number;
}

interface InitMsg {
  readonly type: "init";
  readonly id: number;
  // Shared memory the merge worker created so every sub-worker can
  // use the same one. Letting one sub-worker create its own memory
  // and broadcasting it back would also work; this direction is
  // cheaper because the merge worker is the natural owner.
  readonly memory: WebAssembly.Memory;
  // Pre-fetched wasm bytes the merge worker has already loaded —
  // saves each sub-worker from re-fetching the same binary.
  readonly wasmBytes: ArrayBuffer;
}

interface CloseMsg {
  readonly type: "close";
}

type IncomingMsg =
  | InitMsg
  | RegisterDictMsg
  | DecompressMsg
  | FreeMsg
  | CloseMsg;

// Filled in by handleInit.
let wasmModule: WebAssembly.Module | undefined;
let wasm: {
  memory: WebAssembly.Memory;
  __wbindgen_export: (size: number, align: number) => number; // malloc
  __wbindgen_export2: (ptr: number, size: number, align: number) => void; // free
} | undefined;

const dictDecoders = new Map<number, CtopoDecompressor>();

async function handleInit(msg: InitMsg): Promise<void> {
  wasmModule = await WebAssembly.compile(new Uint8Array(msg.wasmBytes));
  // `init` accepts `{ module_or_path, memory, thread_stack_size }`.
  // Using a precompiled `WebAssembly.Module` here avoids re-parsing
  // the wasm bytes; the shared memory is the one from the merge
  // worker so every sub-worker maps to the same SAB.
  wasm = (await init({ module_or_path: wasmModule, memory: msg.memory })) as never;
}

function getWasm(): NonNullable<typeof wasm> {
  if (wasm === undefined) {
    throw new Error("ctopo-zstd-worker: received decompress before init");
  }
  return wasm;
}

async function handle(
  host: import("./worker-host").WorkerHost,
  msg: IncomingMsg,
): Promise<void> {
  if (msg.type === "close") {
    host.close();
    return;
  }
  if (msg.type === "init") {
    await handleInit(msg);
    host.postMessage({ id: msg.id, ok: true });
    return;
  }
  if (msg.type === "register-dict") {
    const dec = new CtopoDecompressor(new Uint8Array(msg.dict));
    dictDecoders.set(msg.dictId, dec);
    host.postMessage({ id: msg.id, ok: true });
    return;
  }
  if (msg.type === "free") {
    getWasm().__wbindgen_export2(msg.ptr, msg.capacity, 1);
    // No reply — frees are fire-and-forget. If the worker dies, the
    // entire wasm memory goes with it.
    return;
  }
  // decompress
  const w = getWasm();
  const compressed = new Uint8Array(msg.bytes);
  // Allocate output in the (shared) wasm memory. The caller will read
  // the result via a Uint8Array view into this SAB at the returned
  // pointer; the buffer survives until the caller posts a `free`
  // message back.
  const dst_ptr = w.__wbindgen_export(msg.capacity, 1);
  let byteLength: number;
  if (msg.dictId !== undefined) {
    const dec = dictDecoders.get(msg.dictId);
    if (dec === undefined) {
      throw new Error(
        `ctopo-zstd-worker: unknown dictId ${msg.dictId} (register-dict was never received or arrived after decompress)`,
      );
    }
    byteLength = dec.decompress_into(compressed, dst_ptr, msg.capacity);
  } else {
    byteLength = decompress_no_dict_into(compressed, dst_ptr, msg.capacity);
  }
  host.postMessage({
    id: msg.id,
    ok: true,
    ptr: dst_ptr,
    byteLength,
    capacity: msg.capacity,
  });
}

void getWorkerHost().then((host) => {
  // Signal readiness so the client knows where to send the init
  // payload (we need the merge worker's pre-loaded wasm bytes +
  // shared memory before we can do anything else).
  host.postMessage({ kind: "spawn-ready" });
  host.onMessage((data) => {
    const msg = data as IncomingMsg;
    const id = (msg as { id?: number }).id ?? 0;
    handle(host, msg).catch((err: unknown) => {
      const message = err instanceof Error ? err.message : String(err);
      const name = err instanceof Error ? err.name : "Error";
      host.postMessage({ id, ok: false, error: { name, message } });
    });
  });
});

