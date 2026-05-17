// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Sub-worker that owns the zstd WASM instance + DDict cache.
 * Spawned by the merge worker on first decompress; protocol lives in
 * `zstd-decoder-client.ts`.
 */

import {
  initZstdWasm,
  CtopoDecompressor,
  decompress_no_dict,
} from "./zstd-wasm";
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

interface CloseMsg {
  readonly type: "close";
}

type IncomingMsg = RegisterDictMsg | DecompressMsg | CloseMsg;

let readyPromise: Promise<void> | undefined;
function ensureReady(): Promise<void> {
  if (readyPromise === undefined) readyPromise = initZstdWasm();
  return readyPromise;
}

const dictDecoders = new Map<number, CtopoDecompressor>();

async function handle(
  host: import("./worker-host").WorkerHost,
  msg: IncomingMsg,
): Promise<void> {
  if (msg.type === "close") {
    host.close();
    return;
  }
  await ensureReady();
  if (msg.type === "register-dict") {
    const dec = new CtopoDecompressor(new Uint8Array(msg.dict));
    dictDecoders.set(msg.dictId, dec);
    host.postMessage({ id: msg.id, ok: true });
    return;
  }
  // decompress
  const compressed = new Uint8Array(msg.bytes);
  let out: Uint8Array;
  if (msg.dictId !== undefined) {
    const dec = dictDecoders.get(msg.dictId);
    if (dec === undefined) {
      throw new Error(
        `ctopo-zstd-worker: unknown dictId ${msg.dictId} (register-dict was never received or arrived after decompress)`,
      );
    }
    out = dec.decompress(compressed, msg.capacity);
  } else {
    out = decompress_no_dict(compressed, msg.capacity);
  }
  // The wasm decoder returns a fresh Uint8Array backed by its own
  // buffer (allocated per call inside the decoder). Transferring its
  // ArrayBuffer is safe — the worker has no other view into it.
  host.postMessage({ id: msg.id, ok: true, bytes: out.buffer }, [out.buffer]);
}

void getWorkerHost().then((host) => {
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
