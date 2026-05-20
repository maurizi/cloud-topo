// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * In-process zstd decode using the non-shared ("plain") wasm build.
 *
 * This is the capability fallback used when a SharedArrayBuffer can't
 * cross the worker boundary — i.e. a browser page that isn't
 * cross-origin isolated (see core/util.ts `sabUsable`). The
 * shared-memory sub-worker path (zstd-decoder-client.ts) can't run
 * there because its wasm imports a shared memory; this module
 * instantiates the plain build's own internal memory and decodes
 * synchronously on the calling thread. Correct everywhere, but loses
 * the decode/graph-build overlap the shared path gets.
 *
 * Mirrors the slice of ZstdDecoderClient's surface that decompress.ts
 * uses: `warm()` and `decompress(bytes, capacity, dict?)`.
 */

import initPlain, {
  CtopoDecompressor,
  decompress_no_dict,
} from "./ctopo_zstd_decoder_plain.js";
import { readNodeFile } from "./index";

const plainWasmUrl = new URL(
  "./ctopo_zstd_decoder_plain_bg.wasm",
  import.meta.url,
);

let ready: Promise<void> | undefined;

function init(): Promise<void> {
  if (ready === undefined) {
    ready = (async () => {
      if (plainWasmUrl.protocol === "file:") {
        // Node / file URL — built-in fetch can't load file:// so we
        // hand wasm-bindgen raw bytes; it falls back to
        // WebAssembly.instantiate(bytes, imports).
        const bytes = await readNodeFile(plainWasmUrl);
        await initPlain({ module_or_path: bytes });
      } else {
        // Browser — pass the URL; wasm-bindgen streams the compile.
        await initPlain({ module_or_path: plainWasmUrl });
      }
    })();
  }
  return ready;
}

// One decoder per dict object. Each CtopoDecompressor leaks its dict
// inside wasm memory (Box::leak in the crate, see lib.rs), so building
// a fresh one per call would leak ~110 KiB on every arc_coords block.
// The client holds one dict buffer for the session, so a WeakMap keyed
// on it reuses a single decoder and lets GC reclaim if the dict drops.
const dictDecoders = new WeakMap<Uint8Array, CtopoDecompressor>();

export const plainZstd = {
  warm: init,
  async decompress(
    bytes: Uint8Array,
    capacity: number,
    dict?: Uint8Array,
  ): Promise<Uint8Array> {
    await init();
    if (dict !== undefined) {
      let dec = dictDecoders.get(dict);
      if (dec === undefined) {
        dec = new CtopoDecompressor(dict);
        dictDecoders.set(dict, dec);
      }
      return dec.decompress(bytes, capacity);
    }
    return decompress_no_dict(bytes, capacity);
  },
};
