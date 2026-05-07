// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

// Typed wrapper around the wasm-bindgen-generated zstd decoder.
// Source crate at crates/zstd-decoder/. Generated outputs (the
// adjacent .js + .d.ts + wasm-bytes.ts) are committed to keep
// `npm install` toolchain-free; rebuild via `npm run build:wasm`.

// Types come via the @ts-self-types directive in
// ./ctopo_zstd_decoder.js pointing at the sibling .d.ts.
import init, {
  CtopoDecompressor,
  decompress_no_dict,
} from "./ctopo_zstd_decoder.js";
import { wasmBase64 } from "./wasm-bytes";

let ready: Promise<void> | undefined;

function base64ToBytes(b64: string): Uint8Array {
  // atob exists in browsers and Node ≥16. Buffer.from would be
  // faster on Node-only paths, but atob keeps this isomorphic.
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// Initialize the wasm module once. Async-only — browsers reject
// synchronous `new WebAssembly.Module()` of buffers >4 KiB on the
// main thread, and our wasm is ~103 KiB.
export function initZstdWasm(): Promise<void> {
  if (ready === undefined) {
    ready = (async () => {
      // wasm-bindgen's __wbg_init accepts either a fetch URL/Request
      // or raw bytes; passing bytes goes through
      // WebAssembly.instantiate(bytes, imports) which compiles
      // off-main-thread. No fetch, no separate .wasm asset.
      await init(base64ToBytes(wasmBase64));
    })();
  }
  return ready;
}

export { CtopoDecompressor, decompress_no_dict };
