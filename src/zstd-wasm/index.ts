// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

// Typed wrapper around the wasm-bindgen-generated zstd decoder.
// Source crate at crates/zstd-decoder/. Generated outputs (the
// adjacent .js + .d.ts + .wasm) are committed to keep `npm install`
// toolchain-free; rebuild via `npm run build:wasm`.

// Types come via the @ts-self-types directive in
// ./ctopo_zstd_decoder.js pointing at the sibling .d.ts.
import type * as NodeFsPromises from "node:fs/promises";
import init, {
  CtopoDecompressor,
  decompress_no_dict,
} from "./ctopo_zstd_decoder.js";

let ready: Promise<void> | undefined;

// Resolves to the wasm asset URL. With tsup/esbuild's `file` loader
// this gets rewritten to point at the bundled dist/<hash>.wasm; in
// source / vitest it's a file:// URL alongside this module.
const wasmUrl = new URL("./ctopo_zstd_decoder_bg.wasm", import.meta.url);

// Lazy `node:fs/promises` import for the Node code path. The module
// name goes through a variable so browser bundlers can't statically
// resolve it, and the magic comments tell Vite/webpack to skip the
// dynamic import entirely. The import only ever runs when wasmUrl is
// a file:// URL (i.e. real Node, not a browser).
async function readNodeFile(url: URL): Promise<Uint8Array> {
  const moduleName = "node:fs/promises";
  const fs = (await import(
    /* @vite-ignore */ /* webpackIgnore: true */ moduleName
  )) as typeof NodeFsPromises;
  return await fs.readFile(url);
}

export function initZstdWasm(): Promise<void> {
  if (ready === undefined) {
    ready = (async () => {
      if (wasmUrl.protocol === "file:") {
        // Node / file URL — built-in fetch can't load file:// so we
        // hand wasm-bindgen raw bytes; it falls back to
        // WebAssembly.instantiate(bytes, imports).
        const bytes = await readNodeFile(wasmUrl);
        await init({ module_or_path: bytes });
        return;
      }
      // Browser / worker — pass the URL through; wasm-bindgen calls
      // fetch() + WebAssembly.instantiateStreaming for an
      // off-main-thread streaming compile that overlaps with download.
      await init({ module_or_path: wasmUrl });
    })();
  }
  return ready;
}

export { CtopoDecompressor, decompress_no_dict };
