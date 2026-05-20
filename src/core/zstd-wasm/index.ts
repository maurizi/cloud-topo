// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

// Loader helpers around the wasm-bindgen-generated zstd decoder.
// Source crate at crates/zstd-decoder/. Two builds are committed (see
// scripts/build-wasm.mjs): the shared-memory variant
// (ctopo_zstd_decoder*) drives the zero-copy sub-worker; the plain
// variant (ctopo_zstd_decoder_plain*) is decoded in-process as the
// no-SharedArrayBuffer fallback (see ./plain-decoder). Generated
// outputs are committed to keep `npm install` toolchain-free; rebuild
// via `npm run build:wasm`.

import type * as NodeFsPromises from "node:fs/promises";

// Resolves to the shared-memory wasm asset URL. With tsup/esbuild's
// `file` loader this gets rewritten to point at the bundled
// dist/<hash>.wasm; in source / vitest it's a file:// URL alongside
// this module.
const sharedWasmUrl = new URL("./ctopo_zstd_decoder_bg.wasm", import.meta.url);

// Lazy `node:fs/promises` import for the Node code path. The module
// name goes through a variable so browser bundlers can't statically
// resolve it, and the magic comments tell Vite/webpack to skip the
// dynamic import entirely. Only ever runs when the URL is a file:// URL
// (i.e. real Node, not a browser).
export async function readNodeFile(url: URL): Promise<Uint8Array> {
  const moduleName = "node:fs/promises";
  const fs = (await import(
    /* @vite-ignore */ /* webpackIgnore: true */ moduleName
  )) as typeof NodeFsPromises;
  return await fs.readFile(url);
}

// Exposed for the merge worker's zstd-decoder-client: it wants the raw
// shared-memory wasm bytes so it can pass them to every spawned
// sub-worker once (avoiding N concurrent fetches of the same asset).
export async function loadZstdWasmBytes(): Promise<ArrayBuffer> {
  if (sharedWasmUrl.protocol === "file:") {
    const bytes = await readNodeFile(sharedWasmUrl);
    const ab = new ArrayBuffer(bytes.byteLength);
    new Uint8Array(ab).set(bytes);
    return ab;
  }
  const res = await fetch(sharedWasmUrl);
  return res.arrayBuffer();
}
