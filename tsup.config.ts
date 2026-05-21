import { defineConfig } from "tsup";
import { copyFileSync } from "node:fs";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    encode: "src/encode.ts",
    // Emitted as `dist/worker.js`. The proxy references it via
    // `new URL("../worker.js", import.meta.url)` from
    // `dist/proxy/client.js` (after splitting), which esbuild leaves
    // alone — Vite/webpack/Rollup pick up the URL pattern and bundle
    // the chunk as a worker module.
    worker: "src/worker.ts",
    // Sub-worker spawned by the merge worker on first decompress. The
    // merge worker resolves it via `new URL("./zstd-worker.js",
    // import.meta.url)` from inside `zstd-decoder-client.ts`; emitted
    // here as a top-level entry so that URL resolves to a real file
    // sitting next to the chunk that imports it.
    "zstd-worker": "src/core/zstd-worker.ts",
    // Compute worker for the merge pool. The coordinator's
    // `MergePool` spawns one of these per pool slot via
    // `new URL("./merge-worker.js", import.meta.url)` from inside
    // `merge-pool.ts`. Emitted as a top-level entry so the URL
    // resolves to a real `dist/merge-worker.js`.
    "merge-worker": "src/core/merge-worker.ts",
  },
  format: ["esm"],
  dts: true,
  sourcemap: true,
  clean: true,
  target: "es2022",
  splitting: true,
  treeshake: true,
  // Copy the wasm assets alongside the bundled chunks. The runtime
  // resolves them via `new URL("./<name>.wasm", import.meta.url)` —
  // esbuild leaves that pattern alone, so the files just need to land
  // in dist/ next to the importing chunk. Browsers then stream-compile
  // via fetch + instantiateStreaming; Node reads the bytes via fs (see
  // src/core/zstd-wasm/index.ts).
  //
  // BOTH variants must ship: the shared-memory build drives the SAB
  // sub-worker path, and the "plain" build is the in-process fallback
  // used whenever the page isn't cross-origin isolated (see
  // core/zstd-wasm/plain-decoder.ts). Omitting the plain wasm leaves a
  // consuming bundler's `new URL(...plain_bg.wasm)` pointing at a
  // missing file, so it's never emitted and 404s at runtime on any
  // non-COI page.
  onSuccess: async () => {
    for (const name of [
      "ctopo_zstd_decoder_bg.wasm",
      "ctopo_zstd_decoder_plain_bg.wasm",
    ]) {
      copyFileSync(`src/core/zstd-wasm/${name}`, `dist/${name}`);
    }
  },
});
