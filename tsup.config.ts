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
  },
  format: ["esm"],
  dts: true,
  sourcemap: true,
  clean: true,
  target: "es2022",
  splitting: true,
  treeshake: true,
  // Copy the wasm asset alongside the bundled chunks. The runtime
  // resolves it via `new URL("./ctopo_zstd_decoder_bg.wasm",
  // import.meta.url)` — esbuild leaves that pattern alone, so the
  // file just needs to land in dist/ next to the importing chunk.
  // Browsers then stream-compile via fetch + instantiateStreaming;
  // Node reads the bytes via fs (see src/zstd-wasm/index.ts).
  onSuccess: async () => {
    copyFileSync(
      "src/core/zstd-wasm/ctopo_zstd_decoder_bg.wasm",
      "dist/ctopo_zstd_decoder_bg.wasm",
    );
  },
});
