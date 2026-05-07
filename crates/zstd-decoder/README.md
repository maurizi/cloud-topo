# ctopo-zstd-decoder

Tiny wasm-bindgen wrapper around [zstd-rs](https://crates.io/crates/zstd) used by `cloud-topo` for decompressing `.ctopo` sections — both plain frames and dict-aware frames (where the dict is digested once via `ZSTD_createDDict` and reused across calls).

The output (`pkg/`) is post-processed by `scripts/build-wasm.mjs` into `src/zstd-wasm/`:

- `ctopo_zstd_decoder.js` + `.d.ts` — wasm-bindgen-generated loader, copied verbatim
- `wasm-bytes.ts` — the optimized wasm, base64-inlined as a string

These three files are checked in so `npm install` doesn't need a Rust toolchain. Rebuild only when this crate's source changes:

```sh
npm run build:wasm
```

That requires `rustc`, `cargo`, `wasm-pack`, `clang` (for the bundled libzstd C source), and `wasm-opt` (binaryen). The script picks up `wasm-opt` from wasm-pack's cache if not on PATH.

## Why a custom build?

`@bokuweb/zstd-wasm` (~252 KiB raw, ~78 KiB gzip) was the previous decoder. A purpose-built decoder using newer libzstd, the prepared-DDict path, and aggressive `wasm-opt -Oz` is ~103 KiB raw / ~40 KiB gzip and ~2.6× faster on dict-aware decode. The tradeoff is owning a small Rust subproject; given how performance-sensitive the per-block decompress loop is for merge sweeps, that's worth it.
