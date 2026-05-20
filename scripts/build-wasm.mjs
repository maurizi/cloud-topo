#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.
//
// Build the zstd-decoder Rust crate to wasm and wire its output
// into src/zstd-wasm/. Run via `npm run build:wasm`. Output files
// are checked in so `npm install` doesn't need a Rust toolchain.
//
// Steps:
//   1. wasm-pack build --release --target web (skips its bundled
//      wasm-opt — see Cargo.toml metadata; that copy of wasm-opt
//      pre-dates --enable-bulk-memory).
//   2. wasm-opt -Oz with bulk-memory enabled.
//   3. Copy the generated JS + .d.ts + .wasm into src/zstd-wasm/.
//      The .wasm is loaded via `new URL(..., import.meta.url)` so
//      browsers get streaming compile and JS bundles stay small.

import { spawnSync } from "node:child_process";
import { mkdirSync, copyFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = dirname(here);
const crateDir = join(repoRoot, "crates", "zstd-decoder");
const pkgDir = join(crateDir, "pkg");
const outDir = join(repoRoot, "src", "core", "zstd-wasm");

// Atomics + bulk-memory + mutable-globals are required for the
// shared-memory wasm path: the worker's linear memory becomes a
// SharedArrayBuffer that the main thread can also view, so decoded
// bytes don't have to round-trip through a JS-heap copy. Building
// with shared memory requires nightly Rust + `-Z build-std` because
// the stable libstd isn't built with atomics enabled.
// `+atomics,+bulk-memory,+mutable-globals` flags the rustc backend to
// emit shared-memory wasm. `--shared-memory --import-memory` tell
// wasm-ld to declare the memory as shared AND import it from the
// host — this lets the main thread create one Memory({shared: true})
// and pass the same instance to every zstd sub-worker. `--max-memory`
// is mandatory whenever shared memory is on (the wasm threads
// proposal requires bounded memory). 4 GiB caps it at the wasm32
// address space; the OS demand-pages, so this isn't physical RAM.
const env = {
  ...process.env,
  CC: process.env.CC ?? "clang",
  AR: process.env.AR ?? "llvm-ar",
  RUSTFLAGS:
    (process.env.RUSTFLAGS ? process.env.RUSTFLAGS + " " : "") +
    "-C target-feature=+atomics,+bulk-memory,+mutable-globals " +
    // Shared memory + imported memory: the host (the merge worker)
    // creates one Memory({shared: true}) and passes it to every zstd
    // sub-worker so they all instantiate against the same wasm
    // linear memory. Caller-side Uint8Array views over the shared
    // SAB see the worker's writes without any cross-thread copy.
    // wasm-bindgen's threading pass requires `mem.import.is_some()`.
    "-C link-arg=--shared-memory " +
    "-C link-arg=--import-memory " +
    "-C link-arg=--max-memory=4294967296 " +
    // wasm-ld doesn't auto-export `__heap_base` / TLS bootstrap
    // symbols when shared-memory is on; wasm-bindgen's threading
    // pass needs them, so opt in explicitly.
    "-C link-arg=--export=__heap_base " +
    "-C link-arg=--export=__tls_base " +
    "-C link-arg=--export=__tls_size " +
    "-C link-arg=--export=__tls_align " +
    "-C link-arg=--export=__wasm_init_tls",
};

function run(cmd, args, opts = {}) {
  process.stderr.write(`[build-wasm] ${cmd} ${args.join(" ")}\n`);
  const res = spawnSync(cmd, args, { stdio: "inherit", env, ...opts });
  if (res.status !== 0) {
    throw new Error(`${cmd} exited with status ${res.status}`);
  }
}

function findWasmOpt() {
  // wasm-pack downloads wasm-opt under ~/.cache/.wasm-pack/. Use
  // that copy if a system one isn't on PATH — saves the user from
  // installing binaryen separately.
  const which = spawnSync("which", ["wasm-opt"]);
  if (which.status === 0) return which.stdout.toString().trim();
  const cache = join(
    process.env.HOME ?? "",
    ".cache",
    ".wasm-pack",
  );
  const cached = spawnSync("find", [cache, "-name", "wasm-opt", "-type", "f"]);
  const found = cached.stdout.toString().trim().split("\n")[0];
  if (found) return found;
  throw new Error(
    "wasm-opt not found. Either run a wasm-pack build first (it caches one), or install binaryen.",
  );
}

// Nightly Rust + build-std so std is rebuilt with atomics enabled.
// `+nightly` is a rustup proxy; cargo / wasm-pack invocations inside
// `cargo +nightly ...` thread the toolchain through correctly. Use a
// CARGO_BUILD_STD flag pair passed via env so wasm-pack doesn't need
// to forward `-Z` args (it's picky about that).
run(
  "wasm-pack",
  ["build", "--release", "--target", "web"],
  {
    cwd: crateDir,
    env: {
      ...env,
      RUSTUP_TOOLCHAIN: "nightly",
      CARGO_UNSTABLE_BUILD_STD: "std,panic_abort",
    },
  },
);

const rawWasm = join(pkgDir, "ctopo_zstd_decoder_bg.wasm");
const optWasm = join(pkgDir, "ctopo_zstd_decoder_bg.opt.wasm");
const wasmOpt = findWasmOpt();
run(wasmOpt, [
  "--enable-bulk-memory",
  "--enable-mutable-globals",
  "--enable-sign-ext",
  // Required when input wasm declares shared memory + atomics
  // (the +atomics target feature pulls in the threads proposal).
  "--enable-threads",
  "-Oz",
  rawWasm,
  "-o",
  optWasm,
]);

mkdirSync(outDir, { recursive: true });

// Copy the wasm-bindgen-generated loader + types unmodified. We
// load via __wbg_init(bytes), which is async-safe even for large
// modules (browsers reject sync WebAssembly.Module compilation
// >4 KiB on the main thread).
copyFileSync(
  join(pkgDir, "ctopo_zstd_decoder.js"),
  join(outDir, "ctopo_zstd_decoder.js"),
);
copyFileSync(
  join(pkgDir, "ctopo_zstd_decoder.d.ts"),
  join(outDir, "ctopo_zstd_decoder.d.ts"),
);

copyFileSync(optWasm, join(outDir, "ctopo_zstd_decoder_bg.wasm"));

const optSize = readFileSync(optWasm).byteLength;
process.stderr.write(
  `[build-wasm] wasm: ${optSize} bytes (${(optSize / 1024).toFixed(1)} KiB)\n`,
);
process.stderr.write(`[build-wasm] wrote ${outDir}/{ctopo_zstd_decoder.js, .d.ts, .wasm}\n`);
