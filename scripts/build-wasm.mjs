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
const outDir = join(repoRoot, "src", "zstd-wasm");

const env = {
  ...process.env,
  CC: process.env.CC ?? "clang",
  AR: process.env.AR ?? "llvm-ar",
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

run("wasm-pack", ["build", "--release", "--target", "web"], { cwd: crateDir });

const rawWasm = join(pkgDir, "ctopo_zstd_decoder_bg.wasm");
const optWasm = join(pkgDir, "ctopo_zstd_decoder_bg.opt.wasm");
const wasmOpt = findWasmOpt();
run(wasmOpt, [
  "--enable-bulk-memory",
  "--enable-mutable-globals",
  "--enable-sign-ext",
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
