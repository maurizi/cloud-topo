#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.
//
// Build the zstd-decoder Rust crate to wasm and wire its output into
// src/core/zstd-wasm/. Run via `npm run build:wasm`. Output files are
// checked in so `npm install` doesn't need a Rust toolchain.
//
// We build the SAME crate twice, producing two artifacts:
//
//   shared (ctopo_zstd_decoder*) — linear memory is a SharedArrayBuffer
//     imported from the host. The merge worker creates one
//     Memory({shared:true}) and passes it to every zstd sub-worker so
//     decoded bytes are read cross-thread with no copy. Requires the
//     page to be cross-origin isolated (COOP/COEP). Needs nightly Rust
//     + build-std because stable libstd isn't built with atomics.
//
//   plain (ctopo_zstd_decoder_plain*) — ordinary non-shared internal
//     memory. Instantiates anywhere (no cross-origin isolation needed)
//     and is decoded in-process on the worker thread. The capability
//     fallback used when SharedArrayBuffer can't cross the worker
//     boundary. Builds on stable Rust.
//
// Steps per variant:
//   1. wasm-pack build --release --target web (skips its bundled
//      wasm-opt — see Cargo.toml metadata; that copy of wasm-opt
//      pre-dates --enable-bulk-memory).
//   2. wasm-opt -Oz with the right feature flags enabled.
//   3. Copy the generated JS + .d.ts + .wasm into src/core/zstd-wasm/,
//      renaming the plain variant and patching its wasm URL.
//      The .wasm is loaded via `new URL(..., import.meta.url)` so
//      browsers get streaming compile and JS bundles stay small.

import { spawnSync } from "node:child_process";
import {
  mkdirSync,
  copyFileSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = dirname(here);
const crateDir = join(repoRoot, "crates", "zstd-decoder");
const outDir = join(repoRoot, "src", "core", "zstd-wasm");

// Atomics + bulk-memory + mutable-globals are required for the
// shared-memory wasm path: the worker's linear memory becomes a
// SharedArrayBuffer that the main thread can also view, so decoded
// bytes don't have to round-trip through a JS-heap copy. Building with
// shared memory requires nightly Rust + `-Z build-std` because the
// stable libstd isn't built with atomics enabled.
// `+atomics,+bulk-memory,+mutable-globals` flags the rustc backend to
// emit shared-memory wasm. `--shared-memory --import-memory` tell
// wasm-ld to declare the memory as shared AND import it from the host —
// this lets the main thread create one Memory({shared: true}) and pass
// the same instance to every zstd sub-worker. `--max-memory` is
// mandatory whenever shared memory is on (the wasm threads proposal
// requires bounded memory). 4 GiB caps it at the wasm32 address space;
// the OS demand-pages, so this isn't physical RAM.
const SHARED_RUSTFLAGS =
  "-C target-feature=+atomics,+bulk-memory,+mutable-globals " +
  "-C link-arg=--shared-memory " +
  "-C link-arg=--import-memory " +
  "-C link-arg=--max-memory=4294967296 " +
  // wasm-ld doesn't auto-export `__heap_base` / TLS bootstrap symbols
  // when shared-memory is on; wasm-bindgen's threading pass needs them.
  "-C link-arg=--export=__heap_base " +
  "-C link-arg=--export=__tls_base " +
  "-C link-arg=--export=__tls_size " +
  "-C link-arg=--export=__tls_align " +
  "-C link-arg=--export=__wasm_init_tls";

const baseEnv = {
  ...process.env,
  CC: process.env.CC ?? "clang",
  AR: process.env.AR ?? "llvm-ar",
};

function run(cmd, args, opts = {}) {
  process.stderr.write(`[build-wasm] ${cmd} ${args.join(" ")}\n`);
  const res = spawnSync(cmd, args, { stdio: "inherit", ...opts });
  if (res.status !== 0) {
    throw new Error(`${cmd} exited with status ${res.status}`);
  }
}

function findWasmOpt() {
  // wasm-pack downloads wasm-opt under ~/.cache/.wasm-pack/. Use that
  // copy if a system one isn't on PATH — saves the user from installing
  // binaryen separately.
  const which = spawnSync("which", ["wasm-opt"]);
  if (which.status === 0) return which.stdout.toString().trim();
  const cache = join(process.env.HOME ?? "", ".cache", ".wasm-pack");
  const cached = spawnSync("find", [cache, "-name", "wasm-opt", "-type", "f"]);
  const found = cached.stdout.toString().trim().split("\n")[0];
  if (found) return found;
  throw new Error(
    "wasm-opt not found. Either run a wasm-pack build first (it caches one), or install binaryen.",
  );
}

const wasmOpt = findWasmOpt();

// Build one variant. `shared` toggles the shared-memory link flags and
// the nightly/build-std toolchain. `name` is the output basename in
// src/core/zstd-wasm/ (the generated crate name `ctopo_zstd_decoder` is
// renamed to this).
function buildVariant({ shared, name }) {
  const pkgDir = join(crateDir, shared ? "pkg-shared" : "pkg-plain");
  const env = { ...baseEnv };
  if (shared) {
    env.RUSTFLAGS =
      (process.env.RUSTFLAGS ? process.env.RUSTFLAGS + " " : "") +
      SHARED_RUSTFLAGS;
  }
  // Nightly + build-std so std is rebuilt with atomics enabled (shared
  // only). `+nightly` threads through wasm-pack's cargo invocations.
  const buildEnv = shared
    ? {
        ...env,
        RUSTUP_TOOLCHAIN: "nightly",
        CARGO_UNSTABLE_BUILD_STD: "std,panic_abort",
      }
    : env;

  run("wasm-pack", ["build", "--release", "--target", "web", "--out-dir", pkgDir], {
    cwd: crateDir,
    env: buildEnv,
  });

  const rawWasm = join(pkgDir, "ctopo_zstd_decoder_bg.wasm");
  const optWasm = join(pkgDir, "ctopo_zstd_decoder_bg.opt.wasm");
  run(wasmOpt, [
    "--enable-bulk-memory",
    "--enable-mutable-globals",
    "--enable-sign-ext",
    // The +atomics target feature pulls in the threads proposal; only
    // the shared build's input wasm declares shared memory + atomics.
    ...(shared ? ["--enable-threads"] : []),
    "-Oz",
    rawWasm,
    "-o",
    optWasm,
  ]);

  mkdirSync(outDir, { recursive: true });

  const jsName = `${name}.js`;
  const dtsName = `${name}.d.ts`;
  const wasmName = `${name}_bg.wasm`;

  // Copy the wasm-bindgen-generated loader + types. The generated glue
  // references `ctopo_zstd_decoder_bg.wasm`; rewrite that to the
  // variant's wasm name so the two builds don't collide on disk.
  const glue = readFileSync(join(pkgDir, "ctopo_zstd_decoder.js"), "utf8");
  writeFileSync(
    join(outDir, jsName),
    glue.replaceAll("ctopo_zstd_decoder_bg.wasm", wasmName),
  );
  copyFileSync(join(pkgDir, "ctopo_zstd_decoder.d.ts"), join(outDir, dtsName));
  copyFileSync(optWasm, join(outDir, wasmName));

  const optSize = readFileSync(optWasm).byteLength;
  process.stderr.write(
    `[build-wasm] ${name}: ${optSize} bytes (${(optSize / 1024).toFixed(1)} KiB)\n`,
  );
}

buildVariant({ shared: true, name: "ctopo_zstd_decoder" });
buildVariant({ shared: false, name: "ctopo_zstd_decoder_plain" });

process.stderr.write(`[build-wasm] wrote variants to ${outDir}\n`);
