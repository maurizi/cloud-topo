// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Section decompression for `.ctopo` containers.
 *
 * zstd is the default — native via DecompressionStream("zstd") on
 * recent browsers, falling back to @bokuweb/zstd-wasm (~50 KB wasm,
 * ~16x faster than pure-JS) elsewhere. Brotli is supported as an
 * alternative codec for producers that prefer faster encode at the
 * cost of slightly worse ratio; reading "br" relies on native
 * DecompressionStream("brotli") — no JS fallback.
 */

import { type SectionEntry } from "./types";
import { perfLog } from "./fetcher";

// --- Types ---

// Decoder takes the compressed bytes and an optional uncompressed-size
// hint. The hint is required when the zstd frame header lacks FCS
// (Node's async zstdCompress does this) — without it bokuweb's wasm
// decoder defaults to a 1 MiB output buffer and -70's on anything
// bigger. Our META carries the value per section as
// `uncompressedRegionLength`.
//
// Two-arg shape covers both paths:
//   - decode(bytes, hint)         — no shared dict (most sections)
//   - decode(bytes, hint, dict)   — shared dict (arc_coord blocks)
export type WasmZstdDecode = (
  bytes: Uint8Array,
  uncompressedSizeHint?: number,
  dict?: Uint8Array,
) => Uint8Array;

// --- Module-level state ---

let zstdNativeChecked = false;
let zstdNativeAvailable = false;
let wasmZstdReady: Promise<WasmZstdDecode> | undefined;

// --- Public API ---

// Decompress a section's bytes per its declared codec. zstd is the
// default — native via DecompressionStream("zstd") on recent
// browsers, falling back to @bokuweb/zstd-wasm (~50 KB wasm, ~16×
// faster than pure-JS) elsewhere. Brotli is supported as an
// alternative codec for producers that prefer faster encode at the
// cost of slightly worse ratio; reading "br" relies on native
// DecompressionStream("brotli") (Firefox today, Chrome rolling) — no
// JS fallback. Unknown codecs throw a clear error so adding a new
// one is a one-place change.
export async function decompressSection(
  bytes: Uint8Array,
  entry: SectionEntry,
): Promise<Uint8Array> {
  const codec = entry.compression;
  if (codec === undefined) return bytes;
  // Perf instrumentation — log compressed/decompressed sizes and
  // decode wall-clock for weighing wire savings vs CPU cost.
  const t0 = performance.now();
  let out: Uint8Array;
  if (codec === "zst") {
    out = await decompressZstd(bytes, entry);
  } else if (codec === "br") {
    out = await decompressNativeOrThrow(bytes, entry, "brotli");
  } else {
    throw new Error(
      `ctopo: section "${entry.name}" has unknown codec "${codec as string}" — this build understands "zst" and "br"`,
    );
  }
  const elapsed = performance.now() - t0;
  perfLog(
    `[ctopo] decompress "${entry.name}" (${codec}): ${bytes.byteLength}B → ` +
      `${out.byteLength}B (ratio ${((bytes.byteLength / out.byteLength) * 100).toFixed(1)}%) ` +
      `in ${elapsed.toFixed(1)}ms`,
  );
  return out;
}

// Triggered at openContainer time (not lazily on first decompress)
// so the wasm-zstd init runs concurrently with the header round-trip
// instead of gating the first compressed-section decompress.
//
// Always fires when the file uses block-compressed arc_coords —
// per-block decode happens many times per merge, so even native
// DecompressionStream("zstd") (which lacks a dict parameter, costs
// a Stream-API setup per call, and is not yet on the dict path
// for #20) loses to the synchronous wasm decoder.
// Otherwise fires only as a fallback when the runtime lacks native
// zstd. Idempotent.
export function preloadZstdWasmIfNeeded(forceLoad: boolean = false): void {
  if (wasmZstdReady !== undefined) return;
  if (!forceLoad && zstdNativeOk()) return;
  wasmZstdReady = (async () => {
    const mod = await import("@bokuweb/zstd-wasm");
    await mod.init();
    // One DCtx for the lifetime of the module — shared across all
    // dict-aware decompresses. bokuweb's decompressUsingDict still
    // mallocs+memcopies the dict bytes on every call (~60 µs for a
    // 1 MiB dict), but the savings on compressed bytes more
    // than pay for it. A future bokuweb upgrade exposing
    // ZSTD_DCtx_loadDictionary would let us skip the per-call copy.
    const dctx = mod.createDCtx();
    const slack = 64;
    return (
      bytes: Uint8Array,
      uncompressedSizeHint?: number,
      dict?: Uint8Array,
    ) => {
      const opts = {
        defaultHeapSize:
          uncompressedSizeHint !== undefined
            ? uncompressedSizeHint + slack
            : 32 * 1024 * 1024,
      };
      if (dict !== undefined) {
        return mod.decompressUsingDict(dctx, bytes, dict, opts);
      }
      return mod.decompress(bytes, opts);
    };
  })().catch((err) => {
    // Reset so a real decode attempt can produce a fresh error
    // with section context, rather than caching a generic one.
    wasmZstdReady = undefined;
    throw err;
  });
}

export async function loadZstdWasmDecode(
  entry: SectionEntry,
): Promise<WasmZstdDecode> {
  preloadZstdWasmIfNeeded(true);
  if (wasmZstdReady === undefined) {
    throw new Error(
      `ctopo: section "${entry.name}" needs zstd decompression but neither DecompressionStream("zstd") nor the wasm fallback could be loaded`,
    );
  }
  try {
    return await wasmZstdReady;
  } catch (err) {
    throw new Error(
      `ctopo: section "${entry.name}" needs zstd decompression but neither DecompressionStream("zstd") nor the wasm fallback is available (${
        (err as Error).message
      })`,
    );
  }
}

// --- Internal helpers ---

// One-shot native-zstd capability probe. Memoized so repeat checks
// are free.
function zstdNativeOk(): boolean {
  if (zstdNativeChecked) return zstdNativeAvailable;
  zstdNativeChecked = true;
  if (typeof DecompressionStream === "undefined") return false;
  try {
    new DecompressionStream("zstd" as CompressionFormat);
    zstdNativeAvailable = true;
  } catch {
    zstdNativeAvailable = false;
  }
  return zstdNativeAvailable;
}

async function decompressZstd(
  bytes: Uint8Array,
  entry: SectionEntry,
): Promise<Uint8Array> {
  if (zstdNativeOk()) return decompressNativeOrThrow(bytes, entry, "zstd");
  const decode = await loadZstdWasmDecode(entry);
  return decode(bytes, entry.uncompressedRegionLength);
}

async function decompressNativeOrThrow(
  bytes: Uint8Array,
  entry: SectionEntry,
  format: string,
): Promise<Uint8Array> {
  if (typeof DecompressionStream === "undefined") {
    throw new Error(
      `ctopo: section "${entry.name}" is ${format}-compressed but DecompressionStream is unavailable in this runtime`,
    );
  }
  let decompressor: DecompressionStream;
  try {
    decompressor = new DecompressionStream(format as CompressionFormat);
  } catch (err) {
    throw new Error(
      `ctopo: section "${entry.name}" needs DecompressionStream("${format}") which this runtime rejected (${
        (err as Error).message
      }). Re-encode the region with a codec your user-base supports.`,
    );
  }
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  }).pipeThrough(decompressor as ReadableWritablePair<Uint8Array, Uint8Array>);
  return new Uint8Array(await new Response(stream).arrayBuffer());
}
