// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Section decompression for `.ctopo` containers.
 *
 * zstd is the default — native via DecompressionStream("zstd") on
 * recent browsers, falling back to a custom wasm decoder built from
 * zstd-rs (~103 KiB raw / ~40 KiB gzipped, base64-inlined; see
 * crates/zstd-decoder/). The wasm decoder is also used unconditionally
 * for dict-aware sections, since DecompressionStream has no dict
 * parameter. Brotli is supported as an alternative codec for
 * producers that prefer faster encode at the cost of slightly worse
 * ratio; reading "br" relies on native DecompressionStream("brotli")
 * — no JS fallback.
 */

import { type SectionEntry } from "./types";
import { perfLog } from "./fetcher";
// Type-only — the wasm module (with its 137 KiB base64-inlined
// payload) is loaded lazily via `await import("./zstd-wasm")` inside
// preloadZstdWasmIfNeeded, so tsup emits it as a separate chunk and
// modern browsers with native DecompressionStream("zstd") never
// fetch it at all.
import { type CtopoDecompressor } from "./zstd-wasm";

// --- Types ---

// Decoder takes the compressed bytes and the exact uncompressed
// size. Required because zstd-rs needs an explicit upper bound for
// its output Vec, and our frames don't carry FCS (Node's
// createZstdCompress + flush(ZSTD_e_end) writes frames without it).
// Our META carries the value per section as
// `uncompressedRegionLength`; the block table carries it per block.
// Callers without a size — i.e. third-party files that omit the
// META field — should fail at the boundary (decompressZstd) with a
// clear "section X is compressed but missing uncompressedRegionLength"
// error rather than guessing.
//
// Two-arg shape covers both paths:
//   - decode(bytes, uncSize)         — no shared dict (most sections)
//   - decode(bytes, uncSize, dict)   — shared dict (arc_coord blocks)
export type WasmZstdDecode = (
  bytes: Uint8Array,
  uncompressedSize: number,
  dict?: Uint8Array,
) => Uint8Array;

// --- Module-level state ---

let zstdNativeChecked = false;
let zstdNativeAvailable = false;
let wasmZstdReady: Promise<WasmZstdDecode> | undefined;

// zstd-rs needs an explicit upper bound for its output Vec; the
// exact uncompressed size from the section/block metadata is the
// upper bound, plus this small slack for any rounding-up libzstd
// does internally on the output buffer.
const CAPACITY_SLACK = 64;

// --- Public API ---

// Decompress a section's bytes per its declared codec. zstd is the
// default — native via DecompressionStream("zstd") on recent
// browsers, falling back to the bundled zstd-rs wasm decoder
// (~103 KiB) elsewhere. Brotli is supported as an alternative codec
// for producers that prefer faster encode at the cost of slightly
// worse ratio; reading "br" relies on native
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
// per-block decode happens many times per merge, and the wasm
// decoder's prepared-DDict path is markedly faster than any native
// alternative (DecompressionStream lacks a dict parameter and pays
// stream-API setup per call). Otherwise fires only as a fallback
// when the runtime lacks native zstd. Idempotent.
export function preloadZstdWasmIfNeeded(forceLoad: boolean = false): void {
  if (wasmZstdReady !== undefined) return;
  if (!forceLoad && zstdNativeOk()) return;
  wasmZstdReady = (async () => {
    // Lazy import — splits the ~137 KiB base64 wasm payload into
    // its own chunk so it isn't dragged into the main entry bundle.
    const wasm = await import("./zstd-wasm");
    await wasm.initZstdWasm();
    // Cache one CtopoDecompressor per shared-dict identity. The
    // first call with a given dict pays ZSTD_createDDict once
    // (~3 ms for a ~110 KiB dict); subsequent calls reuse the
    // digested handle. Keyed by dict-bytes object identity, so the
    // same Uint8Array passed by the client across calls hits the
    // cache; a different dict (e.g. on container reopen) builds a
    // fresh DDict. WeakMap so the dict bytes can be GC'd when the
    // container closes.
    const dictDecoders = new WeakMap<Uint8Array, CtopoDecompressor>();
    const getDictDecoder = (dict: Uint8Array): CtopoDecompressor => {
      let dec = dictDecoders.get(dict);
      if (dec === undefined) {
        dec = new wasm.CtopoDecompressor(dict);
        dictDecoders.set(dict, dec);
      }
      return dec;
    };
    return (bytes: Uint8Array, uncompressedSize: number, dict?: Uint8Array) => {
      const capacity = uncompressedSize + CAPACITY_SLACK;
      if (dict !== undefined) {
        return getDictDecoder(dict).decompress(bytes, capacity);
      }
      return wasm.decompress_no_dict(bytes, capacity);
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
  // Wasm decoder needs an explicit upper bound (zstd-rs allocates a
  // Vec of this capacity; our frames lack FCS so it can't be derived
  // from the bytes). Our encoder always sets this for compressed
  // sections — see encode.ts:1113. A foreign producer that omits it
  // is a malformed-input error, not a "guess 32 MiB" situation.
  if (entry.uncompressedRegionLength === undefined) {
    throw new Error(
      `ctopo: section "${entry.name}" is zstd-compressed but META is missing uncompressedRegionLength — wasm decoder needs the exact uncompressed size to size its output buffer`,
    );
  }
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
