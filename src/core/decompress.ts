// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Section decompression for `.ctopo` containers.
 *
 * zstd is the default — native via DecompressionStream("zstd") on
 * recent browsers / Node 22.4+, falling back to a custom wasm decoder
 * built from zstd-rs (~103 KiB raw / ~40 KiB gzipped, base64-inlined;
 * see crates/zstd-decoder/). The wasm decoder is also used
 * unconditionally for dict-aware sections, since DecompressionStream
 * has no dict parameter.
 *
 * The wasm path runs in a dedicated sub-worker (zstd-worker.ts) so
 * decode and graph-build work overlap on the merge worker. See
 * `zstd-decoder-client.ts` for the channel; this module just routes.
 *
 * Brotli is supported as an alternative codec for producers that
 * prefer faster encode at the cost of slightly worse ratio; reading
 * "brotli" relies on native DecompressionStream("brotli") (Firefox
 * today, Chrome rolling) — no JS fallback.
 */

import { type SectionEntry } from "./types";
import { perfLog } from "./fetcher";
import { getZstdDecoderClient } from "./zstd-decoder-client";
import { plainZstd } from "./zstd-wasm/plain-decoder";
import { sabUsable } from "./util";

// The wasm zstd decoder needs a `warm()` + `decompress(bytes, capacity,
// dict?)` pair. The shared-memory sub-worker path gives the best
// throughput but needs a cross-origin-isolated page; where SAB can't
// cross the worker boundary we fall back to decoding in-process with
// the plain (non-shared) wasm build. `sabUsable()` picks once.
interface WasmZstdDecoder {
  warm(): Promise<void>;
  decompress(
    bytes: Uint8Array,
    capacity: number,
    dict?: Uint8Array,
  ): Promise<Uint8Array>;
}

function zstdDecoder(): WasmZstdDecoder {
  return sabUsable() ? getZstdDecoderClient() : plainZstd;
}

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
//
// Returns a Promise: the decode happens in a sub-worker over
// postMessage, and the round-trip is what lets graph build overlap
// with decode on the merge worker.
export type WasmZstdDecode = (
  bytes: Uint8Array,
  uncompressedSize: number,
  dict?: Uint8Array,
) => Promise<Uint8Array>;

// --- Module-level state ---

let zstdNativeChecked = false;
let zstdNativeAvailable = false;

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
// worse ratio; reading "brotli" relies on native
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
  if (codec === "zstd") {
    out = await decompressZstd(bytes, entry);
  } else if (codec === "brotli") {
    out = await decompressNativeOrThrow(bytes, entry, "brotli");
  } else {
    throw new Error(
      `ctopo: section "${entry.name}" has unknown codec "${codec as string}" — this build understands "zstd" and "brotli"`,
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
// instead of gating the first compressed-section decompress. Now
// translates to "spawn the zstd sub-worker" — the WASM init runs
// inside that worker, in parallel with whatever the merge worker
// does next.
//
// Always fires when the file uses block-compressed arc_coords —
// per-block decode happens many times per merge, and the wasm
// decoder's prepared-DDict path is markedly faster than any native
// alternative (DecompressionStream lacks a dict parameter and pays
// stream-API setup per call). Otherwise fires only as a fallback
// when the runtime lacks native zstd. Idempotent.
export function preloadZstdWasmIfNeeded(forceLoad: boolean = false): void {
  if (!forceLoad && zstdNativeOk()) return;
  void zstdDecoder().warm();
}

export async function loadZstdWasmDecode(
  entry: SectionEntry,
): Promise<WasmZstdDecode> {
  const client = zstdDecoder();
  try {
    await client.warm();
  } catch (err) {
    throw new Error(
      `ctopo: section "${entry.name}" needs zstd decompression but the wasm decoder could not be initialized (${
        (err as Error).message
      })`,
    );
  }
  return (
    bytes: Uint8Array,
    uncompressedSize: number,
    dict?: Uint8Array,
  ): Promise<Uint8Array> =>
    client.decompress(bytes, uncompressedSize + CAPACITY_SLACK, dict);
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
  return await decode(bytes, entry.uncompressedRegionLength);
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
