// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Internal types, tuning constants, and small pure helpers for
 * CtopoCore (./client.ts). Split out to keep client.ts focused on the
 * class itself. Nothing here is part of the public surface.
 */

import { type FetchPriority } from "./fetcher";

// --- Byte-range fetch/coalesce internal types ---

export interface PendingSectionFetch {
  readonly family: string;
  readonly start: number;
  readonly end: number;
  readonly priority: FetchPriority;
  readonly signal?: AbortSignal;
  readonly resolve: (bytes: Uint8Array) => void;
  readonly reject: (err: unknown) => void;
}

export interface LogicalRange {
  readonly family: string;
  readonly start: number;
  end: number;
  readonly items: PendingSectionFetch[];
  // Highest-urgency priority among constituent items — coalesced
  // chunks inherit it so a small high-priority fetch upgrades any
  // bulk fetches it gets fused with.
  priority: FetchPriority;
  // Filled in as chunks complete. Length = number of chunks.
  chunkBytes: Uint8Array[];
  error: unknown;
}

export interface ChunkTask {
  readonly logical: LogicalRange;
  readonly index: number;
  readonly start: number;
  readonly end: number;
}

export interface CachedByteRange {
  readonly start: number;
  readonly end: number;
  readonly bytes: Uint8Array;
}

export interface InFlightRange {
  readonly start: number;
  readonly end: number;
  readonly promise: Promise<Uint8Array>;
}

// --- Constants ---

// Default front-prefetch is OFF — callers explicitly opt in based on
// their front-loaded set size. Open path still works without it: the
// suffix-range footer GET delivers META, and lazy section fetches
// take the round trip when first needed.
export const DEFAULT_FRONT_PREFETCH = 0;
// Suffix-range footer GET. 256 KiB is generous for any realistic
// section table + META JSON; the reader validates the trailing length
// marker and errors if the footer overflows, so over-budgeting is
// safe.
export const DEFAULT_BACK_PREFETCH = 256 * 1024;
export const DEFAULT_COALESCE_GAP = 64 * 1024;
// Arcs default — see doc on OpenContainerOptions.coalesceGapByFamily.
// 0 = merge truly-adjacent ranges only, never jump a gap. Combined
// with multi-range packing (DEFAULT_MAX_RANGES_PER_REQUEST = 64) the
// blocks a sparse merge actually needs ride together in a handful of
// `multipart/byteranges` requests at zero gap-content overhead. The
// previous 8 KiB default pulled ~6–7 MiB of unwanted block content
// per merge; measured on national CDs: ~−6.6 MiB downloaded /
// −2–3% wall-clock at every latency vs the 8 KiB default. Wider
// gaps (1 MiB, the long-ago default) collapsed nearly every arcs
// fetch into one giant range that pulled all of arc_coords.
export const DEFAULT_ARCS_COALESCE_GAP = 0;
// Offsets default. Used by per-partition fetches in the
// partitioned-arc_offsets path. Partitions are 100s of bytes each, so
// a generous gap bridges huge numbers of unfetched partitions and
// erases the savings; the simulation in
// bench-out/national/*.offsets-partition-sim.csv showed 4 KiB as the
// sweet spot for hierarchical-hilbert on national CDs (~4.86 MiB
// fetched / 11 reqs vs ~13.6 MiB fetched at 64 KiB).
export const DEFAULT_OFFSETS_COALESCE_GAP = 4 * 1024;
// Endpoints default — partitions are tiny (4-16 B per arc, hundreds
// to a few thousand bytes per partition compressed). Mirror the
// offsets gap so sparse merges coalesce nearby endpoint fetches
// without dragging in huge swaths of arcs we didn't ask for. Tunable
// per-call via OpenContainerOptions.coalesceGapByFamily.endpoints.
export const DEFAULT_ENDPOINTS_COALESCE_GAP = 4 * 1024;
export const DEFAULT_GAP_BY_FAMILY: Readonly<Record<string, number>> = {
  arcs: DEFAULT_ARCS_COALESCE_GAP,
  offsets: DEFAULT_OFFSETS_COALESCE_GAP,
  endpoints: DEFAULT_ENDPOINTS_COALESCE_GAP,
};
// 4 MiB physical chunk cap. See doc on OpenContainerOptions.maxChunkBytes.
export const DEFAULT_MAX_CHUNK = 4 * 1024 * 1024;
// 8 parallel chunks. HTTP/2 lets us multiplex these on one connection,
// so going above 6 is fine; we cap to keep flight bytes bounded
// (8 × 4 MiB = 32 MiB max in-flight).
export const DEFAULT_MAX_PARALLEL_RANGES = 8;
// Sized to comfortably hold the open-time front prefetch (typically
// a few MiB) alongside one full pass of property/strings/arc_coords
// fetches during boundary compute, without evicting front-prefetched
// structural sections (arc_offsets, CSR triples) before they're
// re-read by stitching.
export const DEFAULT_BYTE_RANGE_CACHE = 128 * 1024 * 1024;
// Multi-range request defaults.
export const DEFAULT_MAX_RANGES_PER_REQUEST = 64;
// Open-time prefetch size for arc_coords. Block-compressed arc_coords
// is read through the byte-range cache; warming the section's prefix
// lets the first N blocks' compressed bytes serve from memory. The
// encoder front-loads top-layer boundary arcs (outer perimeter +
// parent-layer interior boundaries) by virtue of the visit-order
// assignment, so 512 KiB comfortably covers those for typical
// topologies; the rest is fetched on demand by the merge.
export const DEFAULT_ARC_COORDS_PREFETCH = 512 * 1024;

// Sentinel placeholder used to mark an arc id as "claimed" in the
// fetchArcs result map before its real bytes arrive — keeps the
// dedupe loop synchronous without storing a second tracking set.
export const EMPTY_BYTES = new Uint8Array(0);
export const EMPTY_INT32 = new Int32Array(0);

// --- Small pure helpers ---

export function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw signal.reason ?? new Error("aborted");
  }
}

// Pick the more-urgent priority of two values. high > auto > low.
export function mergePriority(
  a: FetchPriority,
  b: FetchPriority,
): FetchPriority {
  if (a === "high" || b === "high") return "high";
  if (a === "auto" || b === "auto") return "auto";
  return "low";
}

export function stitchChunks(
  start: number,
  end: number,
  chunkBytes: Uint8Array[],
): Uint8Array {
  const out = new Uint8Array(end - start);
  let cursor = 0;
  for (const c of chunkBytes) {
    out.set(c, cursor);
    cursor += c.byteLength;
  }
  return out;
}

// Binary search the block table for the block containing a given
// logical (uncompressed) byte offset. Block table is u32 triples
// [uncEnd, compOff, compLen]; uncEnd is monotonically increasing and
// exclusive, so we want the smallest i such that uncEnd[i] > offset.
export function findArcCoordBlock(
  blocks: Uint32Array,
  logicalOffset: number,
): number {
  const blockCount = blocks.length / 3;
  let lo = 0;
  let hi = blockCount - 1;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (blocks[mid * 3] <= logicalOffset) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

// Wrap section bytes as a Uint32Array, undoing first-order delta
// encoding when the section was emitted that way. Non-delta path
// shares the underlying buffer (zero copy); delta path runs a
// running prefix sum into a fresh buffer with u32 wraparound that
// mirrors the encoder side. Roughly a few ms one-time cost per
// million entries on a multi-MiB arc_offsets.
export function viewU32WithDelta(
  bytes: Uint8Array,
  delta: boolean,
): Uint32Array {
  if (!delta)
    return new Uint32Array(
      bytes.buffer,
      bytes.byteOffset,
      bytes.byteLength / 4,
    );
  const src = new Uint32Array(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength / 4,
  );
  const dst = new Uint32Array(src.length);
  if (src.length === 0) return dst;
  dst[0] = src[0];
  for (let i = 1; i < src.length; i++) dst[i] = (dst[i - 1] + src[i]) >>> 0;
  return dst;
}
