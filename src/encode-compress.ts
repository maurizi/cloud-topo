// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Block compression and dictionary training for arc_coords in the
 * `.ctopo` encoder.
 *
 * arc_coords ships as a sequence of independently-decodable zstd frames
 * sharing a trained dictionary. The dict + block table are small and
 * fetched eagerly at open; individual blocks decompress on demand
 * during fetchArcs. Block boundaries align to whole arcs so no arc
 * spans two blocks.
 */

import { readFileSync, writeFileSync, mkdtempSync, rmSync } from "fs";
import { createZstdCompress, constants as zlibConstants } from "zlib";
import type { ZstdCompress } from "zlib";
import { spawnSync } from "child_process";
import { tmpdir } from "os";
import { join } from "path";

import { stderrLog, memSnapshot } from "./util";
import { VARINT_MAX_BYTES, writeVarintZigzag } from "./format";

// --- Types ---

// One block of arc_coords: walks arcs in arc-id order, accumulates
// arcCoordsBytes[startUncompressed:endUncompressed] until the next
// arc would overflow targetBlockSize. Exposed on the return so the
// arc_offsets-partition encoder can align its partitions to the
// same arc-id ranges.
export interface ArcCoordBlockSpec {
  readonly startUncompressed: number;
  readonly endUncompressed: number;
}

export interface BlockCompressedArcCoords {
  // Trained zstd shared dict, or undefined for inputs too small to
  // train one (the encoder skips the dictSection in META in that
  // case; the reader falls back to no-dict decode).
  readonly dictBytes: Uint8Array | undefined;
  // u32 array of triples [uncompressedEnd, compressedOffset, compressedLength]
  // per block. uncompressedEnd is exclusive — the arc_offsets
  // value of the first arc that *doesn't* live in this block.
  readonly blockTableBytes: Uint8Array;
  // Concatenation of compressed-block frames. Sections in the
  // section table reference this via a single `arc_coords` entry
  // whose length matches.
  readonly compressedBytes: Uint8Array;
  readonly blockCount: number;
  readonly targetBlockSize: number;
  // The per-block uncompressed ranges (walking arcs in id order),
  // exposed so a downstream encoder (e.g. blockCompressArcOffsets)
  // can align its own per-partition payloads to the same arc-id
  // boundaries.
  readonly blockSpecs: ReadonlyArray<ArcCoordBlockSpec>;
}

export interface BlockCompressedArcOffsets {
  // Trained zstd shared dict, or undefined when block count is below
  // the dict-training threshold (mirrors BlockCompressedArcCoords).
  readonly dictBytes: Uint8Array | undefined;
  // u32 array of triples [firstArcId, compressedOffset, compressedLength]
  // per partition. firstArcId is the smallest arc id whose offsets
  // live in this partition; the partition covers arc ids
  // [firstArcId_i, firstArcId_{i+1}). compressedOffset is relative
  // to the start of the concatenated partitions blob.
  readonly blockTableBytes: Uint8Array;
  // Concatenation of compressed per-partition zstd frames, each
  // covering one partition's local u32 offsets (delta-encoded).
  readonly compressedBytes: Uint8Array;
  readonly blockCount: number;
}

// --- Constants ---

// Default block size for block-compressed arc_coords.
// Aligned to whole arcs at emit time. 4 KiB pays a small per-block
// compression-ratio cost (~10–20% vs 8 KiB) and ~2x the block-table
// overhead in exchange for materially less block-edge waste on
// sparse merges — measured on national CDs: ~−12 MiB downloaded /
// −9% mobile wall-clock vs the previous 8 KiB default. See
// cloud-topo-bench/bench-out/national/.
export const DEFAULT_ARC_COORD_BLOCK_BYTES = 4 * 1024;

// Auto-pick dict size from arc_coords uncompressed length.
// We use a *trained* dict via `zstd --train`.
//
// Trained dicts pack the most-frequent multi-byte sequences
// directly, so they're effective at much smaller sizes — zstd's
// own CLI default is 112640 (~110 KiB), which is what most
// production trained-dict deployments use. We default to that
// for any reasonably-sized region and clamp into a tight band.
// The first-paint download cost is ~25 ms at 50 Mbps on a 110 KiB
// dict — basically free.
const ARC_COORD_DICT_DEFAULT = 112640; // zstd's --maxdict default
const ARC_COORD_DICT_MIN = 16 * 1024;
const ARC_COORD_DICT_MAX = 256 * 1024;

// Auto-pick policy constants for selecting among trained-dict
// candidates and the no-dict baseline. Tuned to "stick with
// established defaults when measured differences are noise floor."
//
// MIN_DICT_GAIN: among trained-dict candidates, a non-standard size
// must beat the standard 110 KiB by at least this fraction of the
// projected file bytes to win. Sub-threshold differences across
// fixtures are mostly run-to-run noise from the sample-block subset.
const MIN_DICT_GAIN = 0.005; // 0.5%

// NO_DICT_PENALTY: when picking between the best dict and the no-dict
// baseline, dicts get a bonus equal to this fraction. Dict-trained
// blocks decode meaningfully faster (a separate optimization goal
// from raw byte size), so a dict that ties on bytes still wins.
// Effectively: dict wins iff dict.projected < noDict.projected
// * (1 + NO_DICT_PENALTY).
const NO_DICT_PENALTY = 0.01; // 1%

// Max in-flight block-compress tasks.
const BLOCK_COMPRESS_CONCURRENCY = 8;

// One persistent ZstdCompress stream + the chunks array its 'data'
// handler pushes into. Each stream registers its dict exactly once at
// construction; we then write one block, flush(ZSTD_e_end) to seal a
// frame, harvest the frame bytes, clear the chunks, and repeat. Calling
// `zstdCompress(buf, { dictionary })` 1882 times instead leaks ~600 KB
// per call (the binding rebuilds the CDict each time and never frees
// the prior copies), eating gigabytes on large encodes.
interface ZstdFrameStream {
  readonly stream: ZstdCompress;
  readonly chunks: Buffer[];
}

function makeZstdFrameStream(
  dictBytes: Uint8Array | undefined,
): ZstdFrameStream {
  const opts = (dictBytes !== undefined
    ? {
        dictionary: dictBytes,
        params: { [zlibConstants.ZSTD_c_compressionLevel]: 19 },
      }
    : {
        params: { [zlibConstants.ZSTD_c_compressionLevel]: 19 },
      }) as unknown as Parameters<typeof createZstdCompress>[0];
  const stream = createZstdCompress(opts);
  const chunks: Buffer[] = [];
  stream.on("data", (c: Buffer) => chunks.push(c));
  // The bench encodes never write enough to need backpressure; throw
  // on stream errors so we don't silently swallow a binding crash.
  stream.on("error", (err) => {
    throw err;
  });
  return { stream, chunks };
}

// Compress one block as an independent zstd frame through `s.stream`.
// Resets the chunks buffer first so this frame's bytes are exactly
// what's collected during this call.
async function compressFrame(
  s: ZstdFrameStream,
  block: Uint8Array,
): Promise<Buffer> {
  s.chunks.length = 0;
  s.stream.write(block);
  await new Promise<void>((resolve) =>
    s.stream.flush(zlibConstants.ZSTD_e_end, resolve),
  );
  return Buffer.concat(s.chunks);
}

function closeZstdFrameStream(s: ZstdFrameStream): void {
  s.stream.end();
  s.stream.close();
}

// --- Public API ---

// For very small inputs the dict can't exceed the input length;
// also, regions below MIN_SAMPLES blocks skip dict training
// entirely (see trainArcCoordsDict).
export function autoArcCoordDictBytes(arcCoordsLength: number): number {
  return Math.min(
    arcCoordsLength,
    Math.max(
      ARC_COORD_DICT_MIN,
      Math.min(ARC_COORD_DICT_DEFAULT, ARC_COORD_DICT_MAX),
    ),
  );
}

export async function blockCompressArcCoords(
  arcCoordsBytes: Uint8Array,
  arcOffsetsBytes: Uint8Array,
  options: { targetBlockSize: number; dictBytes: number },
): Promise<BlockCompressedArcCoords> {
  const arcOffsets = new Uint32Array(
    arcOffsetsBytes.buffer,
    arcOffsetsBytes.byteOffset,
    arcOffsetsBytes.byteLength / 4,
  );
  const numArcs = arcOffsets.length - 1;
  const totalUncompressed = arcCoordsBytes.byteLength;
  const target = options.targetBlockSize;

  // Walk arcs, accumulating into the current block until adding
  // the next arc would exceed `target` bytes. Each arc lands fully
  // within one block. Tiny edge case: a single arc larger than
  // target — emit it as its own oversized block.
  const blockSpecs: ArcCoordBlockSpec[] = [];
  let cursorArc = 0;
  let blockStart = 0;
  while (cursorArc < numArcs) {
    let pos = blockStart;
    while (cursorArc < numArcs) {
      const next = arcOffsets[cursorArc + 1];
      if (pos === blockStart && next - pos > target) {
        // Single oversize arc — close it as its own block.
        cursorArc++;
        pos = next;
        break;
      }
      if (next - blockStart > target) break;
      cursorArc++;
      pos = next;
    }
    blockSpecs.push({ startUncompressed: blockStart, endUncompressed: pos });
    blockStart = pos;
  }
  if (blockStart < totalUncompressed) {
    // Trailing bytes (degenerate fixture without arcs, etc.).
    blockSpecs.push({
      startUncompressed: blockStart,
      endUncompressed: totalUncompressed,
    });
  }

  // Build the per-block uncompressed payloads — fresh subarrays into
  // arcCoordsBytes, no copy — and hand them to the shared dict-picker.
  const blockPayloads: Uint8Array[] = new Array(blockSpecs.length);
  for (let i = 0; i < blockSpecs.length; i++) {
    blockPayloads[i] = arcCoordsBytes.subarray(
      blockSpecs[i].startUncompressed,
      blockSpecs[i].endUncompressed,
    );
  }

  // Auto-pick the dict size empirically rather than guess. We
  // train candidate dicts at several sizes, compress a sample of
  // blocks with each (and with no dict at all), and pick the
  // option with the lowest projected `compressed_total + dict_size`.
  // Empirical pick > heuristic pick: dict effectiveness varies
  // enormously by geometry (coord deltas for organic boundaries may
  // not dictionary-compress meaningfully; grid-shaped ones might).
  // The user-supplied options.dictBytes is treated as the upper
  // bound — we'll consider sizes up to that.
  const dictBytes = await autoPickSharedZstdDict(
    blockPayloads,
    options.dictBytes,
    "arc_coords",
  );

  // Build BLOCK_COMPRESS_CONCURRENCY persistent streams sharing the
  // picked dict, then run a worker loop per stream pulling blocks off
  // a shared cursor. Each stream registers its dict once and emits one
  // independent zstd frame per block via flush(ZSTD_e_end) — no
  // per-block CDict rebuild, no per-block leak.
  const compressedBlocks = new Array<Buffer>(blockSpecs.length);
  const PROGRESS_EVERY = 100;
  const t0 = Date.now();
  let completed = 0;
  stderrLog(
    `[blockCompress] start: ${blockSpecs.length} blocks, ` +
      `dict=${dictBytes !== undefined ? `${(dictBytes.byteLength / 1024).toFixed(0)} KiB` : "(none)"}, ` +
      `arc_coords=${(totalUncompressed / 1024 / 1024).toFixed(0)} MiB, ` +
      `concurrency=${BLOCK_COMPRESS_CONCURRENCY}`,
  );
  stderrLog(`[blockCompress] before-loop ${memSnapshot()}`);

  const poolSize = Math.min(BLOCK_COMPRESS_CONCURRENCY, blockSpecs.length);
  const streams = new Array<ZstdFrameStream>(poolSize);
  for (let s = 0; s < poolSize; s++)
    streams[s] = makeZstdFrameStream(dictBytes);

  let cursor = 0;
  const worker = async (s: ZstdFrameStream): Promise<void> => {
    while (cursor < blockSpecs.length) {
      const i = cursor++;
      compressedBlocks[i] = await compressFrame(s, blockPayloads[i]);
      completed++;
      if (completed % PROGRESS_EVERY === 0 || completed === blockSpecs.length) {
        stderrLog(
          `[blockCompress] ${completed}/${blockSpecs.length} ` +
            `t=${((Date.now() - t0) / 1000).toFixed(1)}s ${memSnapshot()}`,
        );
      }
    }
  };
  try {
    await Promise.all(streams.map((s) => worker(s)));
  } finally {
    for (const s of streams) closeZstdFrameStream(s);
  }
  stderrLog(
    `[blockCompress] done in ${((Date.now() - t0) / 1000).toFixed(1)}s`,
  );

  // Concatenate compressed blocks and build the block table.
  // Null each Buffer slot after copying so V8 GC can reclaim the
  // per-block allocations as we go (tens of MB for large topologies,
  // potentially 100s of MB with smaller block sizes).
  let totalCompressed = 0;
  for (const cb of compressedBlocks) totalCompressed += cb.byteLength;
  const compressedBytes = new Uint8Array(totalCompressed);
  // 3 u32s per block: uncompressedEnd, compressedOffset, compressedLength.
  const blockTableBytes = new Uint8Array(blockSpecs.length * 12);
  const tableView = new DataView(blockTableBytes.buffer);
  let cOff = 0;
  const compressedBlocksWritable =
    compressedBlocks as unknown as (Buffer | null)[];
  for (let i = 0; i < blockSpecs.length; i++) {
    const cb = compressedBlocks[i];
    compressedBytes.set(cb, cOff);
    tableView.setUint32(i * 12 + 0, blockSpecs[i].endUncompressed, true);
    tableView.setUint32(i * 12 + 4, cOff, true);
    tableView.setUint32(i * 12 + 8, cb.byteLength, true);
    cOff += cb.byteLength;
    compressedBlocksWritable[i] = null;
  }

  return {
    dictBytes,
    blockTableBytes,
    compressedBytes,
    blockCount: blockSpecs.length,
    targetBlockSize: target,
    blockSpecs,
  };
}

// Below this raw-arc_offsets size the encoder ships arc_offsets as a
// single zstd-compressed delta-encoded blob (the original format).
// At this scale, per-partition zstd-frame overhead + the block table
// + dict cost outweigh the savings from selective fetching, AND the
// whole monolithic blob already fits in a small number of round-trips
// on typical networks. Above the threshold, the partitioned path
// dominates — see bench-out/national/*.offsets-partition-sim.csv.
export const ARC_OFFSETS_PARTITION_MIN_BYTES = 750 * 1024;

// Dict-size auto-pick for arc_offsets partitions. Each partition is
// hundreds of bytes (not KiB-MiB like arc_coords), so a tiny dict is
// the right ballpark — geometric range that the sample-projection
// picker explores stays the same; the per-call max here just bounds
// the largest candidate we'd consider training. Matches the
// auto-dict-size pattern used for arc_coords (auto-pick over 32 KiB →
// 512 KiB candidates).
export function autoArcOffsetsDictBytes(arcOffsetsLength: number): number {
  return Math.min(
    arcOffsetsLength,
    Math.max(
      ARC_COORD_DICT_MIN,
      Math.min(ARC_COORD_DICT_DEFAULT, ARC_COORD_DICT_MAX),
    ),
  );
}

// Block-compress the global arc_offsets table along the same arc-id
// boundaries that blockCompressArcCoords produced. For each block
// covering arc ids [firstArcId_i, firstArcId_{i+1}):
//   - Extract the N+1 u32 entries arc_offsets[firstArcId_i ..
//     firstArcId_{i+1}] (N == arcs in block).
//   - Subtract the block's uncompressed start so the stored values
//     are *local* byte offsets within the decompressed arc-coords
//     block (entry[0] == 0, entry[N] == block size in uncompressed
//     bytes). Local deltas are tiny — typical arc length in bytes —
//     and compress 3-4× better than the global cumulative form.
//   - First-order delta-encode (same trick as the monolithic path).
//   - Compress as one independent zstd frame against a shared dict
//     trained over the per-block delta-encoded payloads.
//
// Returns the trained dict (or undefined), a 3 × u32 block table
// [firstArcId, compOff, compLen] per partition, and the concatenated
// frame bytes.
export async function blockCompressArcOffsets(
  arcOffsetsBytes: Uint8Array,
  blockSpecs: ReadonlyArray<ArcCoordBlockSpec>,
  options: { dictBytes: number },
): Promise<BlockCompressedArcOffsets> {
  const arcOffsets = new Uint32Array(
    arcOffsetsBytes.buffer,
    arcOffsetsBytes.byteOffset,
    arcOffsetsBytes.byteLength / 4,
  );
  const numArcs = arcOffsets.length - 1;

  // Walk arcs in id order, snapping each to the block whose
  // uncompressed range covers its start byte. blockSpecs is in arc-id
  // order and contiguous; a single sweep with a moving block cursor
  // gives us each partition's [firstArcId, lastArcIdExclusive) range
  // without binary-searching per arc.
  const firstArcIdByBlock = new Uint32Array(blockSpecs.length + 1);
  {
    let blockIdx = 0;
    let blockEnd = blockSpecs[0].endUncompressed;
    for (let arcId = 0; arcId < numArcs; arcId++) {
      const off = arcOffsets[arcId];
      while (blockIdx < blockSpecs.length - 1 && off >= blockEnd) {
        blockIdx++;
        firstArcIdByBlock[blockIdx] = arcId;
        blockEnd = blockSpecs[blockIdx].endUncompressed;
      }
    }
    firstArcIdByBlock[blockSpecs.length] = numArcs;
  }

  // Build per-partition delta-encoded payloads.
  const payloads = new Array<Uint8Array>(blockSpecs.length);
  for (let i = 0; i < blockSpecs.length; i++) {
    const firstArcId = firstArcIdByBlock[i];
    const nextFirstArcId = firstArcIdByBlock[i + 1];
    const blockStart = blockSpecs[i].startUncompressed;
    const entries = nextFirstArcId - firstArcId + 1;
    const local = new Uint32Array(entries);
    let prev = 0;
    for (let k = 0; k < entries; k++) {
      const v = arcOffsets[firstArcId + k] - blockStart;
      local[k] = v - prev;
      prev = v;
    }
    payloads[i] = new Uint8Array(
      local.buffer,
      local.byteOffset,
      local.byteLength,
    );
  }

  const dictBytes = await autoPickSharedZstdDict(
    payloads,
    options.dictBytes,
    "arc_offsets",
  );

  // Mirror blockCompressArcCoords' worker-pool pattern: persistent
  // zstd streams sharing the picked dict, each compressing one
  // partition as one frame and emitting it via flush(ZSTD_e_end).
  const compressedFrames = new Array<Buffer>(blockSpecs.length);
  const PROGRESS_EVERY = 1000;
  const t0 = Date.now();
  let completed = 0;
  stderrLog(
    `[blockCompressOffsets] start: ${blockSpecs.length} partitions, ` +
      `dict=${dictBytes !== undefined ? `${(dictBytes.byteLength / 1024).toFixed(0)} KiB` : "(none)"}, ` +
      `arc_offsets=${(arcOffsetsBytes.byteLength / 1024 / 1024).toFixed(1)} MiB raw, ` +
      `concurrency=${BLOCK_COMPRESS_CONCURRENCY}`,
  );

  const poolSize = Math.min(BLOCK_COMPRESS_CONCURRENCY, blockSpecs.length);
  const streams = new Array<ZstdFrameStream>(poolSize);
  for (let s = 0; s < poolSize; s++) {
    streams[s] = makeZstdFrameStream(dictBytes);
  }
  let cursor = 0;
  const worker = async (s: ZstdFrameStream): Promise<void> => {
    while (cursor < blockSpecs.length) {
      const i = cursor++;
      compressedFrames[i] = await compressFrame(s, payloads[i]);
      completed++;
      if (
        completed % PROGRESS_EVERY === 0 ||
        completed === blockSpecs.length
      ) {
        stderrLog(
          `[blockCompressOffsets] ${completed}/${blockSpecs.length} ` +
            `t=${((Date.now() - t0) / 1000).toFixed(1)}s ${memSnapshot()}`,
        );
      }
    }
  };
  try {
    await Promise.all(streams.map((s) => worker(s)));
  } finally {
    for (const s of streams) closeZstdFrameStream(s);
  }
  stderrLog(
    `[blockCompressOffsets] done in ${((Date.now() - t0) / 1000).toFixed(1)}s`,
  );

  // Concatenate frames + build the [firstArcId, compOff, compLen] table.
  let totalCompressed = 0;
  for (const f of compressedFrames) totalCompressed += f.byteLength;
  const compressedBytes = new Uint8Array(totalCompressed);
  const blockTableBytes = new Uint8Array(blockSpecs.length * 12);
  const tableView = new DataView(blockTableBytes.buffer);
  let cOff = 0;
  const framesWritable = compressedFrames as unknown as (Buffer | null)[];
  for (let i = 0; i < blockSpecs.length; i++) {
    const f = compressedFrames[i];
    compressedBytes.set(f, cOff);
    tableView.setUint32(i * 12 + 0, firstArcIdByBlock[i], true);
    tableView.setUint32(i * 12 + 4, cOff, true);
    tableView.setUint32(i * 12 + 8, f.byteLength, true);
    cOff += f.byteLength;
    framesWritable[i] = null;
  }

  return {
    dictBytes,
    blockTableBytes,
    compressedBytes,
    blockCount: blockSpecs.length,
  };
}

// --- arc_endpoints (per-arc absolute start + end, partitioned) ---

// Same block boundaries as arc_coords / arc_offsets — partition i
// covers arc ids [firstArcIdByBlock[i], firstArcIdByBlock[i+1]). Each
// partition's payload is a zigzag-varint stream over four interleaved
// channels per arc:
//   d_startX = startX - prevStartX  (prev = 0 at block start)
//   d_startY = startY - prevStartY
//   d_endX   = endX   - startX      (intra-arc span on X)
//   d_endY   = endY   - startY      (intra-arc span on Y)
// Interleaved (not concatenated) so the reader decodes one arc at a
// time without two passes over the buffer. With hierarchical-Hilbert
// ordering the start-to-start deltas stay small (spatially adjacent
// arcs), and intra-arc spans are bounded by the arc's quantized
// bbox — most arcs encode in 4-8 bytes of varint.
//
// A partition is independently decodable: the first arc's start is a
// delta from (0,0), so the decoder doesn't need state from any earlier
// partition.
export interface BlockCompressedArcEndpoints {
  readonly dictBytes: Uint8Array | undefined;
  // u32 triples [firstArcId, compressedOffset, compressedLength] per
  // partition. compressedOffset is relative to the start of the
  // concatenated partitions blob.
  readonly blockTableBytes: Uint8Array;
  readonly compressedBytes: Uint8Array;
  readonly blockCount: number;
}

// Auto-pick dict size. Same band as arc_offsets — partitions are
// hundreds of bytes to a few KiB, well below the arc_coords dict
// regime.
export function autoArcEndpointsDictBytes(arcEndpointsLength: number): number {
  return Math.min(
    arcEndpointsLength,
    Math.max(
      ARC_COORD_DICT_MIN,
      Math.min(ARC_COORD_DICT_DEFAULT, ARC_COORD_DICT_MAX),
    ),
  );
}

export async function blockCompressArcEndpoints(
  arcEndpoints: Int32Array,
  blockSpecs: ReadonlyArray<ArcCoordBlockSpec>,
  arcOffsetsBytes: Uint8Array,
  options: { dictBytes: number },
): Promise<BlockCompressedArcEndpoints> {
  const arcOffsets = new Uint32Array(
    arcOffsetsBytes.buffer,
    arcOffsetsBytes.byteOffset,
    arcOffsetsBytes.byteLength / 4,
  );
  const numArcs = arcOffsets.length - 1;
  if (arcEndpoints.length !== numArcs * 4) {
    throw new Error(
      `ctopo: blockCompressArcEndpoints expected ${numArcs * 4} entries, got ${arcEndpoints.length}`,
    );
  }

  // Mirror blockCompressArcOffsets' arc-id → block mapping. blockSpecs
  // is arc-id-ordered and contiguous; one sweep with a moving cursor
  // recovers each partition's [firstArcId, nextFirstArcId).
  const firstArcIdByBlock = new Uint32Array(blockSpecs.length + 1);
  {
    let blockIdx = 0;
    let blockEnd = blockSpecs[0].endUncompressed;
    for (let arcId = 0; arcId < numArcs; arcId++) {
      const off = arcOffsets[arcId];
      while (blockIdx < blockSpecs.length - 1 && off >= blockEnd) {
        blockIdx++;
        firstArcIdByBlock[blockIdx] = arcId;
        blockEnd = blockSpecs[blockIdx].endUncompressed;
      }
    }
    firstArcIdByBlock[blockSpecs.length] = numArcs;
  }

  // Per-partition varint payloads. Worst case 5 bytes × 4 ints per arc
  // = 20 bytes/arc; allocate scratch at that ceiling and slice down.
  const payloads = new Array<Uint8Array>(blockSpecs.length);
  for (let i = 0; i < blockSpecs.length; i++) {
    const firstArcId = firstArcIdByBlock[i];
    const nextFirstArcId = firstArcIdByBlock[i + 1];
    const arcsInBlock = nextFirstArcId - firstArcId;
    const scratch = new Uint8Array(arcsInBlock * 4 * VARINT_MAX_BYTES);
    let off = 0;
    let prevStartX = 0;
    let prevStartY = 0;
    for (let a = firstArcId; a < nextFirstArcId; a++) {
      const base = a * 4;
      const sx = arcEndpoints[base];
      const sy = arcEndpoints[base + 1];
      const ex = arcEndpoints[base + 2];
      const ey = arcEndpoints[base + 3];
      off = writeVarintZigzag(sx - prevStartX, scratch, off);
      off = writeVarintZigzag(sy - prevStartY, scratch, off);
      off = writeVarintZigzag(ex - sx, scratch, off);
      off = writeVarintZigzag(ey - sy, scratch, off);
      prevStartX = sx;
      prevStartY = sy;
    }
    payloads[i] = scratch.subarray(0, off);
  }

  const dictBytes = await autoPickSharedZstdDict(
    payloads,
    options.dictBytes,
    "arc_endpoints",
  );

  // Mirror blockCompressArcOffsets' worker-pool pattern.
  const compressedFrames = new Array<Buffer>(blockSpecs.length);
  const PROGRESS_EVERY = 1000;
  const t0 = Date.now();
  let completed = 0;
  let rawTotal = 0;
  for (const p of payloads) rawTotal += p.byteLength;
  stderrLog(
    `[blockCompressEndpoints] start: ${blockSpecs.length} partitions, ` +
      `dict=${dictBytes !== undefined ? `${(dictBytes.byteLength / 1024).toFixed(0)} KiB` : "(none)"}, ` +
      `arc_endpoints=${(rawTotal / 1024 / 1024).toFixed(2)} MiB raw, ` +
      `concurrency=${BLOCK_COMPRESS_CONCURRENCY}`,
  );

  const poolSize = Math.min(BLOCK_COMPRESS_CONCURRENCY, blockSpecs.length);
  const streams = new Array<ZstdFrameStream>(poolSize);
  for (let s = 0; s < poolSize; s++) {
    streams[s] = makeZstdFrameStream(dictBytes);
  }
  let cursor = 0;
  const worker = async (s: ZstdFrameStream): Promise<void> => {
    while (cursor < blockSpecs.length) {
      const i = cursor++;
      compressedFrames[i] = await compressFrame(s, payloads[i]);
      completed++;
      if (
        completed % PROGRESS_EVERY === 0 ||
        completed === blockSpecs.length
      ) {
        stderrLog(
          `[blockCompressEndpoints] ${completed}/${blockSpecs.length} ` +
            `t=${((Date.now() - t0) / 1000).toFixed(1)}s ${memSnapshot()}`,
        );
      }
    }
  };
  try {
    await Promise.all(streams.map((s) => worker(s)));
  } finally {
    for (const s of streams) closeZstdFrameStream(s);
  }
  stderrLog(
    `[blockCompressEndpoints] done in ${((Date.now() - t0) / 1000).toFixed(1)}s`,
  );

  let totalCompressed = 0;
  for (const f of compressedFrames) totalCompressed += f.byteLength;
  const compressedBytes = new Uint8Array(totalCompressed);
  const blockTableBytes = new Uint8Array(blockSpecs.length * 12);
  const tableView = new DataView(blockTableBytes.buffer);
  let cOff = 0;
  const framesWritable = compressedFrames as unknown as (Buffer | null)[];
  for (let i = 0; i < blockSpecs.length; i++) {
    const f = compressedFrames[i];
    compressedBytes.set(f, cOff);
    tableView.setUint32(i * 12 + 0, firstArcIdByBlock[i], true);
    tableView.setUint32(i * 12 + 4, cOff, true);
    tableView.setUint32(i * 12 + 8, f.byteLength, true);
    cOff += f.byteLength;
    framesWritable[i] = null;
  }

  return {
    dictBytes,
    blockTableBytes,
    compressedBytes,
    blockCount: blockSpecs.length,
  };
}

// --- Internal: dictionary training ---

// Train a zstd shared dictionary from a list of per-block payloads via
// the `zstd --train` CLI. No JS package exposes ZDICT_trainFromBuffer,
// so the shell-out is currently the simplest path.
//
// Trained dicts pack the most-frequent multi-byte sequences from the
// samples into a compact (10s of KB) dictionary.
//
// `logLabel` is the section label printed in stderr progress lines
// ("arc_coords" / "arc_offsets") so concurrent training passes don't
// confuse each other in the log.
//
// Throws if `zstd` isn't on PATH.
function trainSharedZstdDict(
  samples: ReadonlyArray<Uint8Array>,
  targetDictBytes: number,
  logLabel: string,
): Uint8Array | undefined {
  // zstd --train needs ≥~100 samples for stable output. For tiny
  // inputs (test fixtures, very small topologies) we skip the dict
  // entirely — the savings would be tiny anyway, and the encoder
  // emits …Blocks META without dictSection so the reader falls back
  // to no-dict decode.
  const MIN_SAMPLES = 100;
  if (samples.length < MIN_SAMPLES) return undefined;

  const tmp = mkdtempSync(join(tmpdir(), "ctopo-dict-"));
  try {
    // Each block becomes one sample file. Names are zero-padded so
    // shell glob ordering doesn't matter to the trainer.
    const sampleDir = join(tmp, "samples");
    const dictPath = join(tmp, "dict");
    spawnSync("mkdir", ["-p", sampleDir], { stdio: "ignore" });
    let totalSampleBytes = 0;
    for (let i = 0; i < samples.length; i++) {
      const slice = samples[i];
      const name = `s${i.toString().padStart(6, "0")}`;
      writeFileSync(join(sampleDir, name), slice);
      totalSampleBytes += slice.byteLength;
    }
    stderrLog(
      `[trainDict ${logLabel}] training on ${samples.length} samples ` +
        `(${(totalSampleBytes / 1024 / 1024).toFixed(1)} MiB), maxdict=${(targetDictBytes / 1024).toFixed(0)} KiB`,
    );
    const t0 = Date.now();
    const result = spawnSync(
      "zstd",
      [
        "--train",
        `--maxdict=${targetDictBytes}`,
        "-o",
        dictPath,
        "-r",
        sampleDir,
      ],
      { stdio: ["ignore", "ignore", "pipe"] },
    );
    if (result.error !== undefined) {
      throw new Error(
        `ctopo: trainSharedZstdDict requires the \`zstd\` CLI on PATH for dict training (got: ${result.error.message})`,
      );
    }
    if (result.status !== 0) {
      throw new Error(
        `ctopo: zstd --train failed (exit ${result.status}): ${result.stderr?.toString() ?? "<no stderr>"}`,
      );
    }
    const dictBuf = readFileSync(dictPath);
    stderrLog(
      `[trainDict ${logLabel}] trained ${dictBuf.byteLength} byte dict in ${((Date.now() - t0) / 1000).toFixed(1)}s`,
    );
    return new Uint8Array(
      dictBuf.buffer,
      dictBuf.byteOffset,
      dictBuf.byteLength,
    );
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

// Empirically pick the best shared-dict size by trying a few candidate
// sizes (plus no-dict) on a small block sample and projecting to full
// file size. Returns the trained dict for the best candidate, or
// undefined if no-dict wins.
//
// `blockPayloads` is the per-block bytes that will be compressed
// (arc_coords slices for the arc_coords path; per-partition
// delta-encoded local offsets for the arc_offsets path). `logLabel`
// disambiguates progress lines.
//
// The cost is one `zstd --train` per candidate (~10s each) and one
// sample compress per candidate (~1s). For ~3-4 candidates that's
// ~40s of overhead at encode time.
async function autoPickSharedZstdDict(
  blockPayloads: ReadonlyArray<Uint8Array>,
  maxDictBytes: number,
  logLabel: string,
): Promise<Uint8Array | undefined> {
  // Need a meaningful number of blocks for both training and the
  // sample test. Skip auto-tuning (and dict entirely) for tiny
  // regions — the savings are too small to justify the overhead.
  const MIN_SAMPLES_FOR_AUTO = 100;
  if (blockPayloads.length < MIN_SAMPLES_FOR_AUTO) return undefined;

  // Pick a small evaluation sample — every Nth block by stride so
  // we cover early/late blocks (different tiers). 100 is enough
  // for a stable ratio estimate without making the sample-compress
  // step expensive.
  const SAMPLE_BLOCKS = 100;
  const stride = Math.max(1, Math.floor(blockPayloads.length / SAMPLE_BLOCKS));
  const sampleSlices: Uint8Array[] = [];
  for (let i = 0; i < blockPayloads.length; i += stride) {
    sampleSlices.push(blockPayloads[i]);
    if (sampleSlices.length >= SAMPLE_BLOCKS) break;
  }
  const projectionFactor = blockPayloads.length / sampleSlices.length;
  stderrLog(
    `[autoDict ${logLabel}] tuning on ${sampleSlices.length} sample blocks (${blockPayloads.length} total, ` +
      `projection×${projectionFactor.toFixed(1)}, max dict=${(maxDictBytes / 1024).toFixed(0)} KiB)`,
  );

  // Helper: compress all sample slices with optional dict, return total
  // bytes. One persistent stream per candidate dict — registering the
  // dict ~700 times across the seven candidates would otherwise leak
  // hundreds of MB before blockCompress even starts.
  const compressSampleTotal = async (
    dict: Uint8Array | undefined,
  ): Promise<number> => {
    const s = makeZstdFrameStream(dict);
    try {
      let total = 0;
      for (const slice of sampleSlices) {
        const frame = await compressFrame(s, slice);
        total += frame.byteLength;
      }
      return total;
    } finally {
      closeZstdFrameStream(s);
    }
  };

  // Candidate dict sizes — geometric range. Don't clamp to
  // maxDictBytes here: that "max" is the auto-default heuristic
  // (currently 110 KiB), which we want to *test against*, not use
  // as a ceiling for exploration. Let the picker see candidates on
  // both sides of the heuristic, including ones meaningfully larger,
  // so it can find the actual minimum of the size-vs-file-bytes
  // curve. Sizes that exceed arc_coords itself get filtered later
  // (no point training a dict bigger than the data).
  const candidateSizes = [32, 64, 110, 192, 256, 384, 512].map((k) => k * 1024);
  void maxDictBytes; // intentionally unused — see above

  type Candidate = {
    readonly label: string;
    readonly dict: Uint8Array | undefined;
    readonly projectedFileBytes: number;
  };
  const candidates: Candidate[] = [];

  // Baseline: no dict.
  const noDictSampleTotal = await compressSampleTotal(undefined);
  const noDictProjected = noDictSampleTotal * projectionFactor;
  candidates.push({
    label: "no-dict",
    dict: undefined,
    projectedFileBytes: noDictProjected,
  });
  stderrLog(
    `[autoDict ${logLabel}]   no-dict: sample=${(noDictSampleTotal / 1024).toFixed(0)} KiB, ` +
      `projected=${(noDictProjected / 1024 / 1024).toFixed(2)} MiB`,
  );

  // Trained-dict candidates.
  for (const size of candidateSizes) {
    const dict = trainSharedZstdDict(blockPayloads, size, logLabel);
    if (dict === undefined) continue;
    const sampleTotal = await compressSampleTotal(dict);
    const projected = sampleTotal * projectionFactor + dict.byteLength;
    candidates.push({
      label: `${(dict.byteLength / 1024).toFixed(0)} KiB dict`,
      dict,
      projectedFileBytes: projected,
    });
    stderrLog(
      `[autoDict ${logLabel}]   ${(dict.byteLength / 1024).toFixed(0)} KiB dict: ` +
        `sample=${(sampleTotal / 1024).toFixed(0)} KiB, ` +
        `projected=${(projected / 1024 / 1024).toFixed(2)} MiB ` +
        `(incl ${(dict.byteLength / 1024).toFixed(0)} KiB dict)`,
    );
  }

  // Two-stage pick:
  //
  // 1. Among trained-dict candidates, prefer the standard 110 KiB
  //    default unless another size beats it by more than
  //    MIN_DICT_GAIN. Sub-threshold differences are noise from the
  //    sample-block subset and not worth picking a non-standard size.
  //
  // 2. Compare the best dict against the no-dict baseline with a
  //    NO_DICT_PENALTY bias against no-dict. Dicts win ties (and
  //    tolerate small byte regressions) because dict-trained blocks
  //    decode faster — a benefit the byte projection ignores.
  const dicts = candidates.filter(
    (c): c is Candidate & { dict: Uint8Array } => c.dict !== undefined,
  );
  const noDict = candidates.find((c) => c.dict === undefined);

  // Stage 1: best dict, default-preferring.
  let bestDict: (Candidate & { dict: Uint8Array }) | undefined = undefined;
  if (dicts.length > 0) {
    const standard = dicts.find(
      (c) => c.dict.byteLength === ARC_COORD_DICT_DEFAULT,
    );
    bestDict = standard ?? dicts[0];
    for (const c of dicts) {
      if (
        c.projectedFileBytes <
        bestDict.projectedFileBytes * (1 - MIN_DICT_GAIN)
      ) {
        bestDict = c;
      }
    }
  }

  // Stage 2: best dict vs no-dict.
  let chosen: Candidate;
  let rationale: string;
  if (bestDict === undefined) {
    chosen = noDict ?? candidates[0];
    rationale = "only candidate";
  } else if (noDict === undefined) {
    chosen = bestDict;
    rationale = "no no-dict baseline";
  } else if (
    bestDict.projectedFileBytes <
    noDict.projectedFileBytes * (1 + NO_DICT_PENALTY)
  ) {
    chosen = bestDict;
    rationale = `dict beats no-dict (${(bestDict.projectedFileBytes / noDict.projectedFileBytes).toFixed(3)}× projected, decode-speed bonus applied)`;
  } else {
    chosen = noDict;
    rationale = `no-dict wins by >${(NO_DICT_PENALTY * 100).toFixed(1)}%`;
  }
  stderrLog(`[autoDict ${logLabel}] picked: ${chosen.label} — ${rationale}`);
  return chosen.dict;
}
