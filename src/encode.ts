// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Encoder for the .ctopo container format. Node-only — uses Buffer + fs.
 *
 * Walks a TopoJSON Topology and emits one container with:
 *   - global arcs (arc_offsets + arc_coords)
 *   - per-layer geometry CSR triples (poly_offsets, ring_offsets, arc_refs)
 *   - per-layer per-property sections (auto-detected dtype)
 *   - per-layer string sections for non-numeric properties
 *
 * Section order (writer enforces): arc_offsets, then per-layer CSR triples
 * grouped by layer, then property/strings sections in stable order, then
 * arc_coords last (largest, only Range-fetched in slices).
 */

import {
  writeFileSync,
  readFileSync,
  writeSync,
  mkdtempSync,
  rmSync,
} from "fs";
import {
  brotliCompress,
  brotliDecompressSync,
  zstdCompress,
  zstdCompressSync,
  zstdDecompressSync,
  constants as zlibConstants,
} from "zlib";
import { promisify } from "util";
import { spawnSync } from "child_process";
import { tmpdir } from "os";
import { join } from "path";

import {
  type GeometryObject,
  type Properties,
  type Topology,
} from "topojson-specification";

import {
  FOOTER_PREFIX_SIZE,
  FOOTER_TRAILER_SIZE,
  HEADER_SIZE,
  MAGIC,
  OFFSET_FOOTER_META_LENGTH,
  OFFSET_FOOTER_SECTION_COUNT,
  OFFSET_MAGIC,
  OFFSET_VERSION,
  SECTION_ALIGNMENT,
  SECTION_ENTRY_SIZE,
  SECTION_NAME_SIZE,
  VARINT_MAX_BYTES,
  VERSION,
  alignUp,
  writeVarintZigzag,
} from "./format";
import { parseContainer } from "./reader";
import {
  type Compression,
  type ContainerMeta,
  type DType,
  type PropertyOverride,
} from "./types";

interface BuiltSection {
  readonly name: string;
  readonly dtype: DType;
  // The bytes that go on disk. If `compression` is set these are the
  // already-compressed bytes; otherwise they're the raw section bytes.
  // The reader infers which from the META `compression` field.
  readonly bytes: Uint8Array;
  readonly compression?: Compression;
  // True when `bytes` already had first-order delta encoding applied
  // (cumulative-monotone u32 sections). The reader undoes the
  // transform with a running prefix sum after decompression.
  readonly delta?: boolean;
  // True for sections the encoder lays out at the front of the data
  // area so a single front-prefetch GET can cover them. Set by the
  // encoder for structural sections (arc_coords_dict, arc_coord_blocks,
  // arc_offsets, per-layer CSR triples, arc_coords) and augmented by
  // the caller via EncodeOptions.frontLoadedSectionNames for app-
  // specific sections (e.g., index strings).
  readonly frontLoad?: boolean;
}

// Sections that contain typed numeric data or strings — everything
// callers always read whole — get compressed. Only arc_coords stays
// uncompressed because it's range-fetched in slices; its
// compression needs the blocked-format work. arc_offsets is read
// whole at open time, so it compresses freely.
function shouldCompressSection(name: string, dtype: DType): boolean {
  if (name === "arc_coords") return false;
  // Block-compressed arc_coords block table: small (~12 bytes per
  // block, ~20 KiB for large topologies) and fetched eagerly at open. Skip
  // compression so it serves directly out of one Range fetch with
  // no decompressor setup.
  if (name === "arc_coord_blocks") return false;
  // Shared dict is sample bytes already; would barely compress
  // further, and must be available before any block can decompress
  // (chicken-and-egg if it were compressed against another dict).
  if (name === "arc_coords_dict") return false;
  // CSR triples (poly_offsets, ring_offsets, arc_refs per layer) are
  // small (~few MB for large topologies) and read whole, so compressing them is a
  // free win even though they're not the bottleneck.
  if (
    name.endsWith("/poly_offsets") ||
    name.endsWith("/ring_offsets") ||
    name.endsWith("/arc_refs")
  ) {
    return true;
  }
  // Property + strings: yes.
  if (dtype === "strings") return true;
  // Treat any remaining named section (arc_offsets and properties)
  // as compressible.
  return true;
}

// Cap each compression group at this many *uncompressed* bytes.
// Matches the client's per-fetch chunk size so a group decompresses
// within one batched request, and bounds per-decompress CPU on the
// reader to one ~4 MiB chunk regardless of how many member sections
// it covers.
const MAX_COMPRESSION_GROUP_BYTES = 4 * 1024 * 1024;

// Default block size for block-compressed arc_coords. Chosen so a
// single-arc fetch pulls ~10-30 KiB compressed (~1000 avg-sized arcs
// per block); smaller blocks waste compress overhead, larger blocks
// waste bandwidth on selective fetches. Aligned to whole arcs at
// emit time.
const DEFAULT_ARC_COORD_BLOCK_BYTES = 64 * 1024;

// Auto-pick dict size from arc_coords uncompressed length.
// We use a *trained* dict via `zstd --train`, so the size scaling
// is very different from the raw-content sampling we tried first
// (where you needed lots of bytes to capture useful patterns).
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
function autoArcCoordDictBytes(arcCoordsLength: number): number {
  // For very small inputs the dict can't exceed the input length;
  // also, regions below MIN_SAMPLES blocks skip dict training
  // entirely (see trainArcCoordsDict).
  return Math.min(
    arcCoordsLength,
    Math.max(
      ARC_COORD_DICT_MIN,
      Math.min(ARC_COORD_DICT_DEFAULT, ARC_COORD_DICT_MAX),
    ),
  );
}

// Max in-flight block-compress tasks. Matches the libuv default
// pool size (4) so we never queue more dict-loaded compression
// contexts than there are workers actively using one. Higher
// values appeared to leak off-heap memory when combined with the
// `dictionary` option (1700+ blocks × dict-aware compress silently
// OOM-killed the container at concurrency 16, even though only 16
// should be in-flight). Bumping back up if the leak hypothesis
// turns out wrong is a one-line change.
const BLOCK_COMPRESS_CONCURRENCY = 4;

// Same concern in assembleContainer's group-compress phase: we
// have ~150 region tasks, each holding several MiB of input bytes
// (4 MiB cap per group). Promise.all over all of them would pin
// every input buffer for the full duration of the phase. The
// worker pool keeps in-flight bytes-refs equal to N rather than
// N × all-tasks, plus we null the bytes ref the moment compress
// finishes for an extra peak-memory drop.
const GROUP_COMPRESS_CONCURRENCY = 4;

// Element size (and required byte alignment) for each dtype. Member
// byteOffsets within a compression group must be aligned to this so
// the reader can wrap the bytes as a typed-array view without
// copying — `new Uint16Array(buf, byteOffset, length)` throws if
// byteOffset isn't a multiple of 2, and similarly for u32/i32 (4)
// and f64 (8).
function dtypeAlignment(dtype: DType): number {
  switch (dtype) {
    case "i8":
    case "u8":
    case "blob":
    case "strings":
      return 1;
    case "i16":
    case "u16":
      return 2;
    case "i32":
    case "u32":
      return 4;
    case "f64":
      return 8;
  }
}

// First-order delta encoding for cumulative-monotone u32 sections.
// out[0] = in[0]; out[i] = in[i] - in[i-1]. Operates over u32 with
// natural wraparound — the inverse pass on the reader is also a
// running u32 sum, so values that happen to wrap encode-side
// recover exactly. Returns a fresh buffer; caller doesn't need to
// copy. Bytes are little-endian everywhere.
function deltaEncodeU32(bytes: Uint8Array): Uint8Array {
  const src = new Uint32Array(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength / 4,
  );
  const dst = new Uint32Array(src.length);
  if (src.length === 0) return new Uint8Array(dst.buffer);
  dst[0] = src[0];
  for (let i = 1; i < src.length; i++) dst[i] = src[i] - src[i - 1];
  return new Uint8Array(dst.buffer);
}

function noopProgress(_event: EncodeProgress): void {
  /* default progress sink — discards events */
}

// Compression runs on Node's libuv thread pool via the async API
// — multiple concurrent calls parallelize across UV_THREADPOOL_SIZE
// (default 4). Group compression dominates encode time at zstd
// L19, so firing every group through Promise.all in
// assembleContainer cuts wall-clock by ~Nx where N is
// min(group_count, pool_size).
const zstdCompressAsync = promisify(zstdCompress);
const brotliCompressAsync = promisify(brotliCompress);

async function compressBytesAsync(
  bytes: Uint8Array,
  codec: Compression,
): Promise<Uint8Array> {
  if (codec === "zst") {
    const compressed = await zstdCompressAsync(bytes, {
      params: { [zlibConstants.ZSTD_c_compressionLevel]: 19 },
    });
    return new Uint8Array(
      compressed.buffer,
      compressed.byteOffset,
      compressed.byteLength,
    );
  }
  const compressed = await brotliCompressAsync(bytes, {
    params: { [zlibConstants.BROTLI_PARAM_QUALITY]: 5 },
  });
  return new Uint8Array(
    compressed.buffer,
    compressed.byteOffset,
    compressed.byteLength,
  );
}

// Train a zstd shared dictionary from a sample of arc-coord blocks via
// the `zstd --train` CLI. No JS package exposes ZDICT_trainFromBuffer
// (we surveyed zstd-napi, zstd-codec, @bokuweb/zstd-wasm, and
// @mongodb-js/zstd), so the shell-out is currently the only path.
//
// Trained dicts pack the most-frequent multi-byte sequences from the
// samples into a compact (10s of KB) dictionary, vs a raw-content
// "first N bytes" sample which only captures whatever literal bytes
// happened to appear at the front. Real-world wins: 2-4× better
// ratio per byte of dict.
//
// Throws if `zstd` isn't on PATH — block-compressed arc_coords needs
// the dict, so silently degrading to no-dict would produce a file
// that doesn't match the format spec readers expect.
function trainArcCoordsDict(
  arcCoordsBytes: Uint8Array,
  blockSpecs: ReadonlyArray<{
    readonly startUncompressed: number;
    readonly endUncompressed: number;
  }>,
  targetDictBytes: number,
): Uint8Array | undefined {
  // zstd --train needs ≥~100 samples for stable output. For tiny
  // regions (test fixtures, single-county states) we skip the dict
  // entirely — the savings would be tiny anyway, and the encoder
  // emits arcCoordsBlocks META without dictSection so the reader
  // falls back to no-dict decode.
  const MIN_SAMPLES = 100;
  if (blockSpecs.length < MIN_SAMPLES) return undefined;
  // Train on every block. For large topologies this is in the
  // 1-5K range and zstd --train completes in a few seconds.
  const sampleIndices: number[] = [];
  for (let i = 0; i < blockSpecs.length; i++) sampleIndices.push(i);

  const tmp = mkdtempSync(join(tmpdir(), "ctopo-dict-"));
  try {
    // Each block becomes one sample file. Names are zero-padded so
    // shell glob ordering doesn't matter to the trainer.
    const sampleDir = join(tmp, "samples");
    const dictPath = join(tmp, "dict");
    spawnSync("mkdir", ["-p", sampleDir], { stdio: "ignore" });
    let totalSampleBytes = 0;
    for (let i = 0; i < sampleIndices.length; i++) {
      const spec = blockSpecs[sampleIndices[i]];
      const slice = arcCoordsBytes.subarray(
        spec.startUncompressed,
        spec.endUncompressed,
      );
      const name = `s${i.toString().padStart(6, "0")}`;
      writeFileSync(join(sampleDir, name), slice);
      totalSampleBytes += slice.byteLength;
    }
    writeSync(
      2,
      `[trainDict] training on ${sampleIndices.length} samples ` +
        `(${(totalSampleBytes / 1024 / 1024).toFixed(1)} MiB), maxdict=${(targetDictBytes / 1024).toFixed(0)} KiB\n`,
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
        `ctopo: blockCompressArcCoords requires the \`zstd\` CLI on PATH for dict training (got: ${result.error.message})`,
      );
    }
    if (result.status !== 0) {
      throw new Error(
        `ctopo: zstd --train failed (exit ${result.status}): ${result.stderr?.toString() ?? "<no stderr>"}`,
      );
    }
    const dictBytes = readFileSync(dictPath);
    writeSync(
      2,
      `[trainDict] trained ${dictBytes.byteLength} byte dict in ${((Date.now() - t0) / 1000).toFixed(1)}s\n`,
    );
    return new Uint8Array(
      dictBytes.buffer,
      dictBytes.byteOffset,
      dictBytes.byteLength,
    );
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

// Empirically pick the best arc_coords dict size by trying a few
// candidate sizes (plus no-dict) on a small block sample and
// projecting to full file size. Returns the trained dict for the
// best candidate, or undefined if no-dict wins.
//
// The cost is one `zstd --train` per candidate (~10s each) and
// one sample compress per candidate (~1s). For ~3-4 candidates
// that's ~40s of overhead at encode time — pays for itself the
// first time it picks "no dict" on a region where dict was
// hurting, or the right intermediate size when the auto-default
// would have been wrong.
function autoPickArcCoordDict(
  arcCoordsBytes: Uint8Array,
  blockSpecs: ReadonlyArray<{
    readonly startUncompressed: number;
    readonly endUncompressed: number;
  }>,
  maxDictBytes: number,
): Uint8Array | undefined {
  // Need a meaningful number of blocks for both training and the
  // sample test. Skip auto-tuning (and dict entirely) for tiny
  // regions — the savings are too small to justify the overhead.
  const MIN_SAMPLES_FOR_AUTO = 100;
  if (blockSpecs.length < MIN_SAMPLES_FOR_AUTO) return undefined;

  // Pick a small evaluation sample — every Nth block by stride so
  // we cover early/late blocks (different tiers). 100 is enough
  // for a stable ratio estimate without making the sample-compress
  // step expensive.
  const SAMPLE_BLOCKS = 100;
  const stride = Math.max(1, Math.floor(blockSpecs.length / SAMPLE_BLOCKS));
  const sampleSlices: Uint8Array[] = [];
  for (let i = 0; i < blockSpecs.length; i += stride) {
    const spec = blockSpecs[i];
    sampleSlices.push(
      arcCoordsBytes.subarray(spec.startUncompressed, spec.endUncompressed),
    );
    if (sampleSlices.length >= SAMPLE_BLOCKS) break;
  }
  const projectionFactor = blockSpecs.length / sampleSlices.length;
  writeSync(
    2,
    `[autoDict] tuning on ${sampleSlices.length} sample blocks (${blockSpecs.length} total, ` +
      `projection×${projectionFactor.toFixed(1)}, max dict=${(maxDictBytes / 1024).toFixed(0)} KiB)\n`,
  );

  // Helper: compress all sample slices with optional dict, return total bytes.
  const compressSampleTotal = (dict: Uint8Array | undefined): number => {
    let total = 0;
    for (const slice of sampleSlices) {
      const opts =
        dict !== undefined
          ? ({
              dictionary: dict,
              params: { [zlibConstants.ZSTD_c_compressionLevel]: 19 },
            } as unknown as Parameters<typeof zstdCompressSync>[1])
          : ({
              params: { [zlibConstants.ZSTD_c_compressionLevel]: 19 },
            } as unknown as Parameters<typeof zstdCompressSync>[1]);
      total += zstdCompressSync(slice, opts).byteLength;
    }
    return total;
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
  const noDictSampleTotal = compressSampleTotal(undefined);
  const noDictProjected = noDictSampleTotal * projectionFactor;
  candidates.push({
    label: "no-dict",
    dict: undefined,
    projectedFileBytes: noDictProjected,
  });
  writeSync(
    2,
    `[autoDict]   no-dict: sample=${(noDictSampleTotal / 1024).toFixed(0)} KiB, ` +
      `projected=${(noDictProjected / 1024 / 1024).toFixed(2)} MiB\n`,
  );

  // Trained-dict candidates.
  for (const size of candidateSizes) {
    const dict = trainArcCoordsDict(arcCoordsBytes, blockSpecs, size);
    if (dict === undefined) continue;
    const sampleTotal = compressSampleTotal(dict);
    const projected = sampleTotal * projectionFactor + dict.byteLength;
    candidates.push({
      label: `${(dict.byteLength / 1024).toFixed(0)} KiB dict`,
      dict,
      projectedFileBytes: projected,
    });
    writeSync(
      2,
      `[autoDict]   ${(dict.byteLength / 1024).toFixed(0)} KiB dict: ` +
        `sample=${(sampleTotal / 1024).toFixed(0)} KiB, ` +
        `projected=${(projected / 1024 / 1024).toFixed(2)} MiB ` +
        `(incl ${(dict.byteLength / 1024).toFixed(0)} KiB dict)\n`,
    );
  }

  // Pick the smallest projected file size.
  let best = candidates[0];
  for (const c of candidates) {
    if (c.projectedFileBytes < best.projectedFileBytes) best = c;
  }
  writeSync(2, `[autoDict] picked: ${best.label}\n`);
  return best.dict;
}

// Classify a section name into a family for the dict what-if analysis.
// Heuristic — we group by the patterns most likely to share byte
// sequences (string suffixes, numeric value distributions, CSR
// structural data, etc.).
// Compress arc_coords as a sequence of independently-decodable
// zstd frames sharing a raw-content dictionary. Returns the pieces
// the caller emits as separate sections, plus the META fields the
// reader needs to map logical arc-byte ranges back to physical
// block fetches.
//
// Block boundaries are aligned to whole arcs — no arc spans two
// blocks, so a fetchArcs call that wants `[arcOffsets[i],
// arcOffsets[i+1])` decompresses exactly one block. Last block may
// be shorter than targetBlockSize.
//
// The dict is the first `dictBytes` bytes of arc_coords used as a
// raw content dict (not a `zstd --train` output). Reader fetches
// it once at open and passes to every per-block decompress.
interface BlockCompressedArcCoords {
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
}

async function blockCompressArcCoords(
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
  interface BlockSpec {
    readonly startUncompressed: number;
    readonly endUncompressed: number;
  }
  const blockSpecs: BlockSpec[] = [];
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

  // Auto-pick the dict size empirically rather than guess. We
  // train candidate dicts at several sizes, compress a sample of
  // blocks with each (and with no dict at all), and pick the
  // option with the lowest projected `compressed_total + dict_size`.
  // Empirical pick > heuristic pick: dict effectiveness varies
  // enormously by geometry (coord deltas for organic boundaries may
  // not dictionary-compress meaningfully; grid-shaped ones might).
  // The user-supplied options.dictBytes is treated as the upper
  // bound — we'll consider sizes up to that.
  const dictBytes = autoPickArcCoordDict(
    arcCoordsBytes,
    blockSpecs,
    options.dictBytes,
  );

  // Compress blocks through the libuv thread pool with bounded
  // concurrency — `Promise.all` over the full block list would
  // materialize one Promise + closure per block, which is fine for
  // tens of blocks but catastrophic at scale (large topologies have
  // 1700+ blocks and we've also seen this called with 50K-100K
  // blocks for tighter targets — million-Promise allocations
  // OOM-killed the host on a 62 GB box). The worker pool keeps queue depth
  // bounded to BLOCK_COMPRESS_CONCURRENCY, so memory stays linear
  // in pool size, not block count.
  const compressedBlocks = new Array<Buffer>(blockSpecs.length);
  // TEMP: progress + memory instrumentation while we're chasing a
  // suspected off-heap leak in the dict-aware async compress path.
  // Uses fs.writeSync(2, ...) so output is unbuffered — Node's
  // process.stderr.write is block-buffered through the docker pipe
  // and the lines get swallowed if the process is killed before
  // the buffer flushes. Remove once #20 is stable.
  const PROGRESS_EVERY = 100;
  const t0 = Date.now();
  let completed = 0;
  const log = (msg: string): void => {
    writeSync(2, msg + "\n");
  };
  log(
    `[blockCompress] start: ${blockSpecs.length} blocks, ` +
      `dict=${dictBytes !== undefined ? `${(dictBytes.byteLength / 1024).toFixed(0)} KiB` : "(none)"}, ` +
      `arc_coords=${(totalUncompressed / 1024 / 1024).toFixed(0)} MiB, ` +
      `concurrency=${BLOCK_COMPRESS_CONCURRENCY}`,
  );
  const memSnapshot = (): string => {
    const m = process.memoryUsage();
    return (
      `rss=${(m.rss / 1024 / 1024).toFixed(0)}M ` +
      `heap=${(m.heapUsed / 1024 / 1024).toFixed(0)}/${(m.heapTotal / 1024 / 1024).toFixed(0)}M ` +
      `external=${(m.external / 1024 / 1024).toFixed(0)}M ` +
      `arrayBuffers=${(m.arrayBuffers / 1024 / 1024).toFixed(0)}M`
    );
  };
  log(`[blockCompress] before-loop ${memSnapshot()}`);
  await runConcurrentBlockCompress(blockSpecs, async (spec, i) => {
    const blockBytes = arcCoordsBytes.subarray(
      spec.startUncompressed,
      spec.endUncompressed,
    );
    // Node's zstd accepts `dictionary` at runtime even though
    // ZstdOptions doesn't expose it — same cast pattern as before.
    // dictBytes can be undefined for small regions; in that case
    // we just compress without a dict.
    const opts = (dictBytes !== undefined
      ? {
          dictionary: dictBytes,
          params: { [zlibConstants.ZSTD_c_compressionLevel]: 19 },
        }
      : {
          params: { [zlibConstants.ZSTD_c_compressionLevel]: 19 },
        }) as unknown as Parameters<typeof zstdCompressAsync>[1];
    compressedBlocks[i] = await zstdCompressAsync(blockBytes, opts);
    completed++;
    if (completed % PROGRESS_EVERY === 0 || completed === blockSpecs.length) {
      log(
        `[blockCompress] ${completed}/${blockSpecs.length} ` +
          `t=${((Date.now() - t0) / 1000).toFixed(1)}s ${memSnapshot()}`,
      );
    }
  });
  log(`[blockCompress] done in ${((Date.now() - t0) / 1000).toFixed(1)}s`);

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
  };
}

// Bounded-concurrency worker pool. Each of N workers pulls the
// next item index from a shared cursor, awaits `fn(item, index)`,
// then pulls the next. Total in-flight Promises = N, regardless of
// items.length — so large item lists don't allocate one Promise
// per item up front the way Promise.all(arr.map(...)) does.
async function runConcurrentBlockCompress<T>(
  items: ReadonlyArray<T>,
  fn: (item: T, index: number) => Promise<void>,
): Promise<void> {
  let cursor = 0;
  const worker = async (): Promise<void> => {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;
      await fn(items[i], i);
    }
  };
  const n = Math.min(BLOCK_COMPRESS_CONCURRENCY, items.length);
  const workers: Promise<void>[] = new Array(n);
  for (let i = 0; i < n; i++) workers[i] = worker();
  await Promise.all(workers);
}

const UINT8_MAX = 255;
const UINT16_MAX = 65535;
const INT8_MIN = -128;
const INT8_MAX = 127;
const INT16_MIN = -32768;
const INT16_MAX = 32767;

// --- Public API ---

// Progress event emitted while encoding. Writers wire this up to
// log lines or a spinner so long zstd-L19 encode passes show
// activity instead of looking hung.
export interface EncodeProgress {
  // Stage label: "tier-order" | "build-arcs" | "build-properties" |
  // "compress-group". Free-form so we can add new stages without
  // breaking listeners.
  readonly stage: string;
  // 1-based group index when stage = "compress-group", undefined
  // for stages that aren't enumerable.
  readonly index?: number;
  // Total group count when known. Group total isn't determined
  // until grouping has run, so it's only set on compress-group
  // events.
  readonly total?: number;
  // Human-readable detail (section name, byte sizes, etc).
  readonly detail?: string;
}

// Per-section size + codec event emitted once for every logical
// section after its physical region has been compressed (or laid down
// raw). Bench harnesses use this to attribute file size to individual
// sections without parsing the container back.
export interface SectionEncodedEvent {
  readonly name: string;
  readonly dtype: DType;
  readonly uncompressedBytes: number;
  // For single-member regions: exact compressed bytes for this
  // section. For multi-member compression groups: this section's
  // proportional share of the group's compressed bytes
  // (uncompressedBytes / groupUncompressedBytes * groupCompressedBytes).
  // Approximate — entropy is shared across the group — but the per-
  // section shares sum to the group total exactly.
  readonly compressedBytes: number;
  readonly codec: Compression | undefined;
  // True when this section was bundled with siblings into one zstd
  // frame; compressedBytes is then a proportional split, not exact.
  readonly grouped: boolean;
  // Wall-clock for the region's compress call. Same value reported
  // for every member of a group. Zero for intentionally-uncompressed
  // sections (arc_coords, arc_coord_blocks, arc_coords_dict).
  readonly encodeMs: number;
}

// Coarse-grained phase timing. Bench harnesses use this to break
// down encode wall-clock without touching the encoder internals.
export interface PhaseTimingEvent {
  // Free-form so new phases can be added without a breaking change.
  // Current values: "build-csr" | "tier-order" | "build-arcs" |
  // "block-compress" | "build-properties" | "assemble".
  readonly stage: string;
  readonly elapsedMs: number;
}

export interface EncodeOptions {
  // Codec for compressible sections. Defaults to "zst" — best ratio
  // and works on every browser (native or via fzstd fallback). Pick
  // "br" to skip the fzstd polyfill entirely (relies on native
  // DecompressionStream("brotli") — Firefox today, Chrome rolling).
  readonly compression?: Compression;
  // Optional progress callback. Called synchronously between stages;
  // keep the listener cheap. No-op when omitted.
  readonly onProgress?: (event: EncodeProgress) => void;
  // When true, arc_coords is split into independently-decodable
  // zstd-compressed blocks (against a shared raw-content dict) with
  // a sidecar block table. Required to compress arc_coords (which
  // is otherwise stored raw so per-arc range fetches keep working).
  // Reader must understand the arcCoordsBlocks META field — older
  // readers will treat the section as a raw blob and produce
  // garbage on the first arc lookup, so this opt is gated.
  readonly blockCompressArcCoords?: boolean;
  // Tuning knobs for blockCompressArcCoords. arcCoordDictBytes
  // defaults to a size auto-picked from the input (~1.5% of
  // uncompressed arc_coords, clamped to [64 KiB, 1 MiB]). Override
  // for benchmarks or specialized regions.
  readonly arcCoordBlockBytes?: number;
  readonly arcCoordDictBytes?: number;
  // App-specific sections to front-load alongside the encoder's
  // structural front-loaded set (arc_coords_dict, arc_coord_blocks,
  // arc_offsets, per-layer CSR triples, arc_coords). Sections in
  // this list are physically placed at the front of the data area so
  // the client's front-prefetch GET captures them in one shot. Names
  // must match exactly (including any layer prefix, e.g.
  // "cities/name"). Typical use: index/lookup strings or properties
  // that the app needs immediately after open.
  readonly frontLoadedSectionNames?: ReadonlyArray<string>;
  // Bench instrumentation hooks. Both fire synchronously and should
  // stay cheap. No-op when omitted; no behavior change.
  readonly onSectionEncoded?: (event: SectionEncodedEvent) => void;
  readonly onPhaseTiming?: (event: PhaseTimingEvent) => void;
}

export async function encodeContainer(
  input: Topology,
  opts: EncodeOptions = {},
): Promise<Buffer> {
  // Topojson signals delta encoding via `transform`. Without it, arc
  // coordinates are absolute — arc_coords is much larger and compresses
  // far worse since neighboring points no longer share high-order bits.
  if (input.transform === undefined || input.transform === null) {
    // eslint-disable-next-line no-console
    console.warn(
      "ctopo: input topology has no `transform`; arcs are absolute (not " +
        "delta-encoded). Run topojson.quantize() before encoding for a much " +
        "smaller, better-compressing arc_coords section.",
    );
  }

  const sections: BuiltSection[] = [];
  const layerSummaries: { name: string; numGeometries: number }[] = [];

  const numArcs = input.arcs.length;
  const layerNames = Object.keys(input.objects);

  // Build per-layer CSR triples up front. Two reasons:
  //   1. The tier-based arc reordering (below) needs every layer's
  //      arc_refs to classify each arc.
  //   2. We're going to remap arc ids in those CSRs after computing
  //      the new order, so we hold them in memory rather than emit
  //      first and rewrite later.
  const layerCsrs: {
    name: string;
    geometries: ReadonlyArray<GeometryObject<Properties>>;
    csr: ReturnType<typeof buildLayerCSR>;
  }[] = [];
  const tBuildCsr = performance.now();
  for (const layerName of layerNames) {
    const collection = input.objects[layerName];
    if (collection.type !== "GeometryCollection") {
      throw new Error(
        `ctopo: layer "${layerName}" is ${collection.type}; only GeometryCollection layers are supported`,
      );
    }
    layerSummaries.push({
      name: layerName,
      numGeometries: collection.geometries.length,
    });
    layerCsrs.push({
      name: layerName,
      geometries: collection.geometries,
      csr: buildLayerCSR(collection.geometries),
    });
  }
  opts.onPhaseTiming?.({
    stage: "build-csr",
    elapsedMs: performance.now() - tBuildCsr,
  });

  // Internal layout optimization: arcs in arc_coords are reordered so
  // arcs that bound geometries at the same layer land in a contiguous
  // file region, smallest layer first. Callers that union geometries
  // at a coarser layer then read a short prefix of arc_coords instead
  // of scattering Range GETs across the whole section. Arc semantics
  // and the public client API are unchanged — only the byte order in
  // arc_coords + arc id assignment moves.
  const tTierOrder = performance.now();
  const { arcOrder, tiers } = computeTierBasedArcOrder(
    numArcs,
    layerCsrs,
    input.arcs,
  );
  const newIdOf = invertPermutation(arcOrder);
  for (const { csr } of layerCsrs) remapArcRefs(csr.arcRefs, newIdOf);
  opts.onPhaseTiming?.({
    stage: "tier-order",
    elapsedMs: performance.now() - tTierOrder,
  });

  const tBuildArcs = performance.now();
  const { arcOffsetsBytes, arcCoordsBytes, bytesPerPoint } = buildArcSections(
    input,
    arcOrder,
  );
  opts.onPhaseTiming?.({
    stage: "build-arcs",
    elapsedMs: performance.now() - tBuildArcs,
  });
  // arc_offsets is cumulative-monotone — first-order deltas are tiny
  // (each entry is the previous arc's coord byte length, which fits
  // comfortably in a few bits per arc). Compressors gain ~10-30% on
  // the deltas vs the absolute layout, where neighboring u32 values
  // share most high-order bits and look like noise to LZ.
  // arc_offsets is structurally front-loaded — every fetchArcs call
  // needs random access into it, so the open path warms it eagerly.
  sections.push({
    name: "arc_offsets",
    dtype: "u32",
    bytes: deltaEncodeU32(arcOffsetsBytes),
    delta: true,
    frontLoad: true,
  });

  // Tier byte offsets are computed from the *original* (pre-delta)
  // arcOffsetsBytes — they're absolute byte positions into arc_coords,
  // independent of the on-disk encoding of arc_offsets.
  const tierByteOffsets = computeTierByteOffsets(
    arcOrder,
    tiers,
    arcOffsetsBytes,
  );

  // Block-compressed arc_coords — when enabled, arc_coords ships as
  // a sequence of independently-decodable zstd frames sharing a
  // raw-content dict. The dict + block table are tiny and required
  // before any per-block decompress; mark both front-loaded.
  // META.arcCoordsBlocks tells the reader how to map logical arc
  // byte ranges (arc_offsets) to physical block ranges.
  let arcCoordsBlocksMeta: ContainerMeta["arcCoordsBlocks"] = undefined;
  let arcCoordsSectionBytes = arcCoordsBytes;
  if (opts.blockCompressArcCoords === true) {
    const tBlockCompress = performance.now();
    const block = await blockCompressArcCoords(
      arcCoordsBytes,
      arcOffsetsBytes,
      {
        targetBlockSize:
          opts.arcCoordBlockBytes ?? DEFAULT_ARC_COORD_BLOCK_BYTES,
        dictBytes:
          opts.arcCoordDictBytes ??
          autoArcCoordDictBytes(arcCoordsBytes.byteLength),
      },
    );
    opts.onPhaseTiming?.({
      stage: "block-compress",
      elapsedMs: performance.now() - tBlockCompress,
    });
    if (block.dictBytes !== undefined) {
      sections.push({
        name: "arc_coords_dict",
        dtype: "blob",
        bytes: block.dictBytes,
        frontLoad: true,
      });
    }
    sections.push({
      name: "arc_coord_blocks",
      dtype: "u32",
      bytes: block.blockTableBytes,
      frontLoad: true,
    });
    arcCoordsSectionBytes = block.compressedBytes;
    arcCoordsBlocksMeta = {
      dictSection:
        block.dictBytes !== undefined ? "arc_coords_dict" : undefined,
      blockTableSection: "arc_coord_blocks",
      blockCount: block.blockCount,
      targetBlockSize: block.targetBlockSize,
    };
  }

  // arc_coords lives in the front-loaded region too. Tier ordering
  // (top-layer perimeter → top-layer interior → lower layers) puts
  // skeleton arcs at the front of arc_coords, so a generously-sized
  // front-prefetch GET catches them; the rest of arc_coords falls
  // outside the prefetch window and is fetched per-block on demand.
  const arcCoordsSection: BuiltSection = {
    name: "arc_coords",
    dtype: "blob",
    bytes: arcCoordsSectionBytes,
    frontLoad: true,
  };

  // Per-layer CSR triples (poly_offsets, ring_offsets, arc_refs) are
  // structural — every layerGeometry / mergeArcs call walks them in
  // full. Mark them front-loaded so the open path catches them in
  // the prefetch.
  //
  // App-specific property/strings sections are added next; the
  // caller picks which (if any) of those should ride along via
  // EncodeOptions.frontLoadedSectionNames. Anything not in that
  // list stays lazy and only fetches on demand.
  const frontLoadedExtraSet = new Set<string>(
    opts.frontLoadedSectionNames ?? [],
  );
  const perLayerProperties: { name: string; props: BuiltSection[] }[] = [];
  const tBuildProps = performance.now();
  for (const { name: layerName, geometries, csr } of layerCsrs) {
    // poly_offsets / ring_offsets are also cumulative-monotone u32
    // — their deltas are small constants (1-4 typically: rings per
    // polygon, arcs per ring) which the compressor packs tightly.
    sections.push({
      name: `${layerName}/poly_offsets`,
      dtype: "u32",
      bytes: deltaEncodeU32(typedArrayBytes(csr.polyOffsets)),
      delta: true,
      frontLoad: true,
    });
    sections.push({
      name: `${layerName}/ring_offsets`,
      dtype: "u32",
      bytes: deltaEncodeU32(typedArrayBytes(csr.ringOffsets)),
      delta: true,
      frontLoad: true,
    });
    sections.push({
      name: `${layerName}/arc_refs`,
      dtype: "i32",
      bytes: typedArrayBytes(csr.arcRefs),
      frontLoad: true,
    });
    perLayerProperties.push({
      name: layerName,
      props: collectPropertySections(layerName, geometries),
    });
  }
  for (const { props } of perLayerProperties) {
    for (const section of props) {
      sections.push(
        frontLoadedExtraSet.has(section.name)
          ? { ...section, frontLoad: true }
          : section,
      );
    }
  }
  opts.onPhaseTiming?.({
    stage: "build-properties",
    elapsedMs: performance.now() - tBuildProps,
  });

  sections.push(arcCoordsSection);

  // We deliberately don't apply shared dicts to non-arc_coords
  // sections. A dict's value is priming the LZ window's cold start
  // on each frame; arc_coords benefits because it ships as many
  // ~64 KiB blocks (random arc fetches need block boundaries), so
  // each frame starts cold and the dict pays for itself on every
  // block. Properties / strings / CSR triples are each fetched
  // whole — one frame per section, frame size much larger than
  // zstd's LZ window — so the LZ pass self-primes within the
  // section and a dict adds overhead without recovering it. We
  // confirmed this empirically: every candidate dict
  // size 32-256 KiB came out *worse* than no-dict at the section
  // level, contradicting an earlier per-chunk projection that
  // measured cold-start frame compression rather than realistic
  // whole-section compression.

  return assembleContainer({
    sections,
    bytesPerPoint,
    transform: input.transform ?? null,
    bbox: deriveBbox(input),
    layers: layerSummaries,
    metadata: undefined,
    tierByteOffsets,
    arcCoordsBlocks: arcCoordsBlocksMeta,
    compression: opts.compression ?? "zst",
    onProgress: opts.onProgress,
    onSectionEncoded: opts.onSectionEncoded,
    onPhaseTiming: opts.onPhaseTiming,
  });
}

export async function writeContainer(
  outPath: string,
  input: Topology,
  opts: EncodeOptions = {},
): Promise<void> {
  const buf = await encodeContainer(input, opts);
  writeSync(
    2,
    `[writeContainer] writing ${(buf.byteLength / 1024 / 1024).toFixed(0)} MiB to ${outPath}\n`,
  );
  writeFileSync(outPath, buf);
  writeSync(2, `[writeContainer] write complete\n`);
}

// Re-encode helper — opens an existing container, swaps named property /
// string sections, writes a new file. Other sections (arcs, CSR triples,
// untouched properties) are passed through byte-for-byte. Section order is
// preserved; offsets recompute when override sizes differ.
export async function rewriteContainer(
  inPath: string,
  outPath: string,
  overrides: ReadonlyArray<PropertyOverride>,
  opts: { frontLoadedSectionNames?: ReadonlyArray<string> } = {},
): Promise<void> {
  const original = readFileSync(inPath);
  const { meta, sections } = parseContainer(original);
  const overridesByName = new Map(overrides.map((o) => [o.name, o]));
  const extraFrontLoad = new Set(opts.frontLoadedSectionNames ?? []);

  // Decompress each compression group once so group members get their
  // own uncompressed slice. Keyed by `${offset}:${length}` — the same
  // physical range every member in the group shares.
  const decompressedGroups = new Map<string, Uint8Array>();
  function decompressGroup(entry: {
    offset: number;
    length: number;
    compression?: Compression;
  }): Uint8Array {
    const key = `${entry.offset}:${entry.length}`;
    const cached = decompressedGroups.get(key);
    if (cached !== undefined) return cached;
    const compressed = original.subarray(
      entry.offset,
      entry.offset + entry.length,
    );
    const decompressed =
      entry.compression === "br"
        ? brotliDecompressSync(compressed)
        : zstdDecompressSync(compressed);
    const bytes = new Uint8Array(
      decompressed.buffer,
      decompressed.byteOffset,
      decompressed.byteLength,
    );
    decompressedGroups.set(key, bytes);
    return bytes;
  }

  const builtSections: BuiltSection[] = sections.map((entry) => {
    const frontLoad =
      isStructurallyFrontLoaded(entry.name) || extraFrontLoad.has(entry.name);
    const override = overridesByName.get(entry.name);
    if (override === undefined) {
      // Group member: decompress the group, extract this member's slice,
      // and pass uncompressed bytes so assembleContainer re-groups and
      // re-compresses correctly. Delta-encoded sections are passed with
      // their delta flag preserved — the bytes are already delta-encoded
      // and assembleContainer threads the flag into META as-is.
      if (entry.groupOffset !== undefined && entry.groupLength !== undefined) {
        const group = decompressGroup(entry);
        const memberBytes = group.subarray(
          entry.groupOffset,
          entry.groupOffset + entry.groupLength,
        );
        // Copy so the member's buffer is independent of the group slab.
        const bytes = new Uint8Array(memberBytes.byteLength);
        bytes.set(memberBytes);
        return {
          name: entry.name,
          dtype: entry.dtype,
          bytes,
          delta: entry.delta,
          frontLoad,
        };
      }
      // Non-group section: pass through the on-disk bytes verbatim,
      // preserving any existing compression flag — re-compressing
      // already-compressed bytes would just round-trip more slowly.
      const bytes = original.subarray(
        entry.offset,
        entry.offset + entry.length,
      );
      return {
        name: entry.name,
        dtype: entry.dtype,
        bytes,
        compression: entry.compression,
        delta: entry.delta,
        frontLoad,
      };
    }
    // Replaced sections rebuild from raw values; assembleContainer
    // re-compresses if `shouldCompressSection` says so.
    return { ...buildPropertySection(entry.name, override.data), frontLoad };
  });

  const layers = meta.layers.map((l) => ({
    name: l.name,
    numGeometries: l.numGeometries,
  }));

  const buf = await assembleContainer({
    sections: builtSections,
    bytesPerPoint: meta.bytesPerPoint,
    transform:
      meta.transform === null
        ? null
        : {
            scale: [meta.transform.scale[0], meta.transform.scale[1]],
            translate: [
              meta.transform.translate[0],
              meta.transform.translate[1],
            ],
          },
    bbox: [meta.bbox[0], meta.bbox[1], meta.bbox[2], meta.bbox[3]],
    layers,
    metadata: meta.metadata,
    // Preserve tier byte offsets from the source — only arc reordering
    // changes them, and rewriteContainer never reorders.
    tierByteOffsets: meta.tierByteOffsets?.slice(),
    // Preserve block-compressed arc_coords metadata so the client
    // knows to use the block decoder after a rewrite.
    arcCoordsBlocks: meta.arcCoordsBlocks,
    // Replaced sections re-compress with this default. Pass-through
    // sections preserve their existing codec (their bytes are already
    // compressed; assembleContainer just forwards them).
    compression: "zst",
  });
  writeFileSync(outPath, buf);
}

// Section names the encoder always front-loads — these are needed at
// open time for any consumer of the format. Used by rewriteContainer
// to preserve the front-load ordering across rewrites.
function isStructurallyFrontLoaded(name: string): boolean {
  if (name === "arc_coords_dict") return true;
  if (name === "arc_coord_blocks") return true;
  if (name === "arc_offsets") return true;
  if (name === "arc_coords") return true;
  if (name.endsWith("/poly_offsets")) return true;
  if (name.endsWith("/ring_offsets")) return true;
  if (name.endsWith("/arc_refs")) return true;
  return false;
}

// --- Arc sections ---

function buildArcSections(
  topology: Topology,
  arcOrder: Uint32Array,
): {
  arcOffsetsBytes: Uint8Array;
  arcCoordsBytes: Uint8Array;
  bytesPerPoint: 8 | 16;
} {
  const numArcs = topology.arcs.length;
  const isQuantized = topology.transform !== undefined;
  const bytesPerPoint: 8 | 16 = isQuantized ? 8 : 16;

  let totalPoints = 0;
  for (const arc of topology.arcs) totalPoints += arc.length;

  const offsets = new Uint32Array(numArcs + 1);

  if (!isQuantized) {
    // Float64 absolutes — no transform to delta + varint, store raw.
    const coords = Buffer.alloc(totalPoints * 16);
    let byteOffset = 0;
    for (let newId = 0; newId < numArcs; newId++) {
      offsets[newId] = byteOffset;
      const arc = topology.arcs[arcOrder[newId]];
      for (const point of arc) {
        coords.writeDoubleLE(point[0], byteOffset);
        coords.writeDoubleLE(point[1], byteOffset + 8);
        byteOffset += 16;
      }
    }
    offsets[numArcs] = byteOffset;
    return {
      arcOffsetsBytes: typedArrayBytes(offsets),
      arcCoordsBytes: coords,
      bytesPerPoint,
    };
  }

  // Quantized path: each point is a (dx, dy) int32 delta. Encode as
  // varint(zigzag(dx)) varint(zigzag(dy)) — typical deltas (< 256
  // quantization units) shrink from 8 bytes to 2-3 bytes per point.
  // Allocate the worst case (10 bytes per point) up front, slice at end.
  const scratch = new Uint8Array(totalPoints * 2 * VARINT_MAX_BYTES);
  let byteOffset = 0;
  for (let newId = 0; newId < numArcs; newId++) {
    offsets[newId] = byteOffset;
    const arc = topology.arcs[arcOrder[newId]];
    for (const point of arc) {
      byteOffset = writeVarintZigzag(point[0], scratch, byteOffset);
      byteOffset = writeVarintZigzag(point[1], scratch, byteOffset);
    }
  }
  offsets[numArcs] = byteOffset;
  const coords = scratch.subarray(0, byteOffset);

  return {
    arcOffsetsBytes: typedArrayBytes(offsets),
    arcCoordsBytes: coords,
    bytesPerPoint,
  };
}

// --- Tier-based arc reordering ---

// Classifies each arc by the smallest layer that bounds it, then
// returns a permutation that packs arcs together by tier — lowest
// tier (smallest bounding layer) first. Within a tier, arcs are
// ordered by walking the tier's owning layer's geometries in
// Hilbert-key order over their representative points (first point
// of the geometry's first arc, in absolute coordinates). Hilbert
// is a space-filling curve: geometries with adjacent Hilbert keys
// are spatially close, *globally consistent* — unlike a BFS-from-
// seed walk which only guarantees local connectivity. This lays
// out arcs so that any contiguous subset of geometries (e.g. a
// user-selected region) has its arcs clustered into a few file
// regions instead of scattered.
// FlatGeobuf uses the same Hilbert layout for its spatial index;
// this is the same idea applied to arc storage order.
//
// Tier numbering for an N-layer topology (layers ordered base → top):
//   0    : arc appears in the top layer's arc_refs exactly once
//          (only one top-layer geometry on either side of it)
//   1    : arc appears twice in the top layer's arc_refs (between
//          two top-layer geometries)
//   2..N : arc first appears in a non-top layer, walking top → base.
//          Arcs only present at the base layer get the highest tier.
//
// Single-layer topologies skip reordering entirely.
interface LayerCsrForOrder {
  readonly polyOffsets: Uint32Array;
  readonly ringOffsets: Uint32Array;
  readonly arcRefs: Int32Array;
}

// Topology arc shape — list of points; for quantized topologies
// each point is a delta from the previous (with [0] being the delta
// from the origin = the absolute first coordinate). The geojson
// Position type is variable-length so we read by index.
type TopoArcs = ReadonlyArray<ReadonlyArray<ReadonlyArray<number>>>;

interface TierBasedArcOrder {
  readonly arcOrder: Uint32Array;
  // Tier label per arc, indexed by *original* arc id. 0..numTiers-1, where
  // 0 = top-layer outside-edge, 1 = top-layer interior, 2..N = lower
  // layers (highest tier = base layer). For single-layer topologies this
  // is empty (no tier reordering is performed).
  readonly tiers: Uint8Array;
}

function computeTierBasedArcOrder(
  numArcs: number,
  layerCsrs: ReadonlyArray<{ name: string; csr: LayerCsrForOrder }>,
  arcs: TopoArcs,
): TierBasedArcOrder {
  if (layerCsrs.length <= 1) {
    return { arcOrder: identityOrder(numArcs), tiers: new Uint8Array(0) };
  }

  const tiers = new Uint8Array(numArcs).fill(0xff);
  const numLayers = layerCsrs.length;

  // Top layer: count occurrences to distinguish outside-edge (count=1)
  // from interior-boundary (count=2).
  const topRefs = layerCsrs[numLayers - 1].csr.arcRefs;
  const topCount = new Uint8Array(numArcs);
  for (let i = 0; i < topRefs.length; i++) {
    const id = topRefs[i] >= 0 ? topRefs[i] : ~topRefs[i];
    if (topCount[id] < 2) topCount[id]++;
  }
  for (let id = 0; id < numArcs; id++) {
    if (topCount[id] === 1) tiers[id] = 0;
    else if (topCount[id] >= 2) tiers[id] = 1;
  }

  // Walk remaining layers top → base. Each layer assigns a tier to
  // arcs that haven't been classified yet.
  for (let level = numLayers - 2; level >= 0; level--) {
    const tier = numLayers - level; // 2 for second-from-top, etc.
    const refs = layerCsrs[level].csr.arcRefs;
    for (let i = 0; i < refs.length; i++) {
      const id = refs[i] >= 0 ? refs[i] : ~refs[i];
      if (tiers[id] === 0xff) tiers[id] = tier;
    }
  }

  // Visit indices: each layer top → base contributes a Hilbert
  // pass. Within a pass, sort the layer's geometries by Hilbert
  // key over a representative point, then walk in that order
  // emitting any unclaimed arcs. Geometries with neighboring
  // Hilbert keys are spatially close, so their arcs end up
  // adjacent in the file too.
  const visit = new Int32Array(numArcs).fill(-1);
  let cursor = 0;
  for (let level = numLayers - 1; level >= 0; level--) {
    cursor = hilbertAssignVisit(layerCsrs[level].csr, arcs, visit, cursor);
  }

  // Sort by tier first, then by visit order within the tier.
  // Array.sort with a comparator is fine for ~millions of arcs
  // (~hundreds of ms one-time at producer side).
  const order = identityOrder(numArcs);
  const sorted = Array.from(order).sort(
    (a, b) => tiers[a] - tiers[b] || visit[a] - visit[b],
  );
  return { arcOrder: new Uint32Array(sorted), tiers };
}

// Hilbert-curve visit-order pass for one layer. For each geometry
// take its first arc's first absolute point as a representative
// position, normalize against the layer's bbox onto a 16-bit grid,
// compute the Hilbert key, sort geometries by key, and walk in
// order emitting any not-yet-claimed arcs. Returns the updated
// cursor so subsequent layer passes continue numbering from where
// this one left off.
function hilbertAssignVisit(
  csr: LayerCsrForOrder,
  arcs: TopoArcs,
  visit: Int32Array,
  startCursor: number,
): number {
  const numGeoms = csr.polyOffsets.length - 1;
  if (numGeoms === 0) return startCursor;

  // Representative point per geometry (absolute coords). For
  // quantized topologies the topology's first arc point is the
  // delta from the origin = the absolute coordinate, so this works
  // uniformly without checking for `transform`.
  const repX = new Float64Array(numGeoms);
  const repY = new Float64Array(numGeoms);
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  for (let g = 0; g < numGeoms; g++) {
    const ringStart = csr.polyOffsets[g];
    if (csr.polyOffsets[g + 1] === ringStart) {
      // No rings — geometry has no arcs. Place at origin so the
      // sort is stable; rare in practice.
      repX[g] = 0;
      repY[g] = 0;
      continue;
    }
    const arcStart = csr.ringOffsets[ringStart];
    const signed = csr.arcRefs[arcStart];
    const arcId = signed >= 0 ? signed : ~signed;
    const arc = arcs[arcId];
    if (arc.length === 0) {
      repX[g] = 0;
      repY[g] = 0;
      continue;
    }
    const x = arc[0][0];
    const y = arc[0][1];
    repX[g] = x;
    repY[g] = y;
    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }

  // 16-bit Hilbert grid → keys fit in u32 (2^32 = 16-bit × 16-bit).
  const HSIZE = 1 << 16;
  const xRange = maxX - minX || 1;
  const yRange = maxY - minY || 1;
  const keys = new Uint32Array(numGeoms);
  for (let g = 0; g < numGeoms; g++) {
    let nx = Math.floor(((repX[g] - minX) / xRange) * (HSIZE - 1));
    let ny = Math.floor(((repY[g] - minY) / yRange) * (HSIZE - 1));
    if (nx < 0) nx = 0;
    if (ny < 0) ny = 0;
    if (nx >= HSIZE) nx = HSIZE - 1;
    if (ny >= HSIZE) ny = HSIZE - 1;
    keys[g] = hilbertXYToKey(nx, ny, HSIZE);
  }

  // Sort geometry indices by key. Stable sort isn't required for
  // correctness — ties just get an arbitrary but deterministic
  // order — but we use a tiebreaker on geometry index so the
  // result is identical across runs for fixtures with collisions.
  const sorted = new Array<number>(numGeoms);
  for (let g = 0; g < numGeoms; g++) sorted[g] = g;
  sorted.sort((a, b) => keys[a] - keys[b] || a - b);

  let cursor = startCursor;
  for (const g of sorted) {
    const ringEnd = csr.polyOffsets[g + 1];
    for (let r = csr.polyOffsets[g]; r < ringEnd; r++) {
      const arcEnd = csr.ringOffsets[r + 1];
      for (let a = csr.ringOffsets[r]; a < arcEnd; a++) {
        const id = csr.arcRefs[a] >= 0 ? csr.arcRefs[a] : ~csr.arcRefs[a];
        if (visit[id] === -1) visit[id] = cursor++;
      }
    }
  }
  return cursor;
}

// Map (x, y) on an n × n integer grid (n a power of 2) to its
// Hilbert distance — the position along the order-log2(n) Hilbert
// space-filling curve. Standard iterative implementation; n=2^16
// here so the result fits in u32.
function hilbertXYToKey(x: number, y: number, n: number): number {
  let rx;
  let ry;
  let d = 0;
  for (let s = n >> 1; s > 0; s >>= 1) {
    rx = (x & s) > 0 ? 1 : 0;
    ry = (y & s) > 0 ? 1 : 0;
    d += s * s * ((3 * rx) ^ ry);
    // Rotate quadrant.
    if (ry === 0) {
      if (rx === 1) {
        x = s - 1 - x;
        y = s - 1 - y;
      }
      const t = x;
      x = y;
      y = t;
    }
  }

  return d;
}

function identityOrder(n: number): Uint32Array {
  const out = new Uint32Array(n);
  for (let i = 0; i < n; i++) out[i] = i;
  return out;
}

function invertPermutation(perm: Uint32Array): Int32Array {
  const inv = new Int32Array(perm.length);
  for (let i = 0; i < perm.length; i++) inv[perm[i]] = i;
  return inv;
}

// Walk arcOrder in tier order and emit the byte position in arc_coords
// where each tier ends. Arcs are sorted tier-ascending, so tier `t`
// occupies new ids [start_t, end_t) and ends at byte arcOffsets[end_t].
// Returns a flat number[] (one entry per tier present, including 0 for
// tiers with no arcs); empty when no tier reordering happened.
function computeTierByteOffsets(
  arcOrder: Uint32Array,
  tiers: Uint8Array,
  arcOffsetsBytes: Uint8Array,
): number[] {
  if (tiers.length === 0) return [];
  const arcOffsets = new Uint32Array(
    arcOffsetsBytes.buffer,
    arcOffsetsBytes.byteOffset,
    arcOffsetsBytes.byteLength / 4,
  );
  // Highest tier present caps the array length. Lower-tier-only files
  // (single-layer) are handled by the empty-tiers shortcut above.
  let maxTier = 0;
  for (let i = 0; i < tiers.length; i++)
    if (tiers[i] > maxTier) maxTier = tiers[i];
  const ends = new Array<number>(maxTier + 1).fill(0);
  // Walk new ids; whenever the tier label of arcOrder[i] changes, record
  // i (= number of arcs at or below the previous tier) for that tier.
  let prevTier = -1;
  for (let i = 0; i < arcOrder.length; i++) {
    const t = tiers[arcOrder[i]];
    if (t !== prevTier && prevTier !== -1) {
      // All tiers between prevTier and t exclusive get the same end
      // position (no arcs at those tiers).
      for (let k = prevTier; k < t; k++) ends[k] = arcOffsets[i];
    }
    prevTier = t;
  }
  // Final tier ends at the section's total byte length.
  if (prevTier !== -1) ends[prevTier] = arcOffsets[arcOrder.length];
  return ends;
}

// Rewrite signed arc refs in place from old → new ids (sign preserved).
function remapArcRefs(arcRefs: Int32Array, newIdOf: Int32Array): void {
  for (let i = 0; i < arcRefs.length; i++) {
    const old = arcRefs[i];
    if (old >= 0) arcRefs[i] = newIdOf[old];
    else arcRefs[i] = ~newIdOf[~old];
  }
}

// --- Per-layer CSR triple ---

function buildLayerCSR(geometries: ReadonlyArray<GeometryObject<Properties>>): {
  polyOffsets: Uint32Array;
  ringOffsets: Uint32Array;
  arcRefs: Int32Array;
} {
  // 3-level CSR encoded as geometry → ring → arc. The polygon level is
  // collapsed: a MultiPolygon's rings are flattened across its polygons.
  // Reconstruction (e.g. for `feature()`) groups rings into polygons by
  // area containment — largest ring is the exterior, smaller nested
  // rings are holes.
  let totalRings = 0;
  let totalArcRefs = 0;
  for (const geom of geometries) {
    for (const poly of polygonsOf(geom)) {
      totalRings += poly.length;
      for (const ring of poly) totalArcRefs += ring.length;
    }
  }

  const polyOffsets = new Uint32Array(geometries.length + 1);
  const ringOffsets = new Uint32Array(totalRings + 1);
  const arcRefs = new Int32Array(totalArcRefs);

  let ringCursor = 0;
  let arcCursor = 0;

  for (let g = 0; g < geometries.length; g++) {
    polyOffsets[g] = ringCursor;
    for (const poly of polygonsOf(geometries[g])) {
      for (const ring of poly) {
        ringOffsets[ringCursor++] = arcCursor;
        for (const arcId of ring) arcRefs[arcCursor++] = arcId;
      }
    }
  }
  polyOffsets[geometries.length] = ringCursor;
  ringOffsets[totalRings] = arcCursor;

  return { polyOffsets, ringOffsets, arcRefs };
}

// Treat a single Polygon as a 1-polygon MultiPolygon so the layout is uniform.
// Other geometry types (Point, LineString, etc.) carry no polygon arcs and
// are encoded as zero-polygon entries — they still consume a slot in
// poly_offsets so geometry indices align with the layer's geometries array.
function polygonsOf(
  geom: GeometryObject<Properties>,
): ReadonlyArray<ReadonlyArray<ReadonlyArray<number>>> {
  if (geom.type === "Polygon") return [geom.arcs];
  if (geom.type === "MultiPolygon") return geom.arcs;
  return [];
}

// --- Property sections ---

// Walk every geometry's `properties` for a layer. Each leaf path becomes one
// section named `{layer}/{path}`. Nested objects flatten with `/`. dtype is
// auto-detected by scanning the value column. Mixed types within a column
// throw. Missing values default to 0 (numeric) or "" (strings).
function collectPropertySections(
  layerName: string,
  geometries: ReadonlyArray<GeometryObject<Properties>>,
): BuiltSection[] {
  // Collect column values keyed by leaf path, in stable insertion order.
  const columns = new Map<string, unknown[]>();
  for (let i = 0; i < geometries.length; i++) {
    const props = geometries[i].properties;
    if (props !== undefined && props !== null) {
      walkProperties("", props, i, columns, geometries.length);
    }
  }

  // Pad short columns to numGeometries length (properties absent on later
  // geometries default to undefined → 0/"" downstream).
  const sections: BuiltSection[] = [];
  for (const [path, values] of columns) {
    while (values.length < geometries.length) values.push(undefined);
    sections.push(buildPropertySection(`${layerName}/${path}`, values));
  }
  return sections;
}

function walkProperties(
  prefix: string,
  obj: Record<string, unknown>,
  rowIndex: number,
  columns: Map<string, unknown[]>,
  totalRows: number,
): void {
  for (const key of Object.keys(obj)) {
    const value = obj[key];
    const path = prefix === "" ? key : `${prefix}/${key}`;
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
      walkProperties(
        path,
        value as Record<string, unknown>,
        rowIndex,
        columns,
        totalRows,
      );
      continue;
    }
    if (Array.isArray(value)) {
      throw new Error(`ctopo: array properties unsupported (path ${path})`);
    }
    let column = columns.get(path);
    if (column === undefined) {
      // Backfill any rows that didn't see this key earlier so column index
      // stays aligned with geometry index.
      column = new Array(rowIndex).fill(undefined);
      columns.set(path, column);
    }
    column.push(value);
  }
}

// Build one section from a column of mixed-type values. Internal helper used
// by both initial encode and rewriteContainer overrides.
function buildPropertySection(
  name: string,
  values: ArrayLike<unknown>,
): BuiltSection {
  const length = values.length;

  // Classify the column: numeric, string, or all-empty.
  let sawNumber = false;
  let sawString = false;
  let allInts = true;
  let minVal = Infinity;
  let maxVal = -Infinity;
  for (let i = 0; i < length; i++) {
    const v = values[i];
    if (v === undefined || v === null) continue;
    if (typeof v === "boolean") {
      sawNumber = true;
      const n = v ? 1 : 0;
      if (n < minVal) minVal = n;
      if (n > maxVal) maxVal = n;
      continue;
    }
    if (typeof v === "number") {
      sawNumber = true;
      if (!Number.isInteger(v)) allInts = false;
      if (v < minVal) minVal = v;
      if (v > maxVal) maxVal = v;
      continue;
    }
    if (typeof v === "string") {
      sawString = true;
      continue;
    }
    throw new Error(
      `ctopo: unsupported property type at ${name}[${i}]: ${typeof v}`,
    );
  }
  if (sawNumber && sawString) {
    throw new Error(`ctopo: mixed numeric/string values at property ${name}`);
  }

  if (sawString || !sawNumber) {
    return packStrings(name, values, length);
  }
  return packNumeric(name, values, length, allInts, minVal, maxVal);
}

function packNumeric(
  name: string,
  values: ArrayLike<unknown>,
  length: number,
  allInts: boolean,
  minVal: number,
  maxVal: number,
): BuiltSection {
  const dtype: DType = !allInts
    ? "f64"
    : minVal >= 0
      ? maxVal <= UINT8_MAX
        ? "u8"
        : maxVal <= UINT16_MAX
          ? "u16"
          : "u32"
      : minVal >= INT8_MIN && maxVal <= INT8_MAX
        ? "i8"
        : minVal >= INT16_MIN && maxVal <= INT16_MAX
          ? "i16"
          : "i32";

  const ctor = typedArrayCtor(dtype);
  const arr = new ctor(length);
  for (let i = 0; i < length; i++) {
    const v = values[i];
    if (v === undefined || v === null) {
      arr[i] = 0;
    } else if (typeof v === "boolean") {
      arr[i] = v ? 1 : 0;
    } else if (typeof v === "number") {
      arr[i] = v;
    } else {
      throw new Error(
        `ctopo: non-numeric value slipped past dtype detection at ${name}[${i}]`,
      );
    }
  }
  return { name, dtype, bytes: typedArrayBytes(arr) };
}

function packStrings(
  name: string,
  values: ArrayLike<unknown>,
  length: number,
): BuiltSection {
  // Encode: count u32 + 4 pad + offsets u32[count+1] + utf8 bytes.
  const encoder = new TextEncoder();
  const encoded: Uint8Array[] = new Array(length);
  let totalUtf8 = 0;
  for (let i = 0; i < length; i++) {
    const v = values[i];
    let s: string;
    if (v === undefined || v === null) {
      s = "";
    } else if (typeof v === "string") {
      s = v;
    } else if (typeof v === "number" || typeof v === "boolean") {
      s = String(v);
    } else {
      throw new Error(
        `ctopo: non-stringable value at strings property ${name}[${i}]`,
      );
    }
    const bytes = encoder.encode(s);
    encoded[i] = bytes;
    totalUtf8 += bytes.length;
  }

  const offsetsStart = 8;
  const utf8Start = offsetsStart + 4 * (length + 1);
  const total = utf8Start + totalUtf8;

  const out = new Uint8Array(total);
  const view = new DataView(out.buffer, out.byteOffset, out.byteLength);
  view.setUint32(0, length, true);
  // bytes 4..8 left as zero pad

  let cursor = 0;
  for (let i = 0; i < length; i++) {
    view.setUint32(offsetsStart + i * 4, cursor, true);
    out.set(encoded[i], utf8Start + cursor);
    cursor += encoded[i].length;
  }
  view.setUint32(offsetsStart + length * 4, cursor, true);

  return { name, dtype: "strings", bytes: out };
}

// --- Container assembly ---

interface AssembleInput {
  readonly sections: ReadonlyArray<BuiltSection>;
  readonly bytesPerPoint: 8 | 16;
  readonly transform: {
    readonly scale: readonly [number, number];
    readonly translate: readonly [number, number];
  } | null;
  readonly bbox: readonly [number, number, number, number];
  readonly layers: ReadonlyArray<{ name: string; numGeometries: number }>;
  readonly metadata: string | undefined;
  // Optional. Empty for single-layer topologies where no tier reordering
  // was applied. See computeTierByteOffsets.
  readonly tierByteOffsets?: ReadonlyArray<number>;
  readonly arcCoordsBlocks?: ContainerMeta["arcCoordsBlocks"];
  readonly compression: Compression;
  readonly onProgress?: (event: EncodeProgress) => void;
  readonly onSectionEncoded?: (event: SectionEncodedEvent) => void;
  readonly onPhaseTiming?: (event: PhaseTimingEvent) => void;
}

// One physical region in the file. Single-section regions correspond
// 1:1 with a single logical section. Group regions hold the
// compressed concatenation of multiple consecutive compressible
// sections — every member's section-table entry shares the same
// on-disk offset/length, and members record their slice within the
// decompressed group via groupOffset/groupLength.
interface PhysicalRegion {
  readonly bytes: Uint8Array;
  readonly memberIndices: number[];
  readonly memberOffsets: number[]; // offset within decompressed group bytes
  readonly memberLengths: number[]; // uncompressed length of each member
  readonly compression: Compression | undefined;
  // Pre-compress total bytes (concatenated member bytes + alignment
  // padding). Same as `bytes.byteLength` for uncompressed regions;
  // larger than `bytes.byteLength` for compressed ones. Threaded into
  // META so the wasm decoder can size its output buffer when the zstd
  // frame header lacks FCS (Node's async zstdCompress doesn't write it).
  readonly uncompressedLength: number;
}

async function assembleContainer(input: AssembleInput): Promise<Buffer> {
  const {
    sections: declaredSections,
    bytesPerPoint,
    transform,
    bbox,
    layers,
    metadata,
    tierByteOffsets,
    arcCoordsBlocks,
    compression: codec,
    onProgress,
    onSectionEncoded,
    onPhaseTiming,
  } = input;
  const emit = onProgress ?? noopProgress;
  const tAssemble = performance.now();

  // arc_offsets (numArcs is derived from its uncompressed size) must
  // be locatable up front. Sections that already declare a codec
  // pass through unchanged (rewriteContainer's pass-through path).
  const arcOffsetsRaw = declaredSections.find((s) => s.name === "arc_offsets");
  if (arcOffsetsRaw === undefined) {
    throw new Error("ctopo: encoder requires an arc_offsets section");
  }
  const numArcs = arcOffsetsRaw.bytes.byteLength / 4 - 1;

  // Reorder: front-loaded sections first (in their declared relative
  // order), then everything else. Section table META keeps pointers
  // by name + physical offset, so the on-disk reorder is invisible
  // to consumers — only matters for the open-path prefetch.
  const rawSections: ReadonlyArray<BuiltSection> = [
    ...declaredSections.filter((s) => s.frontLoad === true),
    ...declaredSections.filter((s) => s.frontLoad !== true),
  ];
  // Index of the first non-front-load section (or rawSections.length
  // if everything is front-loaded). We force a group flush at this
  // boundary so a compression group never spans front-load + lazy —
  // that would force the front prefetch to drag lazy bytes along.
  const lazyStartIdx = rawSections.findIndex((s) => s.frontLoad !== true);

  // Walk sections in order and pack consecutive compressible ones
  // into groups. Anything that doesn't compress (arc_offsets,
  // arc_coords, sections with a pre-existing codec) gets its own
  // 1-member region. Groups close when adding the next section
  // would push them past MAX_COMPRESSION_GROUP_BYTES (uncompressed)
  // or the next section is ineligible.
  // Phase A: walk sections, building region slots in section-table
  // order. Compressible groups are queued as compress tasks (with
  // their concatenated uncompressed bytes); their region slots stay
  // null until phase B fills them in. Pre-compressed and
  // intentionally-uncompressed sections fill their slots inline.
  const regionSlots: (PhysicalRegion | null)[] = [];
  // Wall-clock for each region's compress call, indexed by slotIndex.
  // 0 for intentionally-uncompressed and pass-through self-regions
  // (no compress runs there). Filled in by the group-compress worker
  // for all other regions. Used to drive onSectionEncoded.encodeMs.
  const regionEncodeMs: number[] = [];
  interface CompressTask {
    readonly slotIndex: number;
    // Set to null after the worker copies these into libzstd, so
    // V8 can reclaim the input Buffer as soon as compress finishes.
    // For large topologies, ~150+ group inputs averaging ~3 MiB each
    // → ~470 MiB we'd otherwise hold for the duration of the phase.
    bytes: Uint8Array | null;
    readonly memberIndices: number[];
    readonly memberOffsets: number[];
    readonly memberLengths: number[];
    readonly uncompressedLength: number;
    readonly detail: string;
  }
  const compressTasks: CompressTask[] = [];
  // In-progress group accumulator. null when not currently building a group.
  let pending: {
    parts: Uint8Array[];
    memberIndices: number[];
    memberOffsets: number[];
    memberLengths: number[];
    uncompressedSize: number;
  } | null = null;
  function flushPending(): void {
    if (pending === null || pending.parts.length === 0) return;
    const detail =
      pending.parts.length === 1
        ? `${rawSections[pending.memberIndices[0]].name} ` +
          `(${(pending.uncompressedSize / 1024).toFixed(0)} KiB)`
        : `${pending.parts.length} sections ` +
          `(${(pending.uncompressedSize / 1024 / 1024).toFixed(1)} MiB)`;
    const slotIndex = regionSlots.length;
    regionSlots.push(null); // placeholder filled in phase B
    regionEncodeMs.push(0); // filled in by group-compress worker
    if (pending.parts.length === 1) {
      compressTasks.push({
        slotIndex,
        bytes: pending.parts[0],
        memberIndices: pending.memberIndices,
        memberOffsets: [-1], // sentinel: "no group"
        memberLengths: pending.memberLengths,
        uncompressedLength: pending.parts[0].byteLength,
        detail,
      });
    } else {
      // Concatenate group members into a single contiguous buffer
      // for one compress call (one zstd frame, ~30 ms setup
      // amortized across all members).
      const total = pending.uncompressedSize;
      const concat = new Uint8Array(total);
      let cursor = 0;
      for (const part of pending.parts) {
        concat.set(part, cursor);
        cursor += part.byteLength;
      }
      compressTasks.push({
        slotIndex,
        bytes: concat,
        memberIndices: pending.memberIndices.slice(),
        memberOffsets: pending.memberOffsets.slice(),
        memberLengths: pending.memberLengths.slice(),
        uncompressedLength: total,
        detail,
      });
    }
    pending = null;
  }
  for (let i = 0; i < rawSections.length; i++) {
    const section = rawSections[i];
    // Boundary flush: never bundle a front-loaded section with a
    // lazy one, otherwise the front prefetch would drag the lazy
    // members into its window.
    if (i === lazyStartIdx) flushPending();
    // Pre-compressed sections (rewriteContainer pass-through) and
    // intentionally-uncompressed sections each go in their own
    // single-member region with no group metadata.
    if (
      section.compression !== undefined ||
      !shouldCompressSection(section.name, section.dtype)
    ) {
      flushPending();
      regionSlots.push({
        bytes: section.bytes,
        memberIndices: [i],
        memberOffsets: [-1],
        memberLengths: [section.bytes.byteLength],
        compression: section.compression,
        uncompressedLength: section.bytes.byteLength,
      });
      regionEncodeMs.push(0);
      continue;
    }
    // Eligible for grouping. Open or extend the pending group.
    const sectionLen = section.bytes.byteLength;
    // Member-within-group byte offsets must be aligned to the
    // member's dtype element size — Uint16Array, Uint32Array, and
    // Float64Array views over a sliced buffer require 2/4/8-byte
    // byteOffset alignment respectively. Pad before adding the
    // member if the running uncompressed size isn't already aligned.
    const align = dtypeAlignment(section.dtype);
    let alignedSize = pending !== null ? pending.uncompressedSize : 0;
    const aligned = Math.ceil(alignedSize / align) * align;
    const aligningPadding = aligned - alignedSize;
    if (
      pending !== null &&
      pending.uncompressedSize + aligningPadding + sectionLen >
        MAX_COMPRESSION_GROUP_BYTES
    ) {
      flushPending();
      alignedSize = 0;
    }
    if (pending === null) {
      pending = {
        parts: [],
        memberIndices: [],
        memberOffsets: [],
        memberLengths: [],
        uncompressedSize: 0,
      };
      // Fresh group starts at offset 0 → already aligned.
    } else if (aligningPadding > 0) {
      pending.parts.push(new Uint8Array(aligningPadding));
      pending.uncompressedSize += aligningPadding;
    }
    pending.parts.push(section.bytes);
    pending.memberIndices.push(i);
    pending.memberOffsets.push(pending.uncompressedSize);
    pending.memberLengths.push(sectionLen);
    pending.uncompressedSize += sectionLen;
  }
  flushPending();

  // Phase B: compress all queued groups in parallel. Each task runs
  // on the libuv thread pool (default size 4); for typical regions
  // with ~20-30 groups this drops total compression wall-clock to
  // ~1/N of sequential where N = pool size. Group bytes get freed
  // by GC as soon as the task completes — peak memory equals the
  // largest compressed output plus the in-flight uncompressed
  // chunks.
  // TEMP: same unbuffered log helper as blockCompressArcCoords.
  const dlog = (msg: string): void => {
    writeSync(2, msg + "\n");
  };
  const dmem = (): string => {
    const m = process.memoryUsage();
    return (
      `rss=${(m.rss / 1024 / 1024).toFixed(0)}M ` +
      `heap=${(m.heapUsed / 1024 / 1024).toFixed(0)}/${(m.heapTotal / 1024 / 1024).toFixed(0)}M ` +
      `external=${(m.external / 1024 / 1024).toFixed(0)}M ` +
      `arrayBuffers=${(m.arrayBuffers / 1024 / 1024).toFixed(0)}M`
    );
  };
  dlog(
    `[groupCompress] start: ${compressTasks.length} groups (codec=${codec}) ${dmem()}`,
  );
  const tg0 = Date.now();
  let completedCount = 0;
  // Bounded worker pool — same rationale as runConcurrentBlockCompress:
  // Promise.all over the full list materializes one Promise + closure
  // per task up front (each closure pinning ~3 MiB of input bytes).
  // The worker pool keeps live closures + input
  // refs equal to the in-flight count, not the total task count.
  let groupCursor = 0;
  const groupWorker = async (): Promise<void> => {
    while (true) {
      const idx = groupCursor++;
      if (idx >= compressTasks.length) return;
      const task = compressTasks[idx];
      if (task.bytes === null) throw new Error("group task already consumed");
      const tCompress = performance.now();
      const compressed = await compressBytesAsync(task.bytes, codec);
      regionEncodeMs[task.slotIndex] = performance.now() - tCompress;
      regionSlots[task.slotIndex] = {
        bytes: compressed,
        memberIndices: task.memberIndices,
        memberOffsets: task.memberOffsets,
        memberLengths: task.memberLengths,
        compression: codec,
        uncompressedLength: task.uncompressedLength,
      };
      // Drop the input buffer reference now that compression is
      // done — V8 GC can reclaim it before the next task starts.
      task.bytes = null;
      completedCount++;
      dlog(
        `[groupCompress] ${completedCount}/${compressTasks.length} ${task.detail} ` +
          `t=${((Date.now() - tg0) / 1000).toFixed(1)}s ${dmem()}`,
      );
      emit({
        stage: "compress-group",
        index: completedCount,
        total: compressTasks.length,
        detail: task.detail,
      });
    }
  };
  const groupWorkers: Promise<void>[] = [];
  const groupConcurrency = Math.min(
    GROUP_COMPRESS_CONCURRENCY,
    compressTasks.length,
  );
  for (let i = 0; i < groupConcurrency; i++) groupWorkers.push(groupWorker());
  await Promise.all(groupWorkers);
  dlog(
    `[groupCompress] done in ${((Date.now() - tg0) / 1000).toFixed(1)}s ${dmem()}`,
  );
  // All slots are now filled — narrow the type.
  const regions = regionSlots as PhysicalRegion[];

  // Emit per-section size events. Self-regions (intentionally
  // uncompressed or pass-through) report compressedBytes equal to
  // their on-disk bytes. Multi-member compression groups split the
  // group's compressed bytes proportionally by uncompressed size,
  // so per-section shares sum to the group total exactly.
  if (onSectionEncoded !== undefined) {
    for (let r = 0; r < regions.length; r++) {
      const region = regions[r];
      const grouped = region.memberIndices.length > 1;
      const physicalBytes = region.bytes.byteLength;
      const uncompressedTotal = region.uncompressedLength;
      const encodeMs = regionEncodeMs[r];
      let allocated = 0;
      for (let m = 0; m < region.memberIndices.length; m++) {
        const sec = rawSections[region.memberIndices[m]];
        const memberLen = region.memberLengths[m];
        let memberCompressed: number;
        if (!grouped) {
          memberCompressed = physicalBytes;
        } else if (m === region.memberIndices.length - 1) {
          // Last member absorbs rounding so the per-member shares
          // sum to physicalBytes exactly.
          memberCompressed = physicalBytes - allocated;
        } else {
          memberCompressed = Math.floor(
            (memberLen / uncompressedTotal) * physicalBytes,
          );
          allocated += memberCompressed;
        }
        onSectionEncoded({
          name: sec.name,
          dtype: sec.dtype,
          uncompressedBytes: memberLen,
          compressedBytes: memberCompressed,
          codec: region.compression,
          grouped,
          encodeMs,
        });
      }
    }
  }

  // Build the section-table-compatible "logical entries" array,
  // one per rawSection, sharing offsets within their region.
  const sectionPlacement: Array<{
    physicalOffset: number;
    physicalLength: number;
    groupOffset: number | undefined;
    groupLength: number | undefined;
    compression: Compression | undefined;
    uncompressedRegionLength: number | undefined;
  }> = new Array(rawSections.length);

  // New layout: HEADER (16 B) → data sections → FOOTER (section
  // table + META) → 8 B trailing footer length. dataStart is fixed
  // at HEADER_SIZE; META + section table get written into the
  // footer at the very end of the file.
  const dataStart = HEADER_SIZE;
  const regionLayout: Array<{ offset: number; length: number }> = [];
  let regionCursor = 0;
  for (const region of regions) {
    regionLayout.push({
      offset: regionCursor,
      length: region.bytes.byteLength,
    });
    regionCursor = alignUp(
      regionCursor + region.bytes.byteLength,
      SECTION_ALIGNMENT,
    );
  }
  // regionCursor is now totalDataSize relative to dataStart.

  // Fill in placements with absolute file offsets right away —
  // dataStart is fixed (= HEADER_SIZE) since META no longer lives
  // between header and data.
  for (let r = 0; r < regions.length; r++) {
    const region = regions[r];
    const absOffset = dataStart + regionLayout[r].offset;
    const length = regionLayout[r].length;
    for (let m = 0; m < region.memberIndices.length; m++) {
      const sectionIdx = region.memberIndices[m];
      const groupOffset = region.memberOffsets[m];
      sectionPlacement[sectionIdx] = {
        physicalOffset: absOffset,
        physicalLength: length,
        groupOffset: groupOffset === -1 ? undefined : groupOffset,
        groupLength: groupOffset === -1 ? undefined : region.memberLengths[m],
        compression: region.compression,
        // Only useful (and emitted) for compressed regions; for
        // uncompressed sections the wire length already equals the
        // uncompressed length.
        uncompressedRegionLength:
          region.compression === undefined
            ? undefined
            : region.uncompressedLength,
      };
    }
  }

  // Build META.
  const meta: ContainerMeta = {
    version: 1,
    numArcs,
    bytesPerPoint,
    transform,
    bbox,
    metadata,
    tierByteOffsets:
      tierByteOffsets !== undefined && tierByteOffsets.length > 0
        ? tierByteOffsets
        : undefined,
    arcCoordsBlocks,
    layers,
    sections: rawSections.map((s, i) => {
      const p = sectionPlacement[i];
      const entry: ContainerMeta["sections"][number] = {
        name: s.name,
        type: s.dtype,
      };
      if (p.compression !== undefined)
        (entry as { compression: Compression }).compression = p.compression;
      if (p.groupOffset !== undefined) {
        (entry as { groupOffset: number }).groupOffset = p.groupOffset;
        (entry as { groupLength: number }).groupLength = p.groupLength!;
      }
      if (s.delta === true) (entry as { delta: boolean }).delta = true;
      if (p.uncompressedRegionLength !== undefined) {
        (
          entry as { uncompressedRegionLength: number }
        ).uncompressedRegionLength = p.uncompressedRegionLength;
      }
      return entry;
    }),
  };
  const metaJson = JSON.stringify(meta);
  const metaBytes = Buffer.from(metaJson, "utf-8");

  // Footer layout: section_count u32 + meta_length u32 + section_table
  // (32 B per row) + meta_json. The footer's start byte is then
  // recorded in the final 8 bytes of the file as a u64 footer_length
  // so a suffix-range GET can locate it without knowing file size.
  const sectionCount = rawSections.length;
  const footerTableStart = FOOTER_PREFIX_SIZE;
  const footerTableEnd = footerTableStart + sectionCount * SECTION_ENTRY_SIZE;
  const footerMetaStart = footerTableEnd;
  const footerMetaEnd = footerMetaStart + metaBytes.byteLength;
  const footerLength = footerMetaEnd;

  const dataAreaSize = regionCursor;
  const totalSize =
    dataStart + dataAreaSize + footerLength + FOOTER_TRAILER_SIZE;
  dlog(
    `[assemble] allocating final Buffer: ${(totalSize / 1024 / 1024).toFixed(0)} MiB ${dmem()}`,
  );
  const out = Buffer.alloc(totalSize);
  dlog(`[assemble] allocated, writing header+sections ${dmem()}`);
  const view = new DataView(out.buffer, out.byteOffset, out.byteLength);

  // Header (16 B): magic + version + 8 reserved bytes (already zero).
  view.setUint32(OFFSET_MAGIC, MAGIC, true);
  view.setUint32(OFFSET_VERSION, VERSION, true);

  // Region data immediately after the header.
  for (let r = 0; r < regions.length; r++) {
    out.set(regions[r].bytes, dataStart + regionLayout[r].offset);
  }

  // Footer at the end of the file: section_count, meta_length,
  // section table, meta_json.
  const footerStart = dataStart + dataAreaSize;
  view.setUint32(footerStart + OFFSET_FOOTER_SECTION_COUNT, sectionCount, true);
  view.setUint32(
    footerStart + OFFSET_FOOTER_META_LENGTH,
    metaBytes.byteLength,
    true,
  );
  for (let i = 0; i < sectionCount; i++) {
    const entryStart = footerStart + footerTableStart + i * SECTION_ENTRY_SIZE;
    writeShortName(out, entryStart, rawSections[i].name);
    writeBigUint64(
      view,
      entryStart + SECTION_NAME_SIZE,
      sectionPlacement[i].physicalOffset,
    );
    writeBigUint64(
      view,
      entryStart + SECTION_NAME_SIZE + 8,
      sectionPlacement[i].physicalLength,
    );
  }
  metaBytes.copy(out, footerStart + footerMetaStart);

  // Trailing 8 B: footer length so suffix-range readers can locate
  // the footer start without a HEAD round trip.
  writeBigUint64(view, totalSize - FOOTER_TRAILER_SIZE, footerLength);

  dlog(`[assemble] regions copied; returning ${dmem()}`);

  onPhaseTiming?.({
    stage: "assemble",
    elapsedMs: performance.now() - tAssemble,
  });

  return out;
}

// --- Helpers ---

function typedArrayCtor(
  dtype: DType,
): new (
  length: number,
) =>
  | Int8Array
  | Uint8Array
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float64Array {
  switch (dtype) {
    case "i8":
      return Int8Array;
    case "u8":
      return Uint8Array;
    case "i16":
      return Int16Array;
    case "u16":
      return Uint16Array;
    case "i32":
      return Int32Array;
    case "u32":
      return Uint32Array;
    case "f64":
      return Float64Array;
    default:
      throw new Error(
        `ctopo: typedArrayCtor called with non-numeric dtype ${dtype}`,
      );
  }
}

function typedArrayBytes(arr: ArrayBufferView): Uint8Array {
  return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
}

function writeShortName(out: Buffer, offset: number, name: string): void {
  const encoder = new TextEncoder();
  const encoded = encoder.encode(name);
  const writable = encoded.subarray(
    0,
    Math.min(encoded.length, SECTION_NAME_SIZE),
  );
  out.set(writable, offset);
  // Remaining bytes are already zero from Buffer.alloc.
}

function writeBigUint64(view: DataView, offset: number, value: number): void {
  if (value < 0 || !Number.isFinite(value)) {
    throw new Error(`ctopo: invalid u64 value ${value}`);
  }
  view.setBigUint64(offset, BigInt(value), true);
}

function deriveBbox(topology: Topology): [number, number, number, number] {
  // topojson-specification carries `bbox` as an optional field. Fall back to
  // a degenerate envelope if absent — callers can always override later.
  const b = topology.bbox;
  if (b !== undefined && b.length >= 4) {
    return [b[0], b[1], b[2], b[3]];
  }
  return [0, 0, 0, 0];
}

// `void` marker on rewriteContainer return is intentional — TypeScript
// otherwise infers `Buffer` from the writeFileSync chain.
export type { BuiltSection };
