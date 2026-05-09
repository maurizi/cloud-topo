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

// --- Types ---

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
}

// --- Constants ---

// Default block size for block-compressed arc_coords.
// Aligned to whole arcs at emit time.
export const DEFAULT_ARC_COORD_BLOCK_BYTES = 8 * 1024;

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
  const dictBytes = await autoPickArcCoordDict(
    arcCoordsBytes,
    blockSpecs,
    options.dictBytes,
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
      const spec = blockSpecs[i];
      const blockBytes = arcCoordsBytes.subarray(
        spec.startUncompressed,
        spec.endUncompressed,
      );
      compressedBlocks[i] = await compressFrame(s, blockBytes);
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
  };
}

// --- Internal: dictionary training ---

// Train a zstd shared dictionary from a sample of arc-coord blocks via
// the `zstd --train` CLI. No JS package exposes ZDICT_trainFromBuffer,
// so the shell-out is currently the simplest path.
//
// Trained dicts pack the most-frequent multi-byte sequences from the
// samples into a compact (10s of KB) dictionary.
//
// Throws if `zstd` isn't on PATH.
function trainArcCoordsDict(
  arcCoordsBytes: Uint8Array,
  blockSpecs: ReadonlyArray<{
    readonly startUncompressed: number;
    readonly endUncompressed: number;
  }>,
  targetDictBytes: number,
): Uint8Array | undefined {
  // zstd --train needs ≥~100 samples for stable output. For tiny
  // inputs (test fixtures, very small topologies) we skip the dict
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
    stderrLog(
      `[trainDict] training on ${sampleIndices.length} samples ` +
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
        `ctopo: blockCompressArcCoords requires the \`zstd\` CLI on PATH for dict training (got: ${result.error.message})`,
      );
    }
    if (result.status !== 0) {
      throw new Error(
        `ctopo: zstd --train failed (exit ${result.status}): ${result.stderr?.toString() ?? "<no stderr>"}`,
      );
    }
    const dictBuf = readFileSync(dictPath);
    stderrLog(
      `[trainDict] trained ${dictBuf.byteLength} byte dict in ${((Date.now() - t0) / 1000).toFixed(1)}s`,
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

// Empirically pick the best arc_coords dict size by trying a few
// candidate sizes (plus no-dict) on a small block sample and
// projecting to full file size. Returns the trained dict for the
// best candidate, or undefined if no-dict wins.
//
// The cost is one `zstd --train` per candidate (~10s each) and
// one sample compress per candidate (~1s). For ~3-4 candidates
// that's ~40s of overhead at encode time.
async function autoPickArcCoordDict(
  arcCoordsBytes: Uint8Array,
  blockSpecs: ReadonlyArray<{
    readonly startUncompressed: number;
    readonly endUncompressed: number;
  }>,
  maxDictBytes: number,
): Promise<Uint8Array | undefined> {
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
  stderrLog(
    `[autoDict] tuning on ${sampleSlices.length} sample blocks (${blockSpecs.length} total, ` +
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
    `[autoDict]   no-dict: sample=${(noDictSampleTotal / 1024).toFixed(0)} KiB, ` +
      `projected=${(noDictProjected / 1024 / 1024).toFixed(2)} MiB`,
  );

  // Trained-dict candidates.
  for (const size of candidateSizes) {
    const dict = trainArcCoordsDict(arcCoordsBytes, blockSpecs, size);
    if (dict === undefined) continue;
    const sampleTotal = await compressSampleTotal(dict);
    const projected = sampleTotal * projectionFactor + dict.byteLength;
    candidates.push({
      label: `${(dict.byteLength / 1024).toFixed(0)} KiB dict`,
      dict,
      projectedFileBytes: projected,
    });
    stderrLog(
      `[autoDict]   ${(dict.byteLength / 1024).toFixed(0)} KiB dict: ` +
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
  stderrLog(`[autoDict] picked: ${chosen.label} — ${rationale}`);
  return chosen.dict;
}
