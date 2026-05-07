// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Encoder for the .ctopo container format. Node-only — uses Buffer + fs.
 *
 * Walks a TopoJSON Topology and emits one container with:
 *   - global arcs (arc_offsets + arc_coords)
 *   - per-layer geometry CSR triples (poly_offsets, ring_offsets, arc_refs)
 *     plus an optional sparse multi_poly_breaks section per layer
 *   - per-layer per-property sections (auto-detected dtype)
 *   - per-layer string sections for non-numeric properties
 *
 * Section order (writer enforces): arc_offsets, then per-layer CSR triples
 * grouped by layer, then property/strings sections in stable order, then
 * arc_coords last (largest, only Range-fetched in slices).
 */

import { writeFileSync, readFileSync } from "fs";
import {
  brotliCompress,
  brotliDecompressSync,
  zstdCompress,
  zstdDecompressSync,
  constants as zlibConstants,
} from "zlib";
import { promisify } from "util";

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
  VERSION,
  alignUp,
} from "./format";
import { parseContainer } from "./reader";
import {
  type Compression,
  type ContainerMeta,
  type DType,
  type PropertyOverride,
} from "./types";
import {
  buildArcSections,
  computeArcOrder,
  invertPermutation,
  remapArcRefs,
  typedArrayBytes,
  type SpatialSort,
} from "./encode-arcs";
export type { SpatialSort } from "./encode-arcs";
import {
  buildLayerCSR,
  buildPropertySection,
  collectPropertySections,
} from "./encode-properties";
import {
  blockCompressArcCoords,
  autoArcCoordDictBytes,
  DEFAULT_ARC_COORD_BLOCK_BYTES,
} from "./encode-compress";
import { runWithConcurrency, stderrLog, memSnapshot } from "./util";

// --- Types ---

export interface BuiltSection {
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

// Progress event emitted while encoding. Writers wire this up to
// log lines or a spinner so long zstd-L19 encode passes show
// activity instead of looking hung.
export interface EncodeProgress {
  // Stage label: "arc-order" | "build-arcs" | "build-properties" |
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
  // Current values: "build-csr" | "arc-order" | "build-arcs" |
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
  // Target uncompressed size (in bytes) for each independently-
  // decodable zstd block within arc_coords. Smaller blocks reduce
  // wasted bandwidth on selective per-arc fetches; larger blocks
  // improve compression ratio. Aligned to whole-arc boundaries at
  // emit time. Defaults to DEFAULT_ARC_COORD_BLOCK_BYTES (16 KiB).
  readonly arcCoordBlockBytes?: number;
  // App-specific sections to front-load alongside the encoder's
  // structural front-loaded set (arc_coords_dict, arc_coord_blocks,
  // arc_offsets, per-layer CSR triples, arc_coords). Sections in
  // this list are physically placed at the front of the data area so
  // the client's front-prefetch GET captures them in one shot. Names
  // must match exactly (including any layer prefix, e.g.
  // "cities/name"). Typical use: index/lookup strings or properties
  // that the app needs immediately after open.
  readonly frontLoadedSectionNames?: ReadonlyArray<string>;
  // Within-tier visit-order algorithm for arc_coords packing.
  // Default 'hilbert'. See SpatialSort docs for the alternatives;
  // hierarchical-hilbert is the production winner.
  readonly spatialSort?: SpatialSort;
  // Bench instrumentation hooks. Both fire synchronously and should
  // stay cheap. No-op when omitted; no behavior change.
  readonly onSectionEncoded?: (event: SectionEncodedEvent) => void;
  readonly onPhaseTiming?: (event: PhaseTimingEvent) => void;
}

// --- Constants ---

// Cap each compression group at this many *uncompressed* bytes.
// Matches the client's per-fetch chunk size so a group decompresses
// within one batched request, and bounds per-decompress CPU on the
// reader to one ~4 MiB chunk regardless of how many member sections
// it covers.
const MAX_COMPRESSION_GROUP_BYTES = 4 * 1024 * 1024;

// Max group-compress concurrency.
const GROUP_COMPRESS_CONCURRENCY = 8;

// Compression runs on Node's libuv thread pool via the async API
// — multiple concurrent calls parallelize across UV_THREADPOOL_SIZE
// (default 4). Group compression dominates encode time at zstd
// L19, so firing every group through Promise.all in
// assembleContainer cuts wall-clock by ~Nx where N is
// min(group_count, pool_size).
const zstdCompressAsync = promisify(zstdCompress);
const brotliCompressAsync = promisify(brotliCompress);

// --- Public API ---

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
  const tArcOrder = performance.now();
  const arcOrder = computeArcOrder(
    numArcs,
    layerCsrs,
    input.arcs,
    opts.spatialSort ?? "hilbert",
    input.transform !== undefined && input.transform !== null,
  );
  const newIdOf = invertPermutation(arcOrder);
  for (const { csr } of layerCsrs) remapArcRefs(csr.arcRefs, newIdOf);
  opts.onPhaseTiming?.({
    stage: "arc-order",
    elapsedMs: performance.now() - tArcOrder,
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

  // Block-compressed arc_coords — when enabled, arc_coords ships as
  // a sequence of independently-decodable zstd frames sharing a
  // raw-content dict. The dict + block table are tiny and required
  // before any per-block decompress; mark both front-loaded.
  // META.arcCoordsBlocks tells the reader how to map logical arc
  // byte ranges (arc_offsets) to physical block ranges.
  let arcCoordsBlocksMeta: ContainerMeta["arcCoordsBlocks"] = undefined;
  let arcCoordsSectionBytes = arcCoordsBytes;
  {
    const tBlockCompress = performance.now();
    const block = await blockCompressArcCoords(
      arcCoordsBytes,
      arcOffsetsBytes,
      {
        targetBlockSize:
          opts.arcCoordBlockBytes ?? DEFAULT_ARC_COORD_BLOCK_BYTES,
        dictBytes: autoArcCoordDictBytes(arcCoordsBytes.byteLength),
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
    // multi_poly_breaks: sparse — typically empty for layers without
    // multi-entry MultiPolygon features. Front-loaded so merge() can
    // recover the original polygon grouping without an extra round trip.
    if (csr.multiPolyBreaks.length > 0) {
      sections.push({
        name: `${layerName}/multi_poly_breaks`,
        dtype: "u32",
        bytes: typedArrayBytes(csr.multiPolyBreaks),
        frontLoad: true,
      });
    }
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
  stderrLog(
    `[writeContainer] writing ${(buf.byteLength / 1024 / 1024).toFixed(0)} MiB to ${outPath}`,
  );
  writeFileSync(outPath, buf);
  stderrLog(`[writeContainer] write complete`);
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

// --- Internal: section classification ---

// Sections that contain typed numeric data or strings — everything
// callers always read whole — get compressed. Three sections stay
// uncompressed: arc_coords (range-fetched in slices, compressed
// per-block instead), arc_coord_blocks (the block table — fetched
// eagerly), and arc_coords_dict (a trained zstd dict, won't
// compress further). arc_offsets is read whole at open, so it
// compresses freely.
function shouldCompressSection(name: string, dtype: DType): boolean {
  if (name === "arc_coords") return false;
  // Block-compressed arc_coords block table: small (~12 bytes per
  // block, ~20 KiB for large topologies) and fetched eagerly at open. Skip
  // compression so it serves directly out of one Range fetch with
  // no decompressor setup.
  if (name === "arc_coord_blocks") return false;
  // Shared dict is a trained zstd dict (output of `zstd --train`);
  // would barely compress further, and must be available before any
  // block can decompress (chicken-and-egg if it were compressed
  // against another dict).
  if (name === "arc_coords_dict") return false;
  // CSR triples (poly_offsets, ring_offsets, arc_refs per layer) are
  // small (~few MB for large topologies) and read whole, so compressing them is a
  // free win even though they're not the bottleneck.
  if (
    name.endsWith("/poly_offsets") ||
    name.endsWith("/ring_offsets") ||
    name.endsWith("/arc_refs") ||
    name.endsWith("/multi_poly_breaks")
  ) {
    return true;
  }
  // Property + strings: yes.
  if (dtype === "strings") return true;
  // Treat any remaining named section (arc_offsets and properties)
  // as compressible.
  return true;
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
  if (name.endsWith("/multi_poly_breaks")) return true;
  return false;
}

// --- Internal: delta encoding ---

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

// --- Internal: compression helpers ---

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

function noopProgress(_event: EncodeProgress): void {
  /* default progress sink — discards events */
}

// --- Internal: container assembly ---

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
  stderrLog(
    `[groupCompress] start: ${compressTasks.length} groups (codec=${codec}) ${memSnapshot()}`,
  );
  const tg0 = Date.now();
  let completedCount = 0;

  await runWithConcurrency(
    compressTasks,
    GROUP_COMPRESS_CONCURRENCY,
    async (task) => {
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
      stderrLog(
        `[groupCompress] ${completedCount}/${compressTasks.length} ${task.detail} ` +
          `t=${((Date.now() - tg0) / 1000).toFixed(1)}s ${memSnapshot()}`,
      );
      emit({
        stage: "compress-group",
        index: completedCount,
        total: compressTasks.length,
        detail: task.detail,
      });
    },
  );
  stderrLog(
    `[groupCompress] done in ${((Date.now() - tg0) / 1000).toFixed(1)}s ${memSnapshot()}`,
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
  // Build section placements.
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
  stderrLog(
    `[assemble] allocating final Buffer: ${(totalSize / 1024 / 1024).toFixed(0)} MiB ${memSnapshot()}`,
  );
  const out = Buffer.alloc(totalSize);
  stderrLog(`[assemble] allocated, writing header+sections ${memSnapshot()}`);
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

  stderrLog(`[assemble] regions copied; returning ${memSnapshot()}`);

  onPhaseTiming?.({
    stage: "assemble",
    elapsedMs: performance.now() - tAssemble,
  });

  return out;
}

// --- Internal: helpers ---

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
