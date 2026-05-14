// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Public types for the ctopo container format.
 */

// All numeric data sections are typed; `blob` is opaque bytes; `strings` is
// the self-describing offsets+utf8 layout decoded via StringArray.
export type DType =
  | "i8"
  | "u8"
  | "i16"
  | "u16"
  | "i32"
  | "u32"
  | "f64"
  | "blob"
  | "strings";

// Per-section compression algorithm. `undefined` means the section
// is stored uncompressed — used for arc_coords (range-fetched in
// slices), arc_coord_blocks (block table fetched eagerly), and
// arc_coords_dict (already a trained zstd dict, won't compress
// further and must be decodable before any block decompresses).
//
// "zstd" is the default — best ratio on our data and ships with a
// WASM-based fallback decoder (zstd-rs), preloaded in parallel
// with the header fetch at open time, so it works on every browser
//
// "brotli" is supported as an alternative. Native via
// DecompressionStream("brotli") on Firefox today and rolling out in
// Chrome; producers can pick it for slightly faster encode at the
// cost of slightly worse ratio. No polyfill — relies on native
// support; readers throw a clear error if a "brotli" section is
// encountered on a runtime without it.
//
// The format keeps `compression` open as a union so future codecs
// (zstd-with-shared-dict for arc_coords, etc.) can be added without
// a format-version bump.
//
// Public-API names are "zstd" / "brotli" (descriptive). The on-disk
// META JSON keeps the abbreviated forms "zst" / "br" — the encoder
// translates one direction at META emit, the reader translates the
// other at META parse via the helpers below. Wire format unchanged.
export type Compression = "zstd" | "brotli";

// Internal wire-format codec strings written into META on disk.
// Not exported through index.ts — encode/decode boundary only.
export type WireCompression = "zst" | "br";

export function compressionToWire(c: Compression): WireCompression {
  return c === "zstd" ? "zst" : "br";
}

export function compressionFromWire(w: WireCompression): Compression {
  return w === "zst" ? "zstd" : "brotli";
}

// One row of the binary section table. JS numbers safely cover the u64 wire
// values up to 2^53 — well beyond any plausible .ctopo file size.
export interface SectionEntry {
  readonly name: string;
  readonly offset: number;
  readonly length: number;
  readonly dtype: DType;
  // Codec applied to the on-disk bytes for this section. When set,
  // `length` is the *compressed* size; the reader decompresses
  // before exposing typed views.
  readonly compression?: Compression;
  // When set, this section is one of several packed into a single
  // compression group. The group's on-disk bytes live at
  // [offset, offset+length) — the same range as every other
  // section in the group — and decompress to a contiguous run from
  // which this section reads `[groupOffset, groupOffset+groupLength)`.
  // Bundling small sections cuts per-decompress setup overhead
  // (one decompress per ~4 MiB group instead of ~30 ms × N
  // sections) while preserving selective-fetch granularity at the
  // group level.
  readonly groupOffset?: number;
  readonly groupLength?: number;
  // When set, the section's u32 values were first-order delta-encoded
  // before compression — index 0 stays as the original value, every
  // subsequent index holds the difference from its predecessor.
  // Reader undoes the transform with a running prefix sum after
  // decompression. Applied to cumulative-monotone u32 sections
  // (arc_offsets, poly_offsets, ring_offsets) where the deltas are
  // tiny constants the compressor can pack tightly. Compressors
  // gain ~10-30% on the original cumulative form, since adjacent
  // u32 values share most high-order bits in the absolute layout.
  readonly delta?: boolean;
  // Uncompressed byte length of the *physical region* this section
  // lives in (a compression group, or a solo-section region).
  // Required for the wasm zstd decoder, which can't allocate its
  // output buffer accurately when the frame header lacks FCS
  // (Node's async zstdCompress emits frames without it). For group
  // members this equals the total decompressed group size — every
  // member of the same group carries the same value so the reader
  // doesn't have to walk the table looking for the group's max
  // groupOffset+groupLength.
  readonly uncompressedRegionLength?: number;
}

// META JSON shape, parsed once at openContainer time.
export interface ContainerMeta {
  readonly version: number;
  readonly numArcs: number;
  readonly bytesPerPoint: 8 | 16;
  readonly transform: {
    readonly scale: readonly [number, number];
    readonly translate: readonly [number, number];
  } | null;
  readonly bbox: readonly [number, number, number, number];
  // FlatGeobuf-style escape hatch — JSON-string blob for caller-defined
  // metadata.
  readonly metadata?: string;
  // arc_coords is block-compressed with zstd: the section's bytes
  // are a concatenation of independently-decodable zstd frames (one
  // per block), each compressed against a shared raw-content
  // dictionary. The dict is a sample of arc-coord bytes (first
  // `targetDictSize` of arc_coords on encode); reader fetches it
  // once at open time and passes it to every per-block decompress.
  // Decoder uses bokuweb/zstd-wasm's `decompressUsingDict` —
  // ~60 µs/block extra vs no-dict, recovered many times over by
  // 8-10% smaller arc_coords on the wire.
  //
  // arc_offsets references *logical* (uncompressed) arc-coord byte
  // positions; the client maps those through arcCoordsBlocks to find
  // which physical blocks to fetch and decompress.
  readonly arcCoordsBlocks: {
    // Section name carrying the shared trained zstd dictionary
    // (output of `zstd --train` over a sample of arc-coord blocks).
    // Reader fetches it eagerly at open and reuses for every block
    // decode. Optional — files where the encoder skipped dict
    // training (e.g., too few blocks for a stable train) omit this
    // field; readers fall back to plain (no-dict) decode.
    readonly dictSection?: string;
    // Section name carrying the block table — a u32 array of triples
    // [uncompressedEnd, compressedOffset, compressedLength] per
    // block. uncompressedEnd is the highest *exclusive* logical byte
    // index of any arc that lives in the block (= arc_offsets[lastArcInBlock + 1]).
    // compressedOffset is relative to the start of arc_coords.
    readonly blockTableSection: string;
    readonly blockCount: number;
    // Target uncompressed bytes per block (last block may be smaller).
    readonly targetBlockSize: number;
  };
  // Present iff arc_offsets is shipped in partitioned form: one
  // independently-decodable zstd frame per arc-coord block, sharing
  // a trained dict. Absent for the legacy monolithic arc_offsets
  // section (small files, or files produced by older encoders); the
  // reader dispatches on this field's presence. Block ordering and
  // count match arcCoordsBlocks exactly — partition i of arc_offsets
  // covers exactly the arcs that block i of arc_coords does.
  readonly arcOffsetsBlocks?: {
    // Trained dict section name. Optional like arcCoordsBlocks.dictSection.
    readonly dictSection?: string;
    // u32 triples [firstArcId, compressedOffset, compressedLength] per
    // partition. firstArcId is the smallest arc id whose offsets live
    // in this partition; the partition covers arc ids
    // [firstArcId_i, firstArcId_{i+1}). compressedOffset is relative
    // to the start of partitionsSection. Lets block selection bypass
    // the global arc_offsets table — given an arcId, binary-search
    // firstArcId to find blockIdx without first decompressing any
    // arc_offsets bytes.
    readonly blockTableSection: string;
    // Section name carrying the concatenation of per-block compressed
    // arc-offsets frames.
    readonly partitionsSection: string;
    readonly blockCount: number;
  };
  // Present iff the encoder emitted a dedicated arc-endpoints section
  // (quantized inputs only, opt-in via EncodeOptions.emitArcEndpoints,
  // default on). For each arc, holds the absolute (startX, startY,
  // endX, endY) quantized coordinates so mergeArcs / merge endpoint
  // lookup can skip walking the full arc varint stream in arc_coords.
  // Block-partitioned along the same arc-id boundaries as
  // arcCoordsBlocks so a sparse merge's endpoint fetches line up with
  // its coord fetches (or, in the mergeArcs-only case, replace them
  // entirely). Absent on legacy files; reader falls back to the
  // arc_coords varint-walk path when undefined.
  readonly arcEndpointsBlocks?: {
    // Trained dict section name. Optional like the other block-
    // compressed sections.
    readonly dictSection?: string;
    // u32 triples [firstArcId, compressedOffset, compressedLength] per
    // partition. firstArcId is the smallest arc id whose endpoints
    // live in this partition; the partition covers arc ids
    // [firstArcId_i, firstArcId_{i+1}). compressedOffset is relative
    // to the start of partitionsSection. blockCount and the per-block
    // arc-id ranges match arcCoordsBlocks exactly.
    readonly blockTableSection: string;
    // Section name carrying the concatenation of per-block compressed
    // arc-endpoints frames.
    readonly partitionsSection: string;
    readonly blockCount: number;
  };
  readonly layers: ReadonlyArray<{
    readonly name: string;
    readonly numGeometries: number;
  }>;
  readonly sections: ReadonlyArray<{
    readonly name: string;
    readonly type: DType;
    readonly compression?: Compression;
    readonly groupOffset?: number;
    readonly groupLength?: number;
    readonly delta?: boolean;
    readonly uncompressedRegionLength?: number;
  }>;
}

// One LayerSelection per layer in a merge. Multi-layer inputs share global arcs,
// so cancellation works uniformly across them.
export interface LayerSelection {
  readonly layer: string;
  readonly indices: Iterable<number>;
}

// Wire layout for a `strings` section (matches encode.ts/reader.ts):
//   0..4    count u32
//   4..8    pad
//   8..8+4*(count+1)   utf8_offsets u32[count+1]   // last = total utf8 bytes
//   then    utf8_data
//
// Decoding is lazy — `get(i)` slices and decodes a single entry on
// demand. Opening a container with millions of string entries doesn't
// pay any decode cost up front or encounter max string length issues
export class StringArray {
  private readonly view: DataView;
  private readonly utf8: Uint8Array;
  private readonly decoder: TextDecoder;
  readonly length: number;
  // Raw section bytes (count + offsets + utf8 data). Exposed so the
  // worker boundary can ship the underlying ArrayBuffer to the main
  // thread and rebuild a StringArray there without re-fetching.
  readonly bytes: Uint8Array;

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    this.length = this.view.getUint32(0, true);
    const offsetsStart = 8;
    const utf8Start = offsetsStart + 4 * (this.length + 1);
    this.utf8 = bytes.subarray(utf8Start);
    this.decoder = new TextDecoder("utf-8");
  }

  get(i: number): string {
    const offsetsStart = 8;
    const start = this.view.getUint32(offsetsStart + i * 4, true);
    const end = this.view.getUint32(offsetsStart + (i + 1) * 4, true);
    return this.decoder.decode(this.utf8.subarray(start, end));
  }

  *[Symbol.iterator](): IterableIterator<string> {
    for (let i = 0; i < this.length; i++) yield this.get(i);
  }
}

// Per-layer geometry triple (CSR-encoded geometry → ring → arc), plus a
// sparse `multiPolyBreaks` side-table marking polygon boundaries WITHIN
// any geometry whose source is a MultiPolygon with more than one polygon
// entry. Encoded as flat (geomIndex, ringIndex) u32 pairs (sorted by
// geomIndex). Empty for typical layers where every feature is a single
// Polygon — the implicit polygon at ring polyOffsets[g] is never recorded.
export interface LayerGeometry {
  readonly polyOffsets: Uint32Array;
  readonly ringOffsets: Uint32Array;
  readonly arcRefs: Int32Array;
  readonly multiPolyBreaks: Uint32Array;
}

// Property override for rewriteContainer — pass raw arrays and the encoder
// picks the dtype the same way the initial encode does.
export interface PropertyOverride {
  readonly name: string;
  readonly data:
    | ReadonlyArray<number>
    | ReadonlyArray<string>
    | ArrayLike<number>;
}
