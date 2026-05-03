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
// is stored uncompressed (currently only arc_coords, until #12).
//
// "zst" is the default — best ratio on our data and ships with a
// tiny (~7 KB) pure-JS fallback decoder (fzstd), preloaded in
// parallel with the header fetch at open time, so it works on every
// browser regardless of native support.
//
// "br" is supported as an alternative. Native via
// DecompressionStream("brotli") on Firefox today and rolling out in
// Chrome; producers can pick it for slightly faster encode at the
// cost of slightly worse ratio. No polyfill — relies on native
// support; readers throw a clear error if a "br" section is
// encountered on a runtime without it.
//
// The format keeps `compression` open as a union so future codecs
// (zstd-with-shared-dict for arc_coords, etc.) can be added without
// a format-version bump.
export type Compression = "zst" | "br";

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
  // When set, arc_coords is block-compressed with zstd: the section's
  // bytes are a concatenation of independently-decodable zstd frames
  // (one per block), each compressed against a shared raw-content
  // dictionary. The dict is a sample of arc-coord bytes (first
  // `targetDictSize` of arc_coords on encode); reader fetches it
  // once at open time and passes it to every per-block decompress.
  // Decoder uses bokuweb/zstd-wasm's `decompressUsingDict` —
  // ~60 µs/block extra vs no-dict, recovered many times over by
  // 8-10% smaller arc_coords on the wire.
  //
  // arc_offsets references *logical* (uncompressed) arc-coord byte
  // positions; the client maps those through arcCoordsBlocks to find
  // which physical blocks to fetch and decompress. Absent when
  // arc_coords is stored raw (the pre-#12 layout).
  readonly arcCoordsBlocks?: {
    // Section name carrying the shared raw-content dictionary
    // (just sample bytes — not a `zstd --train` output). Reader
    // fetches it eagerly at open and reuses for every block decode.
    // Optional — pre-#20 files that block-compressed without a
    // shared dict omit this field; readers fall back to plain
    // (no-dict) decode when it's absent.
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

// One LayerSelection per layer in a merge/mesh/feature call. Multi-layer
// inputs share global arcs, so cancellation works uniformly across them.
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
// pay any decode cost up front, and individual entries can exceed
// V8's max-string limit because no concatenated form is ever
// materialized.
export class StringArray {
  private readonly view: DataView;
  private readonly utf8: Uint8Array;
  private readonly decoder: TextDecoder;
  readonly length: number;

  constructor(bytes: Uint8Array) {
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

// Per-layer geometry triple (CSR-encoded MultiPolygon → polygon → ring → arc).
export interface LayerGeometry {
  readonly polyOffsets: Uint32Array;
  readonly ringOffsets: Uint32Array;
  readonly arcRefs: Int32Array;
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
