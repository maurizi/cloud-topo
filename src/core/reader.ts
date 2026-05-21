// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Pure, isomorphic header / footer parser. No I/O —
 * `client.ts` owns the Range fetcher and lazy section loading.
 *
 * Wire layout has two metadata regions: a 16-B HEADER at the front
 * (magic + version) and a variable-size FOOTER at the end (section
 * table + META + trailing length). Open path fetches both in
 * parallel via `bytes=[0,N)` and `bytes=-M` to save one round-trip.
 */

import {
  FOOTER_PREFIX_SIZE,
  FOOTER_TRAILER_SIZE,
  HEADER_SIZE,
  MAGIC,
  OFFSET_FOOTER_META_LENGTH,
  OFFSET_FOOTER_SECTION_COUNT,
  OFFSET_MAGIC,
  OFFSET_VERSION,
  SECTION_ENTRY_SIZE,
  SECTION_NAME_SIZE,
  VERSION_MAJOR,
  unpackVersion,
} from "./format";
import {
  type ContainerMeta,
  type SectionEntry,
  type WireCompression,
  compressionFromWire,
} from "./types";

export interface ParsedHeader {
  readonly meta: ContainerMeta;
  readonly sections: ReadonlyArray<SectionEntry>;
  // Byte offset where the data area begins (start of the first section).
  // Always HEADER_SIZE (16) in the current format; preserved for callers
  // that want to validate / inspect the layout.
  readonly dataStart: number;
}

// Read the trailing 8-byte footer-length marker out of a buffer that
// covers at least the last FOOTER_TRAILER_SIZE bytes of the file.
// `bufferEndIsFileEnd` declares whether the buffer's last byte is also
// the file's last byte, so we know where to read from.
export function readFooterLength(buffer: Uint8Array): number {
  if (buffer.byteLength < FOOTER_TRAILER_SIZE) {
    throw new Error(
      `ctopo: buffer too small for footer trailer (${buffer.byteLength} < ${FOOTER_TRAILER_SIZE})`,
    );
  }
  const view = new DataView(
    buffer.buffer,
    buffer.byteOffset,
    buffer.byteLength,
  );
  return readBigUint64AsNumber(view, buffer.byteLength - FOOTER_TRAILER_SIZE);
}

// Validate the front-of-file header. Cheap — only checks magic and
// version. The actual section table + META live in the footer; use
// parseFooter for those.
export function parseFrontHeader(bytes: Uint8Array): void {
  if (bytes.byteLength < HEADER_SIZE) {
    throw new Error(
      `ctopo: buffer too small for header (${bytes.byteLength} < ${HEADER_SIZE})`,
    );
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const magic = view.getUint32(OFFSET_MAGIC, true);
  if (magic !== MAGIC) {
    throw new Error(
      `ctopo: bad magic 0x${magic.toString(16).padStart(8, "0")} — not a .ctopo file`,
    );
  }
  const version = view.getUint32(OFFSET_VERSION, true);
  const v = unpackVersion(version);
  if (v.major !== VERSION_MAJOR) {
    throw new Error(
      `ctopo: incompatible major version ${v.major} (this build understands ${VERSION_MAJOR})`,
    );
  }
}

// Parse a footer buffer (section_count + meta_length + section_table
// + meta_json) into a ParsedHeader. The buffer must start at the
// footer's first byte and be at least footer_length bytes long.
export function parseFooter(footerBytes: Uint8Array): ParsedHeader {
  if (footerBytes.byteLength < FOOTER_PREFIX_SIZE) {
    throw new Error(
      `ctopo: footer too small for prefix (${footerBytes.byteLength} < ${FOOTER_PREFIX_SIZE})`,
    );
  }
  const view = new DataView(
    footerBytes.buffer,
    footerBytes.byteOffset,
    footerBytes.byteLength,
  );

  const sectionCount = view.getUint32(OFFSET_FOOTER_SECTION_COUNT, true);
  const metaLength = view.getUint32(OFFSET_FOOTER_META_LENGTH, true);

  const tableStart = FOOTER_PREFIX_SIZE;
  const tableEnd = tableStart + sectionCount * SECTION_ENTRY_SIZE;
  const metaStart = tableEnd;
  const metaEnd = metaStart + metaLength;

  if (footerBytes.byteLength < metaEnd) {
    throw new Error(
      `ctopo: footer truncated (${footerBytes.byteLength} < ${metaEnd}) — fetch more bytes`,
    );
  }

  const metaJson = new TextDecoder("utf-8").decode(
    footerBytes.subarray(metaStart, metaEnd),
  );
  // META on disk uses wire codec strings ("zst" / "br"); translate
  // to public ("zstd" / "brotli") so the rest of the codebase only
  // sees the long names. Wire format unchanged.
  type WireSectionMeta = Omit<
    ContainerMeta["sections"][number],
    "compression"
  > & {
    compression?: WireCompression;
  };
  type WireMeta = Omit<ContainerMeta, "sections"> & {
    sections: ReadonlyArray<WireSectionMeta>;
  };
  const rawMeta = JSON.parse(metaJson) as WireMeta;
  const meta: ContainerMeta = {
    ...rawMeta,
    sections: rawMeta.sections.map((s) => ({
      ...s,
      compression:
        s.compression !== undefined
          ? compressionFromWire(s.compression)
          : undefined,
    })),
  };

  if (meta.sections.length !== sectionCount) {
    throw new Error(
      `ctopo: META section count (${meta.sections.length}) disagrees with footer (${sectionCount})`,
    );
  }
  if (
    typeof (meta as { arcCoordsBlocks?: unknown }).arcCoordsBlocks !== "object"
  ) {
    throw new Error(
      "ctopo: META is missing arcCoordsBlocks — file was produced by an unsupported encoder",
    );
  }

  // Walk the binary table to pick up offsets/lengths; pair each row
  // with the META descriptor at the same index. The 16-byte name in
  // the binary table is a diagnostic short identifier — META is
  // authoritative.
  const sections = new Array<SectionEntry>(sectionCount);
  let dataStart = Number.POSITIVE_INFINITY;
  for (let i = 0; i < sectionCount; i++) {
    const entryStart = tableStart + i * SECTION_ENTRY_SIZE;
    const offset = readBigUint64AsNumber(view, entryStart + SECTION_NAME_SIZE);
    const length = readBigUint64AsNumber(
      view,
      entryStart + SECTION_NAME_SIZE + 8,
    );
    if (offset < dataStart) dataStart = offset;
    sections[i] = {
      name: meta.sections[i].name,
      dtype: meta.sections[i].type,
      offset,
      length,
      compression: meta.sections[i].compression,
      groupOffset: meta.sections[i].groupOffset,
      groupLength: meta.sections[i].groupLength,
      delta: meta.sections[i].delta,
      uncompressedRegionLength: meta.sections[i].uncompressedRegionLength,
    };
  }
  if (sectionCount === 0) dataStart = HEADER_SIZE;

  return { meta, sections, dataStart };
}

// Convenience wrapper for callers that hold the entire file in memory
// (tests, rewriteContainer's pass-through pipeline, build-tools). Validates
// the front header, locates the footer via the trailing length marker,
// and parses it.
//
// Network clients should use parseFrontHeader + parseFooter directly so
// the front and back fetches can run in parallel.
export function parseContainer(bytes: Uint8Array): ParsedHeader {
  parseFrontHeader(bytes);
  const footerLength = readFooterLength(bytes);
  const footerStart = bytes.byteLength - FOOTER_TRAILER_SIZE - footerLength;
  if (footerStart < HEADER_SIZE) {
    throw new Error(
      `ctopo: footer length ${footerLength} doesn't fit in buffer of ${bytes.byteLength} bytes`,
    );
  }
  const footerBytes = bytes.subarray(footerStart, footerStart + footerLength);
  return parseFooter(footerBytes);
}

function readBigUint64AsNumber(view: DataView, offset: number): number {
  const big = view.getBigUint64(offset, true);
  if (big > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(
      `ctopo: section offset/length exceeds Number.MAX_SAFE_INTEGER`,
    );
  }
  return Number(big);
}

// Slice the typed-array view of a section out of a buffer that already
// contains the section's bytes (e.g. an encoded buffer in tests, or a
// prefetched Range payload). Variable-width sections (`blob`, `strings`)
// return raw Uint8Array views.
//
// For compressed sections, callers must decompress the on-disk bytes
// first and pass the result via `viewDecompressedSection` — this
// function operates on raw on-disk bytes and uses `entry.length` as
// the wire length, which is the *compressed* length when
// `entry.compression` is set.
export function viewSection(
  bytes: Uint8Array,
  entry: SectionEntry,
  baseOffset: number = 0,
): TypedArray {
  const start = entry.offset - baseOffset;
  const end = start + entry.length;
  if (start < 0 || end > bytes.byteLength) {
    throw new Error(
      `ctopo: section ${entry.name} not in slice (need [${start}, ${end}) in ${bytes.byteLength} bytes)`,
    );
  }
  return viewBytesAsDtype(bytes.subarray(start, end), entry.dtype);
}

// Wrap an already-extracted (and, if applicable, decompressed)
// Uint8Array as the typed view appropriate to a section's dtype.
// Used by the client after fetching + decompressing a compressed
// section's bytes — the caller knows the uncompressed length, so we
// don't need entry.length here.
export function viewDecompressedSection(
  bytes: Uint8Array,
  dtype: SectionEntry["dtype"],
): TypedArray {
  return viewBytesAsDtype(bytes, dtype);
}

type TypedArray =
  | Int8Array
  | Uint8Array
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float64Array;

// The SAB zstd decode returns a Uint8Array view into the decoder's wasm heap
// and registers a FinalizationRegistry on THAT Uint8Array to free the wasm
// allocation once it's collected (see zstd-decoder-client.ts). A derived
// dtype view (Int32Array, Uint32Array, …) aliases the same buffer but does
// NOT keep the source Uint8Array reachable — so a caller holding only the
// derived view (e.g. cached layerGeometry.arcRefs) lets the source get GC'd,
// the wasm region is freed, a later decode reuses it, and the still-live
// derived view reads corrupted bytes (a silent use-after-free, only on the
// SAB decode path). Pin the source to each derived view's lifetime via a
// WeakMap so the free defers until the derived view itself is collected.
// Harmless on the plain (copy) decode path, which has no registry.
const decodeSourceRetainer = new WeakMap<ArrayBufferView, Uint8Array>();
export function retainDecodeSource<T extends ArrayBufferView>(
  view: T,
  src: Uint8Array,
): T {
  decodeSourceRetainer.set(view, src);
  return view;
}

function viewBytesAsDtype(
  sectionBytes: Uint8Array,
  dtype: SectionEntry["dtype"],
): TypedArray {
  const { buffer, byteOffset, byteLength } = sectionBytes;
  switch (dtype) {
    case "i8":
      return retainDecodeSource(
        new Int8Array(buffer, byteOffset, byteLength),
        sectionBytes,
      );
    case "u8":
    case "blob":
    case "strings":
      // Returns the source Uint8Array itself — already keeps the wasm
      // allocation alive, so no extra retain needed.
      return sectionBytes;
    case "i16":
      return retainDecodeSource(
        new Int16Array(buffer, byteOffset, byteLength / 2),
        sectionBytes,
      );
    case "u16":
      return retainDecodeSource(
        new Uint16Array(buffer, byteOffset, byteLength / 2),
        sectionBytes,
      );
    case "i32":
      return retainDecodeSource(
        new Int32Array(buffer, byteOffset, byteLength / 4),
        sectionBytes,
      );
    case "u32":
      return retainDecodeSource(
        new Uint32Array(buffer, byteOffset, byteLength / 4),
        sectionBytes,
      );
    case "f64":
      return retainDecodeSource(
        new Float64Array(buffer, byteOffset, byteLength / 8),
        sectionBytes,
      );
  }
}
