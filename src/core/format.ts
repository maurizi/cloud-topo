// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Wire format constants for .ctopo containers.
 *
 * Layout (little-endian throughout, data sections 16-byte aligned):
 *
 *   0..4              magic               "CTPO"
 *   4..8              version             u32  major:u8 | minor:u8 | patch:u16
 *   8..16             reserved            8 B zeros
 *   16..              data sections (front-loaded sections first; encoder
 *                     orders them so a single front-prefetch GET captures
 *                     everything needed at open + first merge)
 *   ..                (pad to 16 B)
 *   end-FOOTER..end-8 footer:
 *                       section_count   u32
 *                       meta_length     u32
 *                       section_table   {name[16], offset:u64, length:u64}*N
 *                       meta_json       utf-8 bytes
 *   end-8..end        footer_length     u64  total bytes in footer (above 4 fields)
 *
 * Open path: client issues two GETs in parallel — front [0, prefetchBytes)
 * and suffix bytes=-backPrefetchBytes. The footer's trailing length lets
 * the suffix GET locate the footer start without a separate HEAD/round
 * trip; META lives in the suffix payload so all section offsets are known
 * after that one fetch. Front-GET bytes land in the byte-range cache, so
 * lookups for front-loaded sections hit cache without further GETs.
 */

import type { DType } from "./types";

// "CTPO" packed as little-endian u32. C=0x43 T=0x54 P=0x50 O=0x4F.
export const MAGIC = 0x4f505443;

// v1.0.0 — major in the high byte so a parser can hard-fail on byte 8.
export const VERSION_MAJOR = 1;
export const VERSION_MINOR = 0;
export const VERSION_PATCH = 0;
export const VERSION =
  (VERSION_MAJOR << 24) | (VERSION_MINOR << 16) | (VERSION_PATCH & 0xffff);

// Front header is fixed-size: magic (4) + version (4) + reserved (8) = 16 B.
// Section table + META live in the footer — see the layout block above.
export const HEADER_SIZE = 16;
export const SECTION_NAME_SIZE = 16;
export const SECTION_ENTRY_SIZE = SECTION_NAME_SIZE + 8 + 8; // name + u64 offset + u64 length
export const SECTION_ALIGNMENT = 16;
// Trailer holds a u64 footer_length. Always the last 8 bytes of the file;
// suffix-range readers locate the footer start via end - 8 - footer_length.
export const FOOTER_TRAILER_SIZE = 8;
// Per-footer fixed prefix (section_count u32 + meta_length u32) before the
// section table.
export const FOOTER_PREFIX_SIZE = 8;

// Header field offsets — used by reader/encode for direct DataView access.
export const OFFSET_MAGIC = 0;
export const OFFSET_VERSION = 4;
// Footer field offsets are relative to the start of the footer.
export const OFFSET_FOOTER_SECTION_COUNT = 0;
export const OFFSET_FOOTER_META_LENGTH = 4;

export function unpackVersion(version: number): {
  major: number;
  minor: number;
  patch: number;
} {
  return {
    major: (version >>> 24) & 0xff,
    minor: (version >>> 16) & 0xff,
    patch: version & 0xffff,
  };
}

// Bytes per element for typed sections. Variable-length sections (`blob`,
// `strings`) report 1 — they're addressed by raw byte length.
export function bytesPerElement(dtype: DType): number {
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

// Round `n` up to the next multiple of `alignment`. Used for section padding.
export function alignUp(
  n: number,
  alignment: number = SECTION_ALIGNMENT,
): number {
  return (n + alignment - 1) & ~(alignment - 1);
}

// --- LEB128 varint with zigzag for signed integers ---
//
// Used by the quantized arc_coords path: per-point dx/dy deltas are
// typically small (< 256 quantization units), so each pair encodes in
// 2-4 bytes instead of the 8 bytes that two int32LE values cost. zstd
// L19 then compresses the varint stream to a similar tail; the win is
// the smaller raw form fed to the compressor.
//
// Worst case is 5 bytes per int32 (when |value| ≥ 2^27); we size
// encode buffers for 10 bytes per point (5 dx + 5 dy) to make this
// branchless.
export const VARINT_MAX_BYTES = 5;

// Write a signed int32 as zigzag-encoded LEB128 starting at `off`.
// Returns the new write offset (bytes consumed = return - off).
export function writeVarintZigzag(
  value: number,
  buf: Uint8Array,
  off: number,
): number {
  // Zigzag: maps (-1, 0, 1, -2, 2, ...) to (1, 0, 2, 3, 4, ...) so
  // negative numbers get small encodings too. `>>> 0` makes it a
  // 32-bit unsigned for the LEB128 emit loop.
  let v = ((value << 1) ^ (value >> 31)) >>> 0;
  while (v >= 0x80) {
    buf[off++] = (v & 0x7f) | 0x80;
    v = v >>> 7;
  }
  buf[off++] = v;

  return off;
}

// Caller-owned cursor for zigzag-LEB128 reads. `off` points at the
// next byte to consume; `value` receives the most recent decoded
// signed value. The cursor is mutated by readVarintZigzagInto so
// callers don't pay a per-call object literal allocation.
export interface VarintCursor {
  value: number;
  off: number;
}

// Decode one zigzag-LEB128 value at `cur.off`. On return:
//   cur.value = decoded signed value
//   cur.off   = position immediately after the consumed bytes
export function readVarintZigzagInto(buf: Uint8Array, cur: VarintCursor): void {
  let off = cur.off;
  let lo = 0;
  // First 4 bytes: bits 0..27 fit in a 32-bit signed int with room.
  for (let shift = 0; shift < 28; shift += 7) {
    const b = buf[off++];
    lo |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) {
      // Zigzag decode within signed-int32 range (lo is non-negative
      // here since we've only set bits 0..27).
      cur.value = (lo >>> 1) ^ -(lo & 1);
      cur.off = off;
      return;
    }
  }
  // 5th byte holds bits 28..31 of the unsigned 32-bit accumulator.
  // High bit on a 5th byte means the encoder went past int32 — bug.
  const b = buf[off++];
  if ((b & 0x80) !== 0 || (b & 0xf0) !== 0) {
    throw new Error("ctopo: varint exceeds int32 range");
  }
  const u32 = ((lo >>> 0) | (b << 28)) >>> 0;
  // For uint32 inputs we can't use the signed-shift trick — split the
  // low bit and divide by 2 in float math to land back in JS number range.
  cur.value = u32 & 1 ? -((u32 + 1) / 2) : u32 / 2;
  cur.off = off;
}
