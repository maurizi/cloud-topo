// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Per-layer CSR triple building and property/string section encoding
 * for the `.ctopo` encoder.
 *
 * CSR = Compressed Sparse Row — the 3-level layout
 * (geometry → ring → arc) for each layer's polygon topology.
 *
 * Property sections auto-detect dtype by scanning value columns:
 * numeric columns pick the narrowest integer or f64; string columns
 * encode as the `strings` wire layout (count + offsets + utf8 data).
 */

import { type GeometryObject, type Properties } from "topojson-specification";

import { type DType } from "./types";
import { type BuiltSection } from "./encode";
import { typedArrayBytes } from "./encode-arcs";

// --- Constants ---

const UINT8_MAX = 255;
const UINT16_MAX = 65535;
const INT8_MIN = -128;
const INT8_MAX = 127;
const INT16_MIN = -32768;
const INT16_MAX = 32767;

// --- Public API ---

// 3-level CSR encoded as geometry → ring → arc. The polygon level is
// collapsed: a MultiPolygon's rings are flattened across its polygons.
// Reconstruction (e.g. for `feature()`) groups rings into polygons by
// area containment — largest ring is the exterior, smaller nested
// rings are holes.
export function buildLayerCSR(
  geometries: ReadonlyArray<GeometryObject<Properties>>,
): {
  polyOffsets: Uint32Array;
  ringOffsets: Uint32Array;
  arcRefs: Int32Array;
} {
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

// Walk every geometry's `properties` for a layer. Each leaf path becomes one
// section named `{layer}/{path}`. Nested objects flatten with `/`. dtype is
// auto-detected by scanning the value column. Mixed types within a column
// throw. Missing values default to 0 (numeric) or "" (strings).
export function collectPropertySections(
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

// Build one section from a column of mixed-type values. Internal helper used
// by both initial encode and rewriteContainer overrides.
export function buildPropertySection(
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

// --- Internal helpers ---

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
