// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Decoder for the .ctopo container format — the inverse of `encode.ts`.
 * Node-only (uses Buffer + zlib's sync zstd/brotli), and lives on the
 * `cloud-topo/encode` subpath alongside the encoder.
 *
 * `decodeContainer` reconstructs the original TopoJSON `Topology`
 * (transform, bbox, arcs, and per-layer GeometryCollections with
 * properties) from a container's bytes. It fully supports both container
 * major versions: v1 (cloud-topo 0.1.0) files, which use the monolithic
 * arc_offsets / arc_refs layout, and v2 files, which may partition any of
 * arc_offsets (arcOffsetsBlocks) and per-layer arc_refs (arcRefsBlocks)
 * once they exceed the encoder's size thresholds. Each section is
 * dispatched on its META: monolithic when the section is present,
 * partitioned when the corresponding *Blocks descriptor is. This makes it
 * the read side of a v1→v2 (or v2→v2) re-encode:
 * `encodeContainer(decodeContainer(bytes))`.
 *
 * It inverts the three encoder builders:
 *   - buildArcSections   (arc_coords block stream + arc_offsets → arcs)
 *   - buildLayerCSR      (poly/ring offsets + arc_refs + multi_poly_breaks
 *                         → Polygon / MultiPolygon geometries)
 *   - collect/buildPropertySection (numeric + strings sections → properties)
 */

import { brotliDecompressSync, zstdDecompressSync } from "zlib";

import {
  type GeometryCollection,
  type GeometryObject,
  type MultiPolygon,
  type Polygon,
  type Properties,
  type Topology,
} from "topojson-specification";

import { readVarintZigzagInto, type VarintCursor } from "./core/format";
import { parseContainer } from "./core/reader";
import {
  StringArray,
  type ContainerMeta,
  type DType,
  type SectionEntry,
} from "./core/types";

// --- Public API ---

export function decodeContainer(bytes: Uint8Array): Topology {
  const { meta, sections } = parseContainer(bytes);

  // Uncompressed, delta-undone bytes for every logical section, keyed
  // by name. arc_coords / arc_coord_blocks / arc_coords_dict come back
  // as their on-disk bytes (the block frames are separately compressed
  // and decoded below); everything else is fully materialized here.
  const materialized = materializeSections(bytes, sections);
  const getOptional = (name: string): Uint8Array | undefined =>
    materialized.get(name);
  const get = (name: string): Uint8Array => {
    const b = getOptional(name);
    if (b === undefined)
      throw new Error(`ctopo decode: missing section ${name}`);
    return b;
  };

  const arcs = decodeArcs(meta, materialized);
  const objects = decodeLayers(meta, sections, get, getOptional);

  const topology: Topology = {
    type: "Topology",
    arcs,
    objects,
    bbox: [meta.bbox[0], meta.bbox[1], meta.bbox[2], meta.bbox[3]],
  };
  if (meta.transform !== null) {
    topology.transform = {
      scale: [meta.transform.scale[0], meta.transform.scale[1]],
      translate: [meta.transform.translate[0], meta.transform.translate[1]],
    };
  }
  return topology;
}

// --- Section materialization (decompress + de-delta) ---

// Returns a copy-into-fresh-buffer Uint8Array per section so derived
// typed-array views are naturally aligned (byteOffset 0). Mirrors
// rewriteContainer's group-decompress pass: each physical region is
// decompressed once; group members slice their span out of it.
function materializeSections(
  original: Uint8Array,
  sections: ReadonlyArray<SectionEntry>,
): Map<string, Uint8Array> {
  const out = new Map<string, Uint8Array>();
  const groupCache = new Map<string, Uint8Array>();

  const decompressRegion = (entry: SectionEntry): Uint8Array => {
    const key = `${entry.offset}:${entry.length}`;
    const cached = groupCache.get(key);
    if (cached !== undefined) return cached;
    const compressed = original.subarray(
      entry.offset,
      entry.offset + entry.length,
    );
    const decompressed =
      entry.compression === "brotli"
        ? brotliDecompressSync(compressed)
        : zstdDecompressSync(compressed);
    const view = new Uint8Array(
      decompressed.buffer,
      decompressed.byteOffset,
      decompressed.byteLength,
    );
    groupCache.set(key, view);
    return view;
  };

  for (const entry of sections) {
    let span: Uint8Array;
    if (entry.compression !== undefined) {
      const region = decompressRegion(entry);
      span =
        entry.groupOffset !== undefined && entry.groupLength !== undefined
          ? region.subarray(
              entry.groupOffset,
              entry.groupOffset + entry.groupLength,
            )
          : region;
    } else {
      // Uncompressed sections are always solo (the encoder never groups
      // an uncompressed section). Pass through the on-disk bytes.
      span = original.subarray(entry.offset, entry.offset + entry.length);
    }
    // Copy into a fresh, 0-offset buffer so downstream typed-array views
    // satisfy 2/4/8-byte alignment.
    const copy = new Uint8Array(span.byteLength);
    copy.set(span);
    out.set(entry.name, entry.delta === true ? deltaDecodeU32(copy) : copy);
  }
  return out;
}

// Inverse of encode.ts deltaEncodeU32: running u32 prefix sum.
function deltaDecodeU32(bytes: Uint8Array): Uint8Array {
  const src = new Uint32Array(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength / 4,
  );
  const dst = new Uint32Array(src.length);
  let acc = 0;
  for (let i = 0; i < src.length; i++) {
    acc = (acc + src[i]) >>> 0;
    dst[i] = acc;
  }
  return new Uint8Array(dst.buffer);
}

// --- Arc decode ---

function decodeArcs(
  meta: ContainerMeta,
  materialized: Map<string, Uint8Array>,
): number[][][] {
  const fullCoords = reassembleArcCoords(meta, materialized);
  const arcOffsets = readArcOffsets(meta, materialized);
  const numArcs = meta.numArcs;
  const quantized = meta.bytesPerPoint === 8;

  const arcs = new Array<number[][]>(numArcs);
  const cur: VarintCursor = { value: 0, off: 0 };
  // Un-quantized arcs read raw little-endian f64 pairs through this view;
  // built once and reused across arcs (unused on the quantized path).
  const view = new DataView(
    fullCoords.buffer,
    fullCoords.byteOffset,
    fullCoords.byteLength,
  );
  for (let i = 0; i < numArcs; i++) {
    const start = arcOffsets[i];
    const end = arcOffsets[i + 1];
    const points: number[][] = [];
    if (quantized) {
      cur.off = start;
      while (cur.off < end) {
        readVarintZigzagInto(fullCoords, cur);
        const dx = cur.value;
        readVarintZigzagInto(fullCoords, cur);
        const dy = cur.value;
        points.push([dx, dy]);
      }
    } else {
      // 16 bytes per point: two f64s.
      for (let off = start; off < end; off += 16) {
        points.push([
          view.getFloat64(off, true),
          view.getFloat64(off + 8, true),
        ]);
      }
    }
    arcs[i] = points;
  }
  return arcs;
}

// Decompress every arc_coords block (sharing the trained dict if present)
// and concatenate into the contiguous uncompressed varint/f64 stream that
// arc_offsets indexes into.
function reassembleArcCoords(
  meta: ContainerMeta,
  materialized: Map<string, Uint8Array>,
): Uint8Array {
  const blocks = meta.arcCoordsBlocks;
  const coordsRaw = materialized.get("arc_coords");
  const blockTableBytes = materialized.get(blocks.blockTableSection);
  if (coordsRaw === undefined || blockTableBytes === undefined) {
    throw new Error("ctopo decode: missing arc_coords / block table");
  }
  const dict =
    blocks.dictSection !== undefined
      ? materialized.get(blocks.dictSection)
      : undefined;
  // u32 triples [uncompressedEnd, compressedOffset, compressedLength].
  const table = new Uint32Array(
    blockTableBytes.buffer,
    blockTableBytes.byteOffset,
    blockTableBytes.byteLength / 4,
  );
  const total = blocks.blockCount > 0 ? table[(blocks.blockCount - 1) * 3] : 0;
  const full = new Uint8Array(total);
  let writeOff = 0;
  for (let b = 0; b < blocks.blockCount; b++) {
    const compressedOffset = table[b * 3 + 1];
    const compressedLength = table[b * 3 + 2];
    const frame = coordsRaw.subarray(
      compressedOffset,
      compressedOffset + compressedLength,
    );
    const decompressed =
      dict !== undefined
        ? zstdDecompressSync(frame, { dictionary: dict })
        : zstdDecompressSync(frame);
    full.set(decompressed, writeOff);
    writeOff += decompressed.byteLength;
  }
  return full;
}

// arc_offsets as a cumulative byte-offset table of length numArcs+1.
// Two on-disk shapes, dispatched on META:
//   - monolithic `arc_offsets`: a single delta-encoded u32 table,
//     already de-delta'd by materializeSections. Used by v1 files and
//     small v2 files (arc_offsets below ARC_OFFSETS_PARTITION_MIN_BYTES).
//   - partitioned `arcOffsetsBlocks`: one independently-decodable zstd
//     frame per arc-coords block. Used by large v2 files. Reassembled
//     by readArcOffsetsPartitioned.
function readArcOffsets(
  meta: ContainerMeta,
  materialized: Map<string, Uint8Array>,
): Uint32Array {
  if (meta.arcOffsetsBlocks !== undefined) {
    return readArcOffsetsPartitioned(meta, meta.arcOffsetsBlocks, materialized);
  }
  const mono = materialized.get("arc_offsets");
  if (mono !== undefined) {
    return new Uint32Array(mono.buffer, mono.byteOffset, mono.byteLength / 4);
  }
  throw new Error(
    "ctopo decode: container has neither an arc_offsets section nor arcOffsetsBlocks META",
  );
}

// Reassemble the global arc_offsets table from per-block partitions.
// Inverse of encode-compress.ts blockCompressArcOffsets. Each partition
// is an independently-decodable zstd frame (sharing a trained dict when
// present) holding its block's arc byte-offsets, delta-encoded and
// relative to the block's first-arc start byte. Block b covers arc ids
// [firstArcId_b, firstArcId_{b+1}) and stores one extra trailing entry
// (the block's end byte) so the block's last arc has an end. A block's
// absolute start byte is the running sum of prior blocks' uncompressed
// lengths — which equals each block's final relative offset, so we never
// need the arc_coords block table here.
function readArcOffsetsPartitioned(
  meta: ContainerMeta,
  blocks: NonNullable<ContainerMeta["arcOffsetsBlocks"]>,
  materialized: Map<string, Uint8Array>,
): Uint32Array {
  const partitionsRaw = materialized.get(blocks.partitionsSection);
  const blockTableBytes = materialized.get(blocks.blockTableSection);
  if (partitionsRaw === undefined || blockTableBytes === undefined) {
    throw new Error(
      "ctopo decode: missing arc_offsets partitions / block table",
    );
  }
  const dict =
    blocks.dictSection !== undefined
      ? materialized.get(blocks.dictSection)
      : undefined;
  // u32 triples [firstArcId, compressedOffset, compressedLength], with
  // compressedOffset relative to the start of the partitions section.
  const table = new Uint32Array(
    blockTableBytes.buffer,
    blockTableBytes.byteOffset,
    blockTableBytes.byteLength / 4,
  );
  const numArcs = meta.numArcs;
  const arcOffsets = new Uint32Array(numArcs + 1);
  // Absolute uncompressed start byte of the current block. Block 0 starts
  // at 0; each subsequent block starts where the previous one ended.
  let blockStart = 0;
  for (let b = 0; b < blocks.blockCount; b++) {
    const firstArcId = table[b * 3];
    const compressedOffset = table[b * 3 + 1];
    const compressedLength = table[b * 3 + 2];
    const nextFirstArcId =
      b + 1 < blocks.blockCount ? table[(b + 1) * 3] : numArcs;
    // Offsets for arcs [firstArcId, nextFirstArcId] inclusive — the extra
    // trailing entry is the shared boundary offset with the next block.
    const entries = nextFirstArcId - firstArcId + 1;
    const frame = partitionsRaw.subarray(
      compressedOffset,
      compressedOffset + compressedLength,
    );
    const decompressed =
      dict !== undefined
        ? zstdDecompressSync(frame, { dictionary: dict })
        : zstdDecompressSync(frame);
    // Delta-decode the block-relative offsets. Read through a DataView —
    // the decompressed buffer's byteOffset isn't guaranteed 4-aligned, so
    // a Uint32Array view over it could throw.
    const dv = new DataView(
      decompressed.buffer,
      decompressed.byteOffset,
      decompressed.byteLength,
    );
    let rel = 0;
    for (let k = 0; k < entries; k++) {
      rel = (rel + dv.getUint32(k * 4, true)) >>> 0;
      arcOffsets[firstArcId + k] = blockStart + rel;
    }
    // rel is now this block's uncompressed length; advance the cursor.
    blockStart += rel;
  }
  return arcOffsets;
}

// --- Per-layer geometry + property decode ---

const STRUCTURAL_SUFFIXES = new Set([
  "poly_offsets",
  "ring_offsets",
  "arc_refs",
  "arc_refs_partitions",
  "arc_refs_blocks",
  "multi_poly_breaks",
]);

function decodeLayers(
  meta: ContainerMeta,
  sections: ReadonlyArray<SectionEntry>,
  get: (name: string) => Uint8Array,
  getOptional: (name: string) => Uint8Array | undefined,
): Topology["objects"] {
  const layerNames = new Set(meta.layers.map((l) => l.name));
  const objects: Topology["objects"] = {};

  for (const layer of meta.layers) {
    const geometries = decodeLayerGeometry(
      layer.name,
      layer.numGeometries,
      layer.arcRefsBlocks,
      get,
      getOptional,
    );
    const collection: GeometryCollection<Properties> = {
      type: "GeometryCollection",
      geometries,
    };
    objects[layer.name] = collection;
  }

  // Properties: any `{layer}/{path}` section that isn't a structural
  // suffix is a property column. Re-nest the path onto each geometry.
  for (const entry of sections) {
    const slash = entry.name.indexOf("/");
    if (slash < 0) continue;
    const layer = entry.name.slice(0, slash);
    const path = entry.name.slice(slash + 1);
    if (!layerNames.has(layer)) continue;
    if (STRUCTURAL_SUFFIXES.has(path)) continue;
    const collection = objects[layer] as GeometryCollection<Properties>;
    applyPropertyColumn(
      collection.geometries,
      path,
      entry.dtype,
      get(entry.name),
    );
  }

  return objects;
}

function decodeLayerGeometry(
  layerName: string,
  numGeometries: number,
  arcRefsBlocks: ContainerMeta["layers"][number]["arcRefsBlocks"],
  get: (name: string) => Uint8Array,
  getOptional: (name: string) => Uint8Array | undefined,
): GeometryObject<Properties>[] {
  const polyOffsets = asU32(get(`${layerName}/poly_offsets`));
  const ringOffsets = asU32(get(`${layerName}/ring_offsets`));
  const arcRefs = readLayerArcRefs(layerName, arcRefsBlocks, get, getOptional);
  // Optional: sparse, and only emitted for layers with multi-entry
  // MultiPolygons (see encode.ts).
  const breaksBytes = getOptional(`${layerName}/multi_poly_breaks`);

  // Map geomIndex → sorted ring indices where a new polygon starts
  // (interleaved (geomIndex, ringIndex) u32 pairs).
  const breaksByGeom = new Map<number, number[]>();
  if (breaksBytes !== undefined) {
    const breaks = asU32(breaksBytes);
    for (let i = 0; i < breaks.length; i += 2) {
      const g = breaks[i];
      const ringIdx = breaks[i + 1];
      const list = breaksByGeom.get(g);
      if (list === undefined) breaksByGeom.set(g, [ringIdx]);
      else list.push(ringIdx);
    }
  }

  const ringArcs = (ringIdx: number): number[] => {
    const a = ringOffsets[ringIdx];
    const b = ringOffsets[ringIdx + 1];
    const ring = new Array<number>(b - a);
    for (let k = a; k < b; k++) ring[k - a] = arcRefs[k];
    return ring;
  };

  const geometries = new Array<GeometryObject<Properties>>(numGeometries);
  for (let g = 0; g < numGeometries; g++) {
    const ringStart = polyOffsets[g];
    const ringEnd = polyOffsets[g + 1];
    // Polygon boundaries within [ringStart, ringEnd): ringStart, then
    // each recorded break, then ringEnd.
    const starts = [ringStart, ...(breaksByGeom.get(g) ?? []), ringEnd];
    const polys: number[][][] = [];
    for (let p = 0; p < starts.length - 1; p++) {
      const rings: number[][] = [];
      for (let r = starts[p]; r < starts[p + 1]; r++) rings.push(ringArcs(r));
      polys.push(rings);
    }
    if (polys.length === 1) {
      const poly: Polygon = { type: "Polygon", arcs: polys[0] };
      geometries[g] = poly;
    } else {
      const mp: MultiPolygon = { type: "MultiPolygon", arcs: polys };
      geometries[g] = mp;
    }
  }
  return geometries;
}

// A layer's arc_refs as a single Int32Array, in two on-disk shapes
// dispatched on the layer's META:
//   - monolithic `${layer}/arc_refs`: one i32 section (v1 files and
//     small v2 layers, below ARC_REFS_PARTITION_MIN_BYTES).
//   - partitioned `arcRefsBlocks` (large v2 layers): one zstd frame per
//     geom-id partition. Reassembled by reassembleArcRefs.
function readLayerArcRefs(
  layerName: string,
  arcRefsBlocks: ContainerMeta["layers"][number]["arcRefsBlocks"],
  get: (name: string) => Uint8Array,
  getOptional: (name: string) => Uint8Array | undefined,
): Int32Array {
  if (arcRefsBlocks === undefined) {
    return asI32(get(`${layerName}/arc_refs`));
  }
  return reassembleArcRefs(layerName, arcRefsBlocks, getOptional);
}

// Reassemble a layer's full arc_refs table from its partitions. Inverse
// of encode-compress.ts blockCompressArcRefs. Partitions are in geom-id
// order and contiguous, so concatenating their decompressed payloads in
// table order reproduces the layer's arc_refs exactly. Each partition is
// one zstd frame (no shared dict — the encoder trains none here); its
// payload is the raw little-endian Int32 arc_refs slice for its geoms.
function reassembleArcRefs(
  layerName: string,
  blocks: NonNullable<ContainerMeta["layers"][number]["arcRefsBlocks"]>,
  getOptional: (name: string) => Uint8Array | undefined,
): Int32Array {
  const partitionsRaw = getOptional(blocks.partitionsSection);
  const blockTableBytes = getOptional(blocks.blockTableSection);
  if (partitionsRaw === undefined || blockTableBytes === undefined) {
    throw new Error(
      `ctopo decode: missing arc_refs partitions / block table for layer ${layerName}`,
    );
  }
  // u32 quads [firstGeomId, compressedOffset, compressedLength,
  // uncompressedLength]; compressedOffset is relative to the start of the
  // partitions blob, uncompressedLength is the raw i32 byte count.
  const table = new Uint32Array(
    blockTableBytes.buffer,
    blockTableBytes.byteOffset,
    blockTableBytes.byteLength / 4,
  );
  let totalBytes = 0;
  for (let b = 0; b < blocks.blockCount; b++) totalBytes += table[b * 4 + 3];
  const full = new Uint8Array(totalBytes);
  let writeOff = 0;
  for (let b = 0; b < blocks.blockCount; b++) {
    const compressedOffset = table[b * 4 + 1];
    const compressedLength = table[b * 4 + 2];
    const frame = partitionsRaw.subarray(
      compressedOffset,
      compressedOffset + compressedLength,
    );
    const decompressed = zstdDecompressSync(frame);
    full.set(decompressed, writeOff);
    writeOff += decompressed.byteLength;
  }
  // full is a fresh 0-offset buffer, so the i32 view is aligned.
  return new Int32Array(full.buffer, full.byteOffset, full.byteLength / 4);
}

function applyPropertyColumn(
  geometries: GeometryObject<Properties>[],
  path: string,
  dtype: DType,
  bytes: Uint8Array,
): void {
  const keys = path.split("/");
  const setNested = (
    geom: GeometryObject<Properties>,
    value: unknown,
  ): void => {
    if (geom.properties === undefined || geom.properties === null) {
      (geom as { properties: Properties }).properties = {};
    }
    let obj = geom.properties as Record<string, unknown>;
    for (let k = 0; k < keys.length - 1; k++) {
      const key = keys[k];
      if (typeof obj[key] !== "object" || obj[key] === null) obj[key] = {};
      obj = obj[key] as Record<string, unknown>;
    }
    obj[keys[keys.length - 1]] = value;
  };

  if (dtype === "strings") {
    const sa = new StringArray(bytes);
    const n = Math.min(sa.length, geometries.length);
    for (let i = 0; i < n; i++) setNested(geometries[i], sa.get(i));
    return;
  }
  const arr = asNumeric(dtype, bytes);
  const n = Math.min(arr.length, geometries.length);
  for (let i = 0; i < n; i++) setNested(geometries[i], arr[i]);
}

// --- Typed-array view helpers (bytes are 0-offset copies, so aligned) ---

function asU32(bytes: Uint8Array): Uint32Array {
  return new Uint32Array(bytes.buffer, bytes.byteOffset, bytes.byteLength / 4);
}

function asI32(bytes: Uint8Array): Int32Array {
  return new Int32Array(bytes.buffer, bytes.byteOffset, bytes.byteLength / 4);
}

function asNumeric(
  dtype: DType,
  bytes: Uint8Array,
):
  | Int8Array
  | Uint8Array
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float64Array {
  const { buffer, byteOffset, byteLength } = bytes;
  switch (dtype) {
    case "i8":
      return new Int8Array(buffer, byteOffset, byteLength);
    case "u8":
    case "blob":
      return new Uint8Array(buffer, byteOffset, byteLength);
    case "i16":
      return new Int16Array(buffer, byteOffset, byteLength / 2);
    case "u16":
      return new Uint16Array(buffer, byteOffset, byteLength / 2);
    case "i32":
      return new Int32Array(buffer, byteOffset, byteLength / 4);
    case "u32":
      return new Uint32Array(buffer, byteOffset, byteLength / 4);
    case "f64":
      return new Float64Array(buffer, byteOffset, byteLength / 8);
    default:
      throw new Error(`ctopo decode: unexpected numeric dtype ${dtype}`);
  }
}
