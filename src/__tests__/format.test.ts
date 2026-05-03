// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

import { describe, expect, it } from "vitest";

import { type Topology } from "topojson-specification";

import { encodeContainer, rewriteContainer } from "../encode";
import {
  MAGIC,
  VERSION_MAJOR,
  readVarintZigzag,
  unpackVersion,
  writeVarintZigzag,
} from "../format";
import {
  parseContainer,
  viewDecompressedSection,
  viewSection,
} from "../reader";
import { type SectionEntry, StringArray } from "../types";
import { brotliDecompressSync, zstdDecompressSync } from "zlib";
import { mkdtempSync, readFileSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

// Helper for tests that want to inspect a compressed section's
// original bytes — mirrors what the client does at runtime, but
// runs synchronously through Node's zlib for ergonomics. Group
// members slice their span out of the decompressed group bytes.
function viewSectionAuto(
  buf: Uint8Array,
  entry: SectionEntry,
  baseOffset: number = 0,
) {
  const sectionBytes = buf.subarray(
    entry.offset - baseOffset,
    entry.offset - baseOffset + entry.length,
  );
  if (entry.compression === undefined) {
    return viewSection(buf, entry, baseOffset);
  }
  const decompressed =
    entry.compression === "br"
      ? brotliDecompressSync(sectionBytes)
      : zstdDecompressSync(sectionBytes);
  const all = new Uint8Array(
    decompressed.buffer,
    decompressed.byteOffset,
    decompressed.byteLength,
  );
  const memberBytes =
    entry.groupOffset !== undefined && entry.groupLength !== undefined
      ? all.subarray(entry.groupOffset, entry.groupOffset + entry.groupLength)
      : all;
  if (entry.delta === true) {
    // Mirror the client's inverse-delta path so test assertions
    // operate on the logical (cumulative) values.
    const src = new Uint32Array(
      memberBytes.buffer,
      memberBytes.byteOffset,
      memberBytes.byteLength / 4,
    );
    const dst = new Uint32Array(src.length);
    if (src.length > 0) {
      dst[0] = src[0];
      for (let i = 1; i < src.length; i++) dst[i] = (dst[i - 1] + src[i]) >>> 0;
    }
    return dst;
  }
  return viewDecompressedSection(memberBytes, entry.dtype);
}

// 2×1 grid: two unit-square blocks sharing one vertical arc. Quantized
// transform so the encoder packs deltas as Int32 pairs (8 bytes/pt).
function fixtureTopology(): Topology {
  return {
    type: "Topology",
    arcs: [
      [
        [0, 0],
        [10000, 0],
      ],
      [
        [10000, 0],
        [0, 10000],
      ],
      [
        [10000, 10000],
        [-10000, 0],
      ],
      [
        [0, 10000],
        [0, -10000],
      ],
      [
        [10000, 0],
        [10000, 0],
      ],
      [
        [20000, 0],
        [0, 10000],
      ],
      [
        [20000, 10000],
        [-10000, 0],
      ],
    ],
    transform: { scale: [1e-7, 1e-7], translate: [-180, -90] },
    bbox: [-180, -90, 180, 90],
    objects: {
      block: {
        type: "GeometryCollection",
        geometries: [
          {
            type: "Polygon",
            arcs: [[0, 1, 2, 3]],
            properties: { id: "L", population: 100, name: "Left" },
          },
          {
            type: "Polygon",
            arcs: [[4, 5, 6, ~1]],
            properties: { id: "R", population: 200, name: "Right" },
          },
        ],
      },
    },
  };
}

describe("ctopo format", () => {
  it("writes magic + version in the front header and section_count in the footer", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    expect(view.getUint32(0, true)).toBe(MAGIC);
    const v = unpackVersion(view.getUint32(4, true));
    expect(v.major).toBe(VERSION_MAJOR);
    // Section count and meta length live at the start of the footer
    // — locate the footer via the trailing 8-byte length marker.
    const footerLength = Number(view.getBigUint64(buf.byteLength - 8, true));
    const footerStart = buf.byteLength - 8 - footerLength;
    const sectionCount = view.getUint32(footerStart, true);
    expect(sectionCount).toBeGreaterThan(0);
  });

  it("round-trips every section type (numeric, strings, blob, CSR)", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { meta, sections } = parseContainer(buf);

    expect(meta.numArcs).toBe(7);
    expect(meta.bytesPerPoint).toBe(8);
    expect(meta.layers).toEqual([{ name: "block", numGeometries: 2 }]);

    // arc_offsets — 8 entries (numArcs+1) of u32. The section is
    // gzipped on disk; viewSectionAuto handles decompression.
    const arcOffsetsEntry = sections.find((s) => s.name === "arc_offsets");
    expect(arcOffsetsEntry).toBeDefined();
    expect(arcOffsetsEntry?.dtype).toBe("u32");
    const arcOffsets = viewSectionAuto(buf, arcOffsetsEntry!, 0) as Uint32Array;
    expect(arcOffsets.length).toBe(8);
    expect(arcOffsets[0]).toBe(0);
    // Quantized arc points are varint-encoded — exact byte count
    // depends on delta magnitudes. Just check monotonic + non-empty;
    // round-trip correctness lives in the merge / client tests.
    for (let i = 1; i < arcOffsets.length; i++) {
      expect(arcOffsets[i]).toBeGreaterThan(arcOffsets[i - 1]);
    }

    // Per-layer CSR triple. Sections may be zstd-compressed —
    // viewSectionAuto decompresses (using Node's zlib for tests)
    // before viewing, mirroring what the client does at runtime.
    const polyOffsets = viewSectionAuto(
      buf,
      sections.find((s) => s.name === "block/poly_offsets")!,
      0,
    ) as Uint32Array;
    expect(Array.from(polyOffsets)).toEqual([0, 1, 2]);

    const ringOffsets = viewSectionAuto(
      buf,
      sections.find((s) => s.name === "block/ring_offsets")!,
      0,
    ) as Uint32Array;
    expect(Array.from(ringOffsets)).toEqual([0, 4, 8]);

    const arcRefs = viewSectionAuto(
      buf,
      sections.find((s) => s.name === "block/arc_refs")!,
      0,
    ) as Int32Array;
    expect(Array.from(arcRefs)).toEqual([0, 1, 2, 3, 4, 5, 6, ~1]);

    // Numeric property — population fits in u8 (max 200).
    const popEntry = sections.find((s) => s.name === "block/population")!;
    expect(popEntry.dtype).toBe("u8");
    const pop = viewSectionAuto(buf, popEntry, 0) as Uint8Array;
    expect(Array.from(pop)).toEqual([100, 200]);

    // Strings property.
    const idsEntry = sections.find((s) => s.name === "block/id")!;
    expect(idsEntry.dtype).toBe("strings");
    const idsBytes = viewSectionAuto(buf, idsEntry, 0) as Uint8Array;
    const ids = new StringArray(idsBytes);
    expect(ids.length).toBe(2);
    expect(ids.get(0)).toBe("L");
    expect(ids.get(1)).toBe("R");

    // arc_coords — opaque blob; varint-encoded so length depends on
    // delta magnitudes. Just check dtype + non-empty + matches the
    // last arcOffsets entry.
    const arcCoordsEntry = sections.find((s) => s.name === "arc_coords")!;
    expect(arcCoordsEntry.dtype).toBe("blob");
    expect(arcCoordsEntry.length).toBeGreaterThan(0);
    expect(arcOffsets[arcOffsets.length - 1]).toBe(arcCoordsEntry.length);
  });

  it("places front-loaded sections (CSR triples + arc_coords) before lazy properties", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { sections } = parseContainer(buf);
    // Front-loaded sections — the encoder marks these so a single
    // open-time prefetch GET can cover them. arc_coords is included
    // because its tier-ordered prefix holds the skeleton blocks.
    const frontLoadedNames = [
      "arc_offsets",
      "block/poly_offsets",
      "block/ring_offsets",
      "block/arc_refs",
      "arc_coords",
    ];
    const frontLoaded = sections.filter((s) =>
      frontLoadedNames.includes(s.name),
    );
    const lazy = sections.filter((s) => !frontLoadedNames.includes(s.name));
    const maxFrontOffset = Math.max(...frontLoaded.map((s) => s.offset));
    for (const s of lazy) {
      expect(s.offset).toBeGreaterThan(maxFrontOffset);
    }
  });

  it("rejects bad magic with a clear error", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const corrupted = Buffer.from(buf);
    corrupted.writeUInt32LE(0xdeadbeef, 0);
    expect(() => parseContainer(corrupted)).toThrow(/bad magic/);
  });

  it("rejects an incompatible major version at byte 8", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const corrupted = Buffer.from(buf);
    // Bump major version past what the parser accepts.
    corrupted.writeUInt32LE(0xff_00_00_00, 4);
    expect(() => parseContainer(corrupted)).toThrow(
      /incompatible major version/,
    );
  });

  it("reorders arcs by tier so coarser-layer boundaries land first", async () => {
    // Two-layer fixture: 4 base units arranged in a 2×2 grid grouped
    // into 2 top-layer parents (left half + right half). Arcs:
    //   0..3: top-layer outside edges  (count=1 in top arc_refs → tier 0)
    //   4..5: top-layer inner boundary (count=2 in top arc_refs → tier 1)
    //   6..7: base-only boundaries     (only in base arc_refs    → tier 2)
    //
    // Topology gives them out-of-order; encoder should pack tier 0 →
    // tier 1 → tier 2 in arc_coords.
    const topology: Topology = {
      type: "Topology",
      arcs: [
        // Use distinct, identifiable points so we can recover original
        // arc identity from the encoded coords.
        [
          [10, 0],
          [10, 1],
        ],
        [
          [20, 0],
          [20, 1],
        ],
        [
          [30, 0],
          [30, 1],
        ],
        [
          [40, 0],
          [40, 1],
        ],
        [
          [50, 0],
          [50, 1],
        ],
        [
          [60, 0],
          [60, 1],
        ],
        [
          [70, 0],
          [70, 1],
        ],
        [
          [80, 0],
          [80, 1],
        ],
      ],
      bbox: [0, 0, 100, 1],
      objects: {
        block: {
          type: "GeometryCollection",
          geometries: [
            { type: "Polygon", arcs: [[6, 0]], properties: { id: "A" } },
            { type: "Polygon", arcs: [[~6, 4, 1]], properties: { id: "B" } },
            { type: "Polygon", arcs: [[7, ~4, 2]], properties: { id: "C" } },
            { type: "Polygon", arcs: [[~7, 5, 3]], properties: { id: "D" } },
          ],
        },
        county: {
          type: "GeometryCollection",
          geometries: [
            { type: "Polygon", arcs: [[0, 4, 2, ~5]], properties: { id: "L" } },
            {
              type: "Polygon",
              arcs: [[~5, 1, 4, ~3]],
              properties: { id: "R" },
            },
          ],
        },
      },
    };
    const buf = await encodeContainer(topology);
    const { sections } = parseContainer(buf);

    // Walk arc_offsets to read each new-arc's first point and recover
    // the original arc index. arc_offsets is gzipped on disk.
    const arcOffsets = viewSectionAuto(
      buf,
      sections.find((s) => s.name === "arc_offsets")!,
      0,
    ) as Uint32Array;
    const coordsEntry = sections.find((s) => s.name === "arc_coords")!;
    const coordsBytes = viewSection(buf, coordsEntry, 0) as Uint8Array;
    const view = new DataView(
      coordsBytes.buffer,
      coordsBytes.byteOffset,
      coordsBytes.byteLength,
    );
    // Each point is 2×float64 = 16 bytes (no transform).
    const recoveredOrder: number[] = [];
    for (let newId = 0; newId < arcOffsets.length - 1; newId++) {
      const x = view.getFloat64(arcOffsets[newId], true);
      // x ∈ {10, 20, 30, 40, 50, 60, 70, 80} → original arc id is x/10 - 1
      recoveredOrder.push(x / 10 - 1);
    }
    // Within a tier, encoder sorts by first-encounter visit order
    // (walking county→block, geom→ring→arc). For this fixture:
    //   visit  : arc 0=0, 4=1, 2=2, 5=3, 1=4, 3=5, 6=6, 7=7
    //   tier 0 arcs (county outside-edges, count=1 in top): {0,1,2,3}
    //              sorted by visit → 0, 2, 1, 3
    //   tier 1 arcs (county interior, count=2):             {4,5}
    //              sorted by visit → 4, 5
    //   tier 2 arcs (block-only):                           {6,7}
    //              sorted by visit → 6, 7
    expect(recoveredOrder).toEqual([0, 2, 1, 3, 4, 5, 6, 7]);
  });

  it("emits tierByteOffsets so the client can size an exact skeleton prefetch", async () => {
    // Same 2-layer fixture as above. Tier 0 = 4 arcs, tier 1 = 2 arcs,
    // tier 2 = 2 arcs. No transform → float64 points = 16 bytes/pt,
    // and each arc has 2 points = 32 bytes/arc, so:
    //   tier 0 ends at byte 4 × 32 = 128
    //   tier 1 ends at byte 6 × 32 = 192
    //   tier 2 ends at byte 8 × 32 = 256
    const topology: Topology = {
      type: "Topology",
      arcs: [
        [
          [10, 0],
          [10, 1],
        ],
        [
          [20, 0],
          [20, 1],
        ],
        [
          [30, 0],
          [30, 1],
        ],
        [
          [40, 0],
          [40, 1],
        ],
        [
          [50, 0],
          [50, 1],
        ],
        [
          [60, 0],
          [60, 1],
        ],
        [
          [70, 0],
          [70, 1],
        ],
        [
          [80, 0],
          [80, 1],
        ],
      ],
      bbox: [0, 0, 100, 1],
      objects: {
        block: {
          type: "GeometryCollection",
          geometries: [
            { type: "Polygon", arcs: [[6, 0]], properties: { id: "A" } },
            { type: "Polygon", arcs: [[~6, 4, 1]], properties: { id: "B" } },
            { type: "Polygon", arcs: [[7, ~4, 2]], properties: { id: "C" } },
            { type: "Polygon", arcs: [[~7, 5, 3]], properties: { id: "D" } },
          ],
        },
        county: {
          type: "GeometryCollection",
          geometries: [
            { type: "Polygon", arcs: [[0, 4, 2, ~5]], properties: { id: "L" } },
            {
              type: "Polygon",
              arcs: [[~5, 1, 4, ~3]],
              properties: { id: "R" },
            },
          ],
        },
      },
    };
    const buf = await encodeContainer(topology);
    const { meta } = parseContainer(buf);
    expect(meta.tierByteOffsets).toEqual([128, 192, 256]);
  });

  it("delta-encodes cumulative-monotone u32 sections; reader undoes it", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { sections } = parseContainer(buf);
    const arcOffsetsEntry = sections.find((s) => s.name === "arc_offsets")!;
    const polyOffsetsEntry = sections.find(
      (s) => s.name === "block/poly_offsets",
    )!;
    const ringOffsetsEntry = sections.find(
      (s) => s.name === "block/ring_offsets",
    )!;
    const arcRefsEntry = sections.find((s) => s.name === "block/arc_refs")!;

    // Cumulative-monotone sections carry the delta flag; arc_refs
    // (signed CSR ids) does not.
    expect(arcOffsetsEntry.delta).toBe(true);
    expect(polyOffsetsEntry.delta).toBe(true);
    expect(ringOffsetsEntry.delta).toBe(true);
    expect(arcRefsEntry.delta).toBeFalsy();

    // viewSectionAuto undoes the delta — reconstructed values match
    // the encoder's input. (Cumulative byte positions for arc_offsets,
    // [0, 1, 2] / [0, 4, 8] for poly/ring offsets in the 2-block fixture.)
    const arcOffsets = viewSectionAuto(buf, arcOffsetsEntry, 0) as Uint32Array;
    expect(arcOffsets[0]).toBe(0);
    // Quantized arc_coords is varint-encoded; just verify
    // arc_offsets reconstructed monotonically (delta flag undid).
    for (let i = 1; i < arcOffsets.length; i++) {
      expect(arcOffsets[i]).toBeGreaterThan(arcOffsets[i - 1]);
    }
    expect(
      Array.from(viewSectionAuto(buf, polyOffsetsEntry, 0) as Uint32Array),
    ).toEqual([0, 1, 2]);
    expect(
      Array.from(viewSectionAuto(buf, ringOffsetsEntry, 0) as Uint32Array),
    ).toEqual([0, 4, 8]);
  });

  it("omits tierByteOffsets on single-layer topologies (no tier reordering)", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { meta } = parseContainer(buf);
    expect(meta.tierByteOffsets).toBeUndefined();
  });

  it("block-compresses arc_coords; tiny inputs skip dict training (sample threshold)", async () => {
    // Trained dict via `zstd --train` requires ≥100 samples (we
    // hard-coded MIN_SAMPLES). The 7-arc fixture only produces a
    // handful of blocks, so the encoder skips the dict entirely
    // and emits arcCoordsBlocks META without dictSection. Reader
    // back-compat path handles that.
    const buf = await encodeContainer(fixtureTopology(), {
      blockCompressArcCoords: true,
      arcCoordBlockBytes: 32,
    });
    const { meta, sections } = parseContainer(buf);

    expect(meta.arcCoordsBlocks).toBeDefined();
    expect(meta.arcCoordsBlocks?.dictSection).toBeUndefined();
    expect(meta.arcCoordsBlocks?.blockTableSection).toBe("arc_coord_blocks");
    expect(meta.arcCoordsBlocks?.targetBlockSize).toBe(32);
    expect(meta.arcCoordsBlocks!.blockCount).toBeGreaterThan(1);

    const blocksEntry = sections.find((s) => s.name === "arc_coord_blocks")!;
    expect(blocksEntry.compression).toBeUndefined();
    expect(sections.find((s) => s.name === "arc_coords_dict")).toBeUndefined();

    const blockTable = new Uint32Array(
      buf.buffer,
      buf.byteOffset + blocksEntry.offset,
      blocksEntry.length / 4,
    );
    const arcCoordsEntry = sections.find((s) => s.name === "arc_coords")!;
    const compressedArcCoords = buf.subarray(
      arcCoordsEntry.offset,
      arcCoordsEntry.offset + arcCoordsEntry.length,
    );
    // Decompress each standalone-frame block (no dict) and confirm
    // the recorded uncompressedEnd matches the actual decoded length.
    let cumUncEnd = 0;
    for (let i = 0; i < meta.arcCoordsBlocks!.blockCount; i++) {
      const uncEnd = blockTable[i * 3 + 0];
      const compOff = blockTable[i * 3 + 1];
      const compLen = blockTable[i * 3 + 2];
      const blockBytes = zstdDecompressSync(
        compressedArcCoords.subarray(compOff, compOff + compLen),
      );
      expect(blockBytes.byteLength).toBe(uncEnd - cumUncEnd);
      cumUncEnd = uncEnd;
    }
  });

  it("rewriteContainer swaps just the named property; other sections pass through byte-for-byte", async () => {
    const dir = mkdtempSync(join(tmpdir(), "ctopo-rewrite-"));
    const inPath = join(dir, "in.ctopo");
    const outPath = join(dir, "out.ctopo");
    writeFileSync(inPath, await encodeContainer(fixtureTopology()));

    await rewriteContainer(inPath, outPath, [
      { name: "block/population", data: [42, 99] },
    ]);

    const original = readFileSync(inPath);
    const rewritten = readFileSync(outPath);
    const { sections: origSections } = parseContainer(original);
    const { sections: newSections } = parseContainer(rewritten);

    // Population values updated.
    const popEntry = newSections.find((s) => s.name === "block/population")!;
    const pop = viewSectionAuto(rewritten, popEntry, 0) as Uint8Array;
    expect(Array.from(pop)).toEqual([42, 99]);

    // Every other section's decoded values unchanged. Group members
    // go through decompress→re-compress so compressed bytes may differ;
    // non-group sections pass through byte-for-byte.
    for (const newSec of newSections) {
      if (newSec.name === "block/population") continue;
      const origSec = origSections.find((s) => s.name === newSec.name)!;
      if (origSec.groupOffset === undefined) {
        // Non-group section: byte-identical pass-through.
        expect(newSec.length).toBe(origSec.length);
        const newBytes = rewritten.subarray(
          newSec.offset,
          newSec.offset + newSec.length,
        );
        const origBytes = original.subarray(
          origSec.offset,
          origSec.offset + origSec.length,
        );
        expect(Buffer.compare(newBytes, origBytes)).toBe(0);
      } else {
        // Group member: decoded values must match (compressed form may differ).
        const origValues = Array.from(viewSectionAuto(original, origSec, 0));
        const newValues = Array.from(viewSectionAuto(rewritten, newSec, 0));
        expect(newValues).toEqual(origValues);
      }
    }
  });

  it("rewriteContainer preserves decoded pass-through sections that share compression groups", async () => {
    const dir = mkdtempSync(join(tmpdir(), "ctopo-rewrite-groups-"));
    const inPath = join(dir, "in.ctopo");
    const outPath = join(dir, "out.ctopo");
    writeFileSync(inPath, await encodeContainer(fixtureTopology()));

    await rewriteContainer(inPath, outPath, [
      { name: "block/population", data: [42, 99] },
    ]);

    const original = readFileSync(inPath);
    const rewritten = readFileSync(outPath);
    const { sections: origSections } = parseContainer(original);
    const { sections: newSections } = parseContainer(rewritten);

    for (const name of [
      "arc_offsets",
      "block/poly_offsets",
      "block/ring_offsets",
      "block/arc_refs",
    ]) {
      const origEntry = origSections.find((s) => s.name === name)!;
      const newEntry = newSections.find((s) => s.name === name)!;
      const origValues = Array.from(viewSectionAuto(original, origEntry, 0));
      const newValues = Array.from(viewSectionAuto(rewritten, newEntry, 0));
      expect(newValues).toEqual(origValues);
    }

    const origIds = new StringArray(
      viewSectionAuto(
        original,
        origSections.find((s) => s.name === "block/id")!,
        0,
      ) as Uint8Array,
    );
    const newIds = new StringArray(
      viewSectionAuto(
        rewritten,
        newSections.find((s) => s.name === "block/id")!,
        0,
      ) as Uint8Array,
    );
    expect(Array.from(newIds)).toEqual(Array.from(origIds));
  });

  it("rewriteContainer preserves block-compressed arc_coords metadata", async () => {
    const dir = mkdtempSync(join(tmpdir(), "ctopo-rewrite-blocks-"));
    const inPath = join(dir, "in.ctopo");
    const outPath = join(dir, "out.ctopo");
    writeFileSync(
      inPath,
      await encodeContainer(fixtureTopology(), {
        blockCompressArcCoords: true,
        arcCoordBlockBytes: 32,
      }),
    );

    await rewriteContainer(inPath, outPath, [
      { name: "block/population", data: [42, 99] },
    ]);

    const { meta: origMeta } = parseContainer(readFileSync(inPath));
    const { meta: newMeta } = parseContainer(readFileSync(outPath));
    expect(newMeta.arcCoordsBlocks).toEqual(origMeta.arcCoordsBlocks);
  });

  it("round-trips zigzag-varint at signed-int32 boundaries", () => {
    const cases = [
      0, 1, -1, 63, 64, -64, -65, 127, -128, 8191, -8192, 1048575, -1048576,
      0x0fffffff, -0x10000000, 0x7fffffff, -0x80000000,
    ];
    const buf = new Uint8Array(10);
    for (const v of cases) {
      const end = writeVarintZigzag(v, buf, 0);
      const decoded = readVarintZigzag(buf, 0);
      expect(decoded.consumed).toBe(end);
      expect(decoded.value).toBe(v);
    }
  });
});
