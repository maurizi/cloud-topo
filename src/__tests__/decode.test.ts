// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

import { describe, expect, it } from "vitest";

import {
  type GeometryObject,
  type Properties,
  type Topology,
} from "topojson-specification";

import { decodeContainer, encodeContainer } from "../encode";
import { parseContainer } from "../core/reader";
import { VERSION_MAJOR } from "../core/format";

// Single-layer 2×1 grid: two unit squares sharing a vertical arc.
// Quantized so arcs encode as zigzag-varint Int32 deltas.
function gridTopology(): Topology {
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
            properties: { id: "L", population: 100, name: "Left", frac: 1.5 },
          },
          {
            type: "Polygon",
            arcs: [[4, 5, 6, ~1]],
            properties: { id: "R", population: 200, name: "Right", frac: 2.5 },
          },
        ],
      },
    },
  };
}

// One MultiPolygon (two disjoint closed rings) plus one ordinary Polygon,
// to exercise multi_poly_breaks reconstruction + nested properties.
function multiPolyTopology(): Topology {
  return {
    type: "Topology",
    arcs: [
      // closed square A
      [
        [0, 0],
        [1000, 0],
        [0, 1000],
        [-1000, 0],
        [0, -1000],
      ],
      // closed square B (disjoint from A)
      [
        [5000, 0],
        [1000, 0],
        [0, 1000],
        [-1000, 0],
        [0, -1000],
      ],
      // closed square C
      [
        [0, 5000],
        [1000, 0],
        [0, 1000],
        [-1000, 0],
        [0, -1000],
      ],
    ],
    transform: { scale: [1e-6, 1e-6], translate: [0, 0] },
    bbox: [0, 0, 1, 1],
    objects: {
      region: {
        type: "GeometryCollection",
        geometries: [
          {
            type: "MultiPolygon",
            arcs: [[[0]], [[1]]],
            properties: { id: "M", stats: { pop: 5, vap: 3 } },
          },
          {
            type: "Polygon",
            arcs: [[2]],
            properties: { id: "P", stats: { pop: 9, vap: 8 } },
          },
        ],
      },
    },
  };
}

// A topology with enough arcs to push arc_offsets past
// ARC_OFFSETS_PARTITION_MIN_BYTES (750 KiB → ~192k arcs at 4 B/arc), so
// the encoder ships arc_offsets in partitioned form. Geometry stays tiny:
// the encoder's arc reordering keeps every arc regardless of how many are
// referenced, so a handful of refs is enough — the full arc table still
// round-trips through the partition reconstruction.
function manyArcsTopology(numArcs: number): Topology {
  const arcs: number[][][] = new Array<number[][]>(numArcs);
  for (let i = 0; i < numArcs; i++) {
    // Delta-encoded integer points (TopoJSON quantized convention).
    arcs[i] = [
      [i % 1000, (i * 7) % 1000],
      [1 + (i % 5), -(1 + (i % 3))],
    ];
  }
  const mid = Math.floor(numArcs / 2);
  return {
    type: "Topology",
    arcs,
    transform: { scale: [1e-6, 1e-6], translate: [0, 0] },
    bbox: [0, 0, 1, 1],
    objects: {
      net: {
        type: "GeometryCollection",
        geometries: [
          {
            type: "Polygon",
            // Reference arcs spread across the id space.
            arcs: [[0, 1, mid, numArcs - 2, numArcs - 1]],
            properties: { id: "spread" },
          },
        ],
      },
    },
  };
}

// A topology whose single layer has enough arc references to push its
// arc_refs past ARC_REFS_PARTITION_MIN_BYTES (2 MiB) and across the 4 MiB
// per-partition target — so the encoder ships arc_refs in 2+ partitions.
// Few distinct arcs, but each referenced many times: the partition count
// keys off arc_refs byte size (4 B/ref), not the number of arcs.
function manyRefsTopology(
  distinctArcs: number,
  geomCount: number,
  refsPerGeom: number,
): Topology {
  const arcs = new Array<number[][]>(distinctArcs);
  for (let i = 0; i < distinctArcs; i++) {
    // First point's x is `i` — a unique per-arc tag (see taggedGeometry).
    arcs[i] = [
      [i, 0],
      [1, 1],
    ];
  }
  const geometries: GeometryObject<Properties>[] = [];
  for (let g = 0; g < geomCount; g++) {
    const ring = new Array<number>(refsPerGeom);
    for (let k = 0; k < refsPerGeom; k++) ring[k] = (g * 7 + k) % distinctArcs;
    geometries.push({
      type: "Polygon",
      arcs: [ring],
      properties: { id: `g${g}` },
    });
  }
  return {
    type: "Topology",
    arcs,
    transform: { scale: [1e-6, 1e-6], translate: [0, 0] },
    bbox: [0, 0, 1, 1],
    objects: { net: { type: "GeometryCollection", geometries } },
  };
}

// --- Arc-order-independent geometry comparison ---

function polygonsOf(geom: GeometryObject<Properties>): number[][][] {
  if (geom.type === "Polygon") return [geom.arcs];
  if (geom.type === "MultiPolygon") return geom.arcs;
  return [];
}

function arcAbs(topo: Topology, id: number): number[][] {
  const rev = id < 0;
  const arc = topo.arcs[rev ? ~id : id];
  let x = 0;
  let y = 0;
  const pts: number[][] = [];
  for (const p of arc) {
    x += p[0];
    y += p[1];
    pts.push([x, y]);
  }
  return rev ? pts.reverse() : pts;
}

function resolveRing(topo: Topology, ring: number[]): number[][] {
  const out: number[][] = [];
  ring.forEach((id, i) => {
    const pts = arcAbs(topo, id);
    (i > 0 ? pts.slice(1) : pts).forEach((p) => out.push(p));
  });
  return out;
}

// Resolve every layer/geometry/polygon/ring to absolute coordinate
// sequences — independent of arc id assignment / ordering.
function resolveAll(topo: Topology): Record<string, number[][][][]> {
  const out: Record<string, number[][][][]> = {};
  for (const [layer, obj] of Object.entries(topo.objects)) {
    if (obj.type !== "GeometryCollection") continue;
    out[layer] = obj.geometries.map((g) =>
      polygonsOf(g).flatMap((poly) =>
        poly.map((ring) => resolveRing(topo, ring)),
      ),
    );
  }
  return out;
}

function propsOf(
  topo: Topology,
): Record<string, Array<Properties | undefined>> {
  const out: Record<string, Array<Properties | undefined>> = {};
  for (const [layer, obj] of Object.entries(topo.objects)) {
    if (obj.type !== "GeometryCollection") continue;
    out[layer] = obj.geometries.map((g) => g.properties);
  }
  return out;
}

// Per-layer geometry with each arc id replaced by a remap-invariant tag:
// the referenced arc's first-point x, which manyRefsTopology makes unique
// per arc. Survives the encoder's arc-id renumbering (raw ids don't), and
// is far cheaper than resolving full coordinate sequences for fixtures
// with millions of references.
function taggedGeometry(topo: Topology): Record<string, number[][]> {
  const tagOf = (id: number): number => topo.arcs[id >= 0 ? id : ~id][0][0];
  const out: Record<string, number[][]> = {};
  for (const [layer, obj] of Object.entries(topo.objects)) {
    if (obj.type !== "GeometryCollection") continue;
    out[layer] = obj.geometries.map((g) =>
      polygonsOf(g).flatMap((poly) => poly.flatMap((ring) => ring.map(tagOf))),
    );
  }
  return out;
}

describe("decodeContainer", () => {
  it.each([
    ["grid", gridTopology],
    ["multiPolygon", multiPolyTopology],
  ])("round-trips %s geometry + properties", async (_name, make) => {
    const fixture = make();
    const buf = await encodeContainer(fixture, { compression: "zstd" });
    const decoded = decodeContainer(buf);

    // Re-encoded files carry the current (v2) major version.
    expect(parseContainer(buf).meta.version).toBe(VERSION_MAJOR);

    // Geometry is identical once resolved to absolute coordinates
    // (arc ids are reordered by the encoder, so we can't compare them
    // directly).
    expect(resolveAll(decoded)).toEqual(resolveAll(fixture));

    // Properties survive verbatim (booleans aside — none here).
    expect(propsOf(decoded)).toEqual(propsOf(fixture));

    // transform + bbox round-trip.
    expect(decoded.transform).toEqual(fixture.transform);
    expect(decoded.bbox).toEqual(fixture.bbox);
  });

  it("is idempotent across a second encode→decode", async () => {
    const fixture = multiPolyTopology();
    const once = decodeContainer(
      await encodeContainer(fixture, { compression: "zstd" }),
    );
    const twice = decodeContainer(
      await encodeContainer(once, { compression: "zstd" }),
    );
    // After the first canonicalizing encode, arcs + refs are stable,
    // so the second round trip reproduces the topology exactly.
    expect(twice).toEqual(once);
  });

  it("synthesizes v2 sections (arc_endpoints) on re-encode", async () => {
    const buf = await encodeContainer(gridTopology(), { compression: "zstd" });
    const { meta } = parseContainer(buf);
    expect(meta.arcEndpointsBlocks).toBeDefined();
  });

  it("round-trips partitioned arc_offsets (large topology)", async () => {
    // 200k arcs → arc_offsets ~800 KiB, over the partition threshold.
    const fixture = manyArcsTopology(200_000);
    const buf = await encodeContainer(fixture, { compression: "zstd" });

    // Confirm we actually exercised the partitioned arc_offsets path —
    // otherwise this would silently fall back to the monolithic decode.
    expect(parseContainer(buf).meta.arcOffsetsBlocks).toBeDefined();

    const decoded = decodeContainer(buf);

    // Every arc round-trips. Arc ids are reordered by the encoder, so
    // compare the arc *set* rather than position-by-position.
    const arcKey = (a: number[][]): string => JSON.stringify(a);
    expect(decoded.arcs.length).toBe(fixture.arcs.length);
    expect(decoded.arcs.map(arcKey).sort()).toEqual(
      fixture.arcs.map(arcKey).sort(),
    );

    // Referenced geometry + properties survive.
    expect(resolveAll(decoded)).toEqual(resolveAll(fixture));
    expect(propsOf(decoded)).toEqual(propsOf(fixture));
  }, 120_000);

  it("round-trips partitioned arc_refs (large layer)", async () => {
    // 4 geoms × 400k refs → arc_refs ~6.4 MiB, over the 4 MiB partition
    // target, so the layer's arc_refs ships in multiple partitions.
    const fixture = manyRefsTopology(2048, 4, 400_000);
    const buf = await encodeContainer(fixture, { compression: "zstd" });

    // Confirm the partitioned arc_refs path was actually exercised, with
    // more than one partition (so reassembly concatenates frames).
    const layerMeta = parseContainer(buf).meta.layers[0];
    expect(layerMeta.arcRefsBlocks).toBeDefined();
    expect(layerMeta.arcRefsBlocks?.blockCount).toBeGreaterThan(1);

    const decoded = decodeContainer(buf);

    // Every geometry's arc references round-trip (modulo the encoder's
    // arc-id renumbering, which taggedGeometry sees through).
    expect(taggedGeometry(decoded)).toEqual(taggedGeometry(fixture));
    expect(propsOf(decoded)).toEqual(propsOf(fixture));
  }, 120_000);
});
