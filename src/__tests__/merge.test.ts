// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

import { describe, expect, it } from "vitest";

import { type Topology } from "topojson-specification";

import { CtopoCore } from "../core/client";
import { makeBufferFetcher } from "../core/fetcher";
import { encodeContainer } from "../encode";
import { merge, mergeArcs, neighbors } from "../core/merge";

// Canonicalize a stitched ring's vertices: drop the closing duplicate
// and sort. Tests assert on vertex sets rather than exact coordinate
// sequences because cloud-topo's stitcher can pick any rotation as the
// ring start, while topojson-client's tests pin a specific traversal.
function ringVertexSet(ring: ReadonlyArray<ReadonlyArray<number>>): string[] {
  const last = ring[ring.length - 1];
  const first = ring[0];
  const usable =
    last[0] === first[0] && last[1] === first[1] ? ring.slice(0, -1) : ring;
  return usable.map((p) => `${p[0]},${p[1]}`).sort();
}

// Two unit squares sharing arc 1 (the vertical divider). Unquantized
// (no transform) so the assertions can compare exact float coords.
function twoBlockTopology(): Topology {
  return {
    type: "Topology",
    arcs: [
      [
        [0, 0],
        [1, 0],
      ], // 0 — bottom of L
      [
        [1, 0],
        [1, 1],
      ], // 1 — shared divider
      [
        [1, 1],
        [0, 1],
      ], // 2 — top of L
      [
        [0, 1],
        [0, 0],
      ], // 3 — left side of L
      [
        [1, 0],
        [2, 0],
      ], // 4 — bottom of R
      [
        [2, 0],
        [2, 1],
      ], // 5 — right side of R
      [
        [2, 1],
        [1, 1],
      ], // 6 — top of R
    ],
    bbox: [0, 0, 2, 1],
    objects: {
      block: {
        type: "GeometryCollection",
        geometries: [
          { type: "Polygon", arcs: [[0, 1, 2, 3]], properties: { id: "L" } },
          { type: "Polygon", arcs: [[4, 5, 6, ~1]], properties: { id: "R" } },
        ],
      },
    },
  };
}

// Quantized twin of twoBlockTopology: identical absolute geometry (two
// unit squares sharing the vertical divider arc 1), but with a
// `transform` so the encoder takes the delta-varint arc path AND emits
// the dedicated arc_endpoints section (default for quantized inputs).
// Arc coordinates are TopoJSON delta-encoded: first point absolute in
// quantization units, the rest deltas. The transform is identity so the
// decoded floats equal the absolute integer coords — letting these
// tests assert the exact same vertex sets as the un-quantized versions
// while exercising makeNumericEndpointLookup* + the quantized
// decodeRingFlat scale/translate walk end to end.
function quantizedTwoBlockTopology(): Topology {
  return {
    type: "Topology",
    arcs: [
      [
        [0, 0],
        [1, 0],
      ], // 0 — bottom of L: abs (0,0) then Δ(1,0) → (1,0)
      [
        [1, 0],
        [0, 1],
      ], // 1 — shared divider: abs (1,0) then Δ(0,1) → (1,1)
      [
        [1, 1],
        [-1, 0],
      ], // 2 — top of L: abs (1,1) then Δ(-1,0) → (0,1)
      [
        [0, 1],
        [0, -1],
      ], // 3 — left of L: abs (0,1) then Δ(0,-1) → (0,0)
      [
        [1, 0],
        [1, 0],
      ], // 4 — bottom of R: abs (1,0) then Δ(1,0) → (2,0)
      [
        [2, 0],
        [0, 1],
      ], // 5 — right of R: abs (2,0) then Δ(0,1) → (2,1)
      [
        [2, 1],
        [-1, 0],
      ], // 6 — top of R: abs (2,1) then Δ(-1,0) → (1,1)
    ],
    bbox: [0, 0, 2, 1],
    transform: { scale: [1, 1], translate: [0, 0] },
    objects: {
      block: {
        type: "GeometryCollection",
        geometries: [
          { type: "Polygon", arcs: [[0, 1, 2, 3]], properties: { id: "L" } },
          { type: "Polygon", arcs: [[4, 5, 6, ~1]], properties: { id: "R" } },
        ],
      },
    },
  };
}

describe("ctopo merge primitives", () => {
  it("neighbors returns each block's adjacent blocks via shared arcs", async () => {
    const buf = await encodeContainer(twoBlockTopology());
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const adj = await neighbors(client, "block");
    expect(adj).toEqual([[1], [0]]);
  });

  it("mergeArcs cancels the shared arc when both blocks are merged", async () => {
    const buf = await encodeContainer(twoBlockTopology(), {
      spatialSort: "hilbert",
    });
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));

    // Single-block — should keep all four of its arcs.
    const single = await mergeArcs(client, [{ layer: "block", indices: [0] }]);
    expect(single.arcs.flat(2).sort()).toEqual([0, 1, 2, 3]);

    // Both blocks — arc 1 (and its reverse ~1) cancel; only the 6
    // outer arcs remain.
    const both = await mergeArcs(client, [{ layer: "block", indices: [0, 1] }]);
    const remaining = both.arcs
      .flat(2)
      .map((s) => (s >= 0 ? s : ~s))
      .sort();
    expect(remaining).toEqual([0, 2, 3, 4, 5, 6]);
  });

  it("merge returns the decoded outer ring as a closed MultiPolygon", async () => {
    const buf = await encodeContainer(twoBlockTopology());
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const result = await merge(client, [{ layer: "block", indices: [0, 1] }]);

    expect(result.type).toBe("MultiPolygon");
    expect(result.coordinates.length).toBe(1);
    const ring = result.coordinates[0][0];
    // Closed.
    expect(ring[0]).toEqual(ring[ring.length - 1]);
    // Vertices are exactly the corners of the merged 2×1 rectangle.
    const sorted = ring
      .slice(0, -1)
      .map((p) => `${p[0]},${p[1]}`)
      .sort();
    expect(sorted).toEqual(["0,0", "0,1", "1,0", "1,1", "2,0", "2,1"].sort());
  });

  it("mergeArcs on a quantized container cancels the shared arc (arc_endpoints path)", async () => {
    const buf = await encodeContainer(quantizedTwoBlockTopology());
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    // Quantized inputs emit the dedicated arc_endpoints section by
    // default — this is the production-default path the un-quantized
    // fixtures never reach.
    expect(client.hasArcEndpointsSection()).toBe(true);

    // Encoder-assigned arc ids depend on the spatial sort, so assert
    // structure rather than specific ids. Single block: one ring of its
    // 4 boundary arcs.
    const single = await mergeArcs(client, [{ layer: "block", indices: [0] }]);
    expect(single.arcs.flat(2).length).toBe(4);

    // Both blocks: the shared divider (and its reverse) cancel, leaving
    // the 6 outer arcs — not 4 + 4 = 8.
    const both = await mergeArcs(client, [{ layer: "block", indices: [0, 1] }]);
    const remaining = both.arcs.flat(2).map((s) => (s >= 0 ? s : ~s));
    expect(remaining.length).toBe(6);
    expect(new Set(remaining).size).toBe(6);
  });

  it("merge on a quantized container decodes the outer ring with correct coords", async () => {
    const buf = await encodeContainer(quantizedTwoBlockTopology());
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const result = await merge(client, [{ layer: "block", indices: [0, 1] }]);

    expect(result.type).toBe("MultiPolygon");
    expect(result.coordinates.length).toBe(1);
    const ring = result.coordinates[0][0];
    expect(ring[0]).toEqual(ring[ring.length - 1]); // closed
    // Identity transform → decoded floats equal the absolute integer
    // coords: exactly the corners of the merged 2×1 rectangle. A bug in
    // the quantized scale/translate varint walk would shift these.
    const sorted = ring
      .slice(0, -1)
      .map((p) => `${p[0]},${p[1]}`)
      .sort();
    expect(sorted).toEqual(["0,0", "0,1", "1,0", "1,1", "2,0", "2,1"].sort());
  });

  it("merging an empty selection returns an empty MultiPolygon", async () => {
    const buf = await encodeContainer(twoBlockTopology());
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const result = await merge(client, [{ layer: "block", indices: [] }]);
    expect(result.coordinates).toEqual([]);
  });

  it("merging two disconnected polygons yields two separate polygons", async () => {
    // Two unit squares separated by empty space (no shared boundary).
    // Correct GeoJSON: a MultiPolygon with two distinct polygons,
    // not one polygon with the second square as a "hole".
    const topology: Topology = {
      type: "Topology",
      arcs: [
        [
          [0, 0],
          [1, 0],
          [1, 1],
          [0, 1],
          [0, 0],
        ],
        [
          [3, 0],
          [4, 0],
          [4, 1],
          [3, 1],
          [3, 0],
        ],
      ],
      bbox: [0, 0, 4, 1],
      objects: {
        block: {
          type: "GeometryCollection",
          geometries: [
            { type: "Polygon", arcs: [[0]] },
            { type: "Polygon", arcs: [[1]] },
          ],
        },
      },
    };
    const buf = await encodeContainer(topology);
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const result = await merge(client, [{ layer: "block", indices: [0, 1] }]);

    expect(result.type).toBe("MultiPolygon");
    expect(result.coordinates.length).toBe(2);
    // Each polygon should have exactly one ring (its outer), no holes.
    expect(result.coordinates[0].length).toBe(1);
    expect(result.coordinates[1].length).toBe(1);
  });

  // Ported from topojson-client/test/merge-test.js — covers
  // hole/exterior cancellation when a separately-input polygon fills
  // an outer polygon's hole.
  //
  // +-----------+            +-----------+
  // |           |            |           |
  // |   +---+   |    ==>     |           |
  // |   |   |   |            |           |
  // |   +---+   |            |           |
  // |           |            |           |
  // +-----------+            +-----------+
  it("merge stitches together a polygon surrounding another polygon", async () => {
    const topology: Topology = {
      type: "Topology",
      arcs: [
        [
          [0, 0],
          [0, 3],
          [3, 3],
          [3, 0],
          [0, 0],
        ],
        [
          [1, 1],
          [2, 1],
          [2, 2],
          [1, 2],
          [1, 1],
        ],
      ],
      bbox: [0, 0, 3, 3],
      objects: {
        block: {
          type: "GeometryCollection",
          geometries: [
            { type: "Polygon", arcs: [[0], [1]] },
            { type: "Polygon", arcs: [[~1]] },
          ],
        },
      },
    };
    const buf = await encodeContainer(topology);
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const result = await merge(client, [{ layer: "block", indices: [0, 1] }]);

    expect(result.type).toBe("MultiPolygon");
    // One polygon, no hole — arc 1 is shared between both inputs and cancels.
    expect(result.coordinates.length).toBe(1);
    expect(result.coordinates[0].length).toBe(1);
    expect(ringVertexSet(result.coordinates[0][0])).toEqual(
      ["0,0", "0,3", "3,0", "3,3"].sort(),
    );
  });

  // Ported from topojson-client. Two side-by-side polygons each with
  // their own hole — merging cancels the shared vertical and keeps both
  // holes inside the combined exterior.
  //
  // +-----------+-----------+            +-----------+-----------+
  // |           |           |            |                       |
  // |   +---+   |   +---+   |    ==>     |   +---+       +---+   |
  // |   |   |   |   |   |   |            |   |   |       |   |   |
  // |   +---+   |   +---+   |            |   +---+       +---+   |
  // |           |           |            |                       |
  // +-----------+-----------+            +-----------+-----------+
  it("merge stitches together two side-by-side polygons with holes", async () => {
    const topology: Topology = {
      type: "Topology",
      arcs: [
        [
          [3, 3],
          [3, 0],
        ],
        [
          [3, 0],
          [0, 0],
          [0, 3],
          [3, 3],
        ],
        [
          [1, 1],
          [2, 1],
          [2, 2],
          [1, 2],
          [1, 1],
        ],
        [
          [3, 3],
          [6, 3],
          [6, 0],
          [3, 0],
        ],
        [
          [4, 1],
          [5, 1],
          [5, 2],
          [4, 2],
          [4, 1],
        ],
      ],
      bbox: [0, 0, 6, 3],
      objects: {
        block: {
          type: "GeometryCollection",
          geometries: [
            { type: "Polygon", arcs: [[0, 1], [2]] },
            { type: "Polygon", arcs: [[~0, 3], [4]] },
          ],
        },
      },
    };
    const buf = await encodeContainer(topology);
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const result = await merge(client, [{ layer: "block", indices: [0, 1] }]);

    expect(result.coordinates.length).toBe(1);
    // Outer + 2 holes.
    expect(result.coordinates[0].length).toBe(3);
    // Largest area (outer) is first; the two holes follow in some order.
    expect(ringVertexSet(result.coordinates[0][0])).toEqual(
      ["0,0", "0,3", "3,0", "3,3", "6,0", "6,3"].sort(),
    );
    const holeSets = [
      ringVertexSet(result.coordinates[0][1]),
      ringVertexSet(result.coordinates[0][2]),
    ];
    const hole1 = ["1,1", "2,1", "2,2", "1,2"].sort();
    const hole2 = ["4,1", "5,1", "5,2", "4,2"].sort();
    expect(holeSets).toContainEqual(hole1);
    expect(holeSets).toContainEqual(hole2);
  });

  // Ported from topojson-client. Two horseshoe polygons that together
  // enclose a 2×1 interior — the merged result has an outer ring AND a
  // newly emergent hole that didn't exist as a hole in either input.
  //
  // +-------+-------+            +-------+-------+
  // |       |       |            |               |
  // |   +---+---+   |    ==>     |   +---+---+   |
  // |   |       |   |            |   |       |   |
  // |   +---+---+   |            |   +---+---+   |
  // |       |       |            |               |
  // +-------+-------+            +-------+-------+
  it("merge stitches together two horseshoe polygons, creating a hole", async () => {
    const topology: Topology = {
      type: "Topology",
      arcs: [
        [
          [2, 3],
          [2, 2],
        ],
        [
          [2, 2],
          [1, 2],
          [1, 1],
          [2, 1],
        ],
        [
          [2, 1],
          [2, 0],
        ],
        [
          [2, 0],
          [0, 0],
          [0, 3],
          [2, 3],
        ],
        [
          [2, 1],
          [3, 1],
          [3, 2],
          [2, 2],
        ],
        [
          [2, 3],
          [4, 3],
          [4, 0],
          [2, 0],
        ],
      ],
      bbox: [0, 0, 4, 3],
      objects: {
        block: {
          type: "GeometryCollection",
          geometries: [
            { type: "Polygon", arcs: [[0, 1, 2, 3]] },
            { type: "Polygon", arcs: [[~2, 4, ~0, 5]] },
          ],
        },
      },
    };
    const buf = await encodeContainer(topology);
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const result = await merge(client, [{ layer: "block", indices: [0, 1] }]);

    expect(result.coordinates.length).toBe(1);
    expect(result.coordinates[0].length).toBe(2);
    // Outer 4×3 rectangle.
    expect(ringVertexSet(result.coordinates[0][0])).toEqual(
      ["0,0", "0,3", "2,0", "2,3", "4,0", "4,3"].sort(),
    );
    // Inner 2×1 hole.
    expect(ringVertexSet(result.coordinates[0][1])).toEqual(
      ["1,1", "1,2", "2,1", "2,2", "3,1", "3,2"].sort(),
    );
  });

  // Ported from topojson-client. Two horseshoes plus the two interior
  // polygons that fill the would-be hole — when all four are merged,
  // every interior arc cancels and only the outer boundary remains.
  //
  // +-------+-------+            +-------+-------+
  // |       |       |            |               |
  // |   +---+---+   |    ==>     |               |
  // |   |   |   |   |            |               |
  // |   +---+---+   |            |               |
  // |       |       |            |               |
  // +-------+-------+            +-------+-------+
  it("merge stitches together two horseshoe polygons surrounding two other polygons", async () => {
    const topology: Topology = {
      type: "Topology",
      arcs: [
        [
          [2, 3],
          [2, 2],
        ],
        [
          [2, 2],
          [1, 2],
          [1, 1],
          [2, 1],
        ],
        [
          [2, 1],
          [2, 0],
        ],
        [
          [2, 0],
          [0, 0],
          [0, 3],
          [2, 3],
        ],
        [
          [2, 1],
          [3, 1],
          [3, 2],
          [2, 2],
        ],
        [
          [2, 3],
          [4, 3],
          [4, 0],
          [2, 0],
        ],
        [
          [2, 2],
          [2, 1],
        ],
      ],
      bbox: [0, 0, 4, 3],
      objects: {
        block: {
          type: "GeometryCollection",
          geometries: [
            { type: "Polygon", arcs: [[0, 1, 2, 3]] },
            { type: "Polygon", arcs: [[~2, 4, ~0, 5]] },
            { type: "Polygon", arcs: [[6, ~1]] },
            { type: "Polygon", arcs: [[~6, ~4]] },
          ],
        },
      },
    };
    const buf = await encodeContainer(topology);
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const result = await merge(client, [
      { layer: "block", indices: [0, 1, 2, 3] },
    ]);

    // One polygon, one ring — every interior arc canceled.
    expect(result.coordinates.length).toBe(1);
    expect(result.coordinates[0].length).toBe(1);
    expect(ringVertexSet(result.coordinates[0][0])).toEqual(
      ["0,0", "0,3", "2,0", "2,3", "4,0", "4,3"].sort(),
    );
  });

  it("merging an island-in-lake feature preserves nested polygon structure", async () => {
    // A single MultiPolygon feature with island-in-lake topology:
    //   - outer landmass (10x10) with a square lake (6x6) cut out
    //   - an island (2x2) inside the lake
    // Correct GeoJSON: MultiPolygon with TWO polygons — the outer
    // landmass [outer, lake_hole], and the island [island_outer].
    const topology: Topology = {
      type: "Topology",
      arcs: [
        [
          [0, 0],
          [10, 0],
          [10, 10],
          [0, 10],
          [0, 0],
        ],
        [
          [2, 2],
          [2, 8],
          [8, 8],
          [8, 2],
          [2, 2],
        ],
        [
          [4, 4],
          [6, 4],
          [6, 6],
          [4, 6],
          [4, 4],
        ],
      ],
      bbox: [0, 0, 10, 10],
      objects: {
        land: {
          type: "GeometryCollection",
          geometries: [
            {
              type: "MultiPolygon",
              arcs: [[[0], [1]], [[2]]],
            },
          ],
        },
      },
    };
    const buf = await encodeContainer(topology);
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const result = await merge(client, [{ layer: "land", indices: [0] }]);

    expect(result.type).toBe("MultiPolygon");
    // Two polygons: outer-with-hole, and the island.
    expect(result.coordinates.length).toBe(2);

    // One polygon should have 2 rings (outer + hole), the other 1 ring.
    const ringCounts = result.coordinates.map((p) => p.length).sort();
    expect(ringCounts).toEqual([1, 2]);
  });

  // Ported from topojson-client/test/neighbors-test.js — three-polygon
  // case where two share an arc (and the second uses its reversed form)
  // while the third is geometrically isolated. Exercises asymmetric
  // adjacency (some entries are empty) and reversed-arc sharing.
  //
  // A-----B     G
  // |     |\    |\
  // |     | \   | \
  // D-----C  E  I--H
  //          |
  //          F
  it("neighbors handles asymmetric adjacency with isolated polygons", async () => {
    const topology: Topology = {
      type: "Topology",
      arcs: [
        [
          [1, 0],
          [1, 1],
        ],
        [
          [1, 1],
          [0, 1],
          [0, 0],
          [1, 0],
        ],
        [
          [1, 0],
          [2, 0],
          [2, 1],
          [1, 1],
        ],
        [
          [3, 0],
          [4, 1],
          [3, 1],
          [3, 0],
        ],
      ],
      bbox: [0, 0, 4, 1],
      objects: {
        block: {
          type: "GeometryCollection",
          geometries: [
            { type: "Polygon", arcs: [[0, 1]] },
            { type: "Polygon", arcs: [[2, ~0]] },
            { type: "Polygon", arcs: [[3]] },
          ],
        },
      },
    };
    const buf = await encodeContainer(topology);
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const adj = await neighbors(client, "block");
    expect(adj).toEqual([[1], [0], []]);
  });

  // Sort-order check with a "plus" config — the center polygon shares
  // a distinct arc with each of N, S, E (indices 1, 2, 3). Encoder/CSR
  // doesn't visit those arcs in index order, so the result must come
  // back sorted ([1, 2, 3]) rather than visit-ordered.
  //
  //     +---+
  //     | N |
  //     +---+
  // +---+---+---+
  // | (W?) C | E |   (no west neighbor — only N, S, E share arcs with C)
  // +---+---+---+
  //     +---+
  //     | S |
  //     +---+
  it("neighbors returns multi-neighbor lists sorted by geometry index", async () => {
    const topology: Topology = {
      type: "Topology",
      arcs: [
        [
          [1, 1],
          [2, 1],
        ], // 0 — shared C/S
        [
          [2, 1],
          [2, 2],
        ], // 1 — shared C/E
        [
          [2, 2],
          [1, 2],
        ], // 2 — shared C/N
        [
          [1, 2],
          [1, 1],
        ], // 3 — C's left edge (no neighbor on this side)
        [
          [1, 0],
          [2, 0],
          [2, 1],
        ], // 4 — S's outer L
        [
          [1, 1],
          [1, 0],
        ], // 5 — S's left edge
        [
          [2, 2],
          [2, 3],
          [1, 3],
          [1, 2],
        ], // 6 — N's outer L
        [
          [2, 1],
          [3, 1],
          [3, 2],
          [2, 2],
        ], // 7 — E's outer L
      ],
      bbox: [0, 0, 3, 3],
      objects: {
        block: {
          type: "GeometryCollection",
          geometries: [
            // C at (1,1)-(2,2)
            { type: "Polygon", arcs: [[0, 1, 2, 3]] },
            // N at (1,2)-(2,3)
            { type: "Polygon", arcs: [[~2, 6]] },
            // S at (1,0)-(2,1)
            { type: "Polygon", arcs: [[4, ~0, 5]] },
            // E at (2,1)-(3,2)
            { type: "Polygon", arcs: [[7, ~1]] },
          ],
        },
      },
    };
    const buf = await encodeContainer(topology);
    const client = await CtopoCore.openWith(makeBufferFetcher(buf));
    const adj = await neighbors(client, "block");
    expect(adj).toEqual([[1, 2, 3], [0], [0], [0]]);
  });
});
