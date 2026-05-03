// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

import { describe, expect, it } from "vitest";

import { type Topology } from "topojson-specification";

import { CtopoClient, makeBufferFetcher } from "../client";
import { encodeContainer } from "../encode";
import { merge, mergeArcs, neighbors } from "../merge";

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

describe("ctopo merge primitives", () => {
  it("neighbors returns each block's adjacent blocks via shared arcs", async () => {
    const buf = await encodeContainer(twoBlockTopology());
    const client = await CtopoClient.openWith(makeBufferFetcher(buf));
    const adj = await neighbors(client, "block");
    expect(adj).toEqual([[1], [0]]);
  });

  it("mergeArcs cancels the shared arc when both blocks are merged", async () => {
    const buf = await encodeContainer(twoBlockTopology());
    const client = await CtopoClient.openWith(makeBufferFetcher(buf));

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
    const client = await CtopoClient.openWith(makeBufferFetcher(buf));
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

  it("merging an empty selection returns an empty MultiPolygon", async () => {
    const buf = await encodeContainer(twoBlockTopology());
    const client = await CtopoClient.openWith(makeBufferFetcher(buf));
    const result = await merge(client, [{ layer: "block", indices: [] }]);
    expect(result.coordinates).toEqual([]);
  });
});
