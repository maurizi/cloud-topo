// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Covers the per-batch stitch → flat assembly: assembleArcsResult /
 * assembleCoordsResult derive polyRingStarts (one entry per non-empty
 * group) and pass the batch's ring offsets through unchanged, and
 * stitchBatchCoords sorts a group's rings largest-area-first.
 */

import { describe, expect, it } from "vitest";

import {
  assembleArcsResult,
  assembleCoordsResult,
  makeStitchScratch,
  ringAreaFlat,
  stitchBatchArcs,
  stitchBatchCoords,
  type EndpointLookup,
  type StitchBatchContext,
  type StitchBatchCoordsResult,
} from "../core/merge";

// Groups of signed arc ids. Each group is a self-contained set of
// closed rings (no nodes shared across groups), so stitch output per
// group is deterministic.
const groups: number[][] = [
  [0, 1, 2], // triangle → 1 ring
  [], // empty group → 0 rings (exercises the skip)
  [3, 4, 5, 6], // square → 1 ring
  [7, 8, 9, 10, 11, 12], // two disjoint triangles → 2 rings
  [13, 14, 15], // triangle → 1 ring
];

// arcId → [startNode, endNode]. Distinct node ranges per group.
const ends = new Map<number, [number, number]>([
  [0, [0, 1]],
  [1, [1, 2]],
  [2, [2, 0]],
  [3, [10, 11]],
  [4, [11, 12]],
  [5, [12, 13]],
  [6, [13, 10]],
  [7, [20, 21]],
  [8, [21, 22]],
  [9, [22, 20]],
  [10, [30, 31]],
  [11, [31, 32]],
  [12, [32, 30]],
  [13, [40, 41]],
  [14, [41, 42]],
  [15, [42, 40]],
]);

const lookup: EndpointLookup<number> = {
  start: (i) => (i >= 0 ? ends.get(i)![0] : ends.get(~i)![1]),
  end: (i) => (i >= 0 ? ends.get(i)![1] : ends.get(~i)![0]),
};

describe("assembleArcsResult", () => {
  it("passes ring offsets through and derives polyRingStarts", () => {
    const ctx: StitchBatchContext = {
      transform: null,
      endpoints: lookup,
      arcBytes: undefined,
    };
    const result = stitchBatchArcs(ctx, groups, makeStitchScratch());
    const flat = assembleArcsResult(result);

    // arcs / ring offsets are the batch's own arrays, unchanged.
    expect(flat.arcs).toBe(result.arcs);
    expect(flat.ringStarts).toBe(result.ringStarts);
    expect(flat.ringEnds).toBe(result.ringEnds);
    // 4 non-empty groups emitting 1,1,2,1 rings → cumulative boundaries.
    expect(Array.from(flat.polyRingStarts)).toEqual([0, 1, 2, 4, 5]);
  });
});

describe("assembleCoordsResult", () => {
  it("derives polyRingStarts and skips empty groups", () => {
    // One group with two 4-point rings, then an empty group.
    const result: StitchBatchCoordsResult = {
      coords: Float64Array.of(0, 0, 1, 0, 1, 1, 0, 0, 2, 0, 3, 0, 3, 1, 2, 0),
      ringStarts: Uint32Array.of(0, 4),
      ringEnds: Uint32Array.of(4, 8),
      numRingsPerGroup: Uint32Array.of(2, 0),
    };
    const flat = assembleCoordsResult(result);
    expect(flat.coords).toBe(result.coords);
    expect(flat.ringStarts).toBe(result.ringStarts);
    expect(flat.ringEnds).toBe(result.ringEnds);
    // One non-empty group with 2 rings.
    expect(Array.from(flat.polyRingStarts)).toEqual([0, 2]);
  });
});

describe("stitchBatchCoords area sort", () => {
  // Encode a ring's points as un-quantized f64 pairs (16 B/point). The
  // last point repeats the first: decodeRingFlat drops the shared-join
  // tail then re-closes, so the round-trip preserves all corners.
  function arcBytes(
    points: ReadonlyArray<readonly [number, number]>,
  ): Uint8Array {
    const buf = new Uint8Array(points.length * 16);
    const view = new DataView(buf.buffer);
    points.forEach(([x, y], i) => {
      view.setFloat64(i * 16, x, true);
      view.setFloat64(i * 16 + 8, y, true);
    });
    return buf;
  }

  it("orders a group's rings largest-area-first", () => {
    // Two self-closing arcs (start node == end node) → two rings in one
    // group. Fed small-first to prove the area sort reorders them.
    const big = arcBytes([
      [0, 0],
      [10, 0],
      [10, 10],
      [0, 10],
      [0, 0],
    ]);
    const small = arcBytes([
      [0, 0],
      [1, 0],
      [1, 1],
      [0, 1],
      [0, 0],
    ]);
    const bytes = new Map<number, Uint8Array>([
      [100, big],
      [101, small],
    ]);
    const selfClosing: EndpointLookup<number> = {
      // node id == arc id keeps each arc its own closed ring.
      start: (i) => (i >= 0 ? i : ~i),
      end: (i) => (i >= 0 ? i : ~i),
    };
    const ctx: StitchBatchContext = {
      transform: null,
      endpoints: selfClosing,
      arcBytes: bytes,
    };
    const res = stitchBatchCoords(ctx, [[101, 100]], makeStitchScratch());

    expect(Array.from(res.numRingsPerGroup)).toEqual([2]);
    // ringAreaFlat returns the raw shoelace sum (2x true area) — only
    // relative magnitude matters for the sort.
    const areas = [0, 1].map((k) =>
      ringAreaFlat(res.coords, res.ringStarts[k], res.ringEnds[k]),
    );
    expect(areas[0]).toBeCloseTo(200); // 10x10 square
    expect(areas[1]).toBeCloseTo(2); // 1x1 square
    expect(areas[0]).toBeGreaterThan(areas[1]);
  });
});
