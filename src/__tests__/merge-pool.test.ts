// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Equivalence test for the merge pool. The pool fans the stitching
 * phase of mergeFlat / mergeArcsFlat across N compute workers; this
 * test asserts that the four returned typed arrays are byte-for-byte
 * identical to the single-worker (pool: null) path.
 *
 * Requires a built `dist/worker.js` (and `dist/merge-worker.js`) since
 * vitest doesn't load TS into worker_threads. Auto-skips when the
 * artifacts are missing (matches proxy.test.ts).
 */

import { existsSync } from "fs";
import { dirname, resolve as resolvePath } from "path";
import { fileURLToPath } from "url";

import { describe, expect, it } from "vitest";

import { type Topology } from "topojson-specification";

import { encodeContainer } from "../encode";
import { CtopoClient } from "../proxy/client";

const __filename = fileURLToPath(import.meta.url);
const distWorker = resolvePath(
  dirname(__filename),
  "..",
  "..",
  "dist",
  "worker.js",
);
const distMergeWorker = resolvePath(
  dirname(__filename),
  "..",
  "..",
  "dist",
  "merge-worker.js",
);
const workerUrlHref = `file://${distWorker}`;

// Topology with a quartet of detached polygons in a 4x1 strip. With
// multi-polygon-aware merge each polygon is its own connected
// component, so we exercise the partition-into-batches path with N=4
// groups across a configurable pool size.
function fourBlockTopology(): Topology {
  return {
    type: "Topology",
    arcs: [
      [
        [0, 0],
        [1, 0],
      ],
      [
        [1, 0],
        [1, 1],
      ],
      [
        [1, 1],
        [0, 1],
      ],
      [
        [0, 1],
        [0, 0],
      ],
      [
        [2, 0],
        [3, 0],
      ],
      [
        [3, 0],
        [3, 1],
      ],
      [
        [3, 1],
        [2, 1],
      ],
      [
        [2, 1],
        [2, 0],
      ],
      [
        [4, 0],
        [5, 0],
      ],
      [
        [5, 0],
        [5, 1],
      ],
      [
        [5, 1],
        [4, 1],
      ],
      [
        [4, 1],
        [4, 0],
      ],
      [
        [6, 0],
        [7, 0],
      ],
      [
        [7, 0],
        [7, 1],
      ],
      [
        [7, 1],
        [6, 1],
      ],
      [
        [6, 1],
        [6, 0],
      ],
    ],
    objects: {
      block: {
        type: "GeometryCollection",
        geometries: [
          { type: "Polygon", arcs: [[0, 1, 2, 3]], properties: { id: "A" } },
          { type: "Polygon", arcs: [[4, 5, 6, 7]], properties: { id: "B" } },
          { type: "Polygon", arcs: [[8, 9, 10, 11]], properties: { id: "C" } },
          {
            type: "Polygon",
            arcs: [[12, 13, 14, 15]],
            properties: { id: "D" },
          },
        ],
      },
    },
  };
}

// Same 4x1 strip as `fourBlockTopology`, with every x-coordinate
// shifted by `dx`. Arc ids stay identical (0..15) but the geometry
// differs, so a merge that mistakenly reads this container's bytes
// from the unshifted container produces visibly wrong coordinates.
function fourBlockTopologyShifted(dx: number): Topology {
  const base = fourBlockTopology();
  return {
    ...base,
    arcs: base.arcs.map((arc) => arc.map(([x, y]) => [x + dx, y])),
  };
}

// A DIFFERENT topology that also names its layer "block", but with a
// different arc/connectivity structure (3 arcs, one polygon) than
// fourBlockTopology (16 arcs, four polygons). Used to catch a CSR-cache
// that keys on layer name alone: if the process-wide pool reused one
// container's "block" CSR for the other, the merge would read the wrong
// arcRefs and produce wrong geometry (or throw on an out-of-range arc).
function triangleTopology(): Topology {
  return {
    type: "Topology",
    arcs: [
      [
        [0, 0],
        [4, 0],
      ],
      [
        [4, 0],
        [0, 4],
      ],
      [
        [0, 4],
        [0, 0],
      ],
    ],
    objects: {
      block: {
        type: "GeometryCollection",
        geometries: [
          { type: "Polygon", arcs: [[0, 1, 2]], properties: { id: "T" } },
        ],
      },
    },
  };
}

async function openWithPool(
  buf: Uint8Array,
  poolSize: number | null,
): Promise<CtopoClient> {
  // Fresh Uint8Array (independent ArrayBuffer) each call — the proxy
  // transfers the buffer to the worker at open time, leaving the
  // caller's view detached. Two opens on the same `buf` would step
  // on each other.
  const fresh = new Uint8Array(buf);
  return CtopoClient.open(fresh, {
    workerUrl: workerUrlHref,
    pool: poolSize === null ? null : { size: poolSize },
  });
}

const describeIfBuilt =
  existsSync(distWorker) && existsSync(distMergeWorker)
    ? describe
    : describe.skip;

describeIfBuilt("ctopo merge pool", () => {
  it("mergeArcsFlat with pool:4 matches pool:null byte-for-byte", async () => {
    const buf = await encodeContainer(fourBlockTopology());
    const a = await openWithPool(buf, null);
    const b = await openWithPool(buf, 4);
    try {
      const sel = [{ layer: "block", indices: [0, 1, 2, 3] }];
      const rA = await a.mergeArcsFlatWire(sel);
      const rB = await b.mergeArcsFlatWire(sel);
      // Compare each transfer'd buffer. Workers may emit ArrayBuffers
      // backed by different allocations; we compare bytes, not
      // identity.
      expect(new Int32Array(rA.arcs)).toEqual(new Int32Array(rB.arcs));
      expect(new Uint32Array(rA.ringStarts)).toEqual(
        new Uint32Array(rB.ringStarts),
      );
      expect(new Uint32Array(rA.ringEnds)).toEqual(
        new Uint32Array(rB.ringEnds),
      );
      expect(new Uint32Array(rA.polyRingStarts)).toEqual(
        new Uint32Array(rB.polyRingStarts),
      );
    } finally {
      await a.close();
      await b.close();
    }
  });

  it("mergeFlat with pool:4 matches pool:null byte-for-byte", async () => {
    const buf = await encodeContainer(fourBlockTopology());
    const a = await openWithPool(buf, null);
    const b = await openWithPool(buf, 4);
    try {
      const sel = [{ layer: "block", indices: [0, 1, 2, 3] }];
      const rA = await a.mergeFlatWire(sel);
      const rB = await b.mergeFlatWire(sel);
      expect(new Float64Array(rA.coords)).toEqual(new Float64Array(rB.coords));
      expect(new Uint32Array(rA.ringStarts)).toEqual(
        new Uint32Array(rB.ringStarts),
      );
      expect(new Uint32Array(rA.ringEnds)).toEqual(
        new Uint32Array(rB.ringEnds),
      );
      expect(new Uint32Array(rA.polyRingStarts)).toEqual(
        new Uint32Array(rB.polyRingStarts),
      );
    } finally {
      await a.close();
      await b.close();
    }
  });

  it("two containers sharing one pooled worker don't cross cores", async () => {
    // Regression: the pool is a process-wide singleton shared across
    // every client/container in a worker. A second client attached via
    // attachPort opens a DIFFERENT container (different bytes, same arc
    // ids) but shares the same MergePool. When both merge concurrently
    // their bulk-fetch requests must each hit their own core — if the
    // batch keyed only the first caller's core, the second client would
    // get the first container's arc bytes (wrong geometry or a missing-
    // bytes throw).
    const bufA = await encodeContainer(fourBlockTopology());
    const bufB = await encodeContainer(fourBlockTopologyShifted(100));
    const sel = [{ layer: "block", indices: [0, 1, 2, 3] }];

    // Baselines: each container merged on its own isolated pool.
    const baseA = await openWithPool(bufA, 4);
    const baseB = await openWithPool(bufB, 4);
    let expA, expB;
    try {
      expA = await baseA.mergeFlatWire(sel);
      expB = await baseB.mergeFlatWire(sel);
    } finally {
      await baseA.close();
      await baseB.close();
    }

    // Container A opens the worker + pool; container B attaches to the
    // same worker through a transferred port, so both share one pool.
    const a = await openWithPool(bufA, 4);
    const port = a.attachPort();
    const b = await CtopoClient.open(new Uint8Array(bufB), {
      port,
      pool: { size: 4 },
    });
    try {
      // Concurrent so both prep replies fall in the same bulk-fetch
      // batch window — the condition that triggers the cross-core bug.
      const [rA, rB] = await Promise.all([
        a.mergeFlatWire(sel),
        b.mergeFlatWire(sel),
      ]);
      expect(new Float64Array(rA.coords)).toEqual(
        new Float64Array(expA.coords),
      );
      expect(new Float64Array(rB.coords)).toEqual(
        new Float64Array(expB.coords),
      );
      // Sanity: the two containers really do differ, so the assertion
      // above isn't trivially satisfiable by returning either one.
      expect(new Float64Array(rA.coords)).not.toEqual(
        new Float64Array(expB.coords),
      );
    } finally {
      await b.close();
      await a.close();
    }
  });

  it("two containers with a same-named layer but different CSR don't cross", async () => {
    // Regression for the per-layer CSR cache. The pool is a process-wide
    // singleton; both containers name their layer "block" but have
    // different connectivity (4 squares vs 1 triangle). If the CSR cache
    // keyed on layer name alone, the second container's merge would
    // reuse the first's "block" CSR — reading the wrong arcRefs and
    // returning wrong geometry (or throwing on an out-of-range arc id).
    const bufSquares = await encodeContainer(fourBlockTopology());
    const bufTriangle = await encodeContainer(triangleTopology());

    // Isolated baselines.
    const baseSq = await openWithPool(bufSquares, 4);
    const baseTri = await openWithPool(bufTriangle, 4);
    let expSq, expTri;
    try {
      expSq = await baseSq.mergeFlatWire([
        { layer: "block", indices: [0, 1, 2, 3] },
      ]);
      expTri = await baseTri.mergeFlatWire([{ layer: "block", indices: [0] }]);
    } finally {
      await baseSq.close();
      await baseTri.close();
    }

    // Squares container opens the worker + pool; triangle attaches to
    // the same worker through a transferred port, so both share one
    // pool (and would share one CSR cache).
    const sq = await openWithPool(bufSquares, 4);
    const port = sq.attachPort();
    const tri = await CtopoClient.open(new Uint8Array(bufTriangle), {
      port,
      pool: { size: 4 },
    });
    try {
      const [rSq, rTri] = await Promise.all([
        sq.mergeFlatWire([{ layer: "block", indices: [0, 1, 2, 3] }]),
        tri.mergeFlatWire([{ layer: "block", indices: [0] }]),
      ]);
      expect(new Float64Array(rSq.coords)).toEqual(
        new Float64Array(expSq.coords),
      );
      expect(new Float64Array(rTri.coords)).toEqual(
        new Float64Array(expTri.coords),
      );
      // The two geometries genuinely differ, so a cross-up would fail
      // the assertions above rather than be masked.
      expect(rSq.coords.byteLength).not.toBe(rTri.coords.byteLength);
    } finally {
      await tri.close();
      await sq.close();
    }
  });

  it("pool:1 short-circuits to inline (no compute worker spawn)", async () => {
    // Sanity: pool size 1 should behave exactly like pool:null. This
    // exercises the inline fast path inside MergePool.runStitch.
    const buf = await encodeContainer(fourBlockTopology());
    const a = await openWithPool(buf, null);
    const b = await openWithPool(buf, 1);
    try {
      const sel = [{ layer: "block", indices: [0, 1, 2, 3] }];
      const rA = await a.mergeArcsFlatWire(sel);
      const rB = await b.mergeArcsFlatWire(sel);
      expect(new Int32Array(rA.arcs)).toEqual(new Int32Array(rB.arcs));
      expect(new Uint32Array(rA.polyRingStarts)).toEqual(
        new Uint32Array(rB.polyRingStarts),
      );
    } finally {
      await a.close();
      await b.close();
    }
  });
});
