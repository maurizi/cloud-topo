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
      [[0, 0], [1, 0]],
      [[1, 0], [1, 1]],
      [[1, 1], [0, 1]],
      [[0, 1], [0, 0]],
      [[2, 0], [3, 0]],
      [[3, 0], [3, 1]],
      [[3, 1], [2, 1]],
      [[2, 1], [2, 0]],
      [[4, 0], [5, 0]],
      [[5, 0], [5, 1]],
      [[5, 1], [4, 1]],
      [[4, 1], [4, 0]],
      [[6, 0], [7, 0]],
      [[7, 0], [7, 1]],
      [[7, 1], [6, 1]],
      [[6, 1], [6, 0]],
    ],
    objects: {
      block: {
        type: "GeometryCollection",
        geometries: [
          { type: "Polygon", arcs: [[0, 1, 2, 3]], properties: { id: "A" } },
          { type: "Polygon", arcs: [[4, 5, 6, 7]], properties: { id: "B" } },
          { type: "Polygon", arcs: [[8, 9, 10, 11]], properties: { id: "C" } },
          { type: "Polygon", arcs: [[12, 13, 14, 15]], properties: { id: "D" } },
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
  return CtopoClient.open("buffer://test", {
    workerUrl: workerUrlHref,
    fetcher: { kind: "buffer", bytes: fresh },
    pool: poolSize === null ? null : { size: poolSize },
  });
}

const describeIfBuilt =
  existsSync(distWorker) && existsSync(distMergeWorker) ? describe : describe.skip;

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
