// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Fuzz guard for the stitchArcsPure fragment-growth optimization. The
 * optimized version stores prepended arcs in a lazily-allocated reverse
 * `prefix` (O(1) prepend) instead of Array.unshift (O(n)). This test
 * pins its output to a naive reference implementation — the original
 * unshift/array form — across many randomized arc sequences, so any
 * divergence in ring contents, ring order, or degenerate-arc handling
 * is caught.
 */

import { describe, expect, it } from "vitest";

import {
  makeNumericEndpointLookup,
  makeStitchScratch,
  stitchArcsPure,
  type EndpointLookup,
} from "../core/merge";

// Naive reference: the pre-optimization algorithm, verbatim, using a
// plain number[] per fragment with unshift/concat-style growth.
interface RefFragment extends Array<number> {
  start: number;
  end: number;
}

function stitchArcsReference(
  arcs: ReadonlyArray<number>,
  endpoints: EndpointLookup<number>,
): number[][] {
  const fragmentByStart = new Map<number, RefFragment>();
  const fragmentByEnd = new Map<number, RefFragment>();
  const stitched = new Set<number>();
  const fragments: number[][] = [];

  for (const i of arcs) {
    const startKey = endpoints.start(i);
    const endKey = endpoints.end(i);
    let f = fragmentByEnd.get(startKey);
    if (f !== undefined) {
      fragmentByEnd.delete(f.end);
      f.push(i);
      f.end = endKey;
      const g = fragmentByStart.get(endKey);
      if (g !== undefined) {
        fragmentByStart.delete(g.start);
        if (g !== f) for (let k = 0; k < g.length; k++) f.push(g[k]);
        f.end = g.end;
        fragmentByStart.set(f.start, f);
        fragmentByEnd.set(f.end, f);
      } else {
        fragmentByStart.set(f.start, f);
        fragmentByEnd.set(f.end, f);
      }
    } else {
      f = fragmentByStart.get(endKey);
      if (f !== undefined) {
        fragmentByStart.delete(f.start);
        f.unshift(i);
        f.start = startKey;
        const g = fragmentByEnd.get(startKey);
        if (g !== undefined) {
          fragmentByEnd.delete(g.end);
          if (g === f) {
            fragmentByStart.set(f.start, f);
            fragmentByEnd.set(f.end, f);
          } else {
            for (let k = 0; k < f.length; k++) g.push(f[k]);
            g.end = f.end;
            fragmentByStart.set(g.start, g);
            fragmentByEnd.set(g.end, g);
          }
        } else {
          fragmentByStart.set(f.start, f);
          fragmentByEnd.set(f.end, f);
        }
      } else {
        const fresh = [i] as RefFragment;
        fresh.start = startKey;
        fresh.end = endKey;
        fragmentByStart.set(startKey, fresh);
        fragmentByEnd.set(endKey, fresh);
      }
    }
  }

  const drain = (
    byEnd: Map<number, RefFragment>,
    byStart: Map<number, RefFragment>,
  ): void => {
    for (const f of byEnd.values()) {
      byStart.delete(f.start);
      for (const i of f) stitched.add(i < 0 ? ~i : i);
      fragments.push(f.slice());
    }
    byEnd.clear();
  };
  drain(fragmentByEnd, fragmentByStart);
  drain(fragmentByStart, fragmentByEnd);
  for (const i of arcs) {
    if (!stitched.has(i < 0 ? ~i : i)) fragments.push([i]);
  }
  return fragments;
}

// Deterministic LCG so failures reproduce.
function makeRng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s * 1103515245 + 12345) & 0x7fffffff;
    return s / 0x7fffffff;
  };
}

// Build a random arc set over a small node space (so chains link up
// and rings close), each arc with random endpoints, fed in random
// order with random orientation.
function randomCase(rng: () => number): {
  arcs: number[];
  endpoints: EndpointLookup<number>;
} {
  const numArcs = 1 + Math.floor(rng() * 40);
  const numNodes = 1 + Math.floor(rng() * 12);
  const ends = new Map<number, [number, number]>();
  const ids: number[] = [];
  for (let id = 0; id < numArcs; id++) {
    const a = Math.floor(rng() * numNodes);
    const b = Math.floor(rng() * numNodes);
    ends.set(id, [a, b]);
    ids.push(rng() < 0.5 ? id : ~id);
  }
  // Shuffle input order.
  for (let i = ids.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [ids[i], ids[j]] = [ids[j], ids[i]];
  }
  const endpoints: EndpointLookup<number> = {
    start: (i) => (i >= 0 ? ends.get(i)![0] : ends.get(~i)![1]),
    end: (i) => (i >= 0 ? ends.get(i)![1] : ends.get(~i)![0]),
  };
  return { arcs: ids, endpoints };
}

describe("stitchArcsPure", () => {
  it("matches the naive reference across randomized inputs", () => {
    const rng = makeRng(0xc0ffee);
    const scratch = makeStitchScratch();
    for (let t = 0; t < 5000; t++) {
      const { arcs, endpoints } = randomCase(rng);
      const got = stitchArcsPure(scratch, arcs, endpoints);
      const want = stitchArcsReference(arcs, endpoints);
      expect(got).toEqual(want);
    }
  });

  // Regression: the section-backed endpoint key (makeNumericEndpointLookup
  // → packCoord) used to pack two int32 coords into one JS Number as
  // `(x + 2^25) * 2^26 + (y + 2^25)`. That map is NOT injective once a
  // coordinate reaches 2^25, so on a fine grid (NATIONAL_QUANTIZATION =
  // 1e8 reaches ~1e8 per axis) distinct nodes could collide and silently
  // stitch unrelated rings together. packCoord now uses a BigInt key
  // (32 bits/axis) with no such ceiling.
  it("keeps distinct nodes separate when they collide under the old Number key", () => {
    // Two genuinely-different points that map to the SAME old Number key:
    // oldKey(x,y) = (x + 2^25) * 2^26 + (y + 2^25).
    const oldKey = (x: number, y: number): number =>
      (x + 2 ** 25) * 2 ** 26 + (y + 2 ** 25);
    const P: [number, number] = [1, 0];
    const Q: [number, number] = [0, 2 ** 26]; // 67108864 — past 2^25
    expect(oldKey(...P)).toBe(oldKey(...Q)); // the latent collision

    // Triangle A (arcs 0,1,2) closes through P; triangle B (arcs 3,4,5)
    // closes through Q. They share no real node, so a correct stitcher
    // returns two separate rings. Under the old key, P and Q collide and
    // the two triangles fuse.
    const A0: [number, number] = [10, 20];
    const A1: [number, number] = [30, 40];
    const B0: [number, number] = [50, 60];
    const B1: [number, number] = [70, 80];
    const ep = (s: [number, number], e: [number, number]): Int32Array =>
      Int32Array.of(s[0], s[1], e[0], e[1]);
    const endpoints = new Map<number, Int32Array>([
      [0, ep(A0, A1)],
      [1, ep(A1, P)],
      [2, ep(P, A0)],
      [3, ep(B0, B1)],
      [4, ep(B1, Q)],
      [5, ep(Q, B0)],
    ]);

    const lookup = makeNumericEndpointLookup(endpoints);
    // The BigInt key keeps P and Q distinct.
    expect(lookup.start(2)).not.toBe(lookup.end(4));

    const got = stitchArcsPure(makeStitchScratch(), [0, 1, 2, 3, 4, 5], lookup);
    const rings = got
      .map((r) => r.map((a) => (a < 0 ? ~a : a)).sort((m, n) => m - n))
      .sort((m, n) => m[0] - n[0]);
    expect(rings).toEqual([
      [0, 1, 2],
      [3, 4, 5],
    ]);
  });

  it("handles a long prepend-only chain (the unshift worst case)", () => {
    // Arc k connects node k → k+1; feeding them in reverse forces a
    // prepend at every step.
    const N = 300;
    const endpoints: EndpointLookup<number> = {
      start: (i) => (i >= 0 ? i : ~i + 1),
      end: (i) => (i >= 0 ? i + 1 : ~i),
    };
    const arcs = Array.from({ length: N }, (_, i) => N - 1 - i);
    const scratch = makeStitchScratch();
    const got = stitchArcsPure(scratch, arcs, endpoints);
    expect(got).toEqual(stitchArcsReference(arcs, endpoints));
    // One open chain: a single fragment in node order 0,1,...,N-1.
    expect(got).toHaveLength(1);
    expect(got[0]).toEqual(Array.from({ length: N }, (_, i) => i));
  });
});
