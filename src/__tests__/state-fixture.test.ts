// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * End-to-end coverage on a real quantized state topology (full U.S.
 * Census TIGER Vermont: block / VTD / county layers, ~63K arcs). The
 * synthetic fixtures elsewhere are tiny and un-quantized; this exercises
 * the production-default paths against real geometry:
 *   - quantized merge / mergeArcs (delta-varint arcs, scale/translate),
 *   - the dedicated arc_endpoints section, partitioned into ~1.2K blocks,
 *     driven by whole-layer unions whose boundary arcs are scattered
 *     across those partitions (the block layer touches nearly all of
 *     them), and
 *   - cross-block arc_coords decode for the coords-emitting `merge`.
 *
 * The container is encoded from the committed `vt.topojson.gz` by
 * `scripts/build-test-fixtures.mjs`, which runs in `pretest` (so the
 * encode itself is an e2e smoke test of the encoder on real data). The
 * suite auto-skips if the .ctopo hasn't been built — e.g. `npx vitest`
 * without the pretest step. `npm test` always builds it first.
 */

import { existsSync, readFileSync } from "fs";
import { dirname, resolve as resolvePath } from "path";
import { fileURLToPath } from "url";

import { describe, expect, it } from "vitest";

import { CtopoCore } from "../core/client";
import { makeBufferFetcher, type RangeFetcher } from "../core/fetcher";
import { merge, mergeArcs } from "../core/merge";
import { readVarintZigzagInto, type VarintCursor } from "../core/format";

const ctopoPath = resolvePath(
  dirname(fileURLToPath(import.meta.url)),
  "fixtures",
  "vt.ctopo",
);

let cachedBuf: Uint8Array | undefined;
function fixtureBuf(): Uint8Array {
  if (cachedBuf === undefined)
    cachedBuf = new Uint8Array(readFileSync(ctopoPath));
  return cachedBuf;
}

// All geometry indices of a layer (e.g. every county in the state).
function allIndices(core: CtopoCore, layer: string): number[] {
  const l = core.meta.layers.find((x) => x.name === layer);
  if (l === undefined) throw new Error(`layer ${layer} not in fixture`);
  return [...Array(l.numGeometries).keys()];
}

// Decode an arc's first + last quantized point straight from its
// arc_coords delta-varint bytes — the ground truth the partitioned
// arc_endpoints section must agree with.
function endpointsFromBytes(
  bytes: Uint8Array,
): [number, number, number, number] {
  const cur: VarintCursor = { value: 0, off: 0 };
  let x = 0;
  let y = 0;
  let sx = 0;
  let sy = 0;
  let first = true;
  const end = bytes.byteLength;
  while (cur.off < end) {
    readVarintZigzagInto(bytes, cur);
    x += cur.value;
    readVarintZigzagInto(bytes, cur);
    y += cur.value;
    if (first) {
      sx = x;
      sy = y;
      first = false;
    }
  }
  return [sx, sy, x, y];
}

const describeIfBuilt = existsSync(ctopoPath) ? describe : describe.skip;

describeIfBuilt("ctopo real state topology (quantized, partitioned)", () => {
  it("encodes a quantized container with a partitioned arc_endpoints section", async () => {
    const core = await CtopoCore.openWith(makeBufferFetcher(fixtureBuf()));
    expect(core.transform).not.toBeNull(); // quantized
    // arc_endpoints emitted by default for quantized inputs, and this
    // subset has enough arcs to partition it into many blocks.
    expect(core.hasArcEndpointsSection()).toBe(true);
    expect(core.meta.arcEndpointsBlocks).toBeDefined();
    expect(core.meta.arcEndpointsBlocks!.blockCount).toBeGreaterThan(1);
    core.close();
  });

  it("mergeArcs over all counties yields the state outline via the arc_endpoints path", async () => {
    const core = await CtopoCore.openWith(makeBufferFetcher(fixtureBuf()));
    const counties = allIndices(core, "county");
    // The boundary arcs of a whole-state union are scattered across
    // many arc_endpoints partitions — this is the cross-partition fetch
    // the unit fixtures never reach.
    const result = await mergeArcs(core, [
      { layer: "county", indices: counties },
    ]);
    // A contiguous state unions to a single polygon.
    expect(result.arcs.length).toBe(1);
    // Every arc id is a valid signed reference into the container.
    const numArcs = core.meta.numArcs;
    for (const ring of result.arcs.flat(1)) {
      for (const signed of ring) {
        const id = signed >= 0 ? signed : ~signed;
        expect(id).toBeGreaterThanOrEqual(0);
        expect(id).toBeLessThan(numArcs);
      }
    }
    core.close();
  });

  it("merge over all counties decodes a closed outline within the bbox", async () => {
    const core = await CtopoCore.openWith(makeBufferFetcher(fixtureBuf()));
    const counties = allIndices(core, "county");
    const result = await merge(core, [{ layer: "county", indices: counties }]);
    expect(result.type).toBe("MultiPolygon");
    expect(result.coordinates.length).toBeGreaterThanOrEqual(1);
    const [minX, minY, maxX, maxY] = core.meta.bbox;
    const eps = 1e-6;
    for (const poly of result.coordinates) {
      for (const ring of poly) {
        // Closed.
        expect(ring[0]).toEqual(ring[ring.length - 1]);
        for (const [x, y] of ring) {
          expect(x).toBeGreaterThanOrEqual(minX - eps);
          expect(x).toBeLessThanOrEqual(maxX + eps);
          expect(y).toBeGreaterThanOrEqual(minY - eps);
          expect(y).toBeLessThanOrEqual(maxY + eps);
        }
      }
    }
    core.close();
  });

  it("mergeArcs over the whole block layer spans nearly every partition", async () => {
    // The block layer (~24.6K geometries referencing all ~63K arcs) is
    // the production workhorse and the strongest cross-partition case:
    // its boundary-arc union touches almost all ~1.2K arc_endpoints
    // partitions. Unioning every block still resolves to the single
    // contiguous state polygon.
    const core = await CtopoCore.openWith(makeBufferFetcher(fixtureBuf()));
    const blocks = allIndices(core, "block");
    const result = await mergeArcs(core, [{ layer: "block", indices: blocks }]);
    expect(result.arcs.length).toBe(1);
    const numArcs = core.meta.numArcs;
    const boundary = result.arcs.flat(1);
    for (const ring of boundary) {
      for (const signed of ring) {
        const id = signed >= 0 ? signed : ~signed;
        expect(id).toBeGreaterThanOrEqual(0);
        expect(id).toBeLessThan(numArcs);
      }
    }
    core.close();
  });

  it("falls back to single-range GETs when the server rejects multi-range", async () => {
    // Fetching arcs sampled across the whole id range hits disjoint
    // arc_coords blocks that the drain packs into multi-range requests —
    // the realistic trigger the tiny unit fixtures can't reach. Prefetch
    // is disabled so the reads happen on demand rather than being served
    // from the open-time bootstrap.
    const buf = fixtureBuf();
    const noPrefetch = {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
    };

    const control = await CtopoCore.openWith(
      makeBufferFetcher(buf),
      noPrefetch,
    );
    const numArcs = control.meta.numArcs;
    const step = Math.max(1, Math.floor(numArcs / 64));
    const sample: number[] = [];
    for (let id = 0; id < numArcs; id += step) sample.push(id);
    // Control fetcher implements multiRange (returns parts) — supported path.
    const expected = await control.fetchArcs(sample);
    control.close();

    // Fetcher whose multiRange always reports "unsupported", forcing the
    // single-range re-dispatch path (client.ts dispatchMultiRange).
    let multiRangeCalls = 0;
    let singleGets = 0;
    const inner = makeBufferFetcher(buf);
    const fetcher: RangeFetcher = {
      range: (start, end, signal) => {
        singleGets++;
        return inner.range(start, end, signal);
      },
      suffix: (length, signal) => inner.suffix(length, signal),
      multiRange: () => {
        multiRangeCalls++;
        return Promise.resolve({ kind: "unsupported" as const });
      },
    };

    const core = await CtopoCore.openWith(fetcher, noPrefetch);
    const got = await core.fetchArcs(sample);

    // The multi-range path was exercised and rejected at least once...
    expect(multiRangeCalls).toBeGreaterThanOrEqual(1);
    // ...then re-dispatched as individual GETs...
    expect(singleGets).toBeGreaterThan(0);
    // ...and still returned the same bytes as the supported path.
    expect(got).toEqual(expected);

    // multiRange is disabled after the first "unsupported", so a second
    // scattered fetch issues no further multiRange calls.
    const before = multiRangeCalls;
    await core.fetchArcs(sample.map((id) => (id + 1) % numArcs));
    expect(multiRangeCalls).toBe(before);
    core.close();
  });

  it("fetchArcEndpoints (partitioned) matches endpoints decoded from arc_coords", async () => {
    const core = await CtopoCore.openWith(makeBufferFetcher(fixtureBuf()));
    const numArcs = core.meta.numArcs;
    // Sample arcs spread across the whole id range so the fetch spans
    // many arc_endpoints partitions, then cross-check each against the
    // varint walk of its arc_coords bytes.
    const step = Math.max(1, Math.floor(numArcs / 64));
    const sample: number[] = [];
    for (let id = 0; id < numArcs; id += step) sample.push(id);

    const endpoints = await core.fetchArcEndpoints(sample);
    const coords = await core.fetchArcs(sample);
    expect(endpoints.size).toBe(sample.length);
    for (const id of sample) {
      const ep = endpoints.get(id)!;
      const bytes = coords.get(id)!;
      const [sx, sy, ex, ey] = endpointsFromBytes(bytes);
      expect([ep[0], ep[1], ep[2], ep[3]]).toEqual([sx, sy, ex, ey]);
    }
    core.close();
  });
});
