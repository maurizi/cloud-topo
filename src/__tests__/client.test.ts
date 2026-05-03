// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

import { describe, expect, it } from "vitest";

import { type Topology } from "topojson-specification";

import { CtopoClient, makeBufferFetcher, type RangeFetcher } from "../client";
import { encodeContainer } from "../encode";

function fixtureTopology(): Topology {
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
    ],
    bbox: [0, 0, 1, 1],
    objects: {
      block: {
        type: "GeometryCollection",
        geometries: [
          {
            type: "Polygon",
            arcs: [[0, 1, 2, 3]],
            properties: { id: "B", population: 100 },
          },
        ],
      },
    },
  };
}

describe("CtopoClient", () => {
  // Buffer-backed fetcher with call counting — wraps makeBufferFetcher
  // so tests can also assert "how many GETs did this drain trigger".
  function fetcherFor(buf: Buffer): {
    fetcher: RangeFetcher;
    calls: () => number;
    reset: () => void;
  } {
    let n = 0;
    const inner = makeBufferFetcher(buf);
    return {
      fetcher: {
        range: (start, end, signal) => {
          n++;
          return inner.range(start, end, signal);
        },
        suffix: (length, signal) => {
          n++;
          return inner.suffix(length, signal);
        },
      },
      calls: () => n,
      reset: () => {
        n = 0;
      },
    };
  }

  // Buffer-backed fetcher that records every range request. Tests use
  // this to assert exact byte-range patterns (which sections coalesced,
  // how many GETs fired, etc.). Suffix fetches aren't recorded since
  // they're a single open-time fixture; tests that care about GET counts
  // either reset() before the body or assert "no extras" inclusively.
  function recordingFetcher(buf: Buffer): {
    fetcher: RangeFetcher;
    requests: Array<[number, number]>;
  } {
    const requests: Array<[number, number]> = [];
    const inner = makeBufferFetcher(buf);
    return {
      fetcher: {
        range: (start, end, signal) => {
          requests.push([start, end]);
          return inner.range(start, end, signal);
        },
        suffix: (length, signal) => inner.suffix(length, signal),
      },
      requests,
    };
  }

  it("opens a container and exposes meta", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher } = fetcherFor(buf);
    const client = await CtopoClient.openWith(fetcher);

    expect(client.meta.numArcs).toBe(4);
    expect(client.meta.layers[0].numGeometries).toBe(1);
  });

  it("dedupes concurrent property() calls into a single fetch", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, calls, reset } = fetcherFor(buf);
    const client = await CtopoClient.openWith(fetcher);

    reset();
    const [a, b, c] = await Promise.all([
      client.property("block/population"),
      client.property("block/population"),
      client.property("block/population"),
    ]);
    expect(a).toBe(b);
    expect(b).toBe(c);
    // The bootstrap range covers the entire small fixture so no
    // additional GETs fire — the cache + dedupe combination is what's
    // under test, not the byte count itself.
    expect(calls()).toBeLessThanOrEqual(1);
  });

  it("rejects truncated responses via the explicit length check", async () => {
    const buf = await encodeContainer(fixtureTopology());
    // Build a fetcher that serves the bootstrap GET correctly (clamped
    // to file end is fine — the bootstrap path tolerates short
    // responses) but truncates every subsequent GET by one byte.
    // Section fetches go through the strict checker and should reject.
    let firstCall = true;
    const inner = makeBufferFetcher(buf);
    const fetcher: RangeFetcher = {
      range: (start, end, signal) => {
        const clampedEnd = Math.min(end, buf.byteLength);
        if (firstCall) {
          firstCall = false;
          return inner.range(start, end, signal);
        }
        return Promise.resolve(
          new Uint8Array(
            buf.buffer,
            buf.byteOffset + start,
            clampedEnd - start - 1,
          ),
        );
      },
      suffix: (length, signal) => inner.suffix(length, signal),
    };
    const client = await CtopoClient.openWith(fetcher);
    await expect(client.fetchArcs([0])).rejects.toThrow(/short read/);
  });

  it("strings() returns a StringArray that decodes lazily", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher } = fetcherFor(buf);
    const client = await CtopoClient.openWith(fetcher);

    const ids = await client.strings("block/id");
    expect(ids.length).toBe(1);
    expect(ids.get(0)).toBe("B");
  });

  it("aborted per-call signals cancel section fetches", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const inner = makeBufferFetcher(buf);
    const fetcher: RangeFetcher = {
      range: (start, end, signal) => {
        if (signal?.aborted === true)
          return Promise.reject(new Error("aborted"));
        return inner.range(start, end, signal);
      },
      suffix: (length, signal) => inner.suffix(length, signal),
    };
    const client = await CtopoClient.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetchBytes: 0,
    });

    const controller = new AbortController();
    controller.abort();

    await expect(
      client.property("block/population", controller.signal),
    ).rejects.toThrow(/aborted/);
  });

  it("section fetches request exactly the section's bytes — no overfetch", async () => {
    const buf = await encodeContainer(fixtureTopology());
    // Force the bootstrap to be tiny so subsequent section fetches
    // actually fire. Capture every Range request's [start, end).
    const { fetcher, requests } = recordingFetcher(buf);
    const client = await CtopoClient.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetchBytes: 0,
    });

    requests.length = 0;
    await client.property("block/population");

    // The section's compression group is fetched as exactly
    // [offset, offset+length) — not padded out. block/population
    // shares a group with the other compressible fixture sections,
    // so the request range matches the group's offset/length
    // (which is the same as the population entry's table row).
    const popSection = client.sections.find(
      (s) => s.name === "block/population",
    )!;
    expect(requests).toEqual([
      [popSection.offset, popSection.offset + popSection.length],
    ]);
  });

  it("fetchArcs coalesces concurrent arcs into two GETs (offsets + coords) and reuses cache on repeats", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    const client = await CtopoClient.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetchBytes: 0,
    });

    requests.length = 0;
    await client.fetchArcs([0, 1, 2, 3]);
    // Two GETs: one coalesced over the four arcs' offset bytes
    // (8 B per arc, fused because they're back-to-back in
    // arc_offsets), one over their coord bytes (also fused).
    expect(requests.length).toBe(2);

    // Subsequent calls — including for arcs not in the first call —
    // fall inside the cached ranges, so no new network.
    requests.length = 0;
    await client.fetchArcs([0, 2]);
    await client.fetchArcs([1, 3]);
    await client.fetchArcs([2]);
    expect(requests.length).toBe(0);
  });

  it("concurrent fetches across families fire one GET per family (no cross-family bridging)", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    const client = await CtopoClient.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetchBytes: 0,
    });

    requests.length = 0;
    // CSR triples are front-loaded; properties / strings are lazy.
    // The encoder enforces a group boundary between front-load and
    // lazy regions so a front-prefetch GET never drags lazy bytes
    // along with it. So the three concurrent fetches resolve through
    // two compression groups → two physical GETs (one per group),
    // with the group cache still de-duplicating callers within each
    // group's bytes.
    await Promise.all([
      client.property("block/population"),
      client.strings("block/id"),
      client.layerGeometry("block"),
    ]);
    expect(requests.length).toBe(2);
  });

  it("speculative arc_coords prefetch lands a fetch on open and serves later fetchArcs from cache", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    // Tiny prefetch that covers all 4 fixture arcs (their packed
    // bytes are well under 4 KiB). The prefetch fires during open
    // and lands in the byte-range cache.
    const client = await CtopoClient.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 4 * 1024,
    });
    // Drain microtasks so the in-flight prefetch resolves before we
    // count requests; otherwise the assertion is racy.
    await new Promise((r) => setTimeout(r, 0));
    const beforeArcs = requests.length;

    // Now fetchArcs — every id should hit the prefetched cache.
    await client.fetchArcs([0, 1, 2, 3]);
    expect(requests.length).toBe(beforeArcs);
  });

  it("a tight per-family gap can force every fetchArcs id into its own GET", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    // Negative gap makes the coalescer reject every bridge attempt
    // (since adjacent items have a non-negative gap). Useful as a
    // smoke test that the per-family override is wired through —
    // production sets a wide gap, but the override path is the
    // same in either direction.
    const client = await CtopoClient.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetchBytes: 0,
      coalesceGapByFamily: { arcs: -1 },
    });
    requests.length = 0;
    await client.fetchArcs([0, 1, 2, 3]);
    // 4 arc-coord GETs (no coalescing within the arcs family) + 1
    // coalesced offset GET (offsets family's default gap is 1 MiB
    // and the four offset entries are 16 bytes apart).
    expect(requests.length).toBe(5);
  });

  it("concurrent same-family arc fetches coalesce into one GET", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    const client = await CtopoClient.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetchBytes: 0,
    });

    requests.length = 0;
    // Two concurrent fetchArcs calls in the same tick: their offset
    // requests coalesce into one GET (offsets family), then their
    // coord requests coalesce into a second GET (arcs family).
    await Promise.all([client.fetchArcs([0, 1]), client.fetchArcs([2, 3])]);
    expect(requests.length).toBe(2);
  });

  it("byte-range cache serves later sub-range requests from an earlier chunk", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    const client = await CtopoClient.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetchBytes: 0,
    });

    // First drain: fetch all four arcs — two GETs (offsets + coords).
    requests.length = 0;
    await client.fetchArcs([0, 1, 2, 3]);
    expect(requests.length).toBe(2);

    // Second drain: ask for any subset of those arcs — the cached
    // ranges contain both their offset bytes and coord bytes, so
    // no new GETs fire.
    requests.length = 0;
    await client.fetchArcs([1, 2]);
    expect(requests.length).toBe(0);
  });

  it("concurrent fetchArcs calls share the single in-flight arc_coords fetch", async () => {
    const buf = await encodeContainer(fixtureTopology());
    // First, open the client normally so the bootstrap path completes.
    const { fetcher: openFetcher } = fetcherFor(buf);
    const client = await CtopoClient.openWith(openFetcher, {
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetchBytes: 0,
    });
    // Warm the arc_offsets cache via the open-time fetcher so the
    // blocking fetcher only sees the arc_coords GET we're testing.
    await client.fetchArcs([0]);

    // Now swap in a fetcher whose response we control — the arc_coords
    // fetch that the next fetchArcs triggers will block here until we
    // resolve, letting us verify only one GET fires across many
    // concurrent calls. The blocking fetcher serves bytes of the
    // exact requested length so the section fetcher's strict
    // length check is satisfied.
    const requests: Array<[number, number]> = [];
    let resolveFetch: ((b: Uint8Array) => void) | undefined;
    let pendingRange: [number, number] | undefined;
    const blockingFetcher: RangeFetcher = {
      range: (start, end) => {
        requests.push([start, end]);
        pendingRange = [start, end];
        return new Promise<Uint8Array>((resolve) => {
          resolveFetch = resolve;
        });
      },
      // Suffix path is unused after open; provide a no-op so the
      // type-checker is happy and any accidental call is obvious.
      suffix: () =>
        Promise.reject(new Error("blocking fetcher: suffix not implemented")),
    };
    // Reach into the private field to swap fetchers — production code
    // has no need for this; the test pins behavior of the cache.
    (client as unknown as { fetcher: RangeFetcher }).fetcher = blockingFetcher;

    const a = client.fetchArcs([0, 1]);
    const b = client.fetchArcs([2, 3]);
    const c = client.fetchArcs([0, 2]);
    // Drain all pending microtasks so each fetchArcs continuation
    // gets to enqueue and the batched flush fires the (single)
    // coalesced GET. setTimeout 0 flips control to the macrotask
    // queue, which runs after the microtask queue is empty.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(requests.length).toBe(1);

    const [rangeStart, rangeEnd] = pendingRange!;
    resolveFetch!(
      new Uint8Array(
        buf.buffer,
        buf.byteOffset + rangeStart,
        rangeEnd - rangeStart,
      ),
    );
    await Promise.all([a, b, c]);
    expect(requests.length).toBe(1);
  });

  it("fetchArcs returns identical bytes whether arc_coords is raw or block-compressed", async () => {
    // Build the same fixture twice — once raw, once block-compressed.
    // Tiny blocks force multiple-block layout on a 4-arc fixture.
    const rawBuf = await encodeContainer(fixtureTopology());
    const blockBuf = await encodeContainer(fixtureTopology(), {
      blockCompressArcCoords: true,
      arcCoordBlockBytes: 32,
    });

    const rawClient = await CtopoClient.openWith(fetcherFor(rawBuf).fetcher, {
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetchBytes: 0,
    });
    const blockClient = await CtopoClient.openWith(
      fetcherFor(blockBuf).fetcher,
      {
        arcCoordsPrefetchBytes: 0,
        arcOffsetsPrefetchBytes: 0,
      },
    );

    expect(blockClient.meta.arcCoordsBlocks).toBeDefined();
    expect(rawClient.meta.arcCoordsBlocks).toBeUndefined();

    const arcIds = [0, 1, 2, 3];
    const rawArcs = await rawClient.fetchArcs(arcIds);
    const blockArcs = await blockClient.fetchArcs(arcIds);

    for (const id of arcIds) {
      const a = rawArcs.get(id)!;
      const b = blockArcs.get(id)!;
      expect(b.byteLength).toBe(a.byteLength);
      expect(Array.from(b)).toEqual(Array.from(a));
    }
  });

  it("block-compressed fetchArcs decompresses each block at most once across all arcs in it", async () => {
    const buf = await encodeContainer(fixtureTopology(), {
      blockCompressArcCoords: true,
      // Fixture has no transform → float64 points, 32 B/arc × 4 arcs
      // = 128 B total. Use a 256 B target so every arc lives in a
      // single block — that's the property we're testing here.
      arcCoordBlockBytes: 256,
    });
    const { fetcher } = fetcherFor(buf);
    const client = await CtopoClient.openWith(fetcher, {
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetchBytes: 0,
    });

    // First call decompresses the (one) block. Second call should
    // hit the per-block decompressed cache — no new fetches.
    await client.fetchArcs([0, 1]);
    const cache = (
      client as unknown as {
        decompressedArcCoordBlockCache: Map<number, Promise<Uint8Array>>;
      }
    ).decompressedArcCoordBlockCache;
    expect(cache.size).toBe(1);
    await client.fetchArcs([2, 3]);
    expect(cache.size).toBe(1); // still one — same block reused
  });
});
