// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

import { describe, expect, it } from "vitest";

import { type Topology } from "topojson-specification";

import { CtopoCore } from "../core/client";
import { makeBufferFetcher, type RangeFetcher } from "../core/fetcher";
import { encodeContainer } from "../encode";
import { readVarintZigzagInto } from "../core/format";

// Quantized variant of fixtureTopology — adds a `transform` so the
// encoder takes the delta + varint arc path (and emits arc_endpoints
// by default). Same unit square geometry, but coords are deltas in
// quantization units (each delta = 1 unit) and the transform maps
// back to floats so reader round-trips behave like the un-quantized
// version. Used by the arc_endpoints tests where the section's
// presence is the thing under test.
function quantizedFixtureTopology(): Topology {
  return {
    type: "Topology",
    arcs: [
      [
        [0, 0],
        [1, 0],
      ],
      [
        [1, 0],
        [0, 1],
      ],
      [
        [0, 1],
        [-1, 0],
      ],
      [
        [0, 1],
        [0, -1],
      ],
    ],
    bbox: [0, 0, 1, 1],
    transform: { scale: [1, 1], translate: [0, 0] },
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

describe("CtopoCore", () => {
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
    const client = await CtopoCore.openWith(fetcher);

    expect(client.meta.numArcs).toBe(4);
    expect(client.meta.layers[0].numGeometries).toBe(1);
  });

  it("dedupes concurrent property() calls into a single fetch", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, calls, reset } = fetcherFor(buf);
    const client = await CtopoCore.openWith(fetcher);

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
    const client = await CtopoCore.openWith(fetcher);
    await expect(client.fetchArcs([0])).rejects.toThrow(/short read/);
  });

  it("strings() returns a StringArray that decodes lazily", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher } = fetcherFor(buf);
    const client = await CtopoCore.openWith(fetcher);

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
    const client = await CtopoCore.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
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
    const client = await CtopoCore.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
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

  it("fetchArcEndpoints round-trips per-arc start/end coords matching arc_coords", async () => {
    // Quantized fixture: the encoder only emits arc_endpoints for
    // quantized inputs (un-quantized would need a float64 shape we
    // don't have a consumer for).
    const buf = await encodeContainer(quantizedFixtureTopology());
    const { fetcher } = recordingFetcher(buf);
    const client = await CtopoCore.openWith(fetcher);

    expect(client.hasArcEndpointsSection()).toBe(true);

    const arcCount = client.meta.numArcs;
    const ids = Array.from({ length: arcCount }, (_, i) => i);
    const endpoints = await client.fetchArcEndpoints(ids);

    // Independently recover endpoints from the raw arc_coords varint
    // stream and confirm they match the dedicated section.
    const arcBytes = await client.fetchArcs(ids);
    const cur = { value: 0, off: 0 };
    for (const id of ids) {
      const bytes = arcBytes.get(id)!;
      let x = 0;
      let y = 0;
      let sx = 0;
      let sy = 0;
      let pIdx = 0;
      cur.off = 0;
      while (cur.off < bytes.byteLength) {
        readVarintZigzagInto(bytes, cur);
        const dx = cur.value;
        readVarintZigzagInto(bytes, cur);
        x += dx;
        y += cur.value;
        if (pIdx === 0) {
          sx = x;
          sy = y;
        }
        pIdx++;
      }
      const got = endpoints.get(id)!;
      expect([got[0], got[1], got[2], got[3]]).toEqual([sx, sy, x, y]);
    }
  });

  it("fetchArcEndpoints falls back to arc_coords decode when the section is absent", async () => {
    // Encode with the section disabled (simulates legacy files) to
    // exercise the arc_coords varint-walk fallback path. Must be
    // quantized — the fallback only supports quantized files (the
    // public API contract).
    const buf = await encodeContainer(quantizedFixtureTopology(), {
      emitArcEndpoints: false,
    });
    const { fetcher } = recordingFetcher(buf);
    const client = await CtopoCore.openWith(fetcher);
    expect(client.hasArcEndpointsSection()).toBe(false);

    const ids = [0, 1, 2];
    const endpoints = await client.fetchArcEndpoints(ids);
    // Endpoints should still come back, derived from arc_coords. Just
    // sanity-check the shape; the previous test pins the values.
    for (const id of ids) {
      const got = endpoints.get(id);
      expect(got).toBeDefined();
      expect(got!.length).toBe(4);
    }
  });

  it("fetchArcs coalesces concurrent arcs into two GETs (offsets + coords) and reuses cache on repeats", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    const client = await CtopoCore.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
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
    const client = await CtopoCore.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
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

  it("open-time prefetches don't double-request overlapping bytes", async () => {
    // With every prefetch flag at its default, the open path issues
    // multiple concurrent GETs (header, arc_offsets, arc_coord_blocks,
    // arc_coords_dict if present, and an arc_coords prefix). They
    // target distinct sections, so no two GETs should land on
    // overlapping byte ranges. This locks in the invariant that the
    // skeleton prefetch isn't re-fetching bytes the header/footer
    // prefetch already covered (and vice versa for non-zero
    // frontPrefetchBytes).
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    await CtopoCore.openWith(fetcher);
    // Drain microtasks so every speculative prefetch resolves.
    await new Promise((r) => setTimeout(r, 0));

    // Pairwise disjoint: for any two distinct requests [a1, b1) and
    // [a2, b2), they overlap iff a1 < b2 && a2 < b1.
    for (let i = 0; i < requests.length; i++) {
      for (let j = i + 1; j < requests.length; j++) {
        const [a1, b1] = requests[i];
        const [a2, b2] = requests[j];
        const overlaps = a1 < b2 && a2 < b1;
        expect(overlaps, `requests ${i} and ${j} overlap`).toBe(false);
      }
    }
  });

  it("block table + dict are prefetched at open; fetchArcs only needs the block data", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    const client = await CtopoCore.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
    });
    // Drain microtasks so speculative block-table prefetch resolves.
    await new Promise((r) => setTimeout(r, 0));
    const afterOpen = requests.length;

    // fetchArcs needs arc_offsets + the compressed block data.
    await client.fetchArcs([0, 1, 2, 3]);
    // At most 2 new GETs: offsets + one block (tiny fixture fits in
    // one block).
    expect(requests.length - afterOpen).toBeLessThanOrEqual(2);
  });

  it("a tight per-family gap splits offset GETs but block-compressed arcs fetch whole blocks", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    // Negative gap makes the coalescer reject every bridge attempt
    // for offsets (since adjacent items have a non-negative gap).
    // Block-compressed arcs fetch whole blocks regardless of gap.
    const client = await CtopoCore.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
      coalesceGapByFamily: { offsets: -1 },
    });
    requests.length = 0;
    await client.fetchArcs([0, 1, 2, 3]);
    // With block compression all 4 arcs fit in 1 block → 1 block
    // GET. Offsets are split by the negative gap into individual
    // GETs (4 offset entries → up to 4 GETs, though adjacent ones
    // may still be in the same group-compressed region).
    expect(requests.length).toBeGreaterThanOrEqual(2);
  });

  it("concurrent same-family arc fetches coalesce into one GET", async () => {
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    const client = await CtopoCore.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
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
    const client = await CtopoCore.openWith(fetcher, {
      frontPrefetchBytes: 0,
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
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

  it("block cache serves subsequent arc fetches without additional GETs", async () => {
    // The tiny fixture fits in one block at the default block size.
    // After the first fetchArcs decompresses that block, all
    // subsequent calls are served from the block cache — no new
    // network requests needed.
    const buf = await encodeContainer(fixtureTopology());
    const { fetcher, requests } = recordingFetcher(buf);
    const client = await CtopoCore.openWith(fetcher, {
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
    });

    // First call fetches offsets + the one compressed block.
    requests.length = 0;
    await client.fetchArcs([0]);
    const afterFirst = requests.length;
    expect(afterFirst).toBeGreaterThan(0);

    // Subsequent calls hit the decompressed block cache — zero GETs.
    requests.length = 0;
    await Promise.all([
      client.fetchArcs([0, 1]),
      client.fetchArcs([2, 3]),
      client.fetchArcs([0, 2]),
    ]);
    expect(requests.length).toBe(0);
  });

  it("fetchArcs returns identical bytes regardless of block size", async () => {
    // Build the same fixture with default blocks vs tiny blocks.
    // Tiny blocks force multiple-block layout on a 4-arc fixture.
    const defaultBuf = await encodeContainer(fixtureTopology());
    const tinyBuf = await encodeContainer(fixtureTopology(), {
      arcCoordBlockBytes: 32,
    });

    const defaultClient = await CtopoCore.openWith(
      fetcherFor(defaultBuf).fetcher,
      {
        arcCoordsPrefetchBytes: 0,
        arcOffsetsPrefetch: false,
      },
    );
    const tinyClient = await CtopoCore.openWith(fetcherFor(tinyBuf).fetcher, {
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
    });

    expect(defaultClient.meta.arcCoordsBlocks).toBeDefined();
    expect(tinyClient.meta.arcCoordsBlocks).toBeDefined();

    const arcIds = [0, 1, 2, 3];
    const defaultArcs = await defaultClient.fetchArcs(arcIds);
    const tinyArcs = await tinyClient.fetchArcs(arcIds);

    for (const id of arcIds) {
      const a = defaultArcs.get(id)!;
      const b = tinyArcs.get(id)!;
      expect(b.byteLength).toBe(a.byteLength);
      expect(Array.from(b)).toEqual(Array.from(a));
    }
  });

  // -- Partitioned-arc_offsets coverage --
  //
  // Threshold is 750 KB raw arc_offsets → roughly 192K arcs. We
  // synthesize a 200K-arc grid topology so the encoder takes the
  // partitioned branch without leaking any test-only knobs into the
  // production API.

  // Build a synthetic N-arc topology — one polygon per arc to keep
  // the geometry tiny and the encoder fast. Each arc is a unique
  // 2-point line so the encoder's per-arc bytes are deterministic
  // and tests can assert exact byteLengths.
  function syntheticTopology(numArcs: number): Topology {
    const arcs = new Array<number[][]>(numArcs);
    const geometries = new Array<{
      type: "Polygon";
      arcs: number[][];
      properties: { id: string };
    }>(numArcs);
    for (let i = 0; i < numArcs; i++) {
      const x = i;
      const y = i;
      arcs[i] = [
        [x, y],
        [x + 1, y + 1],
      ];
      geometries[i] = {
        type: "Polygon",
        arcs: [[i]],
        properties: { id: `a${i}` },
      };
    }
    return {
      type: "Topology",
      arcs,
      bbox: [0, 0, numArcs + 1, numArcs + 1],
      objects: {
        block: {
          type: "GeometryCollection",
          geometries,
        },
      },
    };
  }

  // Track a built synthetic fixture across the partitioned tests so
  // we only pay the encode cost once. vitest's `beforeAll` would
  // share across the whole describe; this manual memoization is
  // tightly scoped to the partitioned suite. The encode trains shared
  // zstd dicts (7 candidates × `zstd --train` ≈ tens of seconds), so
  // amortizing across the 5 partitioned tests matters.
  const N_PARTITIONED_ARCS = 200_000;
  // Default block size (8 KiB) at 32 B/arc = ~256 arcs/block →
  // ~780 blocks for 200K arcs. Above the 100-block dict-training
  // threshold (so the round-trip / sparse tests exercise the with-dict
  // path), but only ~3× over so dict-training isn't the dominant
  // cost.
  let cachedPartitionedBuf: Uint8Array | undefined;
  async function partitionedFixtureBuf(): Promise<Uint8Array> {
    if (cachedPartitionedBuf === undefined) {
      cachedPartitionedBuf = await encodeContainer(
        syntheticTopology(N_PARTITIONED_ARCS),
      );
    }
    return cachedPartitionedBuf;
  }

  // Encoding 200K arcs + training shared dicts blows the default 5 s
  // vitest timeout. 5 minutes is plenty even on slow machines and
  // still bounded so a real hang fails fast.
  const PARTITIONED_TIMEOUT_MS = 5 * 60_000;

  it(
    "partitioned arc_offsets: 200K-arc topology triggers the partitioned path",
    async () => {
      const buf = await partitionedFixtureBuf();
      const client = await CtopoCore.openWith(fetcherFor(buf).fetcher, {
        arcCoordsPrefetchBytes: 0,
        arcOffsetsPrefetch: false,
      });
      expect(client.meta.arcOffsetsBlocks).toBeDefined();
      expect(client.meta.arcOffsetsBlocks?.blockTableSection).toBe(
        "arc_offsets_blocks",
      );
      expect(client.meta.arcOffsetsBlocks?.partitionsSection).toBe(
        "arc_offsets_partitions",
      );
      expect(client.meta.arcOffsetsBlocks?.blockCount).toBe(
        client.meta.arcCoordsBlocks.blockCount,
      );

      const sections = new Set(client.sections.map((s) => s.name));
      expect(sections.has("arc_offsets")).toBe(false);
      expect(sections.has("arc_offsets_blocks")).toBe(true);
      expect(sections.has("arc_offsets_partitions")).toBe(true);
    },
    PARTITIONED_TIMEOUT_MS,
  );

  it(
    "partitioned arc_offsets: fetchArcs returns expected bytes against the arcOffsets() reference",
    async () => {
      const buf = await partitionedFixtureBuf();
      const client = await CtopoCore.openWith(fetcherFor(buf).fetcher, {
        arcCoordsPrefetchBytes: 0,
        arcOffsetsPrefetch: false,
      });

      // Sample arcs scattered across the section so different
      // partitions get exercised. Each arc's byte length must match
      // arcOffsets[id+1] - arcOffsets[id] — that's the contract
      // whether the path is partitioned or monolithic.
      const offsets = await client.arcOffsets();
      const sampleIds = [0, 1, 100, 1_000, 50_000, 100_000, 199_999];
      const arcs = await client.fetchArcs(sampleIds);
      for (const id of sampleIds) {
        const expectedLen = offsets[id + 1] - offsets[id];
        const got = arcs.get(id);
        expect(got).toBeDefined();
        expect(got!.byteLength).toBe(expectedLen);
      }
    },
    PARTITIONED_TIMEOUT_MS,
  );

  it("partitioned arc_offsets: small fixture stays on the monolithic path", async () => {
    // Default threshold (750 KB) — 4-arc fixture is ~20 B of
    // arc_offsets, well below. Guards backward compatibility: small
    // files keep using the legacy reader code path.
    const buf = await encodeContainer(fixtureTopology());
    const client = await CtopoCore.openWith(fetcherFor(buf).fetcher, {
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
    });
    expect(client.meta.arcOffsetsBlocks).toBeUndefined();
    const arcs = await client.fetchArcs([0, 1, 2, 3]);
    expect(arcs.size).toBe(4);
    for (const id of [0, 1, 2, 3]) {
      expect(arcs.get(id)!.byteLength).toBeGreaterThan(0);
    }
  });

  it(
    "partitioned arc_offsets: sparse fetch decompresses only the touched partitions",
    async () => {
      const buf = await partitionedFixtureBuf();
      const client = await CtopoCore.openWith(fetcherFor(buf).fetcher, {
        arcCoordsPrefetchBytes: 0,
        arcOffsetsPrefetch: false,
      });
      expect(client.meta.arcOffsetsBlocks).toBeDefined();

      // Fetch arcs that land in two adjacent ids → same or adjacent
      // partition. Then fetch one ~halfway and one near the end.
      // Verify the per-partition decompress cache only holds entries
      // for the touched partitions, not the whole table.
      client.resetStats();
      const arcs = await client.fetchArcs([1, 2, 100_000, 199_999]);
      expect(arcs.size).toBe(4);
      const partCache = (
        client as unknown as {
          decompressedArcOffsetsBlockCache: Map<number, Promise<Uint32Array>>;
        }
      ).decompressedArcOffsetsBlockCache;
      // At most one partition per distinct touched arc-id-cluster;
      // far less than the total blockCount.
      expect(partCache.size).toBeLessThan(
        client.meta.arcOffsetsBlocks!.blockCount,
      );
      expect(partCache.size).toBeGreaterThan(0);

      const stats = client.getStats();
      // 12 B per touched block-table entry; matches the per-partition
      // decompress cache size.
      expect(stats.usefulBytesBySection["arc_offsets_blocks"]).toBe(
        12 * partCache.size,
      );
      // 8 B per arc lookup (start + end u32). 4 arcs touched.
      expect(stats.usefulBytesBySection["arc_offsets_partitions"]).toBe(8 * 4);
    },
    PARTITIONED_TIMEOUT_MS,
  );

  it(
    "partitioned arc_offsets: no-dict path works when block count is below the training threshold",
    async () => {
      // Need partitioned path (arc_offsets >= 750 KB) AND fewer than
      // 100 arc-coord blocks (so the shared-dict trainer skips dict
      // training, per encode-compress.ts MIN_SAMPLES = 100). 200K
      // arcs at ~32 B/arc = ~6.4 MB uncompressed; with
      // arcCoordBlockBytes = 256 KiB we get ~25 blocks — under the
      // threshold.
      const buf = await encodeContainer(syntheticTopology(N_PARTITIONED_ARCS), {
        arcCoordBlockBytes: 256 * 1024,
      });
      const client = await CtopoCore.openWith(fetcherFor(buf).fetcher, {
        arcCoordsPrefetchBytes: 0,
        arcOffsetsPrefetch: false,
      });
      expect(client.meta.arcOffsetsBlocks).toBeDefined();
      expect(client.meta.arcOffsetsBlocks!.blockCount).toBeLessThan(100);
      expect(client.meta.arcOffsetsBlocks!.dictSection).toBeUndefined();
      expect(client.meta.arcCoordsBlocks.dictSection).toBeUndefined();

      const offsets = await client.arcOffsets();
      const sampleIds = [0, 1, 1_000, 100_000, 199_999];
      const arcs = await client.fetchArcs(sampleIds);
      for (const id of sampleIds) {
        expect(arcs.get(id)!.byteLength).toBe(offsets[id + 1] - offsets[id]);
      }
    },
    PARTITIONED_TIMEOUT_MS,
  );

  it(
    "partitioned arc_offsets: arcOffsets() slow-path is consistent with fetchArcs",
    async () => {
      const buf = await partitionedFixtureBuf();
      const client = await CtopoCore.openWith(fetcherFor(buf).fetcher, {
        arcCoordsPrefetchBytes: 0,
        arcOffsetsPrefetch: false,
      });

      // The assembled global table must:
      //   - have length = numArcs + 1
      //   - be monotonically non-decreasing
      //   - last offset == total uncompressed arc_coords bytes
      const offsets = await client.arcOffsets();
      expect(offsets.length).toBe(N_PARTITIONED_ARCS + 1);
      expect(offsets[0]).toBe(0);
      let prev = 0;
      for (let i = 1; i < offsets.length; i++) {
        expect(offsets[i]).toBeGreaterThanOrEqual(prev);
        prev = offsets[i];
      }
      const lastBlock = client.meta.arcCoordsBlocks.blockCount - 1;
      const blocks = await (
        client as unknown as { getArcCoordBlocks(): Promise<Uint32Array> }
      ).getArcCoordBlocks();
      expect(offsets[offsets.length - 1]).toBe(blocks[lastBlock * 3]);
    },
    PARTITIONED_TIMEOUT_MS,
  );

  it("block-compressed fetchArcs decompresses each block at most once across all arcs in it", async () => {
    const buf = await encodeContainer(fixtureTopology(), {
      // Fixture has no transform → float64 points, 32 B/arc × 4 arcs
      // = 128 B total. Use a 256 B target so every arc lives in a
      // single block — that's the property we're testing here.
      arcCoordBlockBytes: 256,
    });
    const { fetcher } = fetcherFor(buf);
    const client = await CtopoCore.openWith(fetcher, {
      arcCoordsPrefetchBytes: 0,
      arcOffsetsPrefetch: false,
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
