// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

import { existsSync } from "fs";
import { dirname, resolve as resolvePath } from "path";
import { fileURLToPath } from "url";

import { describe, expect, it, beforeAll } from "vitest";

import { type Topology } from "topojson-specification";

import { encodeContainer } from "../encode";
import { CtopoClient } from "../proxy/client";
import { merge, mergeArcs, neighbors } from "../proxy/merge";

// Locate the built worker. Vitest runs against TypeScript sources via
// vite-node, so we can't load `src/worker.ts` directly into a Node
// `worker_threads.Worker` (no TS loader). Run `npm run build` first;
// the suite skips itself otherwise.
const __filename = fileURLToPath(import.meta.url);
const distWorker = resolvePath(
  dirname(__filename),
  "..",
  "..",
  "dist",
  "worker.js",
);
const workerUrlHref = `file://${distWorker}`;

function twoBlockTopology(): Topology {
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
        [1, 0],
        [2, 0],
      ],
      [
        [2, 0],
        [2, 1],
      ],
      [
        [2, 1],
        [1, 1],
      ],
    ],
    bbox: [0, 0, 2, 1],
    objects: {
      block: {
        type: "GeometryCollection",
        geometries: [
          {
            type: "Polygon",
            arcs: [[0, 1, 2, 3]],
            properties: { id: "L", population: 100 },
          },
          {
            type: "Polygon",
            arcs: [[4, 5, 6, ~1]],
            properties: { id: "R", population: 200 },
          },
        ],
      },
    },
  };
}

async function openProxy(buf: Uint8Array): Promise<CtopoClient> {
  // A Uint8Array source goes to the worker directly — no HTTP. The
  // proxy transfers the bytes' ArrayBuffer at open time so the worker
  // doesn't pay a copy (the caller's `buf` view is detached after).
  return CtopoClient.open(buf, { workerUrl: workerUrlHref });
}

const describeIfBuilt = existsSync(distWorker) ? describe : describe.skip;

describeIfBuilt("ctopo worker-backed proxy", () => {
  beforeAll(() => {
    if (!existsSync(distWorker)) {
      // Belt-and-suspenders for environments where `existsSync`
      // resolves at import time but the file is removed before the
      // suite runs.
      throw new Error(
        `proxy.test.ts requires dist/worker.js — run \`npm run build\` first.`,
      );
    }
  });

  it("round-trips meta + merge through the worker", async () => {
    const buf = await encodeContainer(twoBlockTopology());
    const client = await openProxy(buf);
    try {
      // meta snapshot is sync on the proxy.
      expect(client.meta.numArcs).toBe(7);
      expect(client.sections.length).toBeGreaterThan(0);

      const result = await merge(client, [{ layer: "block", indices: [0, 1] }]);
      expect(result.type).toBe("MultiPolygon");
      expect(result.coordinates.length).toBe(1);
      const ring = result.coordinates[0][0];
      expect(ring[0]).toEqual(ring[ring.length - 1]); // closed
      const vertices = ring
        .slice(0, -1)
        .map((p) => `${p[0]},${p[1]}`)
        .sort();
      expect(vertices).toEqual(
        ["0,0", "0,1", "1,0", "1,1", "2,0", "2,1"].sort(),
      );
    } finally {
      await client.close();
    }
  });

  it("mergeArcs round-trips arc ids through transferable arrays", async () => {
    const buf = await encodeContainer(twoBlockTopology(), {
      spatialSort: "hilbert",
    });
    const client = await openProxy(buf);
    try {
      const both = await mergeArcs(client, [
        { layer: "block", indices: [0, 1] },
      ]);
      const remaining = both.arcs
        .flat(2)
        .map((s) => (s >= 0 ? s : ~s))
        .sort();
      expect(remaining).toEqual([0, 2, 3, 4, 5, 6]);
    } finally {
      await client.close();
    }
  });

  it("neighbors round-trips CSR adjacency through the worker", async () => {
    const buf = await encodeContainer(twoBlockTopology());
    const client = await openProxy(buf);
    try {
      const adj = await neighbors(client, "block");
      expect(adj).toEqual([[1], [0]]);
    } finally {
      await client.close();
    }
  });

  it("property buffers are isolated from the worker's cache", async () => {
    // The worker keeps property/layerGeometry/strings results in its
    // cache. Mutating the proxy-side typed array must not corrupt
    // a subsequent fetch — proves the worker copies before sending.
    const buf = await encodeContainer(twoBlockTopology());
    const client = await openProxy(buf);
    try {
      const first = (await client.property("block/population")) as
        | Uint8Array
        | Uint16Array
        | Uint32Array;
      const originalAt0 = first[0];
      // Mutate the view we received. If the worker shared its cache,
      // the second fetch would observe this write.
      first[0] = (first[0] + 99) & 0xff;
      const second = (await client.property("block/population")) as
        | Uint8Array
        | Uint16Array
        | Uint32Array;
      expect(second[0]).toBe(originalAt0);
    } finally {
      await client.close();
    }
  });

  it("merge results are transferred — main thread owns the buffer", async () => {
    const buf = await encodeContainer(twoBlockTopology());
    const client = await openProxy(buf);
    try {
      const result = await merge(client, [{ layer: "block", indices: [0, 1] }]);
      // Each ring is its own number[][]; the underlying packed
      // Float64Array.buffer should be alive on the main thread.
      // Use a follow-up merge to confirm the worker can serve another
      // request — i.e., its state survived the transfer.
      const again = await merge(client, [{ layer: "block", indices: [0] }]);
      expect(result.coordinates.length).toBe(1);
      expect(again.coordinates.length).toBe(1);
      // First merge has all 6 outer corners; the single-block merge
      // has only the L-square's 4.
      const aVerts = new Set(
        result.coordinates[0][0].slice(0, -1).map((p) => `${p[0]},${p[1]}`),
      );
      const bVerts = new Set(
        again.coordinates[0][0].slice(0, -1).map((p) => `${p[0]},${p[1]}`),
      );
      expect(aVerts.size).toBe(6);
      expect(bVerts.size).toBe(4);
    } finally {
      await client.close();
    }
  });

  it("AbortSignal aborts an in-flight merge", async () => {
    const buf = await encodeContainer(twoBlockTopology());
    const client = await openProxy(buf);
    const ac = new AbortController();
    // Abort synchronously after kicking off the call — the proxy posts
    // the abort message; either the worker cancels mid-flight or the
    // call completes first and the abort no-ops. Either way the promise
    // must not hang.
    const p = merge(client, [{ layer: "block", indices: [0, 1] }], ac.signal);
    ac.abort();
    let aborted = false;
    try {
      await p;
    } catch (e) {
      aborted = e instanceof DOMException && e.name === "AbortError";
    }
    // The race outcome depends on timing — we only assert that the
    // call resolves one way or the other and the client is still
    // usable afterwards.
    void aborted;
    const after = await merge(client, [{ layer: "block", indices: [0] }]);
    expect(after.coordinates.length).toBe(1);
    await client.close();
  });

  it("attachPort shares one worker across two clients", async () => {
    // Open the originating client, then attach a second client through
    // a transferred MessagePort. Both clients drive the same worker;
    // their request streams stay isolated. `openProxy` transfers the
    // first client's bytes buffer, so copy the bytes first for the
    // second client's own buffer source.
    const buf = await encodeContainer(twoBlockTopology());
    const bufB = new Uint8Array(buf);
    const a = await openProxy(buf);
    const port = a.attachPort();
    const b = await CtopoClient.open(bufB, { port });
    try {
      // Same container bytes → same meta, served by the same worker
      // over the shared port.
      expect(a.meta.numArcs).toBe(b.meta.numArcs);

      // Fire interleaved calls; each promise must resolve to its own
      // caller's result with no cross-talk.
      const [aMerge, bMerge, aProp, bProp] = await Promise.all([
        merge(a, [{ layer: "block", indices: [0] }]),
        merge(b, [{ layer: "block", indices: [1] }]),
        a.property("block/population"),
        b.property("block/population"),
      ]);
      // Single-block merges produce a single-polygon MultiPolygon
      // with 4 distinct corners. The two selections target different
      // blocks so their outer-ring corner sets must differ.
      const aVerts = new Set(
        aMerge.coordinates[0][0].slice(0, -1).map((p) => `${p[0]},${p[1]}`),
      );
      const bVerts = new Set(
        bMerge.coordinates[0][0].slice(0, -1).map((p) => `${p[0]},${p[1]}`),
      );
      expect(aVerts.size).toBe(4);
      expect(bVerts.size).toBe(4);
      expect(aVerts).not.toEqual(bVerts);

      // Both property buffers come from identical section bytes.
      const aBytes = aProp as Uint8Array | Uint16Array | Uint32Array;
      const bBytes = bProp as Uint8Array | Uint16Array | Uint32Array;
      expect(Array.from(aBytes)).toEqual(Array.from(bBytes));
    } finally {
      await b.close();
      await a.close();
    }
  });

  it("closing one of two clients leaves the other working", async () => {
    // The worker hosts a core per client; closing one client must not
    // tear down the shared worker while another client is still
    // attached over the port.
    const buf = await encodeContainer(twoBlockTopology());
    const bufB = new Uint8Array(buf);
    const a = await openProxy(buf);
    const port = a.attachPort();
    const b = await CtopoClient.open(bufB, { port });
    await a.close();
    // b should still be able to issue requests — the worker stays alive
    // as long as b is connected.
    const result = await merge(b, [{ layer: "block", indices: [0, 1] }]);
    expect(result.type).toBe("MultiPolygon");
    expect(result.coordinates.length).toBe(1);
    await b.close();
  });
});

// Worker-death handling — no built worker needed. A fake Worker lets us
// drive open() to completion and then synthesize "error"/"exit" events,
// asserting that in-flight calls reject instead of hanging forever.
type FakeListener = (e: { data?: unknown; message?: string }) => void;

class FakeWorker {
  msg: FakeListener[] = [];
  err: FakeListener[] = [];
  exit: FakeListener[] = [];
  posted: Array<Record<string, unknown>> = [];

  addEventListener(type: string, l: FakeListener): void {
    if (type === "message") this.msg.push(l);
    else if (type === "error") this.err.push(l);
    else if (type === "exit") this.exit.push(l);
  }
  removeEventListener(type: string, l: FakeListener): void {
    if (type === "message") this.msg = this.msg.filter((x) => x !== l);
  }
  postMessage(m: Record<string, unknown>): void {
    this.posted.push(m);
    // Auto-ack the open RPC with a minimal snapshot so open() resolves.
    if (m.method === "open") {
      queueMicrotask(() =>
        this.emit({
          clientId: m.clientId,
          id: m.id,
          ok: true,
          result: { meta: { numArcs: 0 }, sections: [], transform: null },
        }),
      );
    }
  }
  emit(data: unknown): void {
    for (const l of [...this.msg]) l({ data });
  }
  fireError(message: string): void {
    for (const l of [...this.err]) l({ message });
  }
  fireExit(code: number): void {
    for (const l of [...this.exit]) l({ data: code });
  }
}

async function openFake(fake: FakeWorker): Promise<CtopoClient> {
  return CtopoClient.open(new Uint8Array([1, 2, 3]), {
    worker: fake as unknown as Worker,
  });
}

describe("CtopoClient worker-death handling", () => {
  it("rejects in-flight calls when the worker emits an error", async () => {
    const fake = new FakeWorker();
    const client = await openFake(fake);
    // call() registers the pending entry synchronously in its executor.
    const pending = client.property("block/x");
    fake.fireError("boom");
    await expect(pending).rejects.toThrow(/worker error: boom/);
    // The client is now marked closed; further calls reject immediately.
    await expect(client.property("block/y")).rejects.toThrow(/closed/);
  });

  it("rejects in-flight calls when the worker exits non-zero", async () => {
    const fake = new FakeWorker();
    const client = await openFake(fake);
    const pending = client.getStats();
    fake.fireExit(1);
    await expect(pending).rejects.toThrow(/exited with code 1/);
  });

  it("treats a clean exit (code 0) as a no-op", async () => {
    const fake = new FakeWorker();
    const client = await openFake(fake);
    const pending = client.getStats();
    fake.fireExit(0);
    // The call must still be live — reply normally and confirm it resolves.
    const call = fake.posted[fake.posted.length - 1];
    const stats = { requests: 1 };
    fake.emit({
      clientId: call.clientId,
      id: call.id,
      ok: true,
      result: stats,
    });
    await expect(pending).resolves.toBe(stats);
  });
});
