// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

// Covers the no-SharedArrayBuffer capability fallback: the `sabUsable`
// gate that decides between the fast (shared sub-worker + merge pool)
// and fallback (in-process) paths, and the plain non-shared wasm
// decoder that the fallback decodes with.

import { describe, it, expect, vi, afterEach } from "vitest";
import { zstdCompressSync } from "node:zlib";
import { sabUsable } from "../core/util";
import { plainZstd } from "../core/zstd-wasm/plain-decoder";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("sabUsable", () => {
  it("is true in Node (SAB defined, crossOriginIsolated undefined)", () => {
    expect(typeof SharedArrayBuffer).toBe("function");
    expect(sabUsable()).toBe(true);
  });

  it("is false when the page is not cross-origin isolated", () => {
    vi.stubGlobal("crossOriginIsolated", false);
    expect(sabUsable()).toBe(false);
  });

  it("is true when the page is cross-origin isolated", () => {
    vi.stubGlobal("crossOriginIsolated", true);
    expect(sabUsable()).toBe(true);
  });

  it("is false when SharedArrayBuffer is absent entirely", () => {
    vi.stubGlobal("SharedArrayBuffer", undefined);
    expect(sabUsable()).toBe(false);
  });
});

describe("plain in-process zstd decoder (no-SAB fallback)", () => {
  it("round-trips a zstd frame without a dictionary", async () => {
    const original = new Uint8Array(4096);
    for (let i = 0; i < original.length; i++) original[i] = (i * 7) & 0xff;
    const compressed = new Uint8Array(zstdCompressSync(original));

    const out = await plainZstd.decompress(compressed, original.length + 64);
    expect(out.byteLength).toBe(original.length);
    expect([...out]).toEqual([...original]);
  });

  it("decodes independently of the shared-memory path", async () => {
    // The plain build instantiates its own non-shared wasm memory, so
    // it must work even with SharedArrayBuffer stubbed out.
    vi.stubGlobal("SharedArrayBuffer", undefined);
    const original = new TextEncoder().encode("cloud-topo".repeat(500));
    const compressed = new Uint8Array(zstdCompressSync(original));
    const out = await plainZstd.decompress(compressed, original.length + 64);
    expect([...out]).toEqual([...original]);
  });
});
