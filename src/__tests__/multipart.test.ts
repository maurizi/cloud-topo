// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

import { describe, it, expect } from "vitest";
import { extractBoundary, parseMultipartByteRanges } from "../core/multipart";

const enc = new TextEncoder();

// Concatenate ASCII strings and raw byte payloads into one body.
function body(...parts: Array<string | Uint8Array>): Uint8Array {
  const chunks = parts.map((p) => (typeof p === "string" ? enc.encode(p) : p));
  const total = chunks.reduce((n, c) => n + c.byteLength, 0);
  const out = new Uint8Array(total);
  let o = 0;
  for (const c of chunks) {
    out.set(c, o);
    o += c.byteLength;
  }
  return out;
}

const CT = "multipart/byteranges; boundary=SEP";

describe("extractBoundary", () => {
  it("pulls the boundary token out of the Content-Type", () => {
    expect(extractBoundary(CT)).toBe("SEP");
    expect(extractBoundary('multipart/byteranges; boundary="q;x"')).toBe('"q'); // stops at ;
  });

  it("returns null when no boundary is present", () => {
    expect(extractBoundary("application/octet-stream")).toBeNull();
  });
});

describe("parseMultipartByteRanges", () => {
  it("parses two disjoint ranges, preserving binary payloads", () => {
    const p0 = new Uint8Array([1, 2, 3, 4]);
    const p1 = new Uint8Array([200, 201, 202]);
    const raw = body(
      "--SEP\r\n",
      "Content-Type: application/octet-stream\r\n",
      "Content-Range: bytes 0-3/100\r\n\r\n",
      p0,
      "\r\n--SEP\r\n",
      "Content-Range: bytes 50-52/100\r\n\r\n",
      p1,
      "\r\n--SEP--\r\n",
    );

    const parts = parseMultipartByteRanges(CT, raw);
    expect(parts).toHaveLength(2);
    expect(parts[0]).toMatchObject({ start: 0, end: 4 });
    expect(parts[1]).toMatchObject({ start: 50, end: 53 });
    expect([...parts[0].bytes]).toEqual([1, 2, 3, 4]);
    expect([...parts[1].bytes]).toEqual([200, 201, 202]);
  });

  it("throws when the Content-Type carries no boundary", () => {
    expect(() => parseMultipartByteRanges("text/plain", body("x"))).toThrow(
      /missing boundary/,
    );
  });

  it("throws when the first boundary is absent from the body", () => {
    expect(() =>
      parseMultipartByteRanges(CT, body("no boundary here at all")),
    ).toThrow(/first boundary not found/);
  });

  it("throws when a part body is shorter than its Content-Range", () => {
    const raw = body(
      "--SEP\r\n",
      "Content-Range: bytes 0-9/100\r\n\r\n", // declares 10 bytes
      new Uint8Array([1, 2, 3]), // only 3
      "\r\n--SEP--\r\n",
    );
    expect(() => parseMultipartByteRanges(CT, raw)).toThrow(
      /multipart part body is 3 bytes but Content-Range declares 10/,
    );
  });
});
