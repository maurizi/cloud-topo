// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Fetcher abstractions and factories for Range-based I/O.
 *
 * Provides `RangeFetcher` — the single abstraction both the open path
 * and lazy section loading use — plus three concrete implementations:
 *   - `makeHttpFetcher`   — browser `fetch()` with Range headers
 *   - `makeBufferFetcher`  — in-memory (tests, build tools)
 *   - `makeRangeFetcher`   — lifts a "give me this Range header" callback
 */

import { parseMultipartByteRanges } from "./multipart.js";

// --- Types ---

// HTTP/2 priority hint passed through to fetch()'s `priority` option
// (Fetch Priority API — Chrome 102+/Firefox 119+/Safari 17+). The
// browser tags streams with these hints so the server allocates more
// bandwidth to "high" while letting "low" yield. We use "high" for
// the small open-path critical fetches (footer, arc_offsets, dict,
// block table) so they don't get throttled behind bulk property
// GETs on shared HTTP/2 connections. Backends that don't support
// priority are expected to ignore it.
export type FetchPriority = "high" | "low" | "auto";

// Result of a multi-range fetch. "parts" = server returned 206
// multipart/byteranges, "unsupported" = server returned 200 and we
// aborted the body download early so the client can fall back to
// single-range requests.
export type MultiRangeResult =
  | {
      kind: "parts";
      parts: Array<{ start: number; end: number; bytes: Uint8Array }>;
    }
  | { kind: "unsupported" };

// Single fetcher abstraction: the open path needs both an absolute
// `[start, end)` range and a suffix-range "last N bytes" form, and
// real backends (HTTP, S3 SDK, in-memory test buffers) implement both
// the same way — one Range header value goes in, bytes come out.
//
// `range(start, end)` must return exactly `end - start` bytes.
// `suffix(length)` returns the last `length` bytes (or the whole
// file if it's shorter than `length`).
//
// `multiRange` is optional — when present the client can batch
// disjoint ranges into a single HTTP request. Implementations that
// don't support it simply omit the method and the client falls back
// to individual `range()` calls.
export interface RangeFetcher {
  range(
    start: number,
    end: number,
    signal?: AbortSignal,
    priority?: FetchPriority,
  ): Promise<Uint8Array>;
  suffix(
    length: number,
    signal?: AbortSignal,
    priority?: FetchPriority,
  ): Promise<Uint8Array>;
  multiRange?(
    ranges: ReadonlyArray<{ start: number; end: number }>,
    signal?: AbortSignal,
    priority?: FetchPriority,
  ): Promise<MultiRangeResult>;
}

// --- Perf instrumentation ---

// Broadcasts every Range GET so the main thread can mirror it to
// the page console.
const perfChannel =
  typeof BroadcastChannel === "undefined"
    ? null
    : new BroadcastChannel("ctopo-perf");

export function perfLog(msg: string): void {
  if (perfChannel !== null) perfChannel.postMessage(msg);
}

// --- Public fetcher spec (cross-worker) ---

// Tagged union describing how the worker should reconstruct the
// `RangeFetcher`. Only kinds the worker can build from postMessage-
// safe data appear here — function-based fetchers (e.g. user-provided
// closures) can't cross the boundary, so the surface intentionally
// excludes them.
//
// Layout per kind:
//   - "http":   URL string only.
//   - "buffer": raw bytes; the worker uses them in place (callers may
//               transfer the underlying ArrayBuffer at open time so
//               there's no copy).
export type FetcherSpec =
  | { readonly kind: "http"; readonly url: string }
  | { readonly kind: "buffer"; readonly bytes: Uint8Array };

// Reconstruct a `RangeFetcher` from a `FetcherSpec`. Worker-side only:
// runs once at open time, on the receiving side of the postMessage
// boundary. Throws on unknown `kind` so format additions can't be
// silently ignored by older worker bundles.
export function reconstructFetcher(spec: FetcherSpec): RangeFetcher {
  switch (spec.kind) {
    case "http":
      return makeHttpFetcher(spec.url);
    case "buffer":
      return makeBufferFetcher(spec.bytes);
    default: {
      const _exhaustive: never = spec;
      void _exhaustive;
      throw new Error(
        `ctopo: unknown fetcher spec kind ${(spec as { kind: string }).kind}`,
      );
    }
  }
}

// --- Fetcher factories ---

// Lift a "fetch this Range header value" callback into a RangeFetcher.
// Most backends already speak HTTP Range — this helper hands them the
// header string and they hand back bytes, so callers don't have to
// duplicate the start/end vs. suffix-length wiring. The priority hint
// is forwarded so HTTP-based backends can pass it to fetch().
export function makeRangeFetcher(
  byHeader: (
    rangeHeader: string,
    signal?: AbortSignal,
    priority?: FetchPriority,
  ) => Promise<Uint8Array>,
): RangeFetcher {
  return {
    range: (start, end, signal, priority) =>
      byHeader(`bytes=${start}-${end - 1}`, signal, priority),
    suffix: (length, signal, priority) =>
      byHeader(`bytes=-${length}`, signal, priority),
  };
}

// In-memory backed fetcher — for tests and any caller that already
// has the entire file in a buffer. Ignores priority (no network).
export function makeBufferFetcher(buf: Uint8Array): RangeFetcher {
  return {
    range: (start, end) => {
      const clampedEnd = Math.min(end, buf.byteLength);
      return Promise.resolve(buf.subarray(start, clampedEnd));
    },
    suffix: (length) => {
      const clamped = Math.min(length, buf.byteLength);
      return Promise.resolve(buf.subarray(buf.byteLength - clamped));
    },
    multiRange: (ranges) => {
      const parts = ranges.map((r) => ({
        start: r.start,
        end: r.end,
        bytes: buf.subarray(r.start, Math.min(r.end, buf.byteLength)),
      }));
      return Promise.resolve({ kind: "parts" as const, parts });
    },
  };
}

// Default HTTP-based fetcher. Issues `fetch(url, { headers: { Range: ... } })`
// for both range and suffix forms, with shared logging + per-call ids
// so concurrent requests are easy to follow in the log. Forwards the
// caller's priority hint via fetch()'s `priority` option (Fetch
// Priority API).
export function makeHttpFetcher(url: string): RangeFetcher {
  let seq = 0;
  perfLog(`[ctopo] open ${url}`);
  const issue = async (
    rangeHeader: string,
    label: string,
    expectedLength: number | undefined,
    signal: AbortSignal | undefined,
    priority: FetchPriority | undefined,
  ): Promise<Uint8Array> => {
    const id = ++seq;
    const t0 = performance.now();
    const prioLabel =
      priority !== undefined && priority !== "auto" ? ` prio=${priority}` : "";
    perfLog(`[ctopo] #${id} GET ${label}${prioLabel} start`);
    const response = await fetch(url, {
      headers: { Range: rangeHeader },
      signal,
      // Cast: `priority` is in lib.dom for modern TS but absent in
      // older targets. Browsers without support ignore it.
      ...(priority !== undefined ? { priority } : {}),
    });
    if (response.status === 200) {
      // Server ignored the Range header and returned the full object.
      // Consuming it would give the caller the entire file instead of
      // the requested slice, corrupting every section read. Abort early.
      void response.body?.cancel();
      throw new Error(
        `ctopo: server does not support Range requests (returned 200 for ${rangeHeader})`,
      );
    }
    if (!response.ok && response.status !== 206) {
      throw new Error(
        `ctopo: HTTP ${response.status} fetching ${url} ${rangeHeader}`,
      );
    }
    if (expectedLength !== undefined) {
      const contentRange = response.headers.get("Content-Range");
      if (contentRange !== null) {
        const match = /bytes (\d+)-(\d+)\/(\d+|\*)/.exec(contentRange);
        if (match !== null) {
          const got = Number(match[2]) - Number(match[1]) + 1;
          if (got !== expectedLength) {
            throw new Error(
              `ctopo: server returned ${got} bytes for ${rangeHeader}, expected ${expectedLength}`,
            );
          }
        }
      }
    }
    const buf = await response.arrayBuffer();
    perfLog(
      `[ctopo] #${id} GET ${label} done in ${(performance.now() - t0).toFixed(0)}ms (${buf.byteLength}B)`,
    );
    return new Uint8Array(buf);
  };
  return {
    range: (start, end, signal, priority) =>
      issue(
        `bytes=${start}-${end - 1}`,
        `[${start}, ${end}) (${end - start}B)`,
        end - start,
        signal,
        priority,
      ),
    suffix: (length, signal, priority) =>
      issue(
        `bytes=-${length}`,
        `bytes=-${length}`,
        undefined,
        signal,
        priority,
      ),
    multiRange: async (ranges, signal, priority) => {
      const id = ++seq;
      const t0 = performance.now();
      const rangeHeader =
        "bytes=" + ranges.map((r) => `${r.start}-${r.end - 1}`).join(", ");
      const totalBytes = ranges.reduce((s, r) => s + (r.end - r.start), 0);
      const prioLabel =
        priority !== undefined && priority !== "auto"
          ? ` prio=${priority}`
          : "";
      perfLog(
        `[ctopo] #${id} MULTI-GET ${ranges.length} ranges (${totalBytes}B)${prioLabel} start`,
      );
      const response = await fetch(url, {
        headers: { Range: rangeHeader },
        signal,
        ...(priority !== undefined ? { priority } : {}),
      });

      if (!response.ok && response.status !== 206) {
        throw new Error(
          `ctopo: HTTP ${response.status} fetching ${url} ${rangeHeader}`,
        );
      }

      // 200 = server doesn't support multi-range. Abort the body
      // download (could be the entire file) and signal the client to
      // fall back to individual range requests.
      if (response.status === 200) {
        void response.body?.cancel();
        perfLog(
          `[ctopo] #${id} MULTI-GET got 200 (no multi-range support), aborting after ${(performance.now() - t0).toFixed(0)}ms`,
        );
        return { kind: "unsupported" as const };
      }

      const buf = new Uint8Array(await response.arrayBuffer());
      perfLog(
        `[ctopo] #${id} MULTI-GET done in ${(performance.now() - t0).toFixed(0)}ms (${buf.byteLength}B)`,
      );

      // 206 — check if multipart or single-range response
      const ct = response.headers.get("Content-Type") ?? "";
      if (ct.includes("multipart/byteranges")) {
        const parts = parseMultipartByteRanges(ct, buf);
        return { kind: "parts" as const, parts };
      }

      // Single-range 206 (server collapsed multi-range to one range,
      // or only one range was requested). Parse Content-Range.
      const contentRange = response.headers.get("Content-Range");
      if (contentRange === null) {
        throw new Error(
          `ctopo: 206 response has neither multipart body nor Content-Range header`,
        );
      }
      const match = /bytes (\d+)-(\d+)\/(\d+|\*)/.exec(contentRange);
      if (match === null) {
        throw new Error(`ctopo: unparseable Content-Range: ${contentRange}`);
      }
      const start = Number(match[1]);
      const end = Number(match[2]) + 1;
      return {
        kind: "parts" as const,
        parts: [{ start, end, bytes: buf }],
      };
    },
  };
}
