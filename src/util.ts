// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Shared utilities used by both the client (browser) and encoder (Node).
 *
 * Tree-shaking ensures browser bundles only pull `runWithConcurrency`;
 * `stderrLog` and `memSnapshot` are Node-only and only imported by the
 * encoder.
 */

import { writeSync } from "fs";

// --- Bounded-concurrency worker pool ---

// Run `fn` on every item with at most `limit` concurrent invocations.
// Each of N workers pulls the next item from a shared cursor, awaits
// `fn`, then pulls the next — total in-flight Promises = N regardless
// of `items.length`.
export async function runWithConcurrency<T>(
  items: ReadonlyArray<T>,
  limit: number,
  fn: (item: T, index: number) => Promise<void>,
): Promise<void> {
  let cursor = 0;
  const worker = async (): Promise<void> => {
    while (cursor < items.length) {
      const i = cursor++;
      await fn(items[i], i);
    }
  };
  const n = Math.min(limit, items.length);
  const workers: Promise<void>[] = new Array(n);
  for (let i = 0; i < n; i++) workers[i] = worker();
  await Promise.all(workers);
}

// --- Node-only stderr logging ---

// Unbuffered write to stderr. Node's `process.stderr.write` is block-
// buffered through docker pipes and lines get swallowed if the process
// is killed before the buffer flushes; `writeSync(2, ...)` bypasses
// that.
export function stderrLog(msg: string): void {
  writeSync(2, msg + "\n");
}

// Format `process.memoryUsage()` as a compact one-liner for progress
// logging during long encode passes.
export function memSnapshot(): string {
  const m = process.memoryUsage();
  return (
    `rss=${(m.rss / 1024 / 1024).toFixed(0)}M ` +
    `heap=${(m.heapUsed / 1024 / 1024).toFixed(0)}/${(m.heapTotal / 1024 / 1024).toFixed(0)}M ` +
    `external=${(m.external / 1024 / 1024).toFixed(0)}M ` +
    `arrayBuffers=${(m.arrayBuffers / 1024 / 1024).toFixed(0)}M`
  );
}
