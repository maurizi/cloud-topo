// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Browser-safe shared utilities. Anything that needs `fs` /
 * `process.memoryUsage()` lives in `util-node.ts` so tsup's chunking
 * doesn't pull Node-only code into the chunk shared with the browser
 * client (which broke under Vite with "Module 'fs' has been
 * externalized for browser compatibility").
 */

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
  const workers = new Array<Promise<void>>(n);
  for (let i = 0; i < n; i++) workers[i] = worker();
  await Promise.all(workers);
}
