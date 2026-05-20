// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Browser-safe shared utilities. Anything that needs `fs` /
 * `process.memoryUsage()` lives in `util-node.ts` so tsup's chunking
 * doesn't pull Node-only code into the chunk shared with the browser
 * client (which broke under Vite with "Module 'fs' has been
 * externalized for browser compatibility").
 */

// --- Quantization transform ---

// A topology's quantization transform: linear scale + translate per
// axis, or null for un-quantized (float) coordinates. Structurally
// identical to ContainerMeta["transform"].
export type TransformDef = {
  readonly scale: readonly [number, number];
  readonly translate: readonly [number, number];
} | null;

// Map a quantized point to absolute coordinates (scale then translate).
export function transform(
  t: TransformDef,
): (point: readonly [number, number]) => [number, number] {
  if (t === null) return (p) => [p[0], p[1]];
  return (p) => [
    p[0] * t.scale[0] + t.translate[0],
    p[1] * t.scale[1] + t.translate[1],
  ];
}

// Inverse of `transform`: absolute coordinates back to quantized grid.
export function untransform(
  t: TransformDef,
): (point: readonly [number, number]) => [number, number] {
  if (t === null) return (p) => [p[0], p[1]];
  return (p) => [
    (p[0] - t.translate[0]) / t.scale[0],
    (p[1] - t.translate[1]) / t.scale[1],
  ];
}

// --- Capability detection ---

// Whether a SharedArrayBuffer can actually be shared across the
// worker boundary — the gate for the fast path (zero-copy zstd
// sub-worker + parallel merge pool).
//
// We test `crossOriginIsolated`, not just `typeof SharedArrayBuffer`,
// because the constructor can exist in a non-isolated page while
// posting a SAB (or a shared WebAssembly.Memory) to a worker still
// throws DataCloneError. Cross-origin isolation is the real gate.
// Node leaves `crossOriginIsolated` undefined (SAB is always
// shareable there), so only an explicit `false` disqualifies.
export function sabUsable(): boolean {
  if (typeof SharedArrayBuffer === "undefined") return false;
  return typeof crossOriginIsolated !== "boolean" || crossOriginIsolated;
}

// --- Buffer copy ---

// Copy the live bytes of `view` into a fresh buffer. Use this rather
// than `slice()`: a Node Buffer's slice returns a view (so
// transferring its `.buffer` would detach the source), and a SAB's
// slice returns another SAB — rejected by transfer lists and it would
// leak shared writes back to the worker. `shared` targets a
// SharedArrayBuffer (for cross-thread sharing without a transfer);
// the default is a private, transferable ArrayBuffer.
export function copyView(view: ArrayBufferView): ArrayBuffer;
export function copyView(
  view: ArrayBufferView,
  shared: true,
): SharedArrayBuffer;
export function copyView(
  view: ArrayBufferView,
  shared?: boolean,
): ArrayBufferLike {
  const out = shared
    ? new SharedArrayBuffer(view.byteLength)
    : new ArrayBuffer(view.byteLength);
  new Uint8Array(out).set(
    new Uint8Array(view.buffer, view.byteOffset, view.byteLength),
  );
  return out;
}

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
