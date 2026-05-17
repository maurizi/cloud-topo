// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * Node-only utilities used by the encoder. Kept out of `util.ts` so
 * the browser-shared chunk doesn't transitively import `fs`.
 */

import { writeSync } from "fs";

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
