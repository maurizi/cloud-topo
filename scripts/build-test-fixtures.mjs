#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.
//
// Encode the committed real-data TopoJSON test fixture into a .ctopo
// container for the e2e suite (src/__tests__/state-fixture.test.ts).
// Runs in `pretest` after `tsup`, so it also serves as an end-to-end
// smoke test of the encoder against real geometry: a broken encoder
// fails the build here, before any test runs.
//
// The fixture is the full U.S. Census TIGER Vermont topology (FIPS 50),
// quantized, with all three layers (block / VTD / county). ~63K arcs →
// the block-partitioned arc_coords / arc_endpoints paths fan out across
// ~1.2K partitions, so a whole-layer union exercises the cross-partition
// fetch the synthetic unit fixtures never reach.
//
// Output (.ctopo) is gitignored — it's a build artifact regenerated
// from the committed .gz on every `npm test`.

import { readFileSync, writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { pathToFileURL } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = dirname(here);
const fixturesDir = join(repoRoot, "src", "__tests__", "fixtures");
const srcGz = join(fixturesDir, "vt.topojson.gz");
const outCtopo = join(fixturesDir, "vt.ctopo");

// Import the built encoder. tsup must have run first (it does, in the
// `pretest` chain). Resolve via file URL so this works on every OS.
const { encodeContainer } = await import(
  pathToFileURL(join(repoRoot, "dist", "encode.js")).href
);

const topo = JSON.parse(gunzipSync(readFileSync(srcGz)).toString("utf8"));
const t0 = Date.now();
const buf = await encodeContainer(topo);
writeFileSync(outCtopo, buf);
process.stdout.write(
  `[fixtures] encoded vt.ctopo: ${buf.byteLength} bytes ` +
    `in ${((Date.now() - t0) / 1000).toFixed(1)}s\n`,
);
