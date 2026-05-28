# cloud-topo

Cloud-optimized binary topology container with HTTP Range support and topojson-compatible primitives.

## What is cloud-topo?

cloud-topo (`.ctopo`) packs a quantized topology — arcs, per-layer geometry, and per-feature properties — into a single HTTP-Range-friendly binary container. It mirrors the [topojson-client](https://github.com/topojson/topojson-client) API (`merge`, `mergeArcs`, `neighbors`, `bbox`, `transform`) but fetches only the byte ranges each operation needs, so you can work with large topologies straight from object storage without downloading the whole file.

It's best for serving large topologies (thousands to millions of features) from static hosting or object storage (S3, GCS, R2) when you need selective operations — merging subsets, reading one property column — without downloading everything.

Hosting behind a CDN that supports **multi-range requests** (multiple byte ranges in one `Range` header, returned as `multipart/byteranges`) lets the client coalesce the disjoint reads a single merge needs into one round trip instead of a dozen. CloudFront supports this; bare S3/GCS/R2 don't. The client falls back to one request per chunk transparently — multi-range is an optimization, not a requirement.

## Installation

```bash
npm install cloud-topo
```

## Usage

### Reading a remote .ctopo file

```ts
import { CtopoClient, merge, neighbors, bbox } from "cloud-topo";

// Open — issues two parallel Range GETs (front + suffix)
const client = await CtopoClient.open("https://example.com/region.ctopo");

// Merge a subset of features → GeoJSON MultiPolygon, one polygon per
// connected component (largest-area ring exterior, rest holes). Matches
// topojson-client's `merge` semantics.
const boundary = await merge(client, [
  { layer: "blocks", indices: [0, 1, 5, 12] },
]);

// Adjacency: adj[i] is a sorted array of feature indices sharing an arc with i
const adj = await neighbors(client, "blocks");

// Typed property column → Uint32Array (or whichever dtype the encoder chose)
const population = await client.property("blocks/population");

// String column → StringArray with lazy UTF-8 decoding (names.get(0), names.length)
const names = await client.strings("blocks/name");

// Bounding box (sync)
const [minX, minY, maxX, maxY] = bbox(client);

// Terminate the backing worker when done
client.close();
```

### Converting TopoJSON to .ctopo

The encoder is Node-only and lives at a separate entrypoint so browser bundles never pull in `fs` or `zlib`:

```ts
import { writeContainer, encodeContainer } from "cloud-topo/encode";

await writeContainer("output.ctopo", topology); // write to a file
const buf = await encodeContainer(topology); // or get a Buffer
```

The input is a standard [TopoJSON Topology](https://github.com/topojson/topojson-specification).

### Rewriting properties in an existing container

```ts
import { rewriteContainer } from "cloud-topo/encode";

await rewriteContainer("input.ctopo", "output.ctopo", [
  { name: "blocks/population", data: [42, 99, 150 /* ... */] },
]);
```

Non-overridden sections pass through byte-for-byte; only the named properties are re-encoded and re-compressed.

### Decoding a container back to TopoJSON

`decodeContainer` is the inverse of `encodeContainer` — it reconstructs the full TopoJSON `Topology` (transform, bbox, arcs, and per-layer `GeometryCollection`s with properties) from a container's bytes:

```ts
import { readFileSync } from "fs";
import { decodeContainer, encodeContainer } from "cloud-topo/encode";

const topology = decodeContainer(readFileSync("input.ctopo"));

// e.g. re-encode a container after editing it in TopoJSON form:
const buf = await encodeContainer(topology);
```

Note that the encoder renumbers arc ids for spatial locality, so a decoded-then-re-encoded container is geometrically identical but not byte-identical to the original. Like the rest of this entrypoint it is Node-only.

## Browser setup

`CtopoClient` runs its network, decompression, and merge work in a Web Worker, backed by a WASM zstd decoder. Two things to know for browser builds:

- **Bundling.** The worker and `.wasm` load as sibling assets via `new URL(…, import.meta.url)`. Vite, webpack 5, Rollup, esbuild, and Parcel handle this automatically. Loading raw from a CDN requires `worker.js`, the zstd worker chunks, and the `.wasm` files to sit alongside the entry (all shipped in `dist/`). Pass `workerUrl` to `open` to point elsewhere.
- **Cross-origin isolation.** The fast path uses `SharedArrayBuffer` for multi-threaded performance, which a browser only allows on a [cross-origin isolated](https://developer.mozilla.org/en-US/docs/Web/API/Window/crossOriginIsolated) page — served with `Cross-Origin-Opener-Policy: same-origin`. **This is optional:** without it, cloud-topo automatically falls back to a slower single-worker.

## Why not just use TopoJSON?

|                       | TopoJSON (JSON)                                    | cloud-topo (binary)                                                                           |
|-----------------------|----------------------------------------------------|-----------------------------------------------------------------------------------------------|
| **Wire size**         | JSON text; gzip helps but can't beat binary + zstd | Varint-encoded arcs + zstd-compressed sections; typically 30–60% smaller                      |
| **Load strategy**     | Download entire file, parse all JSON               | Two small Range GETs open the container; subsequent sections fetched on demand                |
| **Merge performance** | Parse full topology, walk every arc                | Binary arc stitching over pre-indexed CSR geometry; only fetches boundary arcs                |
| **Property access**   | Parse entire file to read one column               | Column-oriented sections; fetch only the property you need                                    |
| **Compression**       | gzip on the wire (server-side)                     | Per-section zstd (with shared dictionary for arc blocks) or Brotli; client-side decompression |

[Benchmarking](https://github.com/maurizi/cloud-topo-bench) against all US census blocks, block groups, counties and states; the `.topojson` file was 4.8gb and the equivalent `.ctopo` file 1.2gb. Running a `merge` operation to recover US congressional districts from their [block-equivalency files](https://www.census.gov/geographies/mapping-files/2025/dec/rdo/119-congressional-district-bef.html) took 15s and consumed ~190mb.

## File format

```
 Offset          Field              Size
 ─────────────────────────────────────────────────────
 0..4            magic "CTPO"       4 B
 4..8            version            4 B  (major:u8 | minor:u8 | patch:u16)
 8..16           reserved           8 B
 16..            data sections      (16-byte aligned, front-loaded first)
 ...             (padding)
 end-F..end-8    footer:
                   section_count    4 B
                   meta_length      4 B
                   section_table    (name[16] + offset:u64 + length:u64) × N
                   meta_json        UTF-8 bytes
 end-8..end      footer_length      8 B
```

All values are little-endian. Opening a container is two parallel Range GETs: a suffix GET (`bytes=-N`) reads the footer — every section's offset plus the metadata JSON — and a front GET covers the front-loaded sections (CSR geometry, arc offsets) so the first merge needs no extra round trip. Everything else (arc coordinate slices, property columns) is fetched on demand, coalesced into multi-range requests where supported.

## API reference

### Main entrypoint (`cloud-topo`)

| Export                                   | Kind     | Description                                                                                                                |
|------------------------------------------|----------|----------------------------------------------------------------------------------------------------------------------------|
| `CtopoClient`                            | class    | Opens and reads `.ctopo` containers over HTTP Range                                                                        |
| `CtopoClient.open(source, opts?)`        | static   | Open a container. `source` is a URL string (HTTP Range) or a `Uint8Array` of container bytes                               |
| `client.property(name, signal?)`         | method   | Fetch a typed property section                                                                                             |
| `client.strings(name, signal?)`          | method   | Fetch a string property section                                                                                            |
| `client.layerGeometry(layer, signal?)`   | method   | Fetch CSR geometry for a layer                                                                                             |
| `client.fetchArcs(ids, signal?)`         | method   | Fetch raw arc coordinate bytes                                                                                             |
| `client.close()`                         | method   | Terminate the backing worker (call when done — otherwise it leaks)                                                         |
| `openContainer(source, opts?)`           | function | Shorthand for `CtopoClient.open`                                                                                           |
| `merge(client, selections, signal?)`     | function | Merge features → GeoJSON MultiPolygon (one polygon per connected component; largest-area ring is exterior, rest are holes) |
| `mergeArcs(client, selections, signal?)` | function | Same grouping as `merge`, but returns signed arc-id rings instead of decoded coords                                        |
| `neighbors(client, layer, signal?)`      | function | Per-feature adjacency via shared arcs                                                                                      |
| `bbox(client)`                           | function | Container bounding box `[minX, minY, maxX, maxY]`                                                                          |
| `transform(t)`                           | function | Returns quantization transform function                                                                                    |
| `untransform(t)`                         | function | Returns inverse quantization transform function                                                                            |
| `parseContainer(bytes)`                  | function | Parse an in-memory container (header + footer) without a worker                                                            |
| `parseFooter(bytes)`                     | function | Parse just the trailing footer (section table + meta JSON)                                                                 |
| `parseFrontHeader(bytes)`                | function | Parse the leading magic + version header                                                                                   |
| `viewSection(bytes, entry)`              | function | Slice a section's bytes out of an in-memory container                                                                      |
| `StringArray`                            | class    | Lazy UTF-8 string decoder for string sections                                                                              |

### Types

| Export                 | Description                                    |
|------------------------|------------------------------------------------|
| `ContainerMeta`        | Parsed metadata JSON from the footer           |
| `SectionEntry`         | One row of the binary section table            |
| `LayerGeometry`        | CSR triple (polyOffsets, ringOffsets, arcRefs) |
| `LayerSelection`       | `{ layer: string; indices: Iterable<number> }` |
| `PropertyOverride`     | Override for `rewriteContainer`                |
| `DType`                | Section data type union                        |
| `OpenContainerOptions` | Options for `CtopoClient.open`                 |
| `CtopoClientStats`     | Fetch/decode counters from `client.getStats()` |
| `MultiPolygonArcs`     | Arc-id geometry from `mergeArcs`               |

### Encoder entrypoint (`cloud-topo/encode`)

| Export                                                | Description                                                                                                               |
|-------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `encodeContainer(topology, opts?)`                    | TopoJSON → `.ctopo` Buffer                                                                                                |
| `decodeContainer(bytes)`                              | `.ctopo` bytes → TopoJSON `Topology` (inverse of `encodeContainer`)                                                       |
| `writeContainer(path, topology, opts?)`               | Encode and write to file                                                                                                  |
| `rewriteContainer(inPath, outPath, overrides, opts?)` | Mutate named property sections in an existing container; `opts.frontLoadedSectionNames` adds extras to the front-load set |

## License

[Apache-2.0](LICENSE)
