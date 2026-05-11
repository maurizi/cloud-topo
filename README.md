# cloud-topo

Cloud-optimized binary topology container with HTTP Range support and topojson-compatible primitives.

## What is cloud-topo?

cloud-topo (`.ctopo`) is a binary file format that packs a quantized topology — arcs, per-layer geometry, and per-feature properties — into a single HTTP-Range-friendly container. It mirrors the [topojson-client](https://github.com/topojson/topojson-client) API (`merge`, `neighbors`, `bbox`, `transform`) but fetches only the arc coordinate slices each operation needs, making it practical to work with large topologies directly from object storage without downloading the entire file.

## Why not just use TopoJSON?

|                       | TopoJSON (JSON)                                    | cloud-topo (binary)                                                                           |
|-----------------------|----------------------------------------------------|-----------------------------------------------------------------------------------------------|
| **Wire size**         | JSON text; gzip helps but can't beat binary + zstd | Varint-encoded arcs + zstd-compressed sections; typically 30–60% smaller                      |
| **Load strategy**     | Download entire file, parse all JSON               | Two small Range GETs open the container; subsequent sections fetched on demand                |
| **Merge performance** | Parse full topology, walk every arc                | Binary arc stitching over pre-indexed CSR geometry; only fetches boundary arcs                |
| **Property access**   | Parse entire file to read one column               | Column-oriented sections; fetch only the property you need                                    |
| **Compression**       | gzip on the wire (server-side)                     | Per-section zstd (with shared dictionary for arc blocks) or Brotli; client-side decompression |

[Benchmarking](https://github.com/maurizi/cloud-topo-bench) against all US census blocks, VTDs, counties and states; the `.topojson` file was 4.4gb and the equivalent `.ctopo` file 1.1gb. Running a `merge` operation to recover US congressional districts from their [block-equivalency files](https://www.census.gov/geographies/mapping-files/2025/dec/rdo/119-congressional-district-bef.html) took 70s and consumed ~240mb.

cloud-topo is best suited for applications that serve large topologies (thousands to millions of features) from static hosting or object storage (S3, GCS, R2) and need to perform selective operations like merging subsets or reading individual properties without downloading everything.

For best performance, host `.ctopo` files behind a CDN that supports **multi-range requests** (multiple byte ranges in one `Range` header, returned as `multipart/byteranges`). The client coalesces the disjoint arc and offset reads a single merge needs into one request when the server supports it, which can collapse a dozen sequential round trips into one. CloudFront supports this; bare S3/GCS/R2 do not (each disjoint chunk becomes its own request). The client falls back transparently — multi-range is a performance optimization, not a requirement.

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

All values are little-endian. The footer lives at the end of the file so a single suffix Range GET (`bytes=-N`) discovers every section's offset and the full metadata JSON. A parallel front Range GET covers front-loaded sections (CSR geometry triples, arc offsets) so the first merge can start without additional round trips. Subsequent on-demand reads (arc coordinate slices, property columns) are batched into multi-range requests when the server supports `multipart/byteranges`, falling back to one request per chunk otherwise.

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

// Merge a subset of features
const boundary = await merge(client, [
  { layer: "blocks", indices: [0, 1, 5, 12] },
]);
// → GeoJSON MultiPolygon. One output polygon per connected component
// of the input (polygons sharing arcs merge; disconnected groups stay
// separate). Within each component the largest-area ring is the
// exterior; smaller rings are holes. Matches topojson-client's
// `merge` semantics.

// Find adjacency (which features share an arc)
const adj = await neighbors(client, "blocks");
// → adj[i] is a sorted array of feature indices adjacent to feature i

// Read a typed property column
const population = await client.property("blocks/population");
// → Uint32Array (or whichever dtype the encoder chose)

// Read string properties
const names = await client.strings("blocks/name");
// → StringArray with lazy UTF-8 decoding: names.get(0), names.length

// Bounding box
const [minX, minY, maxX, maxY] = bbox(client);

// Clean up
client.close();
```

### Converting TopoJSON to .ctopo

The encoder is Node-only and lives at a separate entrypoint so browser bundles never pull in `fs` or `zlib`:

```ts
import { writeContainer, encodeContainer } from "cloud-topo/encode";

// Write directly to a file
await writeContainer("output.ctopo", topology);

// Or get a Buffer
const buf = await encodeContainer(topology);
```

The input is a standard [TopoJSON Topology](https://github.com/topojson/topojson-specification) object. The encoder:

- Extracts global arcs and per-layer geometry into CSR (compressed sparse row) triples
- Auto-detects the narrowest data type for each numeric property (u8, u16, u32, i8, i16, i32, f64)
- Packs string properties into a length-prefixed UTF-8 layout with lazy decoding
- Delta-encodes cumulative offset arrays for better compression
- Optionally block-compresses arc coordinates with a shared zstd dictionary
- Reorders arcs based spatial locality and number of arc references

### Rewriting properties in an existing container

```ts
import { rewriteContainer } from "cloud-topo/encode";

await rewriteContainer("input.ctopo", "output.ctopo", [
  { name: "blocks/population", data: [42, 99, 150 /* ... */] },
]);
```

Non-overridden sections pass through byte-for-byte; only the named properties are re-encoded and re-compressed.

## API reference

### Main entrypoint (`cloud-topo`)

| Export                                   | Kind     | Description                                                                                                                |
|------------------------------------------|----------|----------------------------------------------------------------------------------------------------------------------------|
| `CtopoClient`                            | class    | Opens and reads `.ctopo` containers over HTTP Range                                                                        |
| `CtopoClient.open(url, opts?)`           | static   | Open a remote container                                                                                                    |
| `CtopoClient.openWith(fetcher, opts?)`   | static   | Open with a custom `RangeFetcher`                                                                                          |
| `client.property(name, signal?)`         | method   | Fetch a typed property section                                                                                             |
| `client.strings(name, signal?)`          | method   | Fetch a string property section                                                                                            |
| `client.layerGeometry(layer, signal?)`   | method   | Fetch CSR geometry for a layer                                                                                             |
| `client.fetchArcs(ids, signal?)`         | method   | Fetch raw arc coordinate bytes                                                                                             |
| `openContainer(url, opts?)`              | function | Shorthand for `CtopoClient.open`                                                                                           |
| `merge(client, selections, signal?)`     | function | Merge features → GeoJSON MultiPolygon (one polygon per connected component; largest-area ring is exterior, rest are holes) |
| `mergeArcs(client, selections, signal?)` | function | Same grouping as `merge`, but returns signed arc-id rings instead of decoded coords                                        |
| `neighbors(client, layer, signal?)`      | function | Per-feature adjacency via shared arcs                                                                                      |
| `bbox(client)`                           | function | Container bounding box `[minX, minY, maxX, maxY]`                                                                          |
| `transform(t)`                           | function | Returns quantization transform function                                                                                    |
| `untransform(t)`                         | function | Returns inverse quantization transform function                                                                            |
| `parseContainer(bytes)`                  | function | Parse an in-memory container (header + footer)                                                                             |
| `makeBufferFetcher(buf)`                 | function | In-memory `RangeFetcher` for tests                                                                                         |
| `makeHttpFetcher(url)`                   | function | HTTP Range `RangeFetcher` using `fetch()`                                                                                  |
| `makeRangeFetcher(fn)`                   | function | Wrap a callback into a `RangeFetcher`                                                                                      |
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
| `RangeFetcher`         | Interface for byte-range fetching              |
| `MultiPolygonArcs`     | Arc-id geometry from `mergeArcs`               |

### Encoder entrypoint (`cloud-topo/encode`)

| Export                                                | Description                                                                                                               |
|-------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `encodeContainer(topology, opts?)`                    | TopoJSON → `.ctopo` Buffer                                                                                                |
| `writeContainer(path, topology, opts?)`               | Encode and write to file                                                                                                  |
| `rewriteContainer(inPath, outPath, overrides, opts?)` | Mutate named property sections in an existing container; `opts.frontLoadedSectionNames` adds extras to the front-load set |

## License

[Apache-2.0](LICENSE)
