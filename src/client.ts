// SPDX-License-Identifier: Apache-2.0
// © 2026 Michael Maurizi Jr.

/**
 * CtopoClient — opens a `.ctopo` container over HTTP Range and lazy-loads
 * sections on demand.
 *
 * `openContainer(url)` issues one prefetch GET that covers the header,
 * section table, and META JSON; the client then services per-section
 * fetches by issuing tight `Range` GETs against the same URL.
 *
 * Section fetches (property / strings / layerGeometry) are batched
 * across a microtask: every concurrent call enqueues its required
 * byte interval and a single drain pass sorts, coalesces (bridging
 * small gaps so adjacent sections share one HTTP round-trip), and
 * issues the minimum number of Range GETs. Each caller then receives
 * the slice it asked for. Per-section results cache as Promises at
 * the section-name level so subsequent callers (this drain or later)
 * skip the network entirely.
 *
 * Arc coord lookups go through one whole-section fetch of
 * `arc_coords`, cached for the lifetime of the client — slicing per
 * arc id is then a free subarray view.
 */

import {
  type ParsedHeader,
  parseFooter,
  parseFrontHeader,
  readFooterLength,
  viewDecompressedSection,
} from "./reader";
import { FOOTER_TRAILER_SIZE, HEADER_SIZE } from "./format";
import {
  StringArray,
  type ContainerMeta,
  type LayerGeometry,
  type SectionEntry,
} from "./types";
import {
  type FetchPriority,
  type RangeFetcher,
  makeHttpFetcher,
  perfLog,
} from "./fetcher";
import {
  decompressSection,
  loadZstdWasmDecode,
  preloadZstdWasmIfNeeded,
} from "./decompress";
import { runWithConcurrency } from "./util";

// --- Types ---

// Snapshot of bench-only counters captured by the client. These
// run alongside the existing perfLog/BroadcastChannel path so
// turning them on adds no extra logging — callers snapshot the
// numbers at end of run via getStats().
export interface CtopoClientStats {
  // Total successful Range GETs fired through the fetcher. Cache
  // hits and in-flight de-dupe hits are NOT counted here.
  readonly requests: number;
  // Sum of bytes returned by those Range GETs.
  readonly requestBytes: number;
  // Sum of decompress wall-clock across every section + every arc-
  // coord block decompressed during the lifetime of the client (or
  // since the last resetStats).
  readonly decompressMs: number;
  // Sum of decompressed bytes across the same window.
  readonly decompressBytes: number;
  // Calls to enqueueSectionFetch whose interval was already in the
  // byte-range LRU cache and served without a GET.
  readonly byteRangeCacheHits: number;
  // Calls that fell through to the fetcher (after both cache and
  // in-flight lookup misses).
  readonly byteRangeCacheMisses: number;
  // Calls whose interval lay inside an already-in-flight GET — they
  // attached to the existing Promise instead of issuing a duplicate.
  readonly inFlightHits: number;
  // Per-coalescer-family request counts. Keyed by the `family`
  // string passed into enqueueSectionFetch ("arcs", "offsets",
  // "section:<name>", "group:<offset>:<length>", etc.). Bench code
  // post-processes these into "arc bytes vs. property bytes vs. CSR
  // bytes" buckets as it sees fit.
  readonly perFamily: Readonly<
    Record<string, { requests: number; requestBytes: number }>
  >;
  // Uncompressed section bytes the client actually surfaced to its
  // public accessors, keyed by section name. Tallied at the consume
  // site so benches can compare "what we downloaded" (perFamily /
  // requestBytes) against "what we actually consumed":
  //   - property/strings/layerGeometry: the section's full
  //     uncompressed slice byteLength on first access (and 0 on
  //     subsequent accesses — the cached view bypasses fetch).
  //   - fetchArcs: arc_coords is bumped by the sum of returned arc
  //     slice byteLengths (only the requested arcs' coord bytes —
  //     much less than the whole block's decompressed bytes when
  //     the requested set is sparse within a block); arc_offsets
  //     is bumped by 8 per unique arc id (the two u32 entries each
  //     lookup actually consults); arc_coord_blocks is bumped by
  //     12 per distinct block touched (one block-table entry per
  //     block); arc_coords_dict is bumped once at its first fetch
  //     (the entire dict is needed by every block decompress).
  //   - Sections that never get accessed don't appear here at all.
  // Reported uncompressed; downloads in perFamily are compressed.
  readonly usefulBytesBySection: Readonly<Record<string, number>>;
}

export interface OpenContainerOptions {
  readonly signal?: AbortSignal;
  // Override the HTTP fetcher — used by tests, file-backed callers, and
  // any non-HTTPS transport. Default is the built-in `fetch()`-based
  // RangeFetcher (see makeHttpFetcher).
  readonly fetcher?: RangeFetcher;
  // Bytes to grab in the front-prefetch GET, fired in parallel with
  // the suffix-range footer GET at open. Sized to cover the
  // front-loaded data sections (encoder lays critical-path sections
  // — arc_coords_dict, arc_coord_blocks, arc_offsets, per-layer CSR
  // triples, arc_coords skeleton — at the front of the data area).
  // Set to 0 to skip the front prefetch entirely; sections then fetch
  // on demand after open completes. Default 0 — callers tune per
  // region based on the size of their front-loaded set.
  readonly frontPrefetchBytes?: number;
  // Bytes to grab in the suffix-range footer GET. Must be large
  // enough to cover the entire footer (section table + META JSON +
  // 8 B trailing length marker) in a single fetch. The reader
  // validates that the trailing length fits within the returned
  // buffer and throws if it doesn't, so over-budgeting here costs
  // only a few extra bytes. 256 KiB by default.
  readonly backPrefetchBytes?: number;
  // Within a single fetch family (e.g. all arc-coord slices, all
  // bytes of one named section), pending intervals separated by no
  // more than this many bytes get coalesced into one logical fetch.
  // Across families this gap never bridges — a property section and
  // an unrelated layer CSR triple stay separate even when packed
  // back-to-back. 64 KiB by default.
  readonly coalesceGapBytes?: number;
  // Per-family override for `coalesceGapBytes`. The "arcs" family
  // sees lots of tiny scattered fetches (some <100 B) — the
  // per-request overhead of issuing each as its own GET dominates
  // the actual bytes-in-flight. A larger gap fuses these into a
  // few medium logical ranges; the chunk cap then bounds each
  // physical GET, so the worst case is still parallel-friendly.
  readonly coalesceGapByFamily?: Readonly<Record<string, number>>;
  // Maximum bytes in a single physical Range GET. Logical fetches
  // larger than this are split into chunks that fire in parallel,
  // letting HTTP/2 multiplex many medium GETs instead of waiting on
  // one slow monster. 4 MiB by default — small enough that 6
  // concurrent GETs amount to ~24 MiB of in-flight data
  // (comfortable for HTTP/2 flow control), big enough that
  // per-request overhead (TLS resumption, CloudFront origin
  // round-trip on a miss) doesn't dominate.
  readonly maxChunkBytes?: number;
  // Maximum parallel Range GETs the batched fetcher will issue from a
  // single drain pass. Browsers cap concurrent connections per origin
  // around 6; over HTTP/2 this is a stream-multiplexing limit per
  // connection rather than a connection-count limit, so values up to
  // ~16 are reasonable.
  readonly maxParallelRanges?: number;
  // Soft cap on the in-memory byte-range cache. Every fetched chunk
  // lands here so that a later request whose interval falls inside an
  // already-fetched range serves from memory instead of issuing a
  // fresh GET. 32 MiB by default.
  readonly byteRangeCacheBytes?: number;
  // Speculatively fetch this many bytes from the start of arc_coords
  // immediately after openContainer parses the header. Block-
  // compressed arc_coords is read by fetchArcs through the byte-range
  // cache, so warming the cache with the section's prefix lets the
  // first N blocks' compressed bytes serve from memory without per-
  // block GETs. The encoder front-loads top-layer skeleton arcs at
  // the start of arc_coords (visit-order assignment), so a 512 KiB
  // prefix covers the topology's "skeleton" — arcs every typical
  // merge needs. 0 disables. 512 KiB by default. Pairs with
  // arcOffsetsPrefetch — both are needed for fetchArcs on skeleton
  // arcs to skip the network entirely.
  readonly arcCoordsPrefetchBytes?: number;
  // Speculatively fetch arc_offsets at open time. Without this,
  // every fetchArcs has to round-trip for the matching offset
  // entries before it can use the cached coord bytes — that round-
  // trip lands on the critical path of the merge (eager-loaded
  // offsets ran in parallel with header parsing instead). For
  // typical topologies arc_offsets is single-digit MiB, fetched
  // whole in a single coalesced background GET. Default true; set
  // false to skip the prefetch entirely (round-trip moves into the
  // first fetchArcs).
  readonly arcOffsetsPrefetch?: boolean;
  // Maximum number of disjoint byte ranges to combine into a single
  // multi-range HTTP request. CloudFront supports multiple ranges in
  // one request (ascending, non-overlapping); grouping reduces total
  // request count at the cost of multipart response parsing overhead.
  // Only takes effect when the fetcher implements `multiRange`. 64 by
  // default.
  readonly maxRangesPerRequest?: number;
  // Hard off-switch for multi-range packing. Default true. When false,
  // every disjoint chunk dispatches as its own range request even if
  // the fetcher implements `multiRange`. Setting `maxRangesPerRequest:
  // 1` produces the same observable behavior — both gate-skip the
  // multi-range path entirely. Use this knob for A/B benchmarking the
  // consolidation feature or for backends where multipart parsing is
  // more expensive than the RTTs it saves. Flipped to false at runtime
  // if the server returns "unsupported" from a multiRange call, so
  // subsequent requests don't repeat the failed probe.
  readonly multiRangeEnabled?: boolean;
}

interface PendingSectionFetch {
  readonly family: string;
  readonly start: number;
  readonly end: number;
  readonly priority: FetchPriority;
  readonly signal?: AbortSignal;
  readonly resolve: (bytes: Uint8Array) => void;
  readonly reject: (err: unknown) => void;
}

interface LogicalRange {
  readonly family: string;
  readonly start: number;
  end: number;
  readonly items: PendingSectionFetch[];
  // Highest-urgency priority among constituent items — coalesced
  // chunks inherit it so a small high-priority fetch upgrades any
  // bulk fetches it gets fused with.
  priority: FetchPriority;
  // Filled in as chunks complete. Length = number of chunks.
  chunkBytes: Uint8Array[];
  error: unknown;
}

interface ChunkTask {
  readonly logical: LogicalRange;
  readonly index: number;
  readonly start: number;
  readonly end: number;
}

interface CachedByteRange {
  readonly start: number;
  readonly end: number;
  readonly bytes: Uint8Array;
}

interface InFlightRange {
  readonly start: number;
  readonly end: number;
  readonly promise: Promise<Uint8Array>;
}

// --- Constants ---

// Default front-prefetch is OFF — callers explicitly opt in based on
// their front-loaded set size. Open path still works without it: the
// suffix-range footer GET delivers META, and lazy section fetches
// take the round trip when first needed.
const DEFAULT_FRONT_PREFETCH = 0;
// Suffix-range footer GET. 256 KiB is generous for any realistic
// section table + META JSON; the reader validates the trailing length
// marker and errors if the footer overflows, so over-budgeting is
// safe.
const DEFAULT_BACK_PREFETCH = 256 * 1024;
const DEFAULT_COALESCE_GAP = 64 * 1024;
// Arcs default — see doc on OpenContainerOptions.coalesceGapByFamily.
// 8 KiB matches the smaller arcCoordBlockBytes default (16 KiB) closely
// enough that most needed arcs are within a single block of each
// other, while still letting the coalescer merge truly-adjacent
// requests. Wider gaps (1 MiB, the previous default) collapsed nearly
// every arcs fetch into one giant range that pulled all of arc_coords;
// the median-of-3 sweep showed shrinking to 8 KiB cuts 13-34% of merge
// wall-clock without regressing on mobile (100ms RTT).
const DEFAULT_ARCS_COALESCE_GAP = 8 * 1024;
// Offsets default. Used by per-partition fetches in the
// partitioned-arc_offsets path. Partitions are 100s of bytes each, so
// a generous gap bridges huge numbers of unfetched partitions and
// erases the savings; the simulation in
// bench-out/national/*.offsets-partition-sim.csv showed 4 KiB as the
// sweet spot for hierarchical-hilbert on national CDs (~4.86 MiB
// fetched / 11 reqs vs ~13.6 MiB fetched at 64 KiB).
const DEFAULT_OFFSETS_COALESCE_GAP = 4 * 1024;
const DEFAULT_GAP_BY_FAMILY: Readonly<Record<string, number>> = {
  arcs: DEFAULT_ARCS_COALESCE_GAP,
  offsets: DEFAULT_OFFSETS_COALESCE_GAP,
};
// 4 MiB physical chunk cap. See doc on OpenContainerOptions.maxChunkBytes.
const DEFAULT_MAX_CHUNK = 4 * 1024 * 1024;
// 8 parallel chunks. HTTP/2 lets us multiplex these on one connection,
// so going above 6 is fine; we cap to keep flight bytes bounded
// (8 × 4 MiB = 32 MiB max in-flight).
const DEFAULT_MAX_PARALLEL_RANGES = 8;
// Sized to comfortably hold the open-time front prefetch (typically
// a few MiB) alongside one full pass of property/strings/arc_coords
// fetches during boundary compute, without evicting front-prefetched
// structural sections (arc_offsets, CSR triples) before they're
// re-read by stitching.
const DEFAULT_BYTE_RANGE_CACHE = 128 * 1024 * 1024;
// Multi-range request defaults.
const DEFAULT_MAX_RANGES_PER_REQUEST = 64;
// Open-time prefetch size for arc_coords. Block-compressed arc_coords
// is read through the byte-range cache; warming the section's prefix
// lets the first N blocks' compressed bytes serve from memory. The
// encoder front-loads top-layer boundary arcs (outer perimeter +
// parent-layer interior boundaries) by virtue of the visit-order
// assignment, so 512 KiB comfortably covers those for typical
// topologies; the rest is fetched on demand by the merge.
const DEFAULT_ARC_COORDS_PREFETCH = 512 * 1024;

// Sentinel placeholder used to mark an arc id as "claimed" in the
// fetchArcs result map before its real bytes arrive — keeps the
// dedupe loop synchronous without storing a second tracking set.
const EMPTY_BYTES = new Uint8Array(0);

// --- Public API ---

export async function openContainer(
  url: string,
  opts: OpenContainerOptions = {},
): Promise<CtopoClient> {
  return CtopoClient.open(url, opts);
}

// --- CtopoClient ---

export class CtopoClient {
  readonly meta: ContainerMeta;
  readonly sections: ReadonlyArray<SectionEntry>;
  readonly transform: ContainerMeta["transform"];

  private readonly fetcher: RangeFetcher;
  private readonly sectionByName: Map<string, SectionEntry>;
  // Section base for arc_coords — added to per-arc offsets to produce
  // absolute file byte intervals for fetchArcs.
  private readonly arcCoordsBase: number;
  // Section base for arc_offsets_partitions — only meaningful when
  // meta.arcOffsetsBlocks is present (the partitioned path). Added
  // to per-partition `compressedOffset` from the offsets-blocks table
  // to produce absolute file byte intervals.
  private readonly arcOffsetsPartitionsBase: number = 0;
  // arc_offsets is fetched whole on first access and decompressed
  // once. The resulting Uint32Array is cached for the client's
  // lifetime — fetchArcs needs random access into it for every
  // arc-id lookup, so we can't range-fetch slices of the compressed
  // bytes. The default arcOffsetsPrefetch=true warms this fetch in
  // the background at open time so the first merge doesn't pay the
  // round trip.
  private arcOffsetsPromise: Promise<Uint32Array> | undefined;
  private readonly coalesceGapBytes: number;
  private readonly coalesceGapByFamily: Readonly<Record<string, number>>;
  private readonly maxChunkBytes: number;
  private readonly maxParallelRanges: number;
  private readonly maxRangesPerRequest: number;
  // Mutable: starts at the user's option (default true) and flips
  // to false at runtime if the server returns "unsupported" from a
  // multiRange call.
  private multiRangeEnabled: boolean;
  private readonly byteRangeCacheBytes: number;
  // De-dupe in-flight property/strings/layer fetches via Promise maps.
  // The Promise itself is the cache entry — concurrent callers awaiting
  // the same section share the single in-flight HTTP GET.
  private readonly propertyCache = new Map<string, Promise<ArrayBufferView>>();
  private readonly stringsCache = new Map<string, Promise<StringArray>>();
  private readonly layerGeometryCache = new Map<
    string,
    Promise<LayerGeometry>
  >();
  // Decompressed group bytes keyed by `${physicalOffset}:${physicalLength}`.
  // Members of the same compression group share one decompress here,
  // so e.g. fetching every base-layer property after a group is
  // decoded is just N subarray slices.
  private readonly decompressedGroupCache = new Map<
    string,
    Promise<Uint8Array>
  >();
  // Block-compressed arc_coords state — populated lazily on the
  // first arc fetch when meta.arcCoordsBlocks is present. Both the
  // shared dict and the block table are needed before any block can
  // decompress, so both fetches are kicked off speculatively at
  // open time and run in parallel with the rest of bootstrap.
  private arcCoordBlocksPromise: Promise<Uint32Array> | undefined;
  // Resolves to undefined when the file has no shared dict (too few
  // blocks to train one) — caller passes that straight through to the
  // decoder, which uses the no-dict path.
  private arcCoordDictPromise: Promise<Uint8Array | undefined> | undefined;
  // Decompressed block bytes per block index. fetchArcs typically
  // touches many arcs in a small set of blocks; caching the
  // decompressed block amortizes the per-block decompress over
  // every arc that lives in it.
  // Partitioned-arc_offsets state — populated lazily on first arc
  // fetch when meta.arcOffsetsBlocks is present. Mirror of the
  // arc_coords block-compression machinery above. The block table
  // here stores [firstArcId, compOff, compLen] triples per partition
  // (compOff is relative to arc_offsets_partitions, not arc_coords).
  private arcOffsetsBlocksPromise: Promise<Uint32Array> | undefined;
  private arcOffsetsDictPromise: Promise<Uint8Array | undefined> | undefined;
  // Per-partition decompressed local-offset arrays. Same caching
  // discipline as decompressedArcCoordBlockCache. fetchArcs typically
  // touches several arcs in the same partition, so the per-partition
  // decompress amortizes well.
  private readonly decompressedArcOffsetsBlockCache = new Map<
    number,
    Promise<Uint32Array>
  >();
  private readonly decompressedArcCoordBlockCache = new Map<
    number,
    Promise<Uint8Array>
  >();
  // Microtask-batched section/arc fetcher state. Concurrent calls in
  // the same tick enqueue here; one drain pass coalesces and issues a
  // minimum number of Range GETs.
  private pendingSectionFetches: PendingSectionFetch[] = [];
  private flushScheduled = false;
  // In-flight Range GETs whose response bytes are not yet in the
  // byte-range cache. enqueueSectionFetch checks this list (in
  // addition to the cache) so a request that lands while another GET
  // covering it is still in flight attaches to that GET's Promise
  // instead of firing a duplicate.
  private inFlightRanges: InFlightRange[] = [];
  // In-flight LOGICAL ranges — the full span the coalescer decided to
  // fetch before splitting into maxChunkBytes-sized chunks. A
  // logical-range entry exists from the moment its chunks are queued
  // until the assembled bytes are cached. Lets a later enqueue whose
  // interval is fully covered by the logical range attach via one
  // entry, even when no single chunk's range alone contains it.
  // Without this, a request for the whole arc_offsets section after a
  // prefetch of that same section misses on both inFlightRanges (no
  // chunk covers all 5 chunks' span) and the byte-range cache (each
  // chunk is its own entry), and ends up re-fetching the whole
  // section through a parallel family.
  private inFlightLogicalRanges: InFlightRange[] = [];
  // Byte-range cache: every coalesced GET lands here so that future
  // requests whose [start, end) falls inside an already-fetched range
  // serve from memory. LRU by recency (most recent at the end of the
  // array), bounded by total byte sum.
  private byteRangeCache: CachedByteRange[] = [];
  private byteRangeCacheUsedBytes = 0;
  private closed = false;
  private readonly closeController = new AbortController();

  // Stats counters consumed by getStats(). Bumped at fetch, cache,
  // and decompress sites; the existing perfLog/BroadcastChannel
  // logging stays untouched. resetStats() lets bench harnesses
  // separate open-time work from per-merge work without spinning a
  // fresh client.
  private statRequests = 0;
  private statRequestBytes = 0;
  private statDecompressMs = 0;
  private statDecompressBytes = 0;
  private statByteRangeCacheHits = 0;
  private statByteRangeCacheMisses = 0;
  private statInFlightHits = 0;
  private readonly statPerFamily = new Map<
    string,
    { requests: number; requestBytes: number }
  >();
  // Per-section "bytes actually used" counter. See CtopoClientStats
  // .usefulBytesBySection for what each section's accounting means.
  private readonly statUsefulBytesBySection = new Map<string, number>();

  private constructor(args: {
    fetcher: RangeFetcher;
    parsed: ParsedHeader;
    coalesceGapBytes: number;
    coalesceGapByFamily: Readonly<Record<string, number>>;
    maxChunkBytes: number;
    maxParallelRanges: number;
    maxRangesPerRequest: number;
    multiRangeEnabled: boolean;
    byteRangeCacheBytes: number;
  }) {
    this.fetcher = args.fetcher;
    this.meta = args.parsed.meta;
    this.sections = args.parsed.sections;
    this.transform = this.meta.transform;
    this.sectionByName = new Map(this.sections.map((s) => [s.name, s]));
    this.coalesceGapBytes = args.coalesceGapBytes;
    this.coalesceGapByFamily = args.coalesceGapByFamily;
    this.maxChunkBytes = args.maxChunkBytes;
    this.maxParallelRanges = args.maxParallelRanges;
    this.maxRangesPerRequest = args.maxRangesPerRequest;
    this.multiRangeEnabled = args.multiRangeEnabled;
    this.byteRangeCacheBytes = args.byteRangeCacheBytes;

    const arcCoords = this.sectionByName.get("arc_coords");
    if (arcCoords === undefined) {
      throw new Error("ctopo: container is missing the arc_coords section");
    }
    this.arcCoordsBase = arcCoords.offset;

    // arc_offsets is required in the monolithic format; the
    // partitioned format relies on arc_offsets_partitions + _blocks
    // instead, signaled by meta.arcOffsetsBlocks.
    if (this.meta.arcOffsetsBlocks !== undefined) {
      const partitions = this.sectionByName.get(
        this.meta.arcOffsetsBlocks.partitionsSection,
      );
      if (partitions === undefined) {
        throw new Error(
          `ctopo: container is missing the "${this.meta.arcOffsetsBlocks.partitionsSection}" section referenced by arcOffsetsBlocks META`,
        );
      }
      this.arcOffsetsPartitionsBase = partitions.offset;
    } else if (this.sectionByName.get("arc_offsets") === undefined) {
      throw new Error("ctopo: container is missing the arc_offsets section");
    }
  }

  // --- Factory ---

  static async open(
    url: string,
    opts: OpenContainerOptions = {},
  ): Promise<CtopoClient> {
    const fetcher = opts.fetcher ?? makeHttpFetcher(url);
    return CtopoClient.openWith(fetcher, opts);
  }

  // Fire two GETs in parallel: front prefetch (covers the encoder's
  // front-loaded sections) and a suffix-range footer fetch (delivers
  // the section table + META). Both arrive in ~1 RTT; the front
  // bytes are stashed in the byte-range cache so subsequent section
  // fetches against any front-loaded section hit cache without
  // another GET.
  static async openWith(
    fetcher: RangeFetcher,
    opts: OpenContainerOptions = {},
  ): Promise<CtopoClient> {
    const frontPrefetchBytes =
      opts.frontPrefetchBytes ?? DEFAULT_FRONT_PREFETCH;
    const backPrefetchBytes = opts.backPrefetchBytes ?? DEFAULT_BACK_PREFETCH;
    if (backPrefetchBytes < FOOTER_TRAILER_SIZE) {
      throw new Error(
        `ctopo: backPrefetchBytes (${backPrefetchBytes}) must be >= FOOTER_TRAILER_SIZE (${FOOTER_TRAILER_SIZE}) so the suffix GET captures at least the footer-length marker`,
      );
    }
    const coalesceGapBytes = opts.coalesceGapBytes ?? DEFAULT_COALESCE_GAP;
    const coalesceGapByFamily = {
      ...DEFAULT_GAP_BY_FAMILY,
      ...(opts.coalesceGapByFamily ?? {}),
    };
    const maxChunkBytes = opts.maxChunkBytes ?? DEFAULT_MAX_CHUNK;
    const maxParallelRanges =
      opts.maxParallelRanges ?? DEFAULT_MAX_PARALLEL_RANGES;
    const maxRangesPerRequest =
      opts.maxRangesPerRequest ?? DEFAULT_MAX_RANGES_PER_REQUEST;
    const multiRangeEnabled = opts.multiRangeEnabled ?? true;
    const byteRangeCacheBytes =
      opts.byteRangeCacheBytes ?? DEFAULT_BYTE_RANGE_CACHE;
    const arcOffsetsPrefetch = opts.arcOffsetsPrefetch ?? true;
    const signal = opts.signal;

    // Kick off the zstd JS-fallback module load now (no-op when the
    // runtime has native DecompressionStream("zstd")). Runs in
    // parallel with the header fetch so the module is ready before
    // any compressed-section decompress, off the critical path.
    preloadZstdWasmIfNeeded();

    // Fire the front-prefix GET and the suffix GET in parallel. The
    // front GET covers at minimum the 16-byte header (for magic/version
    // validation) and, if frontPrefetchBytes > 0, extends to cover the
    // front-loaded sections so subsequent section fetches in that
    // region hit the byte-range cache. The suffix GET delivers the
    // footer (section table + META). Both are on the critical open
    // path so an incompatible file fails at open, not on the first
    // merge.
    const frontGetSize = Math.max(HEADER_SIZE, frontPrefetchBytes);
    const [frontBytes, suffixBytes] = await Promise.all([
      fetcher.range(0, frontGetSize, signal, "high"),
      fetcher.suffix(backPrefetchBytes, signal, "high"),
    ]);

    parseFrontHeader(frontBytes.subarray(0, HEADER_SIZE));

    // Locate the footer within the suffix bytes via the trailing 8-byte
    // length marker. If the footer overflows the suffix, refetch with a
    // tighter sizing (we know exact length from the marker now).
    let footerView = suffixBytes;
    const footerLength = readFooterLength(footerView);
    if (footerView.byteLength < footerLength + FOOTER_TRAILER_SIZE) {
      // The suffix budget was too small. Re-issue with the exact size.
      footerView = await fetcher.suffix(
        footerLength + FOOTER_TRAILER_SIZE,
        signal,
        "high",
      );
    }
    const footerStartInView =
      footerView.byteLength - FOOTER_TRAILER_SIZE - footerLength;
    const footerBytes = footerView.subarray(
      footerStartInView,
      footerStartInView + footerLength,
    );
    const parsed = parseFooter(footerBytes);

    const client = new CtopoClient({
      fetcher,
      parsed,
      coalesceGapBytes,
      coalesceGapByFamily,
      maxChunkBytes,
      maxParallelRanges,
      maxRangesPerRequest,
      multiRangeEnabled,
      byteRangeCacheBytes,
    });

    // Seed the byte-range cache with the front bytes we already fetched
    // so subsequent section fetches in the front-loaded region hit
    // cache. Only seed when frontPrefetchBytes > 0 — a 16-byte header-
    // only fetch isn't worth a cache entry.
    if (frontPrefetchBytes > 0) {
      client.cacheByteRange(0, frontGetSize, frontBytes);
    }

    // Skeleton prefetch sizing: the encoder front-loads top-layer
    // boundary arcs by visit-order assignment, so a fixed 512 KiB
    // prefetch covers most typical topologies' top-layer arcs
    // without needing per-file tier metadata.
    const arcCoordsPrefetchBytes =
      opts.arcCoordsPrefetchBytes ?? DEFAULT_ARC_COORDS_PREFETCH;

    // Speculative skeleton fetches — kick off the long-lived caches
    // for arc_offsets, arc_coord_blocks, and arc_coords_dict now so
    // they overlap with whatever the caller does immediately after
    // open. Each of these getter methods memoizes its parsed result
    // in a dedicated per-client promise (arcOffsetsPromise etc.), so
    // the merge path's first fetchArcs call returns from cache with
    // no second fetch.
    //
    // We deliberately do NOT route these through prefetchPrefix +
    // the byte-range LRU. The LRU is sized for transient fetches
    // (default 32 MiB) and the bulk property GETs the caller fires
    // immediately after open can evict a multi-MiB arc_offsets blob
    // before the first fetchArcs touches it — leaving the merge to
    // re-fetch the same bytes through the on-demand path. The
    // dedicated long-lived caches sidestep that eviction entirely.
    //
    // Fire-and-forget: a prefetch failure surfaces on the first
    // dependent call, mirroring the previous prefetchPrefix
    // semantics.
    if (arcOffsetsPrefetch) {
      if (parsed.meta.arcOffsetsBlocks !== undefined) {
        // Partitioned path: the block table + dict are the
        // structural-critical pair (parallel to arc_coord_blocks /
        // arc_coords_dict). The per-partition payload itself stays
        // on-demand.
        void client.getArcOffsetsBlocks().catch(() => {});
        void client.getArcOffsetsDict().catch(() => {});
      } else {
        void client.getArcOffsets().catch(() => {});
      }
    }
    void client.getArcCoordBlocks().catch(() => {});
    void client.getArcCoordDict().catch(() => {});
    // arc_coords prefix prefetch — this one stays as a byte-range
    // cache warmup because fetchArcs reads compressed-block bytes
    // through the byte-range path (each block goes through
    // enqueueSectionFetch("arcs", ...)). The prefix is small (512 KiB
    // default) so eviction pressure is negligible.
    if (arcCoordsPrefetchBytes > 0) {
      client.prefetchPrefix("arc_coords", "arcs", arcCoordsPrefetchBytes);
    }

    return client;
  }

  // --- Lazy section accessors ---

  async property(name: string, signal?: AbortSignal): Promise<ArrayBufferView> {
    this.ensureOpen();
    throwIfAborted(signal);
    const cached = this.propertyCache.get(name);
    if (cached !== undefined) return cached;
    const entry = this.sectionEntry(name);
    if (entry.dtype === "strings" || entry.dtype === "blob") {
      throw new Error(
        `ctopo: property("${name}") is dtype ${entry.dtype}; use strings() or blob() instead`,
      );
    }
    const promise = (async () => {
      const bytes = await this.fetchSectionBytes(entry, signal);
      // property() hands the caller a typed view over the whole
      // column. The client can't see which indices the caller
      // actually reads, but in practice properties are downloaded
      // because the entire column will be summed/scanned (sidebar
      // demos, etc.). Tally the full byteLength so callers see
      // these as ~100% used and the per-section breakdown spot-
      // lights the lookup-table sections (arc_offsets, CSR triples,
      // arc_coord_blocks) where indexed-access metering tells a
      // different story.
      this.tallyUseful(name, bytes.byteLength);
      return viewDecompressedSection(bytes, entry.dtype);
    })();
    this.propertyCache.set(name, promise);
    return promise;
  }

  async strings(name: string, signal?: AbortSignal): Promise<StringArray> {
    this.ensureOpen();
    throwIfAborted(signal);
    const cached = this.stringsCache.get(name);
    if (cached !== undefined) return cached;
    const entry = this.sectionEntry(name);
    if (entry.dtype !== "strings") {
      throw new Error(
        `ctopo: strings("${name}") expected strings dtype, got ${entry.dtype}`,
      );
    }
    const promise = (async () => {
      const bytes = await this.fetchSectionBytes(entry, signal);
      // Same rationale as property(): tally the full slice so
      // strings show up as fully used and the bench's per-section
      // breakdown focuses on the internal lookup tables.
      this.tallyUseful(name, bytes.byteLength);
      return new StringArray(bytes);
    })();
    this.stringsCache.set(name, promise);
    return promise;
  }

  async layerGeometry(
    layer: string,
    signal?: AbortSignal,
  ): Promise<LayerGeometry> {
    this.ensureOpen();
    throwIfAborted(signal);
    const cached = this.layerGeometryCache.get(layer);
    if (cached !== undefined) return cached;
    const polyEntry = this.sectionEntry(`${layer}/poly_offsets`);
    const ringEntry = this.sectionEntry(`${layer}/ring_offsets`);
    const arcRefEntry = this.sectionEntry(`${layer}/arc_refs`);
    // multi_poly_breaks is sparse — encoder omits the section entirely
    // for layers with no multi-entry MultiPolygon features. Treat
    // absence as an empty break list.
    const breaksEntry = this.sectionByName.get(`${layer}/multi_poly_breaks`);

    const promise = (async () => {
      // Fire all section fetches concurrently — when they all share a
      // compression group (the common case after grouping), they hit
      // the same in-flight decompress via the group cache and resolve
      // from one fetch. When they're independent sections (layers
      // larger than one group), they parallelize through the normal
      // range pipeline.
      const [polyBytes, ringBytes, arcRefsBytes, breakBytes] =
        await Promise.all([
          this.fetchSectionBytes(polyEntry, signal),
          this.fetchSectionBytes(ringEntry, signal),
          this.fetchSectionBytes(arcRefEntry, signal),
          breaksEntry !== undefined
            ? this.fetchSectionBytes(breaksEntry, signal)
            : Promise.resolve(undefined),
        ]);
      // Not tallied here: layerGeometry returns the full CSR views,
      // but useful-byte attribution wants index-level reads. The
      // mergeArcs primitive does those reads in expandLayerPolygons
      // and tallies there via the public tallyUseful hook.
      return {
        polyOffsets: viewU32WithDelta(polyBytes, polyEntry.delta === true),
        ringOffsets: viewU32WithDelta(ringBytes, ringEntry.delta === true),
        arcRefs: viewDecompressedSection(arcRefsBytes, "i32") as Int32Array,
        multiPolyBreaks:
          breakBytes !== undefined
            ? (viewDecompressedSection(breakBytes, "u32") as Uint32Array)
            : new Uint32Array(0),
      };
    })();
    this.layerGeometryCache.set(layer, promise);
    return promise;
  }

  // --- Arc coord fetcher (used by merge.ts) ---

  // Returns each requested arc's coord bytes. Translates arc ids to
  // byte intervals via arcOffsets (loaded once on first access,
  // cached for the client's lifetime) and routes each interval
  // through the same batched section fetcher used by property /
  // strings / layerGeometry. Concurrent fetchArcs calls in the
  // same tick share a single drain pass; missing arcs sort and
  // coalesce; previously fetched ranges serve from the byte-range
  // cache; in-flight ranges dedupe across overlapping concurrent
  // calls.
  async fetchArcs(
    arcIds: Iterable<number>,
    signal?: AbortSignal,
  ): Promise<Map<number, Uint8Array>> {
    this.ensureOpen();
    throwIfAborted(signal);
    if (this.meta.arcOffsetsBlocks !== undefined) {
      return this.fetchArcsPartitioned(arcIds);
    }
    const arcOffsets = await this.getArcOffsets();
    return this.fetchArcsFromBlocks(arcIds, arcOffsets);
  }

  // --- Stats ---

  // Bench instrumentation: snapshot of fetch / decompress / cache
  // counters since open or the last resetStats().
  getStats(): CtopoClientStats {
    const perFamily: Record<
      string,
      { requests: number; requestBytes: number }
    > = {};
    for (const [k, v] of this.statPerFamily) {
      perFamily[k] = { requests: v.requests, requestBytes: v.requestBytes };
    }
    const usefulBytesBySection: Record<string, number> = {};
    for (const [k, v] of this.statUsefulBytesBySection) {
      usefulBytesBySection[k] = v;
    }
    return {
      requests: this.statRequests,
      requestBytes: this.statRequestBytes,
      decompressMs: this.statDecompressMs,
      decompressBytes: this.statDecompressBytes,
      byteRangeCacheHits: this.statByteRangeCacheHits,
      byteRangeCacheMisses: this.statByteRangeCacheMisses,
      inFlightHits: this.statInFlightHits,
      perFamily,
      usefulBytesBySection,
    };
  }

  // Zero out all stats counters. Useful when a bench wants to
  // separate open-time work (header + prefetch) from per-merge
  // work without spinning up a fresh client.
  resetStats(): void {
    this.statRequests = 0;
    this.statRequestBytes = 0;
    this.statDecompressMs = 0;
    this.statDecompressBytes = 0;
    this.statByteRangeCacheHits = 0;
    this.statByteRangeCacheMisses = 0;
    this.statInFlightHits = 0;
    this.statPerFamily.clear();
    this.statUsefulBytesBySection.clear();
  }

  // Tally helper for per-section "useful bytes". Called at the
  // consume site so the counter reflects index-level reads, not
  // fetch-level downloads. Public so mergeArcs (which lives in
  // merge.ts but does its CSR reads via the views layerGeometry()
  // returned) can attribute its index walks back to per-section
  // accounting. Silent on non-positive input.
  tallyUseful(name: string, bytes: number): void {
    if (bytes <= 0) return;
    this.statUsefulBytesBySection.set(
      name,
      (this.statUsefulBytesBySection.get(name) ?? 0) + bytes,
    );
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.closeController.abort();
    this.propertyCache.clear();
    this.stringsCache.clear();
    this.layerGeometryCache.clear();
    this.decompressedGroupCache.clear();
    this.decompressedArcCoordBlockCache.clear();
    this.arcCoordBlocksPromise = undefined;
    this.arcCoordDictPromise = undefined;
    this.byteRangeCache = [];
    this.byteRangeCacheUsedBytes = 0;
    this.inFlightRanges = [];
    this.inFlightLogicalRanges = [];
  }

  // --- Private: section bytes ---

  // Fetch a section's on-disk bytes through the range pipeline,
  // decompressing if the section declares a codec. The returned
  // Uint8Array is always the *uncompressed* bytes for the section
  // (sliced out of its group when the section is a group member).
  private async fetchSectionBytes(
    entry: SectionEntry,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    throwIfAborted(signal);
    if (entry.groupOffset !== undefined && entry.groupLength !== undefined) {
      // Group member — share the decompressed group's bytes across
      // every member access. Multiple sections that share the same
      // physical (offset, length) hit the same cache entry.
      const group = await this.fetchDecompressedGroup(entry, signal);
      return group.subarray(
        entry.groupOffset,
        entry.groupOffset + entry.groupLength,
      );
    }
    const bytes = await this.enqueueSectionFetch(
      `section:${entry.name}`,
      entry.offset,
      entry.offset + entry.length,
      "auto",
      signal,
    );
    if (entry.compression === undefined) return bytes;
    return this.decompressSectionTracked(bytes, entry);
  }

  // Fetch + decompress a compression group's bytes, cached by the
  // group's physical (offset, length) so members of the same group
  // share one decompression. Concurrent calls for sibling members
  // attach to the same in-flight Promise via the cache.
  private fetchDecompressedGroup(
    entry: SectionEntry,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    const key = `${entry.offset}:${entry.length}`;
    const cached = this.decompressedGroupCache.get(key);
    if (cached !== undefined) return cached;
    const promise = (async () => {
      const bytes = await this.enqueueSectionFetch(
        `group:${key}`,
        entry.offset,
        entry.offset + entry.length,
        "auto",
        signal,
      );
      if (entry.compression === undefined) return bytes;
      return decompressSection(bytes, entry);
    })();
    this.decompressedGroupCache.set(key, promise);
    return promise;
  }

  // Wrapper around the freestanding decompressSection that bumps
  // the client's stats counters. Both the section path and the
  // group path go through here.
  private async decompressSectionTracked(
    bytes: Uint8Array,
    entry: SectionEntry,
  ): Promise<Uint8Array> {
    const t0 = performance.now();
    const out = await decompressSection(bytes, entry);
    this.statDecompressMs += performance.now() - t0;
    this.statDecompressBytes += out.byteLength;
    return out;
  }

  // Public alias for offline analyzers that need raw arc byte
  // offsets to compute packing-quality metrics (gap stats, span,
  // etc.). Bench-only — production code paths use fetchArcs.
  //
  // On monolithic files this is the same Promise getArcOffsets()
  // returns. On partitioned files it lazy-stitches every partition
  // into a single global Uint32Array — slow (decompresses ALL
  // partitions), but the API stays the same for analyzers.
  arcOffsets(): Promise<Uint32Array> {
    if (this.meta.arcOffsetsBlocks === undefined) {
      return this.getArcOffsets();
    }
    return this.assembleGlobalArcOffsetsFromPartitions();
  }

  private async assembleGlobalArcOffsetsFromPartitions(): Promise<Uint32Array> {
    const offsetsBlocks = await this.getArcOffsetsBlocks();
    const arcBlocks = await this.getArcCoordBlocks();
    const dictPromise = this.getArcOffsetsDict();
    const numArcs = this.meta.numArcs;
    const result = new Uint32Array(numArcs + 1);
    const blockCount = offsetsBlocks.length / 3;
    for (let i = 0; i < blockCount; i++) {
      const firstArcId = offsetsBlocks[i * 3];
      const blockStart = i === 0 ? 0 : arcBlocks[(i - 1) * 3];
      const localOffsets = await this.getDecompressedArcOffsetsBlock(
        i,
        offsetsBlocks,
        dictPromise,
      );
      for (let k = 0; k < localOffsets.length; k++) {
        result[firstArcId + k] = blockStart + localOffsets[k];
      }
    }
    return result;
  }

  // --- Private: arc offsets + block-compressed arc_coords ---

  // Lazy + cached arc_offsets accessor. The first call fetches the
  // whole arc_offsets section through the standard fetchSectionBytes
  // path (which transparently decompresses if the section is
  // gzipped) and wraps the result as a Uint32Array. Subsequent
  // calls return the same Promise, so the underlying buffer is
  // shared across every fetchArcs call.
  private getArcOffsets(): Promise<Uint32Array> {
    if (this.arcOffsetsPromise === undefined) {
      const entry = this.sectionByName.get("arc_offsets");
      if (entry === undefined) {
        throw new Error("ctopo: container is missing the arc_offsets section");
      }
      this.arcOffsetsPromise = this.fetchSectionBytes(entry).then((bytes) =>
        viewU32WithDelta(bytes, entry.delta === true),
      );
    }
    return this.arcOffsetsPromise;
  }

  // Block-compressed arc_coords path. Each arc lives entirely
  // within one block (encoder-side guarantee), so per-arc work is:
  // (1) find the block, (2) ensure it's decompressed (cached
  // per-block via decompressedArcCoordBlockCache), (3) slice the
  // arc's bytes from the block. fetchArcs typically pulls many
  // arcs in a small set of blocks, so the per-block decompress
  // amortizes well.
  private async fetchArcsFromBlocks(
    arcIds: Iterable<number>,
    arcOffsets: Uint32Array,
  ): Promise<Map<number, Uint8Array>> {
    const blocks = await this.getArcCoordBlocks();
    const out = new Map<number, Uint8Array>();
    let uniqueCount = 0;
    const blocksTouched = new Set<number>();
    let arcCoordsUseful = 0;

    const promises: Promise<void>[] = [];
    for (const arcId of arcIds) {
      if (out.has(arcId)) continue;
      uniqueCount++;
      const logicalStart = arcOffsets[arcId];
      const logicalEnd = arcOffsets[arcId + 1];
      const blockIdx = findArcCoordBlock(blocks, logicalStart);
      blocksTouched.add(blockIdx);
      const blockEnd = blocks[blockIdx * 3]; // exclusive uncompressed end
      const blockStart = blockIdx === 0 ? 0 : blocks[(blockIdx - 1) * 3];
      const offsetInBlock = logicalStart - blockStart;
      const lenInBlock = logicalEnd - logicalStart;
      // Sanity — encoder guarantees this; if it ever fires we'd
      // need to widen fetchArcsFromBlocks to cross blocks.
      if (logicalEnd > blockEnd) {
        throw new Error(
          `ctopo: arc ${arcId} spans block boundary (logical [${logicalStart}, ${logicalEnd}) vs block end ${blockEnd}) — encoder bug`,
        );
      }
      arcCoordsUseful += lenInBlock;
      out.set(arcId, EMPTY_BYTES);
      promises.push(
        this.getDecompressedArcCoordBlock(blockIdx, blocks).then(
          (blockBytes) => {
            out.set(
              arcId,
              blockBytes.subarray(offsetInBlock, offsetInBlock + lenInBlock),
            );
          },
        ),
      );
    }
    // Per-arc lookup tally: each arc lookup reads two u32 entries
    // from arc_offsets (start, end) and one 3-u32 entry from the
    // arc_coord_blocks table (uncEnd, compOff, compLen). Counting
    // distinct blocks for arc_coord_blocks gives the floor — what
    // we'd need if arc_coord_blocks were randomly addressable.
    this.tallyUseful("arc_offsets", 8 * uniqueCount);
    this.tallyUseful("arc_coord_blocks", 12 * blocksTouched.size);
    this.tallyUseful("arc_coords", arcCoordsUseful);
    if (uniqueCount > 0) {
      perfLog(
        `[ctopo] fetchArcs (blocks): ${uniqueCount} arcs across ${blocksTouched.size} blocks`,
      );
    }
    await Promise.all(promises);
    return out;
  }

  // Partitioned-arc_offsets variant. Same shape as fetchArcsFromBlocks,
  // but block selection bypasses the monolithic arc_offsets table —
  // the arc_offsets_blocks partition table (one entry per arc-coord
  // block, keyed by firstArcId) tells us which partition contains the
  // arc id, and per-partition arc_offsets + per-block arc_coords fetch
  // in parallel. Local offsets inside the partition slot directly into
  // the decompressed arc_coords block.
  private async fetchArcsPartitioned(
    arcIds: Iterable<number>,
  ): Promise<Map<number, Uint8Array>> {
    const arcBlocks = await this.getArcCoordBlocks();
    const offsetsBlocks = await this.getArcOffsetsBlocks();
    const dictPromise = this.getArcOffsetsDict();
    const out = new Map<number, Uint8Array>();
    const arcsByBlock = new Map<number, number[]>();

    for (const arcId of arcIds) {
      if (out.has(arcId)) continue;
      const blockIdx = CtopoClient.findArcOffsetsBlock(offsetsBlocks, arcId);
      const list = arcsByBlock.get(blockIdx);
      if (list === undefined) arcsByBlock.set(blockIdx, [arcId]);
      else list.push(arcId);
      out.set(arcId, EMPTY_BYTES);
    }

    // Per touched block: partition fetch + arc_coords block fetch
    // fire in parallel — both only depend on blockIdx (not on each
    // other), so the per-block work is one critical-path RTT.
    let uniqueCount = 0;
    let arcCoordsUseful = 0;
    let arcOffsetsUseful = 0;
    await Promise.all(
      Array.from(arcsByBlock.entries()).map(async ([blockIdx, arcsInBlock]) => {
        const [localOffsets, blockBytes] = await Promise.all([
          this.getDecompressedArcOffsetsBlock(
            blockIdx,
            offsetsBlocks,
            dictPromise,
          ),
          this.getDecompressedArcCoordBlock(blockIdx, arcBlocks),
        ]);
        const firstArcId = offsetsBlocks[blockIdx * 3];
        for (const arcId of arcsInBlock) {
          const localIdx = arcId - firstArcId;
          const arcStart = localOffsets[localIdx];
          const arcEnd = localOffsets[localIdx + 1];
          const slice = blockBytes.subarray(arcStart, arcEnd);
          out.set(arcId, slice);
          arcCoordsUseful += slice.byteLength;
          // 2 u32 reads per arc lookup: localOffsets[localIdx] and
          // localOffsets[localIdx + 1]. Mirrors the monolithic
          // tally's 8 bytes per arc.
          arcOffsetsUseful += 8;
          uniqueCount++;
        }
      }),
    );
    this.tallyUseful("arc_offsets_partitions", arcOffsetsUseful);
    this.tallyUseful("arc_offsets_blocks", 12 * arcsByBlock.size);
    this.tallyUseful("arc_coords", arcCoordsUseful);
    if (uniqueCount > 0) {
      perfLog(
        `[ctopo] fetchArcs (partitioned): ${uniqueCount} arcs across ${arcsByBlock.size} partitions`,
      );
    }
    return out;
  }

  // Returns the shared dict bytes when this file has one, or
  // undefined when there's no shared dict (too few blocks to train
  // one). Caller passes the (possibly-undefined) result to the
  // decoder, which uses the no-dict path when absent.
  private getArcCoordDict(): Promise<Uint8Array | undefined> {
    if (this.arcCoordDictPromise === undefined) {
      const meta = this.meta.arcCoordsBlocks;
      if (meta.dictSection === undefined) {
        // No shared dict (too few blocks to train one).
        this.arcCoordDictPromise = Promise.resolve(undefined);
        return this.arcCoordDictPromise;
      }
      const entry = this.sectionByName.get(meta.dictSection);
      if (entry === undefined) {
        throw new Error(
          `ctopo: container is missing the "${meta.dictSection}" section`,
        );
      }
      this.arcCoordDictPromise = this.fetchSectionBytes(entry).then(bytes => {
        // The shared dict is consulted by every per-block decompress
        // — count its full size once at first resolve.
        this.tallyUseful(entry.name, bytes.byteLength);
        return bytes;
      });
    }
    return this.arcCoordDictPromise;
  }

  private getArcCoordBlocks(): Promise<Uint32Array> {
    if (this.arcCoordBlocksPromise === undefined) {
      const meta = this.meta.arcCoordsBlocks;
      const entry = this.sectionByName.get(meta.blockTableSection);
      if (entry === undefined) {
        throw new Error(
          `ctopo: container is missing the "${meta.blockTableSection}" section`,
        );
      }
      this.arcCoordBlocksPromise = this.fetchSectionBytes(entry).then(
        (bytes) =>
          new Uint32Array(bytes.buffer, bytes.byteOffset, bytes.byteLength / 4),
      );
    }
    return this.arcCoordBlocksPromise;
  }

  private getDecompressedArcCoordBlock(
    blockIdx: number,
    blocks: Uint32Array,
  ): Promise<Uint8Array> {
    const cached = this.decompressedArcCoordBlockCache.get(blockIdx);
    if (cached !== undefined) return cached;
    const compOffset = blocks[blockIdx * 3 + 1];
    const compLength = blocks[blockIdx * 3 + 2];
    const physicalStart = this.arcCoordsBase + compOffset;
    const physicalEnd = physicalStart + compLength;
    // Block uncompressed size is the difference between this block's
    // exclusive uncEnd and the previous block's. The wasm decoder
    // uses it to size its output buffer (Node-emitted frames don't
    // include FCS).
    const prevUncEnd = blockIdx === 0 ? 0 : blocks[(blockIdx - 1) * 3];
    const uncSize = blocks[blockIdx * 3] - prevUncEnd;
    // Synthesize a SectionEntry-shaped object for the decompress
    // path so error messages can attribute the block.
    const entry: SectionEntry = {
      name: `arc_coords[block ${blockIdx}]`,
      dtype: "blob",
      offset: physicalStart,
      length: compLength,
      compression: "zstd",
    };
    const promise = (async () => {
      // Fetch compressed bytes + dict + decoder in parallel. Dict and
      // wasm decoder are typically already in flight from open time,
      // so this Promise.all only waits on the per-block GET in steady
      // state.
      const [compressed, dict, decode] = await Promise.all([
        this.enqueueSectionFetch("arcs", physicalStart, physicalEnd),
        this.getArcCoordDict(),
        loadZstdWasmDecode(entry),
      ]);
      const t0 = performance.now();
      const out = decode(compressed, uncSize, dict);
      this.statDecompressMs += performance.now() - t0;
      this.statDecompressBytes += out.byteLength;
      return out;
    })();
    this.decompressedArcCoordBlockCache.set(blockIdx, promise);
    return promise;
  }

  // --- Private: partitioned arc_offsets ---

  // Fetch + view the [firstArcId, compOff, compLen] triples for the
  // partitioned arc_offsets layout. Memoized for the client's lifetime.
  private getArcOffsetsBlocks(): Promise<Uint32Array> {
    if (this.arcOffsetsBlocksPromise === undefined) {
      const meta = this.meta.arcOffsetsBlocks;
      if (meta === undefined) {
        throw new Error(
          "ctopo: getArcOffsetsBlocks called on a monolithic-format file",
        );
      }
      const entry = this.sectionByName.get(meta.blockTableSection);
      if (entry === undefined) {
        throw new Error(
          `ctopo: container is missing the "${meta.blockTableSection}" section`,
        );
      }
      this.arcOffsetsBlocksPromise = this.fetchSectionBytes(entry).then(
        (bytes) =>
          new Uint32Array(bytes.buffer, bytes.byteOffset, bytes.byteLength / 4),
      );
    }
    return this.arcOffsetsBlocksPromise;
  }

  // Resolves to undefined when the file has no shared arc_offsets
  // dict (encoder skipped training; reader uses the no-dict zstd
  // decode path). Tally the full dict size once at first resolve —
  // it's read in full by every partition decompress.
  private getArcOffsetsDict(): Promise<Uint8Array | undefined> {
    if (this.arcOffsetsDictPromise === undefined) {
      const meta = this.meta.arcOffsetsBlocks;
      if (meta === undefined) {
        throw new Error(
          "ctopo: getArcOffsetsDict called on a monolithic-format file",
        );
      }
      if (meta.dictSection === undefined) {
        this.arcOffsetsDictPromise = Promise.resolve(undefined);
        return this.arcOffsetsDictPromise;
      }
      const entry = this.sectionByName.get(meta.dictSection);
      if (entry === undefined) {
        throw new Error(
          `ctopo: container is missing the "${meta.dictSection}" section`,
        );
      }
      this.arcOffsetsDictPromise = this.fetchSectionBytes(entry).then(
        (bytes) => {
          this.tallyUseful(entry.name, bytes.byteLength);
          return bytes;
        },
      );
    }
    return this.arcOffsetsDictPromise;
  }

  // Fetch + decompress one partition of arc_offsets into a
  // delta-decoded Uint32Array of *local* offsets (length = N+1 where
  // N = arcs in this block; entries[0]=0, entries[N]=block uncompressed
  // size). Caller adds the block's uncompressed-start to recover
  // global byte offsets when needed. Mirrors
  // getDecompressedArcCoordBlock but routes per-partition fetches
  // through the "offsets" coalescer family.
  private getDecompressedArcOffsetsBlock(
    blockIdx: number,
    offsetsBlocks: Uint32Array,
    dictPromise: Promise<Uint8Array | undefined>,
  ): Promise<Uint32Array> {
    const cached = this.decompressedArcOffsetsBlockCache.get(blockIdx);
    if (cached !== undefined) return cached;
    const firstArcId = offsetsBlocks[blockIdx * 3];
    const nextFirstArcId =
      blockIdx === offsetsBlocks.length / 3 - 1
        ? this.meta.numArcs
        : offsetsBlocks[(blockIdx + 1) * 3];
    const compOffset = offsetsBlocks[blockIdx * 3 + 1];
    const compLength = offsetsBlocks[blockIdx * 3 + 2];
    const physicalStart = this.arcOffsetsPartitionsBase + compOffset;
    const physicalEnd = physicalStart + compLength;
    const entries = nextFirstArcId - firstArcId + 1;
    const uncSize = entries * 4;
    const entry: SectionEntry = {
      name: `arc_offsets[partition ${blockIdx}]`,
      dtype: "u32",
      offset: physicalStart,
      length: compLength,
      compression: "zstd",
    };
    const promise = (async () => {
      const [compressed, dict, decode] = await Promise.all([
        this.enqueueSectionFetch("offsets", physicalStart, physicalEnd),
        dictPromise,
        loadZstdWasmDecode(entry),
      ]);
      const t0 = performance.now();
      const out = decode(compressed, uncSize, dict);
      this.statDecompressMs += performance.now() - t0;
      this.statDecompressBytes += out.byteLength;
      // Undo first-order delta encoding in place — running prefix sum
      // with u32 wraparound, same as the monolithic arc_offsets path
      // does via viewU32WithDelta. We do it inline here so we can
      // hand back a fresh Uint32Array view without a second copy.
      const view = new Uint32Array(
        out.buffer,
        out.byteOffset,
        out.byteLength / 4,
      );
      const decoded = new Uint32Array(view.length);
      if (view.length > 0) {
        decoded[0] = view[0];
        for (let i = 1; i < view.length; i++) {
          decoded[i] = (decoded[i - 1] + view[i]) >>> 0;
        }
      }
      return decoded;
    })();
    this.decompressedArcOffsetsBlockCache.set(blockIdx, promise);
    return promise;
  }

  // Binary search the offsets-blocks table (3 u32 per row; firstArcId
  // is the first u32 of each row) for the partition that contains
  // arcId. Sibling to findArcCoordBlock at the bottom of this file.
  private static findArcOffsetsBlock(
    offsetsBlocks: Uint32Array,
    arcId: number,
  ): number {
    const blockCount = offsetsBlocks.length / 3;
    // Want the largest i such that offsetsBlocks[i*3] <= arcId.
    let lo = 0;
    let hi = blockCount - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >>> 1;
      if (offsetsBlocks[mid * 3] <= arcId) lo = mid;
      else hi = mid - 1;
    }
    return lo;
  }

  // --- Private: prefetch helpers ---

  // Speculative prefetch of the first `bytes` of a named section.
  // Lands in the byte-range cache via the same coalesce pipeline as
  // a real fetch, so a concurrent fetcher covering the prefix
  // attaches to the in-flight GET instead of duplicating it.
  // Fire-and-forget — exceptions are swallowed.
  private prefetchPrefix(
    sectionName: string,
    family: string,
    bytes: number,
    priority: FetchPriority = "auto",
  ): void {
    if (this.closed || bytes <= 0) return;
    const section = this.sectionByName.get(sectionName);
    if (section === undefined) return;
    const start = section.offset;
    const end = Math.min(start + bytes, start + section.length);
    if (end <= start) return;
    this.enqueueSectionFetch(family, start, end, priority).catch(() => {
      // Silent — prefetch failures don't fail open or surface.
    });
  }

  // Issue a Range fetch for [start, end) through the standard
  // pipeline — chunks at maxChunkBytes and fires up to
  // maxParallelRanges physical GETs in parallel; bytes land in the
  // byte-range cache automatically. Used by the open path to
  // prefetch front-loaded sections without serializing on a single
  // big GET (which throttles on a single HTTP/2 stream).
  // Fire-and-forget — exceptions are swallowed (a failed prefetch
  // surfaces on the first dependent section fetch instead).
  prefetchRange(
    family: string,
    start: number,
    end: number,
    priority: FetchPriority = "auto",
  ): void {
    this.enqueueSectionFetch(family, start, end, priority).catch(() => {
      // Silent — prefetch failures don't fail open or surface.
    });
  }

  // --- Private: batched fetch pipeline ---

  // Enqueue an exact byte interval into the next-microtask drain. The
  // returned Promise resolves with the section's bytes once the batch
  // fetcher has issued its coalesced GETs and sliced them. If the
  // interval is already covered by the byte-range cache (e.g. it
  // landed inside an earlier fetched chunk), resolve immediately
  // without queuing. If a fetch covering it is already in flight,
  // attach to that fetch's Promise instead of firing a duplicate
  // GET.
  //
  // `family` partitions the coalescer: only intervals in the same
  // family bridge across `coalesceGapBytes`, so e.g. a property
  // fetch and a layer-CSR fetch never get fused into one massive
  // GET even when they're packed back-to-back in the file.
  private enqueueSectionFetch(
    family: string,
    start: number,
    end: number,
    priority: FetchPriority = "auto",
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    if (signal?.aborted) {
      const reason: unknown = signal.reason;
      return Promise.reject(
        reason instanceof Error ? reason : new Error("aborted"),
      );
    }
    const cached = this.lookupByteRange(start, end);
    if (cached !== undefined) {
      this.statByteRangeCacheHits++;
      return Promise.resolve(cached);
    }
    const inFlight = this.lookupInFlightRange(start, end);
    if (inFlight !== undefined) {
      this.statInFlightHits++;
      return inFlight;
    }
    this.statByteRangeCacheMisses++;
    return new Promise<Uint8Array>((resolve, reject) => {
      this.pendingSectionFetches.push({
        family,
        start,
        end,
        priority,
        signal,
        resolve,
        reject,
      });
      if (!this.flushScheduled) {
        this.flushScheduled = true;
        queueMicrotask(() => {
          void this.flushPendingSectionFetches();
        });
      }
    });
  }

  private lookupInFlightRange(
    start: number,
    end: number,
  ): Promise<Uint8Array> | undefined {
    // Check logical ranges first — each entry covers the full span of
    // a coalesced fetch (before chunk splitting), so it can match
    // requests larger than any single chunk. Walk newest-first.
    for (let i = this.inFlightLogicalRanges.length - 1; i >= 0; i--) {
      const r = this.inFlightLogicalRanges[i];
      if (r.start <= start && end <= r.end) {
        return r.promise.then((bytes) =>
          bytes.subarray(start - r.start, end - r.start),
        );
      }
    }
    // Fallback: per-chunk in-flight entries (still useful for sub-chunk
    // requests that arrive after the logical range has resolved into
    // chunks but before all chunks complete).
    for (let i = this.inFlightRanges.length - 1; i >= 0; i--) {
      const r = this.inFlightRanges[i];
      if (r.start <= start && end <= r.end) {
        return r.promise.then((bytes) =>
          bytes.subarray(start - r.start, end - r.start),
        );
      }
    }
    return undefined;
  }

  private lookupByteRange(start: number, end: number): Uint8Array | undefined {
    // Walk newest-first so recently-fetched bytes win the LRU bump.
    // The cache is short — one entry per coalesced drain GET, capped
    // by byteRangeCacheBytes — so a linear scan is fine and a sorted
    // structure would just add complexity.
    for (let i = this.byteRangeCache.length - 1; i >= 0; i--) {
      const r = this.byteRangeCache[i];
      if (r.start <= start && end <= r.end) {
        // LRU bump: move to the end.
        if (i !== this.byteRangeCache.length - 1) {
          this.byteRangeCache.splice(i, 1);
          this.byteRangeCache.push(r);
        }
        return r.bytes.subarray(start - r.start, end - r.start);
      }
    }
    return undefined;
  }

  private cacheByteRange(start: number, end: number, bytes: Uint8Array): void {
    const size = bytes.byteLength;
    // Skip caching items larger than the budget — they'd flush
    // everything else and serve no one.
    if (size > this.byteRangeCacheBytes) return;
    while (
      this.byteRangeCacheUsedBytes + size > this.byteRangeCacheBytes &&
      this.byteRangeCache.length > 0
    ) {
      const evicted = this.byteRangeCache.shift();
      if (evicted !== undefined)
        this.byteRangeCacheUsedBytes -= evicted.bytes.byteLength;
    }
    this.byteRangeCache.push({ start, end, bytes });
    this.byteRangeCacheUsedBytes += size;
  }

  private async flushPendingSectionFetches(): Promise<void> {
    const pending = this.pendingSectionFetches;
    this.pendingSectionFetches = [];
    this.flushScheduled = false;
    if (pending.length === 0) return;

    // Bucket pending fetches by family. We only ever bridge gaps
    // *within* a family — across families (e.g. property X and
    // unrelated layer Y), even back-to-back items never share bytes.
    const byFamily = new Map<string, PendingSectionFetch[]>();
    for (const item of pending) {
      const arr = byFamily.get(item.family);
      if (arr === undefined) byFamily.set(item.family, [item]);
      else arr.push(item);
    }

    // Within each family: sort by start, coalesce intervals separated
    // by ≤ family's coalesce gap into one logical range. Each logical
    // range is a span we'd ideally fetch as one GET — but we then
    // split it into MAX_COALESCED-sized physical chunks so a big run
    // fans out into parallel HTTP/2 streams instead of one slow
    // serial GET.
    const logicals: LogicalRange[] = [];
    for (const [family, items] of byFamily) {
      const gap = this.coalesceGapByFamily[family] ?? this.coalesceGapBytes;
      items.sort((a, b) => a.start - b.start);
      let current: LogicalRange | null = null;
      for (const item of items) {
        if (current !== null && item.start - current.end <= gap) {
          if (item.end > current.end) current.end = item.end;
          current.items.push(item);
          current.priority = mergePriority(current.priority, item.priority);
        } else {
          if (current !== null) logicals.push(current);
          current = {
            family: item.family,
            start: item.start,
            end: item.end,
            items: [item],
            priority: item.priority,
            chunkBytes: [],
            error: undefined,
          };
        }
      }
      if (current !== null) logicals.push(current);
    }

    // Register each logical range in the in-flight registry BEFORE
    // chunk dispatch starts. Any enqueueSectionFetch that lands while
    // these are in flight can attach to a single logical entry rather
    // than miss because no single chunk covers the requested span.
    // The promise resolves after stitching (or rejects on error).
    const logicalResolvers: Array<{
      entry: InFlightRange;
      resolve: (bytes: Uint8Array) => void;
      reject: (err: unknown) => void;
    }> = [];
    for (const logical of logicals) {
      let resolve!: (bytes: Uint8Array) => void;
      let reject!: (err: unknown) => void;
      const promise = new Promise<Uint8Array>((res, rej) => {
        resolve = res;
        reject = rej;
      });
      // Don't let an unhandled rejection escape — callers may not
      // attach via lookupInFlightRange before we reject on a fetch
      // error, and the resolver is internal to the dispatch path.
      promise.catch(() => {});
      const entry: InFlightRange = {
        start: logical.start,
        end: logical.end,
        promise,
      };
      this.inFlightLogicalRanges.push(entry);
      logicalResolvers.push({ entry, resolve, reject });
    }

    // Build the flat chunk list. Each chunk references its parent
    // logical range so completions know where to deposit bytes.
    const chunks: ChunkTask[] = [];
    for (const logical of logicals) {
      const numChunks = Math.max(
        1,
        Math.ceil((logical.end - logical.start) / this.maxChunkBytes),
      );
      logical.chunkBytes = new Array<Uint8Array>(numChunks);
      for (let i = 0; i < numChunks; i++) {
        const cs = logical.start + i * this.maxChunkBytes;
        const ce = Math.min(cs + this.maxChunkBytes, logical.end);
        chunks.push({ logical, index: i, start: cs, end: ce });
      }
    }

    // Group chunks into dispatch tasks. When the fetcher supports
    // multi-range, pack multiple disjoint chunks (same family) into a
    // single HTTP request to reduce request count. Otherwise each
    // chunk dispatches as its own GET.
    type DispatchTask =
      | { kind: "single"; chunk: ChunkTask; priority: FetchPriority }
      | { kind: "multi"; chunks: ChunkTask[]; priority: FetchPriority };

    const tasks: DispatchTask[] = [];
    const canMultiRange =
      this.multiRangeEnabled &&
      this.maxRangesPerRequest >= 2 &&
      this.fetcher.multiRange !== undefined;

    if (canMultiRange && chunks.length > 1) {
      // Separate chunks into two buckets per family:
      //  - Chunks from logical ranges that were split by maxChunkBytes
      //    are contiguous pieces of one big range — dispatch them as
      //    individual parallel GETs so they fan out over HTTP/2.
      //  - Chunks that are the sole piece of their logical range are
      //    small/sparse — pack them into multi-range requests to reduce
      //    round trips.
      //
      // Multi-range packing stays within family boundaries so that
      // independent UI operations don't block each other (e.g. a small
      // string-section lookup shouldn't wait on a large coordinate
      // fetch).
      const candidatesByFamily = new Map<string, ChunkTask[]>();
      for (const chunk of chunks) {
        if (chunk.logical.chunkBytes.length > 1) {
          tasks.push({
            kind: "single",
            chunk,
            priority: chunk.logical.priority,
          });
        } else {
          const arr = candidatesByFamily.get(chunk.logical.family);
          if (arr === undefined)
            candidatesByFamily.set(chunk.logical.family, [chunk]);
          else arr.push(chunk);
        }
      }

      for (const familyChunks of candidatesByFamily.values()) {
        let group: ChunkTask[] = [];
        let groupBytes = 0;
        let groupPriority: FetchPriority = "low";

        for (const chunk of familyChunks) {
          const chunkSize = chunk.end - chunk.start;
          const wouldExceedRanges = group.length >= this.maxRangesPerRequest;
          const wouldExceedBytes = groupBytes + chunkSize > this.maxChunkBytes;

          if (group.length > 0 && (wouldExceedRanges || wouldExceedBytes)) {
            if (group.length === 1) {
              tasks.push({
                kind: "single",
                chunk: group[0],
                priority: groupPriority,
              });
            } else {
              tasks.push({
                kind: "multi",
                chunks: group,
                priority: groupPriority,
              });
            }
            group = [];
            groupBytes = 0;
            groupPriority = "low";
          }

          group.push(chunk);
          groupBytes += chunkSize;
          groupPriority = mergePriority(groupPriority, chunk.logical.priority);
        }

        // Flush remaining
        if (group.length === 1) {
          tasks.push({
            kind: "single",
            chunk: group[0],
            priority: groupPriority,
          });
        } else if (group.length > 1) {
          tasks.push({
            kind: "multi",
            chunks: group,
            priority: groupPriority,
          });
        }
      }
    } else {
      // No multi-range support or only one chunk — dispatch individually
      for (const chunk of chunks) {
        tasks.push({
          kind: "single",
          chunk,
          priority: chunk.logical.priority,
        });
      }
    }

    const multiRangeCount = tasks.filter((t) => t.kind === "multi").length;
    perfLog(
      `[ctopo] flush: ${logicals.length} logical range(s) → ${chunks.length} chunk(s) → ${tasks.length} task(s)` +
        (multiRangeCount > 0 ? ` (${multiRangeCount} multi-range)` : "") +
        ` (${this.maxParallelRanges} max parallel, ${this.maxChunkBytes >> 10} KiB cap)`,
    );

    // Fire tasks with bounded concurrency. Each task (single or multi-
    // range) occupies one concurrency slot.
    await runWithConcurrency(tasks, this.maxParallelRanges, async (task) => {
      if (task.kind === "single") {
        await this.dispatchSingleChunk(task.chunk, task.priority);
      } else {
        await this.dispatchMultiRange(task.chunks, task.priority);
      }
    });

    // After all tasks: stitch each logical range back together
    // (single-chunk case is a free passthrough), resolve the per-item
    // promises with the right subarray, resolve the in-flight logical
    // entry so any concurrent enqueueSectionFetch attached to it gets
    // the assembled bytes, cache the full logical range so subsequent
    // enqueues hit the byte-range cache, and then unregister the
    // in-flight entry.
    for (let i = 0; i < logicals.length; i++) {
      const logical = logicals[i];
      const { entry, resolve, reject } = logicalResolvers[i];
      if (logical.error !== undefined) {
        for (const item of logical.items) item.reject(logical.error);
        reject(logical.error);
      } else {
        const bytes =
          logical.chunkBytes.length === 1
            ? logical.chunkBytes[0]
            : stitchChunks(logical.start, logical.end, logical.chunkBytes);
        for (const item of logical.items) {
          if (item.signal?.aborted) {
            item.reject(item.signal.reason ?? new Error("aborted"));
          } else {
            item.resolve(
              bytes.subarray(
                item.start - logical.start,
                item.end - logical.start,
              ),
            );
          }
        }
        resolve(bytes);
        this.cacheByteRange(logical.start, logical.end, bytes);
      }
      const idx = this.inFlightLogicalRanges.indexOf(entry);
      if (idx >= 0) this.inFlightLogicalRanges.splice(idx, 1);
    }
  }

  private async dispatchSingleChunk(
    chunk: ChunkTask,
    priority: FetchPriority,
  ): Promise<void> {
    const logical = chunk.logical;
    let resolveChunk!: (bytes: Uint8Array) => void;
    let rejectChunk!: (err: unknown) => void;
    const chunkPromise = new Promise<Uint8Array>((res, rej) => {
      resolveChunk = res;
      rejectChunk = rej;
    });
    chunkPromise.catch(() => {});
    const inFlightEntry: InFlightRange = {
      start: chunk.start,
      end: chunk.end,
      promise: chunkPromise,
    };
    this.inFlightRanges.push(inFlightEntry);

    try {
      const bytes = await this.fetcher.range(
        chunk.start,
        chunk.end,
        this.closeController.signal,
        priority,
      );
      const expected = chunk.end - chunk.start;
      if (bytes.byteLength !== expected) {
        throw new Error(
          `ctopo: short read at [${chunk.start}, ${chunk.end}) — got ${bytes.byteLength}, expected ${expected}`,
        );
      }
      this.statRequests++;
      this.statRequestBytes += bytes.byteLength;
      const fam = this.statPerFamily.get(logical.family);
      if (fam === undefined) {
        this.statPerFamily.set(logical.family, {
          requests: 1,
          requestBytes: bytes.byteLength,
        });
      } else {
        fam.requests++;
        fam.requestBytes += bytes.byteLength;
      }
      // Cache happens at the logical-range level after stitching (see
      // flushPendingSectionFetches' stitch loop). Not caching the
      // chunk here avoids double-charging the cache budget for
      // single-chunk logicals where the chunk and logical bytes are
      // identical, and avoids stranded chunk fragments when the
      // assembled logical is what later requests target.
      logical.chunkBytes[chunk.index] = bytes;
      resolveChunk(bytes);
    } catch (err) {
      logical.error = err;
      rejectChunk(err);
    } finally {
      const idx = this.inFlightRanges.indexOf(inFlightEntry);
      if (idx >= 0) this.inFlightRanges.splice(idx, 1);
    }
  }

  private async dispatchMultiRange(
    chunks: ChunkTask[],
    priority: FetchPriority,
  ): Promise<void> {
    // Register each chunk as in-flight individually so concurrent
    // enqueueSectionFetch calls can attach to their promises.
    const inFlightEntries: Array<{
      entry: InFlightRange;
      resolve: (bytes: Uint8Array) => void;
      reject: (err: unknown) => void;
    }> = [];

    for (const chunk of chunks) {
      let resolveChunk!: (bytes: Uint8Array) => void;
      let rejectChunk!: (err: unknown) => void;
      const chunkPromise = new Promise<Uint8Array>((res, rej) => {
        resolveChunk = res;
        rejectChunk = rej;
      });
      chunkPromise.catch(() => {});
      const entry: InFlightRange = {
        start: chunk.start,
        end: chunk.end,
        promise: chunkPromise,
      };
      this.inFlightRanges.push(entry);
      inFlightEntries.push({
        entry,
        resolve: resolveChunk,
        reject: rejectChunk,
      });
    }

    try {
      const ranges = chunks.map((c) => ({ start: c.start, end: c.end }));
      const result = await this.fetcher.multiRange!(
        ranges,
        this.closeController.signal,
        priority,
      );

      if (result.kind === "unsupported") {
        // Server doesn't support multi-range — disable for this client
        // and re-dispatch each chunk as an individual range request.
        this.multiRangeEnabled = false;
        perfLog(
          `[ctopo] multi-range unsupported, falling back to single-range`,
        );
        // Clean up our in-flight entries — dispatchSingleChunk registers
        // its own.
        for (const { entry } of inFlightEntries) {
          const idx = this.inFlightRanges.indexOf(entry);
          if (idx >= 0) this.inFlightRanges.splice(idx, 1);
        }
        await runWithConcurrency(
          chunks,
          this.maxParallelRanges,
          async (chunk) => {
            await this.dispatchSingleChunk(chunk, priority);
          },
        );
        return;
      }

      this.statRequests++;

      // Match returned parts to chunks by start offset
      const partsByStart = new Map<
        number,
        { start: number; end: number; bytes: Uint8Array }
      >();
      for (const part of result.parts) {
        partsByStart.set(part.start, part);
      }

      let totalPartBytes = 0;
      for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        const part = partsByStart.get(chunk.start);
        if (part === undefined) {
          const err = new Error(
            `ctopo: multi-range response missing part for [${chunk.start}, ${chunk.end})`,
          );
          chunk.logical.error = err;
          inFlightEntries[i].reject(err);
          continue;
        }
        const expected = chunk.end - chunk.start;
        if (part.bytes.byteLength !== expected) {
          const err = new Error(
            `ctopo: multi-range part size mismatch at [${chunk.start}, ${chunk.end}) — got ${part.bytes.byteLength}, expected ${expected}`,
          );
          chunk.logical.error = err;
          inFlightEntries[i].reject(err);
          continue;
        }
        totalPartBytes += part.bytes.byteLength;
        // Per-chunk caching skipped — the logical range is cached
        // after stitch in flushPendingSectionFetches.
        chunk.logical.chunkBytes[chunk.index] = part.bytes;
        inFlightEntries[i].resolve(part.bytes);
        const fam = this.statPerFamily.get(chunk.logical.family);
        if (fam === undefined) {
          this.statPerFamily.set(chunk.logical.family, {
            requests: 0,
            requestBytes: part.bytes.byteLength,
          });
        } else {
          fam.requestBytes += part.bytes.byteLength;
        }
      }
      this.statRequestBytes += totalPartBytes;
    } catch (err) {
      // Fail all chunks in this multi-range request
      for (let i = 0; i < chunks.length; i++) {
        chunks[i].logical.error = err;
        inFlightEntries[i].reject(err);
      }
    } finally {
      for (const { entry } of inFlightEntries) {
        const idx = this.inFlightRanges.indexOf(entry);
        if (idx >= 0) this.inFlightRanges.splice(idx, 1);
      }
    }
  }

  // --- Private: misc ---

  private sectionEntry(name: string): SectionEntry {
    const entry = this.sectionByName.get(name);
    if (entry === undefined) {
      throw new Error(`ctopo: section "${name}" not in container`);
    }
    return entry;
  }

  private ensureOpen(): void {
    if (this.closed) throw new Error("ctopo: client is closed");
  }
}

// --- Module-level helpers ---

// Check a caller-supplied AbortSignal and throw if already aborted.
// Used at the entry of every public method so pre-aborted signals
// reject immediately without touching the cache or fetch pipeline.
function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw signal.reason ?? new Error("aborted");
  }
}

// Pick the more-urgent priority of two values. high > auto > low.
function mergePriority(a: FetchPriority, b: FetchPriority): FetchPriority {
  if (a === "high" || b === "high") return "high";
  if (a === "auto" || b === "auto") return "auto";
  return "low";
}

function stitchChunks(
  start: number,
  end: number,
  chunkBytes: Uint8Array[],
): Uint8Array {
  const out = new Uint8Array(end - start);
  let cursor = 0;
  for (const c of chunkBytes) {
    out.set(c, cursor);
    cursor += c.byteLength;
  }
  return out;
}

// Binary search the block table for the block containing a given
// logical (uncompressed) byte offset. Block table is u32 triples
// [uncEnd, compOff, compLen]; uncEnd is monotonically increasing and
// exclusive, so we want the smallest i such that uncEnd[i] > offset.
function findArcCoordBlock(blocks: Uint32Array, logicalOffset: number): number {
  const blockCount = blocks.length / 3;
  let lo = 0;
  let hi = blockCount - 1;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (blocks[mid * 3] <= logicalOffset) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

// Wrap section bytes as a Uint32Array, undoing first-order delta
// encoding when the section was emitted that way. Non-delta path
// shares the underlying buffer (zero copy); delta path runs a
// running prefix sum into a fresh buffer with u32 wraparound that
// mirrors the encoder side. Roughly a few ms one-time cost per
// million entries on a multi-MiB arc_offsets.
function viewU32WithDelta(bytes: Uint8Array, delta: boolean): Uint32Array {
  if (!delta)
    return new Uint32Array(
      bytes.buffer,
      bytes.byteOffset,
      bytes.byteLength / 4,
    );
  const src = new Uint32Array(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength / 4,
  );
  const dst = new Uint32Array(src.length);
  if (src.length === 0) return dst;
  dst[0] = src[0];
  for (let i = 1; i < src.length; i++) dst[i] = (dst[i - 1] + src[i]) >>> 0;
  return dst;
}
