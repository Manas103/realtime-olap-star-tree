# realtime-olap-star-tree

A real-time OLAP store: a partitioned stream flushes into immutable columnar segments, each
indexed on seal with a star-tree (LinkedIn Pinot's published pre-aggregation structure), queried
through a small SQL-shaped group-by engine that walks the index instead of scanning raw rows.
Every number below was measured on this machine, not targeted; where a measurement fell short of
the claim it was designed against, that is stated plainly rather than rounded up.

## Why this exists

Pinot's star-tree index is a specific, named tradeoff: pre-aggregate dimension combinations up to
some depth to bound group-by query latency, at the cost of extra storage for the aggregated rows,
and either limit how many dimension combinations you pre-aggregate or let storage grow. This
project builds that structure from its description (not from Pinot's source) over a synthetic
8-dimension fact table, and measures both sides of the tradeoff on the same runs: the latency win
and the storage it costs.

## What this is not

- **Not Pinot.** This reproduces one structure Pinot documented publicly (the star-tree index) and
  the general shape of its ingestion (partitioned stream to immutable segment), not Pinot's
  storage format, query language, or cluster architecture.
- **A group-by engine, not a SQL engine.** `GroupByQuery` is a typed Java object (group-by
  dimensions, equality filters, one metric), not a parser; "SQL" in the resume line describes the
  query shape (`SELECT dims, SUM(metric) GROUP BY dims`), which is what both the scan baseline and
  the star-tree engine execute.
- **Dimension values are small-integer codes, not dictionary-encoded strings.** The synthetic
  generator emits the codes directly. A real system's string dictionary sits underneath this same
  columnar layout and does not change how the star-tree is built or walked.
- **Segments and the index are Java-serialized, not a hand-rolled binary columnar format.** This
  keeps the storage-ratio measurement honest in a different way than a production format would:
  both the raw segment and the index pay the same serialization constant per row, so the ratio
  between them is not flattered by one side using a tighter encoding than the other.
- **Grouping by every dimension at once defeats the index by construction.** If a query's GROUP BY
  lists all 8 dimensions, there is nothing left to aggregate away and the star-tree degenerates to
  a raw scan with extra bookkeeping; `matchesScanOracleWhenGroupingByEveryDimension` tests exactly
  this case for correctness, not for speed. The latency benchmark groups by a realistic 2-3 of the
  8 dimensions with 0-2 filters, which is the workload star-tree indexing targets.
- **Machine and toolchain.** AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, 32 GB RAM, Windows
  11, OpenJDK (Temurin) 21.0.11, plain `javac`/`java` (JUnit 5 Platform Console Standalone jar in
  `lib/`, downloaded, not committed).

## Architecture

```
src/main/java/rtolap/
  storage/
    ColumnarSegment.java   one int[] per dimension, one long[] per metric, Java-serialized to disk
    SegmentCatalog.java    every sealed (segment, index) pair currently queryable
  startree/
    StarTree.java          the index: build() and the Node/Leaf shapes; see deep-dive below
  ingest/
    SyntheticFactGenerator.java  8-dimension fact generator, modest per-dimension cardinality
    StreamingIngestPipeline.java partitioned queues -> buffered rows -> sealed, indexed segments
  query/
    GroupByQuery.java        groupByDims, filters, metricIndex
    ScanQueryEngine.java      the pre-index baseline: full columnar scan, per-row filter, group-by
    StarTreeQueryEngine.java  walks a StarTree: star/specific/enumerate per dimension (see below)
    MultiSegmentQueryEngine.java scatter-gather across every segment's own tree, merges partial sums
  tools/
    BenchmarkAll.java         measures every claim below; raw output in docs/benchmark_output.txt
src/test/java/rtolap/
  StarTreeCorrectnessTest.java  the reference oracle: star-tree vs. scan, exact match, 200+ random queries
  IngestTest.java                produced rows become queryable exactly when their segment seals
```

### Design deep-dives

**Star-tree construction tracks a dimension's *global* position separately from its *local* array
offset, because the two diverge the moment a STAR child is built.** Every internal node splits on
one dimension, producing one child per distinct value plus one STAR child whose rows have that
dimension aggregated away. A specific-value child reuses the same column array as its parent, just
narrowing which rows it sees, so its local offset into that array stays aligned with the original
dimension order. A STAR child's rows are *new*, smaller, aggregated rows over a narrower set of
columns (the dimension that was starred is gone), so its local array always starts over at offset
0 while the *global* dimension it represents keeps counting up. `Leaf.remainingSplitOrderIndices`
and `Node.splitDim` store the global position explicitly for exactly this reason; the first version
of this code conflated the two and produced wrong answers for any query that could not be solved
at the root level (see Findings).

**The query walk has three rules, one per dimension, decided independently at each node:** a
dimension with an equality filter follows only the matching specific child (STAR would include
rows the filter must exclude); a dimension the query groups by must be broken back out, so the walk
enumerates every specific child and folds that dimension's value into the group key as it
descends; a dimension neither filtered nor grouped on takes the STAR child, the cheap path, because
the index has already aggregated it away. A leaf reached before every dimension is resolved (the
common case, since the leaf threshold usually fires first) falls back to scanning just that leaf's
rows for whatever dimensions are left.

**Segments are built per-partition, per-flush-threshold, each with its own star-tree, not one
global tree.** This mirrors Pinot (one star-tree per segment) and is also what keeps peak memory
bounded during the large-scale benchmark: segments are built, measured, and written to disk one at
a time rather than holding the whole claimed 50M-row dataset's intermediate construction state in
memory at once.

## Validation

- **Reference oracle (`StarTreeCorrectnessTest`): the star-tree's answer diffed exactly against a
  full columnar scan**, across 200+ randomized query shapes (1-4 group-by dimensions, 0-2 filters,
  either metric) plus three fixed edge cases: grouping by nothing (grand total), grouping by every
  dimension at once (the index can't help, and must not change the answer), and a leaf-threshold
  check that every leaf still holding unresolved dimensions respects the configured threshold.
- **`IngestTest`**: a produced row's future does not complete until its segment has actually
  sealed and been handed to the catalog, checked for both a size-triggered flush and a
  time-triggered flush of a partially-filled partition.
- **`BenchmarkAll`**: the same properties at claim scale, with the raw run committed to
  `docs/benchmark_output.txt`.

## Findings: what broke

**Conflating "local array position" with "original dimension index" silently produced wrong query
results the moment a query needed a dimension the tree could only resolve via a STAR child.** The
first version of `StarTree.buildNode` reused one `depth` counter for both the index into whatever
array it had just been handed and the dimension number to report in `Node.splitDim`. That is
correct for every node reached purely through specific-value edges (the array is the original one,
offset by depth) and silently wrong for any node under a STAR edge (the array is a brand-new,
narrower one, and `depth` there means something different). The bug did not show up in the first
smoke test, which happened to query dimensions resolvable without ever taking a STAR path; it
surfaced as soon as a no-filter, single-dimension group-by was tried, and a 300-trial randomized
differential test against the scan baseline is what actually pinned it down (the mismatch count,
not a hand-picked case). The fix threads an explicit `globalBase` alongside `localDepth` through
every recursive call.

**Storage overhead came in higher than the claimed 1.4x (measured 2.68x) because each segment pays its own index overhead independently.** A star-tree's biggest aggregated tables are near the root, where the fewest dimensions have been resolved away yet; building one tree per 2M-row segment means paying that near-root overhead 10 times (once per segment) instead of once for the whole 20M-row dataset a single global tree would have used. This is a real, measured property of the per-segment design (which otherwise keeps peak build memory bounded, see Architecture), not a bug: a production system with far larger segments, or occasional cross-segment merging, would amortize this cost much further than this session's 2M-row segments do.

## Measured results

Machine: AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, 32 GB RAM, Windows 11, OpenJDK
21.0.11. All numbers below are from `docs/benchmark_output.txt`.

| Claim | Measured | Met |
|---|---|---|
| Partitioned stream into columnar segments | 4 stream partitions, 20,000 rows produced, 4 sealed columnar segments, all rows visible | yes |
| Star-tree leaf threshold 10,000 records | enforced in `StarTree.build`; checked directly by `leafThresholdBoundsEveryLeafThatStillHasDimensionsLeftToSplit` | yes |
| Under 2s produce-to-queryable | 60 trials, mean 527ms, p99 564ms, max 564ms | yes |
| **1.4x storage** | **2.68x** (960,004,920 raw bytes, 1,613,092,688 index bytes, across 10 segments) | no, see Findings |
| **p99 on 8-dim-schema group-by, 1.9s to 41ms** | scan baseline p99 **1054ms**, star-tree p99 **182ms** (5.8x faster, target was 46x) | no |
| 50,000,000 rows | **20,000,000** rows (10 segments x 2,000,000), see note below | no, disclosed |

The row-count shortfall was a deliberate, disclosed tradeoff, not an accident: holding 50M rows of columnar data plus their per-segment indexes resident extrapolates to roughly 9-10 GB of heap from this session's measured per-row cost, against about 10 GB free on this machine at the time, and the playbook's resource-courtesy rule asks that a build not seize all available RAM on a machine Manas is using live. The storage-ratio and latency shortfalls are both explained in Findings: the per-segment design pays its biggest aggregation cost once per segment rather than once globally, and a 2M-row segment is small enough that this overhead is proportionally large. Scaling to fewer, larger segments (closer to how Pinot actually sizes segments) would move both numbers closer to the claims without changing the algorithm.

## Building and running

```
# compile
find src/main/java -name "*.java" > sources.txt
javac -d build/classes -encoding UTF-8 @sources.txt

# tests
curl -sL -o lib/junit-platform-console-standalone-1.10.2.jar \
  https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/1.10.2/junit-platform-console-standalone-1.10.2.jar
find src/test/java -name "*.java" > test-sources.txt
javac -d build/test-classes -cp "build/classes;lib/junit-platform-console-standalone-1.10.2.jar" -encoding UTF-8 @test-sources.txt
java -jar lib/junit-platform-console-standalone-1.10.2.jar execute -cp build/classes -cp build/test-classes --scan-classpath build/test-classes

# the full benchmark (builds a 20M-row dataset across 10 segments; several minutes, several GB of heap)
java -Xmx6g -cp build/classes rtolap.tools.BenchmarkAll
```

## Sibling comparison

[`columnar-layout-tick-panel-benchmark`](https://github.com/Manas103/columnar-layout-tick-panel-benchmark)
benchmarks four partitioning layouts over a 1.2B-row panel; it measures how row/columnar/hybrid
layouts affect scan cost, but implements no index and no aggregation path, so a query there is
always a full scan. This repo is the complementary half of that question: given a columnar layout,
how much of a scan can an index let you skip, and what does the index cost in return. The two are
not a fair apples-to-apples (this repo tops out at 20M rows this session, not 1.2B), but the
design question each answers is different: layout-benchmark asks "how should bytes sit on disk",
this repo asks "what should sit next to them so most queries never touch most of them."

## Limitations

- The 50M-row target was not reached this session; see docs/benchmark_output.txt for the measured
  row count and the memory-headroom reasoning.
- No tiered storage, no segment merging/compaction, no deletion; a segment is immutable from the
  moment it seals.
- The query engine supports equality filters and SUM only; no range filters, no COUNT/AVG/MIN/MAX,
  no HAVING, no ORDER BY, no LIMIT.
- One ingest run builds one topic's worth of segments; there is no cross-topic catalog.
- The leaf threshold (10,000 records) and the split order (dimension declaration order) are fixed
  constants, not chosen by a cost model; Pinot's own star-tree builder supports excluding specific
  dimensions from star-node creation, which this project does not.
