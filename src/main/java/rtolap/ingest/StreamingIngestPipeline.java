package rtolap.ingest;

import rtolap.startree.StarTree;
import rtolap.storage.ColumnarSegment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

/**
 * A partitioned stream (one queue and one worker thread per partition) feeding immutable
 * columnar segments: rows accumulate in a per-partition buffer until it reaches flushRowCount
 * rows or flushIntervalMs has elapsed since the buffer's first row, whichever comes first, at
 * which point it is sealed into a ColumnarSegment, indexed with a StarTree, and handed to the
 * registered callback. The returned future per row completes exactly when that happens, which is
 * what the "produce to queryable" latency claim measures.
 */
public final class StreamingIngestPipeline {
    private final int numPartitions;
    private final int flushRowCount;
    private final long flushIntervalMs;
    private final String[] dimensionNames;
    private final String[] metricNames;
    private final int[] starTreeSplitOrder;
    private final int leafThreshold;
    private final Path dataDir;
    private final BiConsumer<ColumnarSegment, StarTree> onSealed;
    private final BlockingQueue<Row>[] queues;
    private final Thread[] workers;
    private volatile boolean running = true;
    private final java.util.concurrent.atomic.AtomicInteger segmentCounter = new java.util.concurrent.atomic.AtomicInteger();

    private static final class Row {
        final int[] dims;
        final long[] metrics;
        final CompletableFuture<Void> queryable;
        Row(int[] dims, long[] metrics, CompletableFuture<Void> queryable) {
            this.dims = dims; this.metrics = metrics; this.queryable = queryable;
        }
    }

    @SuppressWarnings("unchecked")
    public StreamingIngestPipeline(int numPartitions, int flushRowCount, long flushIntervalMs,
                                    String[] dimensionNames, String[] metricNames,
                                    int[] starTreeSplitOrder, int leafThreshold, Path dataDir,
                                    BiConsumer<ColumnarSegment, StarTree> onSealed) throws IOException {
        this.numPartitions = numPartitions;
        this.flushRowCount = flushRowCount;
        this.flushIntervalMs = flushIntervalMs;
        this.dimensionNames = dimensionNames;
        this.metricNames = metricNames;
        this.starTreeSplitOrder = starTreeSplitOrder;
        this.leafThreshold = leafThreshold;
        this.dataDir = dataDir;
        this.onSealed = onSealed;
        Files.createDirectories(dataDir);
        this.queues = new BlockingQueue[numPartitions];
        this.workers = new Thread[numPartitions];
        for (int p = 0; p < numPartitions; p++) {
            queues[p] = new LinkedBlockingQueue<>();
            int partitionId = p;
            workers[p] = new Thread(() -> runPartition(partitionId), "ingest-partition-" + p);
            workers[p].setDaemon(true);
            workers[p].start();
        }
    }

    public CompletableFuture<Void> produce(int partition, int[] dims, long[] metrics) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        queues[partition % numPartitions].add(new Row(dims, metrics, f));
        return f;
    }

    private void runPartition(int partitionId) {
        List<Row> buffer = new ArrayList<>(flushRowCount);
        long bufferStartMs = 0;
        while (running) {
            try {
                Row r = queues[partitionId].poll(50, TimeUnit.MILLISECONDS);
                if (r != null) {
                    if (buffer.isEmpty()) bufferStartMs = System.currentTimeMillis();
                    buffer.add(r);
                }
                boolean sizeReached = buffer.size() >= flushRowCount;
                boolean timeReached = !buffer.isEmpty() && (System.currentTimeMillis() - bufferStartMs) >= flushIntervalMs;
                if (!buffer.isEmpty() && (sizeReached || timeReached)) {
                    List<Row> toSeal = buffer;
                    buffer = new ArrayList<>(flushRowCount);
                    seal(partitionId, toSeal);
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void seal(int partitionId, List<Row> rows) {
        int numDims = dimensionNames.length;
        int numMetrics = metricNames.length;
        int n = rows.size();
        int[][] dims = new int[numDims][n];
        long[][] metrics = new long[numMetrics][n];
        for (int i = 0; i < n; i++) {
            Row r = rows.get(i);
            for (int d = 0; d < numDims; d++) dims[d][i] = r.dims[d];
            for (int m = 0; m < numMetrics; m++) metrics[m][i] = r.metrics[m];
        }
        ColumnarSegment segment = new ColumnarSegment(dims, metrics, dimensionNames, metricNames);
        StarTree tree = StarTree.build(segment, starTreeSplitOrder, leafThreshold, metricNames);
        int segId = segmentCounter.getAndIncrement();
        try {
            segment.writeTo(dataDir.resolve("partition-" + partitionId + "-segment-" + segId + ".seg"));
            tree.writeTo(dataDir.resolve("partition-" + partitionId + "-segment-" + segId + ".idx"));
        } catch (IOException e) {
            for (Row r : rows) r.queryable.completeExceptionally(e);
            return;
        }
        onSealed.accept(segment, tree);
        for (Row r : rows) r.queryable.complete(null);
    }

    public void shutdown() {
        running = false;
        for (Thread t : workers) t.interrupt();
    }
}
