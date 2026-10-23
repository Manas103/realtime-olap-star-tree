package rtolap;

import org.junit.jupiter.api.Test;
import rtolap.ingest.StreamingIngestPipeline;
import rtolap.ingest.SyntheticFactGenerator;
import rtolap.storage.SegmentCatalog;

import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class IngestTest {

    @Test
    void producedRowsBecomeQueryableAfterTheirSegmentSeals() throws Exception {
        var dir = Files.createTempDirectory("rtolap-ingest-test");
        SegmentCatalog catalog = new SegmentCatalog();
        AtomicInteger sealedSegments = new AtomicInteger();
        StreamingIngestPipeline pipeline = new StreamingIngestPipeline(
                2, 1000, 500,
                SyntheticFactGenerator.DIMENSION_NAMES, SyntheticFactGenerator.METRIC_NAMES,
                new int[]{0, 1, 2, 3, 4, 5, 6, 7}, 10_000, dir,
                (segment, tree) -> { catalog.add(segment, tree); sealedSegments.incrementAndGet(); });

        SyntheticFactGenerator gen = new SyntheticFactGenerator(5);
        CompletableFuture<Void> last = null;
        for (int i = 0; i < 1000; i++) {
            last = pipeline.produce(0, gen.nextDims(), gen.nextMetrics());
        }
        last.get(5, java.util.concurrent.TimeUnit.SECONDS); // size-triggered flush
        assertEquals(1000, catalog.totalRows());
        assertEquals(1, sealedSegments.get());

        CompletableFuture<Void> small = pipeline.produce(1, gen.nextDims(), gen.nextMetrics());
        small.get(5, java.util.concurrent.TimeUnit.SECONDS); // time-triggered flush, below the size threshold
        assertEquals(1001, catalog.totalRows());
        pipeline.shutdown();
    }
}
