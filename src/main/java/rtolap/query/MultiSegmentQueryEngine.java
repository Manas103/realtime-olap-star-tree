package rtolap.query;

import rtolap.storage.SegmentCatalog;

import java.util.*;

/** Scatter-gather across every sealed segment's own star-tree, merging partial sums, the way a
 * real system fans a query out across segments and brokers and sums the partial aggregates. */
public final class MultiSegmentQueryEngine {
    public static Map<List<Integer>, Long> execute(SegmentCatalog catalog, GroupByQuery query) {
        Map<List<Integer>, Long> merged = new HashMap<>();
        for (SegmentCatalog.Entry e : catalog.entries()) {
            Map<List<Integer>, Long> partial = StarTreeQueryEngine.execute(e.tree, query);
            for (Map.Entry<List<Integer>, Long> p : partial.entrySet()) {
                merged.merge(p.getKey(), p.getValue(), Long::sum);
            }
        }
        return merged;
    }

    public static Map<List<Integer>, Long> executeScan(SegmentCatalog catalog, GroupByQuery query) {
        Map<List<Integer>, Long> merged = new HashMap<>();
        for (SegmentCatalog.Entry e : catalog.entries()) {
            Map<List<Integer>, Long> partial = ScanQueryEngine.execute(e.segment, query);
            for (Map.Entry<List<Integer>, Long> p : partial.entrySet()) {
                merged.merge(p.getKey(), p.getValue(), Long::sum);
            }
        }
        return merged;
    }
}
