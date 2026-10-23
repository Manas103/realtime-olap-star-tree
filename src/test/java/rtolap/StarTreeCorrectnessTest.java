package rtolap;

import org.junit.jupiter.api.Test;
import rtolap.ingest.SyntheticFactGenerator;
import rtolap.query.GroupByQuery;
import rtolap.query.ScanQueryEngine;
import rtolap.query.StarTreeQueryEngine;
import rtolap.startree.StarTree;
import rtolap.storage.ColumnarSegment;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/** The reference oracle: a deliberately slow, obviously correct full scan, diffed exactly
 * against the star-tree's answer for every query shape (grouped, filtered, grouped+filtered,
 * all dimensions grouped at once so the index can't help, and the no-filter/no-group total). */
public class StarTreeCorrectnessTest {

    private static ColumnarSegment buildSegment(int n, long seed) {
        SyntheticFactGenerator gen = new SyntheticFactGenerator(seed);
        int numDims = SyntheticFactGenerator.DIMENSION_NAMES.length;
        int numMetrics = SyntheticFactGenerator.METRIC_NAMES.length;
        int[][] dims = new int[numDims][n];
        long[][] metrics = new long[numMetrics][n];
        for (int i = 0; i < n; i++) {
            int[] d = gen.nextDims();
            long[] m = gen.nextMetrics();
            for (int k = 0; k < numDims; k++) dims[k][i] = d[k];
            for (int k = 0; k < numMetrics; k++) metrics[k][i] = m[k];
        }
        return new ColumnarSegment(dims, metrics, SyntheticFactGenerator.DIMENSION_NAMES, SyntheticFactGenerator.METRIC_NAMES);
    }

    @Test
    void matchesScanOracleAcrossManyRandomQueryShapes() {
        ColumnarSegment segment = buildSegment(300_000, 7);
        StarTree tree = StarTree.build(segment, new int[]{0, 1, 2, 3, 4, 5, 6, 7}, 10_000, SyntheticFactGenerator.METRIC_NAMES);
        Random r = new Random(55);
        int numDims = SyntheticFactGenerator.DIMENSION_NAMES.length;
        for (int t = 0; t < 200; t++) {
            List<Integer> groupDims = new ArrayList<>();
            Set<Integer> used = new HashSet<>();
            int numGroupDims = 1 + r.nextInt(4);
            while (groupDims.size() < numGroupDims) {
                int d = r.nextInt(numDims);
                if (used.add(d)) groupDims.add(d);
            }
            Map<Integer, Integer> filters = new HashMap<>();
            int numFilters = r.nextInt(3);
            for (int f = 0; f < numFilters; f++) {
                int d;
                do { d = r.nextInt(numDims); } while (used.contains(d) || filters.containsKey(d));
                filters.put(d, r.nextInt(SyntheticFactGenerator.CARDINALITY[d]));
            }
            GroupByQuery q = new GroupByQuery(groupDims, filters, r.nextInt(SyntheticFactGenerator.METRIC_NAMES.length));
            assertEquals(ScanQueryEngine.execute(segment, q), StarTreeQueryEngine.execute(tree, q),
                    "mismatch for groupDims=" + groupDims + " filters=" + filters);
        }
    }

    @Test
    void matchesScanOracleWhenGroupingByEveryDimension() {
        ColumnarSegment segment = buildSegment(100_000, 11);
        StarTree tree = StarTree.build(segment, new int[]{0, 1, 2, 3, 4, 5, 6, 7}, 10_000, SyntheticFactGenerator.METRIC_NAMES);
        GroupByQuery q = new GroupByQuery(List.of(0, 1, 2, 3, 4, 5, 6, 7), Map.of(), 0);
        assertEquals(ScanQueryEngine.execute(segment, q), StarTreeQueryEngine.execute(tree, q));
    }

    @Test
    void matchesScanOracleForGrandTotalWithNoGroupingOrFilter() {
        ColumnarSegment segment = buildSegment(50_000, 3);
        StarTree tree = StarTree.build(segment, new int[]{0, 1, 2, 3, 4, 5, 6, 7}, 10_000, SyntheticFactGenerator.METRIC_NAMES);
        GroupByQuery q = new GroupByQuery(List.of(), Map.of(), 1);
        assertEquals(ScanQueryEngine.execute(segment, q), StarTreeQueryEngine.execute(tree, q));
    }

    @Test
    void leafThresholdBoundsEveryLeafThatStillHasDimensionsLeftToSplit() {
        ColumnarSegment segment = buildSegment(200_000, 21);
        int leafThreshold = 5_000;
        StarTree tree = StarTree.build(segment, new int[]{0, 1, 2, 3, 4, 5, 6, 7}, leafThreshold, SyntheticFactGenerator.METRIC_NAMES);
        assertTrue(countLeaves(tree.root) > 0);
        // a leaf that still has dimensions left must be at or under threshold; one that ran out
        // of dimensions first (remainingSplitOrderIndices empty) is allowed to exceed it.
        assertTrue(maxLeafRowsWithDimensionsRemaining(tree.root) <= leafThreshold);
    }

    private int countLeaves(StarTree.Node node) {
        if (node.leaf != null) return 1;
        int total = 0;
        for (StarTree.Node c : node.children.values()) total += countLeaves(c);
        return total;
    }

    private int maxLeafRowsWithDimensionsRemaining(StarTree.Node node) {
        if (node.leaf != null) {
            return node.leaf.remainingSplitOrderIndices.length == 0 ? 0 : node.leaf.rowCount;
        }
        int max = 0;
        for (StarTree.Node c : node.children.values()) max = Math.max(max, maxLeafRowsWithDimensionsRemaining(c));
        return max;
    }
}
