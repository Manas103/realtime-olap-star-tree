package rtolap.query;

import rtolap.storage.ColumnarSegment;

import java.util.*;

/** The pre-index baseline: a full columnar scan with a per-row filter check and group-by. */
public final class ScanQueryEngine {
    public static Map<List<Integer>, Long> execute(ColumnarSegment segment, GroupByQuery query) {
        Map<List<Integer>, Long> out = new HashMap<>();
        int n = segment.numRows;
        int numGroupDims = query.groupByDims.size();
        int[] filterDims = new int[query.filters.size()];
        int[] filterValues = new int[query.filters.size()];
        int fi = 0;
        for (Map.Entry<Integer, Integer> e : query.filters.entrySet()) {
            filterDims[fi] = e.getKey();
            filterValues[fi] = e.getValue();
            fi++;
        }
        long[] metricCol = segment.metrics[query.metricIndex];
        int[][] groupCols = new int[numGroupDims][];
        for (int g = 0; g < numGroupDims; g++) groupCols[g] = segment.dims[query.groupByDims.get(g)];

        rowLoop:
        for (int row = 0; row < n; row++) {
            for (int f = 0; f < filterDims.length; f++) {
                if (segment.dims[filterDims[f]][row] != filterValues[f]) continue rowLoop;
            }
            List<Integer> key = new ArrayList<>(numGroupDims);
            for (int g = 0; g < numGroupDims; g++) key.add(groupCols[g][row]);
            out.merge(key, metricCol[row], Long::sum);
        }
        return out;
    }
}
