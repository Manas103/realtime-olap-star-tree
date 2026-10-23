package rtolap.query;

import java.util.List;
import java.util.Map;

/** SELECT dim..., SUM(metric) FROM fact WHERE filters GROUP BY groupByDims. */
public final class GroupByQuery {
    public final List<Integer> groupByDims; // dimension indices, in the ORIGINAL (unsorted) numbering
    public final Map<Integer, Integer> filters; // dimension index -> required value
    public final int metricIndex;

    public GroupByQuery(List<Integer> groupByDims, Map<Integer, Integer> filters, int metricIndex) {
        this.groupByDims = groupByDims;
        this.filters = filters;
        this.metricIndex = metricIndex;
    }
}
