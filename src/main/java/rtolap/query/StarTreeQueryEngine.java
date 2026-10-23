package rtolap.query;

import rtolap.startree.StarTree;

import java.util.*;

/**
 * Walks a StarTree: a dimension that is neither filtered nor grouped on takes the cheap STAR
 * child (the dimension is already aggregated away there); a filtered dimension follows only the
 * matching specific child; a grouped-but-unfiltered dimension must enumerate every specific child,
 * since the index has to break the value back out. Reaching a leaf before every dimension is
 * resolved (the common case, since the leaf threshold usually fires first) falls back to a scan
 * of just that leaf's rows for whatever is left.
 */
public final class StarTreeQueryEngine {

    public static Map<List<Integer>, Long> execute(StarTree tree, GroupByQuery query) {
        Map<List<Integer>, Long> out = new HashMap<>();
        Map<Integer, Integer> fixedByOriginalDim = new HashMap<>();
        walk(tree, tree.root, 0, fixedByOriginalDim, query, out);
        return out;
    }

    private static void walk(StarTree tree, StarTree.Node node, int depth, Map<Integer, Integer> fixed,
                              GroupByQuery query, Map<List<Integer>, Long> out) {
        if (node.leaf != null) {
            scanLeaf(tree, node.leaf, fixed, query, out);
            return;
        }
        int originalDim = tree.splitOrder[node.splitDim];
        if (query.filters.containsKey(originalDim)) {
            int required = query.filters.get(originalDim);
            StarTree.Node child = node.children.get(required);
            if (child == null) return; // no rows with that value
            Map<Integer, Integer> next = withFixed(fixed, originalDim, required);
            walk(tree, child, depth + 1, next, query, out);
        } else if (query.groupByDims.contains(originalDim)) {
            for (Map.Entry<Integer, StarTree.Node> e : node.children.entrySet()) {
                if (e.getKey() == StarTree.STAR) continue; // must break the value out, star hides it
                Map<Integer, Integer> next = withFixed(fixed, originalDim, e.getKey());
                walk(tree, e.getValue(), depth + 1, next, query, out);
            }
        } else {
            StarTree.Node star = node.children.get(StarTree.STAR);
            if (star != null) walk(tree, star, depth + 1, fixed, query, out);
        }
    }

    private static Map<Integer, Integer> withFixed(Map<Integer, Integer> fixed, int dim, int value) {
        Map<Integer, Integer> next = new HashMap<>(fixed);
        next.put(dim, value);
        return next;
    }

    private static void scanLeaf(StarTree tree, StarTree.Leaf leaf, Map<Integer, Integer> fixed,
                                  GroupByQuery query, Map<List<Integer>, Long> out) {
        int width = leaf.remainingSplitOrderIndices.length;
        int[] originalDimForColumn = new int[width];
        for (int k = 0; k < width; k++) originalDimForColumn[k] = tree.splitOrder[leaf.remainingSplitOrderIndices[k]];

        int[] remainingFilterCols = colsFor(query.filters.keySet(), originalDimForColumn);
        int[] remainingFilterValues = new int[remainingFilterCols.length];
        for (int i = 0; i < remainingFilterCols.length; i++) {
            int originalDim = originalDimForColumn[remainingFilterCols[i]];
            remainingFilterValues[i] = query.filters.get(originalDim);
        }
        int[] remainingGroupCols = colsFor(new HashSet<>(query.groupByDims), originalDimForColumn);

        long[] metricCol = leaf.metricValues[query.metricIndex];
        rowLoop:
        for (int row = 0; row < leaf.rowCount; row++) {
            for (int i = 0; i < remainingFilterCols.length; i++) {
                if (leaf.dimValues[remainingFilterCols[i]][row] != remainingFilterValues[i]) continue rowLoop;
            }
            List<Integer> key = new ArrayList<>(query.groupByDims.size());
            for (int originalDim : query.groupByDims) {
                Integer fixedVal = fixed.get(originalDim);
                if (fixedVal != null) {
                    key.add(fixedVal);
                } else {
                    int col = indexOf(originalDimForColumn, originalDim);
                    key.add(leaf.dimValues[col][row]);
                }
            }
            out.merge(key, metricCol[row], Long::sum);
        }
    }

    private static int[] colsFor(java.util.Collection<Integer> originalDims, int[] originalDimForColumn) {
        List<Integer> cols = new ArrayList<>();
        for (int c = 0; c < originalDimForColumn.length; c++) {
            if (originalDims.contains(originalDimForColumn[c])) cols.add(c);
        }
        int[] arr = new int[cols.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = cols.get(i);
        return arr;
    }

    private static int indexOf(int[] arr, int value) {
        for (int i = 0; i < arr.length; i++) if (arr[i] == value) return i;
        throw new IllegalStateException("dimension " + value + " not found among remaining leaf columns");
    }
}
