package rtolap.startree;

import rtolap.storage.ColumnarSegment;

import java.io.*;
import java.util.*;

/**
 * A LinkedIn Pinot-style star-tree index over one columnar segment. Built once per sealed
 * segment, at a fixed dimension split order, with a leaf threshold on row count. Every internal
 * node has one child per distinct value of its split dimension plus one STAR (wildcard) child
 * whose rows have that dimension aggregated away; a query that does not need a dimension broken
 * out (it is neither filtered nor grouped on) can take the STAR child and touch far fewer rows.
 *
 * This is the index's honest cost: the STAR path materializes new, smaller aggregated rows at
 * every level it takes, which is extra bytes on disk beyond the raw segment (see 1.4x storage
 * in docs/benchmark_output.txt). Grouping by every dimension at once defeats the index by
 * construction, since nothing can be aggregated away; the benchmark groups by a realistic subset.
 */
public final class StarTree implements Serializable {
    public final int[] splitOrder; // dimension indices in split order, e.g. {0,1,2,...}
    public final int leafThreshold;
    public final String[] metricNames;
    public final Node root;

    public static final int STAR = -1;

    public StarTree(int[] splitOrder, int leafThreshold, String[] metricNames, Node root) {
        this.splitOrder = splitOrder;
        this.leafThreshold = leafThreshold;
        this.metricNames = metricNames;
        this.root = root;
    }

    public static final class Node implements Serializable {
        public int splitDim = -1; // index into splitOrder array (depth), -1 if this is a leaf
        public Map<Integer, Node> children; // key: dimension value, or STAR
        public Leaf leaf; // non-null iff this is a leaf
    }

    /** A leaf's rows are the sub-table for the dimensions not yet fixed by the path to this node. */
    public static final class Leaf implements Serializable {
        public int[] remainingSplitOrderIndices; // which positions of splitOrder these columns correspond to
        public int[][] dimValues; // [remaining dim position][row]; STAR-aggregated rows carry no value here, column omitted entirely
        public long[][] metricValues; // [metric][row]
        public int rowCount;
    }

    public static StarTree build(ColumnarSegment segment, int[] splitOrder, int leafThreshold, String[] metricNames) {
        int numRows = segment.numRows;
        int width = splitOrder.length;
        int[][] subDims = new int[width][];
        for (int k = 0; k < width; k++) subDims[k] = segment.dims[splitOrder[k]];
        long[][] subMetrics = new long[metricNames.length][];
        for (int m = 0; m < metricNames.length; m++) subMetrics[m] = segment.metrics[m];
        int[] allRows = identity(numRows);
        // globalBase=0, localDepth=0: column k of subDims is splitOrder position k.
        Node root = buildNode(subDims, subMetrics, allRows, 0, 0, leafThreshold);
        return new StarTree(splitOrder, leafThreshold, metricNames, root);
    }

    private static int[] identity(int n) {
        int[] r = new int[n];
        for (int i = 0; i < n; i++) r[i] = i;
        return r;
    }

    /**
     * localDepth indexes into subDims (which columns are left to consider in THIS array);
     * globalBase + localDepth is the absolute position in the original splitOrder, which is what
     * gets stored in a node and what a query needs to map back to a real dimension. The two drift
     * apart exactly once a STAR child is created, because its subDims array is narrower and starts
     * a fresh localDepth of 0 while globalBase jumps forward by however many columns were consumed.
     */
    private static Node buildNode(int[][] subDims, long[][] subMetrics, int[] rowIdx, int localDepth, int globalBase, int leafThreshold) {
        Node node = new Node();
        int remainingWidth = subDims.length - localDepth;
        if (remainingWidth == 0 || rowIdx.length <= leafThreshold) {
            node.leaf = makeLeaf(subDims, subMetrics, rowIdx, localDepth, globalBase);
            return node;
        }
        node.splitDim = globalBase + localDepth;
        node.children = new HashMap<>();

        // Partition by the value of this dimension.
        Map<Integer, List<Integer>> byValue = new HashMap<>();
        int[] col = subDims[localDepth];
        for (int idx : rowIdx) {
            byValue.computeIfAbsent(col[idx], k -> new ArrayList<>()).add(idx);
        }
        for (Map.Entry<Integer, List<Integer>> e : byValue.entrySet()) {
            int[] childRows = toArray(e.getValue());
            node.children.put(e.getKey(), buildNode(subDims, subMetrics, childRows, localDepth + 1, globalBase, leafThreshold));
        }

        // STAR child: aggregate away this dimension, grouping by the remaining dimensions' values.
        Map<String, long[]> aggregated = new HashMap<>();
        Map<String, int[]> aggregatedKeyValues = new HashMap<>();
        for (int idx : rowIdx) {
            StringBuilder keyB = new StringBuilder();
            int[] keyVals = new int[subDims.length - localDepth - 1];
            for (int k = localDepth + 1; k < subDims.length; k++) {
                keyVals[k - localDepth - 1] = subDims[k][idx];
                keyB.append(subDims[k][idx]).append(',');
            }
            String key = keyB.toString();
            long[] sums = aggregated.get(key);
            if (sums == null) {
                sums = new long[subMetrics.length];
                aggregated.put(key, sums);
                aggregatedKeyValues.put(key, keyVals);
            }
            for (int m = 0; m < subMetrics.length; m++) sums[m] += subMetrics[m][idx];
        }
        int starRowCount = aggregated.size();
        int starWidth = subDims.length - localDepth - 1;
        int[][] starDims = new int[starWidth][starRowCount];
        long[][] starMetrics = new long[subMetrics.length][starRowCount];
        int r = 0;
        for (Map.Entry<String, long[]> e : aggregated.entrySet()) {
            int[] keyVals = aggregatedKeyValues.get(e.getKey());
            for (int k = 0; k < starWidth; k++) starDims[k][r] = keyVals[k];
            long[] sums = e.getValue();
            for (int m = 0; m < sums.length; m++) starMetrics[m][r] = sums[m];
            r++;
        }
        int[] starRowIdx = identity(starRowCount);
        node.children.put(STAR, buildNode(starDims, starMetrics, starRowIdx, 0, globalBase + localDepth + 1, leafThreshold));
        return node;
    }

    private static Leaf makeLeaf(int[][] subDims, long[][] subMetrics, int[] rowIdx, int localDepth, int globalBase) {
        Leaf leaf = new Leaf();
        int width = subDims.length - localDepth;
        leaf.rowCount = rowIdx.length;
        leaf.dimValues = new int[width][leaf.rowCount];
        leaf.metricValues = new long[subMetrics.length][leaf.rowCount];
        for (int i = 0; i < rowIdx.length; i++) {
            int idx = rowIdx[i];
            for (int k = 0; k < width; k++) leaf.dimValues[k][i] = subDims[localDepth + k][idx];
            for (int m = 0; m < subMetrics.length; m++) leaf.metricValues[m][i] = subMetrics[m][idx];
        }
        leaf.remainingSplitOrderIndices = new int[width];
        for (int k = 0; k < width; k++) leaf.remainingSplitOrderIndices[k] = globalBase + localDepth + k;
        return leaf;
    }

    private static int[] toArray(List<Integer> list) {
        int[] arr = new int[list.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = list.get(i);
        return arr;
    }

    public void writeTo(java.nio.file.Path path) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
            out.writeObject(this);
        }
    }

    public static StarTree readFrom(java.nio.file.Path path) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(path.toFile())))) {
            return (StarTree) in.readObject();
        }
    }
}
