package rtolap.storage;

import rtolap.startree.StarTree;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Every sealed (segment, index) pair currently queryable, across however many partitions fed it. */
public final class SegmentCatalog {
    public static final class Entry {
        public final ColumnarSegment segment;
        public final StarTree tree;
        public Entry(ColumnarSegment segment, StarTree tree) { this.segment = segment; this.tree = tree; }
    }

    private final List<Entry> entries = new CopyOnWriteArrayList<>();

    public void add(ColumnarSegment segment, StarTree tree) {
        entries.add(new Entry(segment, tree));
    }

    public List<Entry> entries() {
        return entries;
    }

    public int segmentCount() {
        return entries.size();
    }

    public long totalRows() {
        long total = 0;
        for (Entry e : entries) total += e.segment.numRows;
        return total;
    }
}
