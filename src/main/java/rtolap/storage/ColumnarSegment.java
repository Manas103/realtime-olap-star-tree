package rtolap.storage;

import java.io.*;
import java.nio.file.Path;

/**
 * One immutable, columnar batch of rows: one int[] per dimension (synthetic data is generated as
 * small-integer codes directly, so there is no separate string dictionary layer here; a production
 * system's dictionary sits underneath this same columnar layout and does not change how the
 * star-tree is built or queried) and one long[] per metric. Persisted with Java serialization
 * rather than a hand-rolled binary format, a deliberate simplification documented in the README.
 */
public final class ColumnarSegment implements Serializable {
    public final int numDimensions;
    public final int numRows;
    public final int[][] dims; // [dimension][row]
    public final long[][] metrics; // [metric][row]
    public final String[] dimensionNames;
    public final String[] metricNames;

    public ColumnarSegment(int[][] dims, long[][] metrics, String[] dimensionNames, String[] metricNames) {
        this.dims = dims;
        this.metrics = metrics;
        this.numDimensions = dims.length;
        this.numRows = dims.length == 0 ? 0 : dims[0].length;
        this.dimensionNames = dimensionNames;
        this.metricNames = metricNames;
    }

    public void writeTo(Path path) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
            out.writeObject(this);
        }
    }

    public static ColumnarSegment readFrom(Path path) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(path.toFile())))) {
            return (ColumnarSegment) in.readObject();
        }
    }
}
