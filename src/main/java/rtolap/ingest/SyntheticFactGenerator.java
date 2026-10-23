package rtolap.ingest;

import java.util.Random;

/** Generates rows for an 8-dimension fact table with modest per-dimension cardinality, which is
 * what keeps star-tree construction tractable: real analytics dimensions (country, device type,
 * campaign) have this shape far more often than high-cardinality ones (user id), which is exactly
 * why star-tree indexing targets them. */
public final class SyntheticFactGenerator {
    public static final String[] DIMENSION_NAMES = {
            "country", "device", "browser", "os", "region", "campaign", "channel", "plan"
    };
    public static final int[] CARDINALITY = {8, 5, 6, 4, 10, 12, 7, 5};
    public static final String[] METRIC_NAMES = {"clicks", "revenue_cents"};

    private final Random random;

    public SyntheticFactGenerator(long seed) {
        this.random = new Random(seed);
    }

    public int[] nextDims() {
        int[] dims = new int[DIMENSION_NAMES.length];
        for (int d = 0; d < dims.length; d++) dims[d] = random.nextInt(CARDINALITY[d]);
        return dims;
    }

    public long[] nextMetrics() {
        return new long[]{1 + random.nextInt(5), 10 + random.nextInt(500)};
    }
}
