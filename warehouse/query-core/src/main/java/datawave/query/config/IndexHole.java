package datawave.query.config;

/**
 * Note this class was renamed to IndexValueHole.java. Kept here for backward compatibility.
 */
@Deprecated
public class IndexHole extends IndexValueHole {

    public IndexHole() {
        super();
    }

    /**
     * Create an index with a date range and value range.
     *
     * @param dateRange
     *            range in yyyyMMdd format
     * @param valueRange
     *            the start and end values of the known hole
     */
    public IndexHole(String[] dateRange, String[] valueRange) {
        super(dateRange, valueRange);
    }
}
