package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;

/**
 * Given arbitrary "Document Key", provides the range that covers the document.
 */
public class DocumentRangeProvider implements RangeProvider {

    /**
     * Get the start key for the document range
     *
     * @param k
     *            an initial key
     * @return the start key
     */
    @Override
    public Key getStartKey(Key k) {
        return new Key(k.getRow(), k.getColumnFamily());
    }

    /**
     * Get the stop key for the document range by appending a null byte
     *
     * @param k
     *            an initial key
     * @return the stop key
     */
    @Override
    public Key getStopKey(Key k) {
        return k.followingKey(PartialKey.ROW_COLFAM);
    }

    /**
     * Get the range for the document
     *
     * @param k
     *            an initial key
     * @return the range
     */
    @Override
    public Range getRange(Key k) {
        return new Range(getStartKey(k), true, getStopKey(k), false);
    }
}
