package datawave.query.predicate;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

/**
 * Filters that implement this interface may suggest a new seek range
 */
public interface SeekingFilter {
    /**
     * Get the next seek range suggested by the filter
     *
     * @param current
     *            the current key at the top of the source iterator
     * @param endKey
     *            the current range endKey
     * @param endKeyInclusive
     *            the endKeyInclusive flag from the current range
     * @return the new seek range, or null if the filter does not suggest a seek
     */
    Range getSeekRange(Key current, Key endKey, boolean endKeyInclusive);

    /**
     *
     * @return the max next() calls before a seek() is triggered
     */
    int getMaxNextCount();
}
