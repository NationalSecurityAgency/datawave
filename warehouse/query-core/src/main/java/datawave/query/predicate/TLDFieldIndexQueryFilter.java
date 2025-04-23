package datawave.query.predicate;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import com.google.common.collect.Sets;

import datawave.core.iterators.key.util.FiKeyUtil;

/**
 * An {@link EventDataQueryFilter} that applies TLD logic to Field Index keys
 * <p>
 * Keeps any FI key that is index only and part of the TLD or is not part of the TLD
 */
public class TLDFieldIndexQueryFilter implements EventDataQueryFilter {

    private final Set<String> indexOnlyFields;

    /**
     * Default constructor
     *
     * @param indexOnlyFields
     *            a set of index only fields
     */
    public TLDFieldIndexQueryFilter(Set<String> indexOnlyFields) {
        this.indexOnlyFields = indexOnlyFields;
    }

    /**
     * This method is a no-op
     *
     * @param documentKey
     *            a doc key
     */
    @Override
    public void startNewDocument(Key documentKey) {
        // no-op
    }

    /**
     * Always returns true.
     *
     * @param entry
     *            an entry of type Key-Value
     * @return true, always
     */
    @Override
    public boolean apply(@Nullable Map.Entry<Key,String> entry) {
        return true;
    }

    /**
     * Always returns true
     *
     * @param entry
     *            an entry of type Key-Value
     * @return true, always
     */
    @Override
    public boolean peek(@Nullable Map.Entry<Key,String> entry) {
        return true;
    }

    /**
     * Keep any FI that is index only and part of the TLD or is not part of the TLD
     *
     * @param k
     *            a field index key
     * @return true if the key should be kept for evaluation
     */
    @Override
    public boolean keep(Key k) {
        boolean root = TLDEventDataFilter.isRootPointer(k);
        if (root) {
            return indexOnlyFields.contains(FiKeyUtil.getFieldString(k));
        }
        return true;
    }

    /**
     * Clones this query filter
     *
     * @return a new instance of this query filter
     */
    @Override
    public EventDataQueryFilter clone() {
        return new TLDFieldIndexQueryFilter(Sets.newHashSet(indexOnlyFields));
    }

    /**
     * Not supported.
     *
     * @param current
     *            the current key at the top of the source iterator
     * @param endKey
     *            the current range endKey
     * @param endKeyInclusive
     *            the endKeyInclusive flag from the current range
     * @return an exception
     */
    @Override
    public Range getSeekRange(Key current, Key endKey, boolean endKeyInclusive) {
        throw new UnsupportedOperationException();
    }

    /**
     * Effectively a no-op
     *
     * @return negative one, thus disabling seeking via a maximum next count
     */
    @Override
    public int getMaxNextCount() {
        return -1;
    }

    /**
     * A no-op.
     * <p>
     * See {@link TLDEventDataFilter#transform(Key)} for an actual implementation.
     *
     * @param toTransform
     *            the Key to transform
     * @return null
     */
    @Override
    public Key transform(Key toTransform) {
        return null;
    }
}
