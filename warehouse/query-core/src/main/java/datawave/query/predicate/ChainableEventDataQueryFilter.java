package datawave.query.predicate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

/**
 * Wrap multiple EventDataQueryFilter objects and in order process them
 */
public class ChainableEventDataQueryFilter implements EventDataQueryFilter {
    private List<EventDataQueryFilter> filters;

    public ChainableEventDataQueryFilter() {
        filters = new ArrayList<>();
    }

    private ChainableEventDataQueryFilter(ChainableEventDataQueryFilter other) {
        // create a copy of all the filters
        this.filters = new ArrayList<>(other.filters);
    }

    public void addFilter(EventDataQueryFilter filter) {
        filters.add(filter);
    }

    /**
     * Call startNewDocument on all filters in order
     *
     * @param documentKey
     *            a document key
     */
    @Override
    public void startNewDocument(Key documentKey) {
        for (int i = 0; i < filters.size(); i++) {
            filters.get(i).startNewDocument(documentKey);
        }
    }

    /**
     * Call apply on filters in FIFO order, until one is false otherwise return true
     *
     * @param entry
     *            the entry key
     * @return true until apply on filters is false
     */
    @Override
    public boolean apply(@Nullable Map.Entry<Key,String> entry) {
        return apply(entry, true);
    }

    @Override
    public boolean peek(@Nullable Map.Entry<Key,String> entry) {
        return apply(entry, false);
    }

    private boolean apply(Map.Entry<Key,String> entry, boolean update) {
        Iterator<EventDataQueryFilter> iterator = filters.iterator();
        boolean result = true;

        while (iterator.hasNext() && result) {
            if (update) {
                result = iterator.next().apply(entry);
            } else {
                result = iterator.next().peek(entry);
            }
        }

        return result;
    }

    /**
     * Call keep on filters in FIFO order, until one is false, otherwise return true
     *
     * @param k
     *            the key
     * @return until one is false, otherwise return true
     */
    @Override
    public boolean keep(Key k) {
        Iterator<EventDataQueryFilter> iterator = filters.iterator();
        boolean result = true;

        while (iterator.hasNext() && result) {
            result = iterator.next().keep(k);
        }

        return result;
    }

    private Range updateRange(Range current, Range candidate) {
        Range result = current;

        // test the start key
        if (result == null || result.getStartKey() == null || result.getStartKey().compareTo(candidate.getStartKey()) <= 0) {
            result = new Range(candidate.getStartKey(), candidate.isStartKeyInclusive() && result.isStartKeyInclusive(), result.getEndKey(),
                            result.isEndKeyInclusive());
        }

        // test the end key
        if (result == null || result.getEndKey() == null || result.getEndKey().compareTo(candidate.getEndKey()) >= 0) {
            result = new Range(result.getStartKey(), result.isStartKeyInclusive(), candidate.getEndKey(),
                            result.isEndKeyInclusive() && candidate.isEndKeyInclusive());
        }

        return result;
    }

    @Override
    public EventDataQueryFilter clone() {
        return new ChainableEventDataQueryFilter(this);
    }

    /**
     * Get the minimum seek range across all filters
     *
     * @param current
     *            the current key at the top of the source iterator
     * @param endKey
     *            the current range endKey
     * @param endKeyInclusive
     *            the endKeyInclusive flag from the current range
     * @return minimum seek range across all filters
     */
    @Override
    public Range getSeekRange(Key current, Key endKey, boolean endKeyInclusive) {
        Iterator<EventDataQueryFilter> iterator = filters.iterator();
        Range result = new Range();

        while (iterator.hasNext()) {
            Range candidate = iterator.next().getSeekRange(current, endKey, endKeyInclusive);
            result = updateRange(result, candidate);
        }

        return result;
    }

    /**
     * Get the minimum maxNextCount across all filters or -1 if no maxNextCount is set
     *
     * @return minimum maxNextCount across all filters
     */
    @Override
    public int getMaxNextCount() {
        int result = -1;

        Iterator<EventDataQueryFilter> iterator = filters.iterator();

        while (iterator.hasNext()) {
            int candidate = iterator.next().getMaxNextCount();

            if (candidate > -1 && (result == -1 || candidate < result)) {
                result = candidate;
            }
        }

        return result;
    }

    /**
     * Attempt to transform by each filter that is not keeping the key and return the first successful transformation
     *
     * @param toTransform
     *            the Key to transform
     * @return first successful transformation
     */
    @Override
    public Key transform(Key toTransform) {
        Key result = null;
        Iterator<EventDataQueryFilter> iterator = filters.iterator();

        while (iterator.hasNext() && result == null) {
            EventDataQueryFilter filter = iterator.next();
            if (!filter.keep(toTransform)) {
                result = filter.transform(toTransform);
            }
        }

        return result;
    }
}
