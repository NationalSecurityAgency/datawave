package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Queue;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;

import com.google.common.collect.Queues;

import datawave.query.attributes.Document;

/**
 * Allows an arbitrary nesting of nested iterators
 */
public class NestedQueryIterator<T> implements NestedIterator<T> {

    private static final Logger log = Logger.getLogger(NestedQueryIterator.class);
    protected Queue<NestedQuery<T>> nests;
    protected NestedQuery<T> currentQuery = null;
    protected NestedIterator<T> currentNest = null;

    protected NestedQueryIterator() {

    }

    public NestedQueryIterator(NestedQuery<T> start) {
        nests = Queues.newArrayDeque();
        addNestedIterator(start);
    }

    public NestedQueryIterator(Collection<NestedQuery<T>> newNests) {
        nests = Queues.newArrayDeque();
        for (NestedQuery<T> nest : newNests) {
            addNestedIterator(nest);
        }

    }

    public void addNestedIterator(NestedQuery<T> iter) {
        nests.add(iter);
    }

    public void setCurrentQuery(NestedQuery<T> query) {
        currentQuery = query;
        currentNest = currentQuery.iter;
    }

    @Override
    public boolean hasNext() {
        // if we have no more in the current nest, we peek to the next one
        if (currentNest == null || !currentNest.hasNext()) {
            if (nests.peek() != null) {
                popNextNest();
                if (null == currentNest) {
                    if (nests.peek() == null)
                        return false;
                    else
                        hasNext();
                }
                if (!currentNest.hasNext())
                    if (nests.peek() != null)
                        return hasNext();
                    else
                        return false;
                else
                    return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    @Override
    public T next() {
        return currentNest.next();
    }

    @Override
    public void remove() {
        currentNest.remove();
    }

    @Override
    public void initialize() {
        if (null == currentNest) {
            popNextNest();
        } else {
            currentNest.initialize();
        }
    }

    @Override
    public T move(T minimum) {
        return currentNest.move(minimum);
    }

    /**
     * Seeks the current nest using the provided range. Note: if the range is beyond the current nest it is up to the caller to advance to the next nest via a
     * call to {@link #hasNext()}
     *
     * @param range
     *            the seek range
     * @param columnFamilies
     *            the column families
     * @param inclusive
     *            true if range is inclusive
     * @throws IOException
     *             if the underlying source has a problem
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        currentNest.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Collection<NestedIterator<T>> leaves() {
        return currentNest.leaves();
    }

    @Override
    public Collection<NestedIterator<T>> children() {
        return currentNest.children();
    }

    @Override
    public Document document() {
        return currentNest.document();
    }

    public NestedQuery<T> getNestedQuery() {
        return currentQuery;
    }

    protected void popNextNest() {
        if (nests.peek() != null) {
            if (log.isTraceEnabled()) {
                log.trace("Peekingshows we have a query");
            }

            currentQuery = nests.poll();
            currentNest = currentQuery.getIter();
            currentNest.initialize();
        }
    }

    public Range getRange() {
        return currentQuery.getRange();
    }

    public String getQuery() {
        if (currentQuery == null)
            return null;
        else
            return currentQuery.getQuery();
    }

    @Override
    public boolean isContextRequired() {
        return false;
    }

    @Override
    public void setContext(T context) {
        // no-op
    }

    @Override
    public boolean isNonEventField() {
        return false;
    }
}
