package datawave.query.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import datawave.core.query.configuration.QueryData;
import datawave.query.CloseableIterable;

/**
 * Implementation of {@link CloseableIterable} intended to be used by {@link DatePartitionedQueryPlanner}. This iterable
 */
public class DatePartitionedQueryIterable implements CloseableIterable<QueryData> {

    private final List<CloseableIterable<QueryData>> iterables = new ArrayList<>();

    /**
     * Add an iterable to this {@link DatePartitionedQueryIterable}.
     *
     * @param iterable
     *            the iterable to add
     */
    public void addIterable(CloseableIterable<QueryData> iterable) {
        if (iterable != null) {
            iterables.add(iterable);
        }
    }

    /**
     * Closes and clears each iterable in this {@link DatePartitionedQueryIterable}.
     *
     * @throws IOException
     *             if an error occurred when closing an iterable
     */
    @Override
    public void close() throws IOException {
        for (CloseableIterable<QueryData> iterable : iterables) {
            iterable.close();
        }
        iterables.clear();
    }

    /**
     * Returns an iterator that will iterate over the {@link QueryData} returned by each iterable in this {@link DatePartitionedQueryIterable}.
     *
     * @return the iterator
     */
    @Override
    public Iterator<QueryData> iterator() {
        return new Iter();
    }

    /**
     * Iterator implementation that provides the ability to iterate over each {@link QueryData} of the iterables in {@link #iterables}.
     */
    private class Iter implements Iterator<QueryData> {

        // Iterator that traverses over the iterables.
        private final Iterator<CloseableIterable<QueryData>> iterableIterator = iterables.iterator();

        // The current sub iterator.
        private Iterator<QueryData> currentSubIterator = null;

        @Override
        public boolean hasNext() {
            seekToNextAvailableQueryData();
            return currentSubIterator != null && currentSubIterator.hasNext();
        }

        @Override
        public QueryData next() {
            return currentSubIterator.next();
        }

        /**
         * Seek to the next sub-iterator that has a {@link QueryData} remaining in it.
         */
        private void seekToNextAvailableQueryData() {
            // If the current sub iterator is null, attempt to get the next available iterator, or return early if there are no more iterators.
            if (currentSubIterator == null) {
                if (iterableIterator.hasNext()) {
                    currentSubIterator = iterableIterator.next().iterator();
                } else {
                    return;
                }
            }
            // If the current sub iterator does not have any more elements remaining, move to the next sub iterator that does have elements.
            if (!currentSubIterator.hasNext()) {
                while (iterableIterator.hasNext()) {
                    // We must ensure we only ever call iterator() once on each sub-iterator.
                    currentSubIterator = iterableIterator.next().iterator();
                    if (currentSubIterator.hasNext()) {
                        return;
                    }
                }
            }
        }
    }
}
