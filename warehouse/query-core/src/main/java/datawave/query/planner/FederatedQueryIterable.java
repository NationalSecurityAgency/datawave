package datawave.query.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import datawave.query.CloseableIterable;
import datawave.webservice.query.configuration.QueryData;

/**
 * Implementation of {@link CloseableIterable} intended to be used by {@link FederatedQueryPlanner}. This iterable
 */
public class FederatedQueryIterable implements CloseableIterable<QueryData> {

    private final List<CloseableIterable<QueryData>> iterables = new ArrayList<>();

    /**
     * Add an iterable to this {@link FederatedQueryIterable}.
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
     * Closes and clears each iterable in this {@link FederatedQueryIterable}.
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
     * Returns an iterator that will iterate over the {@link QueryData} returned by each iterable in this {@link FederatedQueryIterable}.
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

        // The current QueryData. Seek to the first available query data.
        private QueryData current;

        @Override
        public boolean hasNext() {
            current = seekToNext();
            return current != null;
        }

        @Override
        public QueryData next() {
            // If current is not null, we should return it as the next query data. However, first current must be updated with the next possible
            if (current != null) {
                return current;
            } else {
                throw new NoSuchElementException();
            }
        }

        /**
         * Return the next available {@link QueryData}, or null if none remain.
         *
         * @return the next {@link QueryData}
         */
        private QueryData seekToNext() {
            // Iterate through the remaining iterables until we find one with a result.
            while (iterableIterator.hasNext()) {
                // Check if the iterator for this iterable has a result.
                Iterator<QueryData> iterator = iterableIterator.next().iterator();
                if (iterator.hasNext()) {
                    // If so, return it.
                    return iterator.next();
                }
            }
            // There are no more iterables with results.
            return null;
        }
    }
}
