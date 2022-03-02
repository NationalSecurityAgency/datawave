package datawave.query.planner;

import datawave.query.CloseableIterable;
import datawave.webservice.query.configuration.QueryData;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public class FederatedQueryDataIterable implements CloseableIterable<QueryData> {
    
    private LinkedList<CloseableIterable<QueryData>> delegates = new LinkedList<>();
    
    /**
     * Add another delegate to the mix
     * 
     * @param delegate
     */
    public void addDelegate(CloseableIterable<QueryData> delegate) {
        this.delegates.add(delegate);
    }
    
    /**
     * Closes this stream and releases any system resources associated with it. If the stream is already closed then invoking this method has no effect.
     *
     * <p>
     * As noted in {@link AutoCloseable#close()}, cases where the close may fail require careful attention. It is strongly advised to relinquish the underlying
     * resources and to internally <em>mark</em> the {@code Closeable} as closed, prior to throwing the {@code IOException}.
     *
     * @throws IOException
     *             if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        for (CloseableIterable<QueryData> delegate : delegates) {
            delegate.close();
        }
        delegates.clear();
    }
    
    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<QueryData> iterator() {
        return new Iterator<QueryData>() {
            
            Iterator<QueryData> current = getNextIterator();
            
            /**
             * A method to get the next iterator that has something to offer
             * 
             * @return The next query data iterator
             */
            private Iterator<QueryData> getNextIterator() {
                Iterator<QueryData> next = null;
                synchronized (delegates) {
                    while ((next == null || !next.hasNext()) && !delegates.isEmpty()) {
                        try {
                            next = delegates.removeFirst().iterator();
                        } catch (NullPointerException npe) {
                            next = null;
                        }
                    }
                }
                if (next != null && !next.hasNext()) {
                    next = null;
                }
                return next;
            }
            
            /**
             * Returns {@code true} if the iteration has more elements. (In other words, returns {@code true} if {@link #next} would return an element rather
             * than throwing an exception.)
             *
             * @return {@code true} if the iteration has more elements
             */
            @Override
            public boolean hasNext() {
                return (current != null && current.hasNext());
            }
            
            /**
             * Returns the next element in the iteration.
             *
             * @return the next element in the iteration
             * @throws NoSuchElementException
             *             if the iteration has no more elements
             */
            @Override
            public QueryData next() throws NoSuchElementException {
                if (hasNext()) {
                    QueryData next = current.next();
                    // if we depleted this iterable, get the next one
                    if (!current.hasNext()) {
                        current = getNextIterator();
                    }
                    return next;
                } else {
                    throw new NoSuchElementException("No more QueryData to be had");
                }
            }
        };
    }
}
