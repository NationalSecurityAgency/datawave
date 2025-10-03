package datawave.query.tables;

import java.util.Iterator;

public class ConsumerIterator<T> implements Iterator<T> {
    private final ProducerConsumerBuffer<T> queue;
    private T next;

    public ConsumerIterator(ProducerConsumerBuffer<T> queue) {
        this.queue = queue;
    }

    /**
     * Fetches the next item for the iterator to return if not already fetched, blocking until available or closed. When fetching from the queue, the queue will
     * be impacted by this call
     *
     * @return true if there is another item available from the queue, false otherwise
     */
    @Override
    public boolean hasNext() {
        if (next == null) {
            try {
                next = queue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return next != null;
    }

    /**
     *
     * @return the next value fetch from hasNext
     */
    @Override
    public T next() {
        T toReturn = next;
        next = null;
        return toReturn;
    }
}
