package datawave.query.iterator;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.PeekingIterator;

public class CachingIterator<T> implements PeekingIterator<T>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CachingIterator.class);

    private Iterator<T> delegate;
    private T next;

    public CachingIterator(Iterator<T> delegate) {
        this.delegate = delegate;

        if (delegate.hasNext()) {
            next = delegate.next();
        }
    }

    @Override
    public T peek() {
        return next;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public T next() {
        T toReturn = next;

        if (delegate.hasNext()) {
            next = delegate.next();
        } else {
            next = null;
        }

        return toReturn;
    }

    @Override
    public void remove() {
        delegate.remove();
    }

    @Override
    public void close() {
        if (delegate instanceof AutoCloseable) {
            try {
                ((AutoCloseable) delegate).close();
            } catch (Exception e) {
                log.debug("Failed to close delegate iterator", e);
            }
        }
        delegate = null;
        next = null;
    }
}
