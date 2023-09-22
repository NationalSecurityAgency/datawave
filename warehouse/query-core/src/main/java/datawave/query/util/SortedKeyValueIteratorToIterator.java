package datawave.query.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * This is an Iterator that simply wraps an underlying {@code SortedKeyValueIterator<Key, Value>}.
 *
 *
 *
 */
public class SortedKeyValueIteratorToIterator implements Iterator<Map.Entry<Key,Value>> {
    private SortedKeyValueIterator<Key,Value> iterator = null;
    private Map.Entry<Key,Value> next = null;
    private boolean loaded = false;

    /**
     * Create an Iterator given a SortedKeyValueIterator. It is presumed that the SortedKeyValueIterator has been initialized.
     *
     * @param iterator
     *            a SortedKeyValueIterator
     */
    public SortedKeyValueIteratorToIterator(SortedKeyValueIterator<Key,Value> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        load();
        return next != null;
    }

    @Override
    public Map.Entry<Key,Value> next() {
        load();
        if (next == null) {
            QueryException qe = new QueryException(DatawaveErrorCode.FETCH_NEXT_ELEMENT_ERROR);
            throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
        }
        loaded = false;
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void load() {
        if (!loaded) {
            this.next = findNext();
            loaded = true;
        }
    }

    private Map.Entry<Key,Value> findNext() {
        Map.Entry<Key,Value> next = null;
        try {
            if (iterator.hasTop()) {
                next = new SortedKeyValueEntry(iterator.getTopKey(), iterator.getTopValue());
                if (next.getKey() == null && next.getValue() == null) {
                    next = null;
                }
                iterator.next();
            }

        } catch (IOException e) {
            throw new IteratorException(e);
        }
        return next;
    }

    public static class IteratorException extends RuntimeException implements Serializable {

        public IteratorException() {
            super();
        }

        public IteratorException(String message, Throwable cause) {
            super(message, cause);
        }

        public IteratorException(String message) {
            super(message);
        }

        public IteratorException(Throwable cause) {
            super(cause);
        }

    }

    public static class SortedKeyValueEntry implements Map.Entry<Key,Value> {
        private Key key = null;
        private Value value = null;

        public SortedKeyValueEntry(Key key, Value value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Key getKey() {
            return key;
        }

        @Override
        public Value getValue() {
            return value;
        }

        @Override
        public Value setValue(Value value) {
            Value org = this.value;
            this.value = value;
            return org;
        }

    }
}
