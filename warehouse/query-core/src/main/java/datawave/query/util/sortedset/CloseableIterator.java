package datawave.query.util.sortedset;

import java.util.Iterator;

import datawave.query.iterator.WrappingIterator;

public interface CloseableIterator<E> extends Iterator<E>, AutoCloseable {
    @Override
    void close();

    static <E> CloseableIterator<E> wrapSafely(Iterator<E> iterator) {
        return iterator instanceof CloseableIterator ? (CloseableIterator<E>) iterator : new SafeCloseableIterator<>(iterator);
    }

    class SafeCloseableIterator<E> extends WrappingIterator<E> implements CloseableIterator<E> {
        private final Iterator<E> iterator;

        SafeCloseableIterator(Iterator<E> iterator) {
            this.iterator = iterator;
            setDelegate(iterator);
        }

        @Override
        public void close() {
            if (iterator instanceof CloseableIterator<?>) {
                ((CloseableIterator<E>) iterator).close();
            }
        }
    }
}
