package datawave.query.iterator;

import java.util.Iterator;

public class WrappingIterator<T> implements Iterator<T> {
    private Iterator<T> delegate;

    public void setDelegate(Iterator<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public T next() {
        return delegate.next();
    }

    @Override
    public void remove() {
        delegate.remove();
    }

}
