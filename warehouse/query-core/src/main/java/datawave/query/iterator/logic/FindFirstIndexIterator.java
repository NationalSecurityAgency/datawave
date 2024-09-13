package datawave.query.iterator.logic;

import java.util.Collection;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;

/**
 * Special case implementation of the {@link NestedIterator} interface that returns only the first element found
 * <p>
 * The short circuit logic is necessary because the call to {@link #hasNext()} may be expensive if it performs an aggregation, as in the TLD case.
 */
public class FindFirstIndexIterator implements NestedIterator<Key> {

    private final NestedIterator<Key> delegate;

    private boolean foundFirst = false;

    public FindFirstIndexIterator(NestedIterator<Key> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void initialize() {
        delegate.initialize();
    }

    @Override
    public Key move(Key minimum) {
        return delegate.move(minimum);
    }

    @Override
    public Collection<NestedIterator<Key>> leaves() {
        return delegate.leaves();
    }

    @Override
    public Collection<NestedIterator<Key>> children() {
        return delegate.children();
    }

    @Override
    public Document document() {
        return delegate.document();
    }

    @Override
    public boolean isContextRequired() {
        return delegate.isContextRequired();
    }

    @Override
    public void setContext(Key context) {
        delegate.setContext(context);
    }

    @Override
    public boolean isNonEventField() {
        return delegate.isNonEventField();
    }

    @Override
    public boolean hasNext() {
        if (!foundFirst) {
            // set the flag to short circuit, even if the underlying iterator did not have a next element
            foundFirst = true;
            return delegate.hasNext();
        }
        return false;
    }

    @Override
    public Key next() {
        return delegate.next();
    }
}
