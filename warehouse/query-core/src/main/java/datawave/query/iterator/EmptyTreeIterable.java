package datawave.query.iterator;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;

import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class EmptyTreeIterable implements NestedIterator<Key> {

    @Override
    public void initialize() {

    }

    @Override
    public Key move(Key minimum) {
        return null;
    }

    @Override
    public Collection<NestedIterator<Key>> leaves() {
        return Collections.EMPTY_SET;
    }

    @Override
    public Collection<NestedIterator<Key>> children() {
        return Collections.EMPTY_SET;
    }

    @Override
    public Document document() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Key next() {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public boolean isContextRequired() {
        return false;
    }

    @Override
    public void setContext(Key context) {
        // no-op
    }
}
