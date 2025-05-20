package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import datawave.query.attributes.Document;

/**
 * A stub for the NestedIterator, functionally equivalent to {@link Collections#emptyIterator()}
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
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // no-op
    }

    @Override
    public Collection<NestedIterator<Key>> leaves() {
        return Collections.emptySet();
    }

    @Override
    public Collection<NestedIterator<Key>> children() {
        return Collections.emptySet();
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

    @Override
    public boolean isNonEventField() {
        return false;
    }
}
