package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import datawave.query.attributes.Document;

/**
 *
 */
public class EmptyTreeIterable implements NestedIterator<Key> {

    @Override
    public void initialize() {

    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // no-op
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
    public boolean isNonEventField() {
        return false;
    }
}
