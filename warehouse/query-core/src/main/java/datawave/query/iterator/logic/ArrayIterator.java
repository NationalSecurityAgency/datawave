package datawave.query.iterator.logic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;

/**
 * A leaf node in a nested iterator tree. This is supposed to be a sample iterator that returns data from a sorted array.
 * <p>
 * This class is deprecated. A suitable replacement is an {@link IndexIteratorBridge} using a {@link SortedMapIterator}.
 *
 * @param <T>
 *            the type of the array iterator
 */
@Deprecated(since = "7.13.0")
public class ArrayIterator<T extends Comparable<T>> implements NestedIterator<T> {
    private static final Document doc = new Document();

    private T[] values;
    private int offset;

    public ArrayIterator(T... ts) {
        values = ts;
        Arrays.sort(values);
        offset = -1;
    }

    public boolean hasNext() {
        return ++offset < values.length;
    }

    public T next() {
        return values[offset];
    }

    public void remove() {}

    public T move(T minimum) {
        if (offset == -1) {
            offset = 0;
        }
        if (values != null && values.length > offset && values[offset].compareTo(minimum) < 0) {
            while (offset < values.length && values[offset].compareTo(minimum) < 0) {
                ++offset;
            }
            if (offset == values.length) {
                return null;
            } else {
                return values[offset];
            }
        } else if (values != null && offset < values.length) {
            return values[offset];
        } else {
            return null;
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // no-op
    }

    public Collection<NestedIterator<T>> leaves() {
        Collection<NestedIterator<T>> c = new LinkedList<>();
        c.add(this);
        return c;
    }

    public Collection<NestedIterator<T>> children() {
        return Collections.emptyList();
    }

    public void initialize() {}

    public Document document() {
        return doc;
    }

    @Override
    public boolean isContextRequired() {
        return false;
    }

    @Override
    public void setContext(T context) {
        // no-op
    }

    @Override
    public boolean isNonEventField() {
        return false;
    }

}
