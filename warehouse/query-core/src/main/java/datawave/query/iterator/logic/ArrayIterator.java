package datawave.query.iterator.logic;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import datawave.query.iterator.NestedIterator;
import datawave.query.attributes.Document;

/**
 * A leaf node in an nested iterator tree. This is supposed to be a sample iterator that returns data from a sorted array.
 * 
 * 
 * 
 * @param <T>
 */
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
        return offset + 1 < values.length;
    }
    
    public T next() {
        if (hasNext()) {
            return values[++offset];
        } else {
            throw new NoSuchElementException();
        }
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
        } else if (values != null && values[offset].compareTo(minimum) == 0) {
            return values[offset];
        } else if (values != null) {
            throw new IllegalStateException("iterator beyond minimum");
        } else {
            throw new NoSuchElementException();
        }
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
    public T peek() {
        if (hasNext()) {
            return values[offset + 1];
        } else {
            return null;
        }
    }
}
