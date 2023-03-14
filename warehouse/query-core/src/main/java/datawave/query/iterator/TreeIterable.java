package datawave.query.iterator;

import java.util.Iterator;

/**
 * A simple iterable that wraps a nested iterator.
 * 
 * @param <T>
 *            the type of the iterable
 */
public class TreeIterable<T extends Comparable<T>> implements Iterable<T> {
    private NestedIterator<T> iterator;
    
    public TreeIterable(NestedIterator<T> iterator) {
        this.iterator = iterator;
    }
    
    public Iterator<T> iterator() {
        iterator.initialize();
        return iterator;
    }
}
