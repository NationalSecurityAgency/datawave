package datawave.query.rewrite.iterator;

import java.util.Iterator;

/**
 * A simple iterable that wraps a nested iterator.
 * 
 * @param <T>
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
