package datawave.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The UniversalSet contains all objects, including itself. It can be used as a 'whitelist' that is completely permissive. Note that isEmpty() returns true,
 * size() is zero, and iterator() will never haveNext().
 *
 * 
 * @param <T>
 */
@SuppressWarnings("rawtypes")
public class UniversalSet<T> implements Set<T>, Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private static final Object[] EMPTY_ARRAY = new Object[0];
    
    private static UniversalSet inst = new UniversalSet();
    
    public static <T> UniversalSet<T> instance() {
        return inst;
    }
    
    private UniversalSet() {}
    
    /**
     * The UniversalSet contains all possible objects
     * 
     * @param o
     *            any Object
     * @return true
     */
    @Override
    public boolean contains(Object o) {
        return true;
    }
    
    /**
     * @return an empty iterator
     */
    @Override
    public Iterator<T> iterator() {
        return Collections.emptyIterator();
    }
    
    /**
     *
     * @return an empty array
     */
    @Override
    public Object[] toArray() {
        return EMPTY_ARRAY;
    }
    
    /**
     *
     * @param a
     *            example array for typing
     * @param <T1>
     *            desired return type
     * @return an empty array cast to T1[]
     */
    @Override
    public <T1> T1[] toArray(T1[] a) {
        return (T1[]) EMPTY_ARRAY;
    }
    
    /**
     *
     * @param e
     *            item to add
     * @return false because this set already included every object
     */
    @Override
    public boolean add(T e) {
        return false;
    }
    
    /**
     *
     * @return zero
     */
    @Override
    public int size() {
        return 0;
    }
    
    /**
     * Must return true to prevent attempts to iterate over the 'infinite but non-existent' members
     * 
     * @return true
     */
    @Override
    public boolean isEmpty() {
        return true;
    }
    
    /**
     *
     * @param o
     *            item to remove
     * @return false because nothing can be removed
     */
    @Override
    public boolean remove(Object o) {
        return false;
    }
    
    /**
     * @param o
     *            another object
     * @return true iffi object is a UniversalSet
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof UniversalSet;
    }
    
    /**
     *
     * @param c
     *            items to remove
     * @return false because nothing can be removed
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }
    
    /**
     * noop
     */
    @Override
    public void clear() {}
    
    /**
     *
     * @param c
     *            collection to test
     * @return true
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        return true;
    }
    
    /**
     *
     * @param c
     *            collection to add
     * @return false because everything is already included
     */
    @Override
    public boolean addAll(Collection<? extends T> c) {
        return false;
    }
    
    /**
     *
     * @param c
     *            collection to retain
     * @return false because nothing can be removed
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }
    
    /**
     *
     * @param filter
     *            filter to apply to elements
     * @return false because nothing can be removed
     */
    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        return false;
    }
    
    @Override
    public String toString() {
        return "UniversalSet{}";
    }
}
