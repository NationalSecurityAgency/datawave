package datawave.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The UniversalSet contains all objects, including itself. It can be used as a 'whitelist' that is completely permissive. Note that isEmpty() must return true
 * to preclude attempts to Iterate over the infinite and imaginary members. UniversalSet is Unmodifiable
 * 
 * @param <T>
 */
@SuppressWarnings("rawtypes")
public class UniversalSet<T> implements Set<T> {
    private static final long serialVersionUID = 1L;
    
    private static UniversalSet inst;
    
    static {
        inst = new UniversalSet();
    }
    
    @SuppressWarnings("unchecked")
    public static <T> UniversalSet<T> instance() {
        return inst;
    }
    
    private UniversalSet() {}
    
    /**
     * The UniversalSet contains all possible objects
     * 
     * @param o
     * @return
     */
    @Override
    public boolean contains(Object o) {
        return true;
    }
    
    @Override
    public Iterator<T> iterator() {
        return Collections.emptyIterator();
    }
    
    @Override
    public Object[] toArray() {
        return new Object[0];
    }
    
    @Override
    public <T1> T1[] toArray(T1[] a) {
        return (T1[]) toArray();
    }
    
    @Override
    public boolean add(T e) {
        return true;
    }
    
    @Override
    public int size() {
        return 0;
    }
    
    /**
     * Must return true to prevent attempts to iterate over the 'infinite but non-existent' members
     * 
     * @return
     */
    @Override
    public boolean isEmpty() {
        return true;
    }
    
    @Override
    public boolean remove(Object o) {
        return false;
    }
    
    @Override
    public boolean equals(Object o) {
        return o instanceof UniversalSet;
    }
    
    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }
    
    @Override
    public void clear() {
        
    }
    
    @Override
    public boolean containsAll(Collection<?> c) {
        return true;
    }
    
    @Override
    public boolean addAll(Collection<? extends T> c) {
        return true;
    }
    
    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }
    
    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        return false;
    }
}
