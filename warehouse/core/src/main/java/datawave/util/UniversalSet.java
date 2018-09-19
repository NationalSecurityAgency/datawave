package datawave.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.function.Predicate;

/**
 * The UniversalSet contains all objects, including itself. It can be used as a 'whitelist' that is completely permissive. Note that isEmpty() must return true
 * to preclude attempts to Iterate over the infinite and imaginary members. UniversalSet is Unmodifiable
 * 
 * @param <T>
 */
@SuppressWarnings("rawtypes")
public class UniversalSet<T> extends HashSet<T> {
    private static final long serialVersionUID = 1L;
    
    private static UniversalSet inst;
    
    static {
        inst = new UniversalSet();
    }
    
    @SuppressWarnings("unchecked")
    public static <T> UniversalSet<T> instance() {
        return inst;
    }
    
    private UniversalSet() {
        super(0);
    }
    
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
    public boolean add(T e) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean equals(Object o) {
        return o instanceof UniversalSet;
    }
    
    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean containsAll(Collection<?> c) {
        return true;
    }
    
    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException();
    }
}
