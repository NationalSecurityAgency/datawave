package datawave.query.iterator;

import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

public class CachingIterator<T> implements PeekingIterator<T> {
    private Iterator<T> delegate;
    private T next;
    
    public CachingIterator(Iterator<T> delegate) {
        this.delegate = delegate;
        
        if (delegate.hasNext()) {
            next = delegate.next();
        }
    }
    
    @Override
    public T peek() {
        return next;
    }
    
    @Override
    public boolean hasNext() {
        return next != null;
    }
    
    @Override
    public T next() {
        T toReturn = next;
        
        if (delegate.hasNext()) {
            next = delegate.next();
        } else {
            next = null;
        }
        
        return toReturn;
    }
    
    @Override
    public void remove() {
        // no-op
    }
}
