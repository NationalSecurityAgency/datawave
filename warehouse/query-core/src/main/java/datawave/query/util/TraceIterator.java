package datawave.query.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

/**
 * TODO: The old htrace-based tracing code has been removed from here, as the htrace project is dead and no longer used by accumulo. Leaving this class in place
 * to make it easier to evolve the code later, for OTEL-based tracing.
 */
public abstract class TraceIterator<F,T> implements Iterator<T> {
    
    protected Iterator<F> source;
    protected String description;
    
    public TraceIterator(Iterator<F> source, String description) {
        checkNotNull(source);
        checkNotNull(description);
        
        this.source = source;
        this.description = description;
    }
    
    public abstract T tracedTransform(F from);
    
    public T transform(F from) {
        return tracedTransform(from);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return this.source.hasNext();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public T next() {
        // Probably don't need to trace these individually..
        F next = this.source.next();
        return transform(next);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        this.source.remove();
    }
}
