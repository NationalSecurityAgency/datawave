package datawave.query.util;

import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

/**
 *
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
        try (TraceScope ignored = Trace.startSpan(description + ": transform")) {
            return tracedTransform(from);
        }
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
        try (TraceScope ignored = Trace.startSpan(description + ": next")) {
            
            // Probably don't need to trace these individually..
            F next = this.source.next();
            
            return transform(next);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        try (TraceScope ignored = Trace.startSpan(description + ": remove")) {
            this.source.remove();
        }
    }
    
}
