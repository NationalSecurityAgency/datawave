package datawave.query.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;

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
        Span s = null;
        try {
            s = Trace.start(description + ": transform");
            return tracedTransform(from);
        } finally {
            if (s != null) {
                s.stop();
            }
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
        Span s = null;
        try {
            s = Trace.start(description + ": next");
            
            // Probably don't need to trace these individually..
            F next = this.source.next();
            
            return transform(next);
        } finally {
            if (s != null) {
                s.stop();
            }
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        Span s = null;
        try {
            s = Trace.start(description + ": remove");
            this.source.remove();
        } finally {
            if (s != null) {
                s.stop();
            }
        }
    }
    
}
