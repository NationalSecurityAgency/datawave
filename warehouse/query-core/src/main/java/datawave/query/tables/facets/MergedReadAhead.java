package datawave.query.tables.facets;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

/**
 * 
 */
public class MergedReadAhead<T> extends AbstractExecutionThreadService implements Iterator<T>, Closeable {
    
    private static final Logger log = Logger.getLogger(MergedReadAhead.class);
    
    private Iterator<T> iter;
    
    protected Buffer buf = null;
    
    protected FacetedConfiguration facetedConfig;
    
    private AtomicBoolean removeEntry = new AtomicBoolean(false);
    
    public MergedReadAhead(FacetedConfiguration facetedConfig, final Iterator<T> iter, Function<T,T> functionalMerge, List<Predicate<T>> filters) {
        
        this.facetedConfig = facetedConfig;
        
        this.iter = Iterators.transform(iter, functionalMerge);
        
        for (Predicate<T> predicate : filters) {
            this.iter = Iterators.filter(this.iter, predicate);
        }
        
        buf = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(1));
        
        startAndWait();
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        if (!facetedConfig.isStreaming) {
            while (state() == State.RUNNING)
                ;
            
        }
        while (buf.size() == 0 && state() == State.RUNNING) {
            
        }
        
        return buf.size() > 0;
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public T next() {
        T val = (T) buf.get();
        if (removeEntry.get() == true)
            buf.remove();
        return val;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    public void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        stop();
        
        if (log.isTraceEnabled()) {
            log.trace("Closing thread");
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.util.concurrent.AbstractExecutionThreadService#run()
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void run() throws Exception {
        while (iter.hasNext()) {
            T d = iter.next();
            if (null != d)
                buf.add(d);
            else if (log.isTraceEnabled())
                log.trace("Data was empty");
        }
        removeEntry.set(true);
    }
    
}
