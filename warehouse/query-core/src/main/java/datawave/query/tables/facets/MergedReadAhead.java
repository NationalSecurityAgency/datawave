package datawave.query.tables.facets;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Decouples an iterator via a Thread and Queue, optionally merging/transformaing and filtering.
 * <p>
 * MergedReadAhead should be used like an iterator. It starts a thread a contsruction time that runs until <code>hasNext()</code> on the underlying iterator
 * returns false. The next()/hasNext() calls exposed by this class will block until there are items available to return in the queue or the underling thread has
 * stopped.
 */
@SuppressWarnings("UnstableApiUsage")
public class MergedReadAhead<T> extends AbstractExecutionThreadService implements Iterator<T>, Closeable {
    
    private static final Logger log = Logger.getLogger(MergedReadAhead.class);
    
    /** the underlying iterator we will read form */
    private final Iterator<T> iterator;
    
    /** holds the data read from iterator */
    protected final BlockingQueue<T> queue;
    
    /** buffers the last item read from the queue to support hasNext/next */
    protected T buffer;
    
    /** Should we wait until the underlying thread has stopped before allowing hasNext to return */
    protected final boolean isStreaming;
    
    /**
     * Create a MergedReadAhead. Will not return until the thread that reads the iterator has started.
     *
     * @param isStreaming
     *            is this streaming?
     * @param iterator
     *            the iterator to read from
     * @param functionalMerge
     *            the merge/transform operator to apply to the objects from the iterator.
     * @param filters
     *            the filters to apply to the objects from the iterator.
     */
    public MergedReadAhead(boolean isStreaming, final Iterator<T> iterator, Function<T,T> functionalMerge, List<Predicate<T>> filters) {
        this.queue = new LinkedBlockingQueue<T>();
        this.isStreaming = isStreaming;
        this.iterator = configureIterator(iterator, functionalMerge, filters);
        
        log.trace("starting...");
        startAsync();
    }
    
    private Iterator<T> configureIterator(Iterator<T> iterator, Function<T,T> functionalMerge, List<Predicate<T>> filters) {
        Iterator<T> i = iterator;
        if (functionalMerge != null) {
            i = Iterators.transform(iterator, functionalMerge);
        }
        
        if (filters != null) {
            for (Predicate<T> predicate : filters) {
                i = Iterators.filter(i, predicate);
            }
        }
        return i;
    }
    
    /**
     * Block until an item available in the queue or the thread is no longer running. Drain the queue if the thread is done and items are in the queue. Wait for
     * the entire underlying iterator to be consumed if in non-streaming mode
     */
    private void readFromQueue() {
        if (!isStreaming) {
            log.trace("Non-streaming, waiting for termination...");
            awaitTerminated();
        }
        
        try {
            // Block & loop while running and no data is available on the queue
            while (state() != State.TERMINATED) {
                log.trace("Waiting for data...");
                this.buffer = queue.poll(1L, TimeUnit.SECONDS);
                if (this.buffer != null) {
                    break;
                }
            }
            
            // Drain elements from the queue after the thread has stopped
            if (this.buffer == null && !queue.isEmpty()) {
                this.buffer = queue.poll(1L, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public boolean hasNext() {
        readFromQueue();
        return this.buffer != null;
    }
    
    @Override
    public T next() {
        if (buffer == null) {
            readFromQueue();
            if (buffer == null) {
                throw new NoSuchElementException();
            }
        }
        
        T buf = buffer;
        buffer = null;
        return buf;
    }
    
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
    
    @Override
    public void close() throws IOException {
        log.trace("stopping...");
        stopAsync();
        awaitTerminated();
        log.trace("stopped.");
    }
    
    @Override
    protected void run() throws Exception {
        while (iterator.hasNext()) {
            T d = iterator.next();
            if (d != null) {
                log.trace("added an item after internal hasNext()");
                queue.add(d);
            } else {
                log.trace("data was empty after internal hasNext()");
            }
        }
        log.trace("done reading from iterator...");
    }
}
