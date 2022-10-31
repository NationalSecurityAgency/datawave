package datawave.webservice.query.logic.composite;

import com.google.common.base.Throwables;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CompositeQueryLogicResultsIterator implements Iterator<Object>, Thread.UncaughtExceptionHandler {
    
    protected static final Logger log = Logger.getLogger(CompositeQueryLogicResultsIterator.class);
    
    private final ArrayBlockingQueue<Object> results;
    private Object nextEntry = null;
    private final Object lock = new Object();
    private final CountDownLatch completionLatch;
    private volatile Throwable failure = null;
    
    public CompositeQueryLogicResultsIterator(ArrayBlockingQueue<Object> results, CountDownLatch completionLatch) {
        this.results = results;
        this.completionLatch = completionLatch;
    }
    
    @Override
    public boolean hasNext() {
        synchronized (lock) {
            if (failure != null) {
                Throwables.propagate(failure);
            }
            if (nextEntry != null)
                return true;
            try {
                while (nextEntry == null && failure == null && (!results.isEmpty() || completionLatch.getCount() > 0)) {
                    nextEntry = results.poll(1, TimeUnit.SECONDS);
                }
                if (failure != null) {
                    Throwables.propagate(failure);
                }
                return true;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override
    public Object next() {
        Object current = null;
        
        synchronized (lock) {
            if (failure != null) {
                Throwables.propagate(failure);
            }
            if (hasNext()) {
                current = nextEntry;
                nextEntry = null;
            }
        }
        return current;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        // keep the first one
        if (this.failure == null) {
            this.failure = e;
        }
    }
}
