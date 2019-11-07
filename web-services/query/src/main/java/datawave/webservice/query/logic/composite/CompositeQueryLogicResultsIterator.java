package datawave.webservice.query.logic.composite;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class CompositeQueryLogicResultsIterator implements Iterator<Object> {
    
    protected static final Logger log = Logger.getLogger(CompositeQueryLogicResultsIterator.class);
    
    private ArrayBlockingQueue<Object> results = null;
    private Object nextEntry = null;
    private Object lock = new Object();
    private CountDownLatch completionLatch = null;
    
    public CompositeQueryLogicResultsIterator(ArrayBlockingQueue<Object> results, CountDownLatch completionLatch) {
        this.results = results;
        this.completionLatch = completionLatch;
    }
    
    @Override
    public boolean hasNext() {
        synchronized (lock) {
            if (nextEntry != null)
                return true;
            try {
                while (nextEntry == null && (!results.isEmpty() || completionLatch.getCount() > 0)) {
                    nextEntry = results.poll(1, TimeUnit.SECONDS);
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
}
