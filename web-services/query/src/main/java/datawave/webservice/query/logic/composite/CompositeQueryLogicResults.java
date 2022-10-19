package datawave.webservice.query.logic.composite;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class CompositeQueryLogicResults implements Iterable<Object>, Thread.UncaughtExceptionHandler {
    
    private final ArrayBlockingQueue<Object> results;
    private final CountDownLatch completionLatch;
    private final List<Thread.UncaughtExceptionHandler> handlers;
    
    public CompositeQueryLogicResults(int pagesize, CountDownLatch completionLatch) {
        this.results = new ArrayBlockingQueue<>(pagesize);
        this.completionLatch = completionLatch;
        this.handlers = new ArrayList<>();
    }
    
    public void add(Object object) throws InterruptedException {
        this.results.put(object);
    }
    
    public void clear() {
        this.results.clear();
    }
    
    public int size() {
        return results.size();
    }
    
    public boolean contains(Object o) {
        return results.contains(o);
    }
    
    @Override
    public Iterator<Object> iterator() {
        CompositeQueryLogicResultsIterator it = new CompositeQueryLogicResultsIterator(this.results, this.completionLatch);
        handlers.add(it);
        return it;
    }
    
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        for (Thread.UncaughtExceptionHandler handler : handlers) {
            handler.uncaughtException(t, e);
        }
    }
}
