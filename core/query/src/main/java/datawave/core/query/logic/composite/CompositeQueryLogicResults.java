package datawave.core.query.logic.composite;

import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class CompositeQueryLogicResults implements Iterable<Object>, Thread.UncaughtExceptionHandler {

    private final ArrayBlockingQueue<Object> results;
    private final CountDownLatch completionLatch;
    private final List<Thread.UncaughtExceptionHandler> handlers;
    private final List<Map.Entry<Thread,Throwable>> exceptions;

    public CompositeQueryLogicResults(int pagesize, CountDownLatch completionLatch) {
        this.results = new ArrayBlockingQueue<>(pagesize);
        this.completionLatch = completionLatch;
        this.handlers = new ArrayList<>();
        this.exceptions = new ArrayList<>();
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
        synchronized (handlers) {
            // first pass any exceptions we have already seen
            for (Map.Entry<Thread,Throwable> exception : exceptions) {
                it.uncaughtException(exception.getKey(), exception.getValue());
            }
            // and add the iterator to the list of handlers
            handlers.add(it);
        }
        return it;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        synchronized (handlers) {
            // add the exception to our list
            exceptions.add(new UnmodifiableMapEntry(t, e));
            // and notify existing handlers of the exception
            for (Thread.UncaughtExceptionHandler handler : handlers) {
                handler.uncaughtException(t, e);
            }
        }
    }
}
