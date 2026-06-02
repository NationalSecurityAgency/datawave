package datawave.core.query.logic.composite;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.keyvalue.UnmodifiableMapEntry;

public class CompositeQueryLogicResults implements Iterable<Object>, Thread.UncaughtExceptionHandler {

    private final CompositeQueryLogic logic;
    private final ArrayBlockingQueue<Object> results;
    private final List<Thread.UncaughtExceptionHandler> handlers;
    private final List<Map.Entry<Thread,Throwable>> exceptions;
    private final long pollTimeout;
    private final TimeUnit pollTimeoutTimeUnit;

    public CompositeQueryLogicResults() {
        this(null, 1, 1000, TimeUnit.MILLISECONDS);
    }

    public CompositeQueryLogicResults(CompositeQueryLogic logic, int pagesize, long pollTimeout, TimeUnit pollTimeoutTimeUnit) {
        this.logic = logic;
        this.results = new ArrayBlockingQueue<>(pagesize);
        this.handlers = new ArrayList<>();
        this.exceptions = new ArrayList<>();
        this.pollTimeout = pollTimeout;
        this.pollTimeoutTimeUnit = pollTimeoutTimeUnit;
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
        CompositeQueryLogicResultsIterator it = new CompositeQueryLogicResultsIterator(logic, this.results, pollTimeout, pollTimeoutTimeUnit);

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
