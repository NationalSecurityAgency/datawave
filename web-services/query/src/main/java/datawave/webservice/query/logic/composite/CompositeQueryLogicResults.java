package datawave.webservice.query.logic.composite;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;

public class CompositeQueryLogicResults implements Iterable<Object>, Thread.UncaughtExceptionHandler {

    private final CompositeQueryLogic logic;
    private final ArrayBlockingQueue<Object> results;
    private final List<Thread.UncaughtExceptionHandler> handlers;
    private final List<Map.Entry<Thread,Throwable>> exceptions;

    public CompositeQueryLogicResults() {
        this.logic = null;
        this.results = new ArrayBlockingQueue<>(1);
        this.handlers = new ArrayList<>();
        this.exceptions = new ArrayList<>();
    }

    public CompositeQueryLogicResults(CompositeQueryLogic logic, int pagesize) {
        this.logic = logic;
        this.results = new ArrayBlockingQueue<>(pagesize);
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
        CompositeQueryLogicResultsIterator it = new CompositeQueryLogicResultsIterator(logic, this.results);
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
