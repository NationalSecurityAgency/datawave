package datawave.webservice.query.logic.composite;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.common.base.Throwables;

import datawave.webservice.query.configuration.GenericQueryConfiguration;

public class CompositeQueryLogicResultsIterator implements Iterator<Object>, Thread.UncaughtExceptionHandler {

    protected static final Logger log = Logger.getLogger(CompositeQueryLogicResultsIterator.class);

    private final CompositeQueryLogic logic;

    private final ArrayBlockingQueue<Object> results;
    private Object nextEntry = null;
    private boolean seenEntries = false;
    private final Object lock = new Object();
    private volatile Throwable failure = null;

    public CompositeQueryLogicResultsIterator(CompositeQueryLogic logic, ArrayBlockingQueue<Object> results) {
        this.logic = logic;
        this.results = results;
    }

    @Override
    public boolean hasNext() {
        synchronized (lock) {
            if (failure != null) {
                Throwables.propagate(failure);
            }
            while (nextEntry == null) {
                try {
                    while (nextEntry == null && failure == null && (!results.isEmpty() || logic.getCompletionLatch().getCount() > 0)) {
                        nextEntry = results.poll(1, TimeUnit.SECONDS);
                    }
                    if (failure != null) {
                        Throwables.propagate(failure);
                    }
                    if (nextEntry == null) {
                        // if the current execution threads are complete
                        // and we are in the sequential execution mode
                        // and we have not seen an result yet
                        // and we have more logics to initialize
                        // then initialize the next logic and continue.
                        if (logic.getCompletionLatch().getCount() == 0 && logic.isShortCircuitExecution() && !seenEntries
                                        && !logic.getUninitializedLogics().isEmpty()) {
                            try {
                                GenericQueryConfiguration config = logic.initialize(logic.getConfig().getClient(), logic.getSettings(),
                                                logic.getConfig().getAuthorizations());
                                logic.setupQuery(config);
                            } catch (Exception e) {
                                Throwables.propagate(e);
                            }
                        } else {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (nextEntry != null) {
            seenEntries = true;
            return true;
        }
        return false;
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
