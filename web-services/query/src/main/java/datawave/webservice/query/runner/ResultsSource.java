package datawave.webservice.query.runner;

import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.iterators.TransformIterator;

import datawave.core.query.logic.WritesQueryMetrics;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetric;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * One execution's iterator and control signals. The consumer must distinguish an exhausted iterator from a deadline, a requested page return, and failure. All
 * state and page requests use the same monitor as the asynchronous handoff; a notification is never itself an outcome.
 */
abstract class ResultsSource {
    enum State {
        NEW, RUNNING, EXHAUSTED, FAILED, CANCELLED
    }

    enum Outcome {
        RESULT, EXHAUSTED, DEADLINE, FORCED
    }

    static final class Attempt {
        static final Attempt EXHAUSTED = new Attempt(Outcome.EXHAUSTED, null);
        static final Attempt DEADLINE = new Attempt(Outcome.DEADLINE, null);
        static final Attempt FORCED = new Attempt(Outcome.FORCED, null);

        final Outcome outcome;
        final Object result;

        private Attempt(Outcome outcome, Object result) {
            this.outcome = outcome;
            this.result = result;
        }

        static Attempt result(Object result) {
            return new Attempt(Outcome.RESULT, result);
        }
    }

    static final class PageRequest {
        private final long startNanos = System.nanoTime();
        private final long timeoutNanos;
        private boolean forced;

        PageRequest(long timeoutMillis) {
            timeoutNanos = timeoutMillis == Long.MAX_VALUE ? Long.MAX_VALUE : TimeUnit.MILLISECONDS.toNanos(Math.max(0, timeoutMillis));
        }

        long remainingNanos() {
            return timeoutNanos == Long.MAX_VALUE ? Long.MAX_VALUE : timeoutNanos - (System.nanoTime() - startNanos);
        }
    }

    protected final Object monitor = new Object();
    protected final TransformIterator<?,?> iterator;
    protected State state = State.NEW;
    protected Thread reader;
    private Throwable failure;
    private PageRequest page;
    private final BaseQueryMetric iteratorMetrics = new QueryMetric();
    private boolean metricsPending;

    ResultsSource(TransformIterator<?,?> iterator) {
        this.iterator = iterator;
    }

    final PageRequest beginPage(long timeoutMillis) {
        synchronized (monitor) {
            page = new PageRequest(timeoutMillis);
            return page;
        }
    }

    final void endPage(PageRequest request) {
        synchronized (monitor) {
            if (page == request) {
                page = null;
            }
        }
    }

    final void requestPageReturn(PageRequest request) {
        synchronized (monitor) {
            if (request != null && page == request) {
                request.forced = true;
                monitor.notifyAll();
            }
        }
    }

    // Called with monitor held. A request belongs to one page, so a late timer cannot force the next page.
    protected final Attempt pageControl(PageRequest request) throws Exception {
        checkFailure();
        if (request != page) {
            throw new IllegalStateException("The results page is no longer active");
        }
        if (request.forced) {
            return Attempt.FORCED;
        }
        return request.remainingNanos() <= 0 ? Attempt.DEADLINE : null;
    }

    abstract Attempt poll(PageRequest request) throws Exception;

    /**
     * Only the iterator's owning thread calls this method. Some legacy transform iterators use null as their explicit end sentinel.
     */
    protected final Attempt read() {
        try {
            if (!iterator.hasNext()) {
                return Attempt.EXHAUSTED;
            }
            Object result = iterator.next();
            return result == null ? Attempt.EXHAUSTED : Attempt.result(result);
        } finally {
            if (iterator.getTransformer() instanceof WritesQueryMetrics) {
                synchronized (monitor) {
                    ((WritesQueryMetrics) iterator.getTransformer()).writeQueryMetrics(iteratorMetrics);
                    metricsPending = true;
                }
            }
        }
    }

    final void flushMetrics(BaseQueryMetric metric) {
        synchronized (monitor) {
            if (metricsPending) {
                metric.setSourceCount(iteratorMetrics.getSourceCount());
                metric.setNextCount(iteratorMetrics.getNextCount());
                metric.setSeekCount(iteratorMetrics.getSeekCount());
                metric.setYieldCount(iteratorMetrics.getYieldCount());
                metric.setDocRanges(iteratorMetrics.getDocRanges());
                metric.setFiRanges(iteratorMetrics.getFiRanges());
                metricsPending = false;
            }
        }
    }

    protected final void fail(Throwable cause) {
        synchronized (monitor) {
            // Closing an iterator can make its read throw. Preserve the cancellation rather than inventing a second query failure.
            if (state != State.CANCELLED) {
                failure = cause;
                state = State.FAILED;
            }
            monitor.notifyAll();
        }
    }

    final void checkFailure() throws Exception {
        synchronized (monitor) {
            if (failure != null) {
                rethrow(failure);
            }
            if (state == State.CANCELLED) {
                throw new QueryException(DatawaveErrorCode.QUERY_CANCELED);
            }
        }
    }

    static void rethrow(Throwable cause) throws Exception {
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        if (cause instanceof Exception) {
            throw (Exception) cause;
        }
        throw new QueryException(cause);
    }

    void cancel() {
        synchronized (monitor) {
            state = State.CANCELLED;
            monitor.notifyAll();
            // Interrupt while holding the monitor: reader cannot exit and be reused for unrelated work before this interrupt.
            if (reader != null) {
                reader.interrupt();
            }
        }
    }

    /**
     * Future cancellation does not prove worker exit. Wait on the actual reader, preserving the caller's interrupt while allowing bounded cleanup.
     */
    final boolean awaitStopped(long timeoutMillis) {
        long start = System.nanoTime();
        long timeout = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        boolean interrupted = false;
        try {
            synchronized (monitor) {
                while (reader != null) {
                    long remaining = timeout - (System.nanoTime() - start);
                    if (remaining <= 0) {
                        return false;
                    }
                    try {
                        TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                return true;
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
