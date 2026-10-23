package datawave.webservice.query.runner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.iterators.TransformIterator;

/**
 * A single producer with one buffered result. No iterator call runs while the buffer is full.
 */
final class AsyncResultsSource extends ResultsSource {
    private final ExecutorService executor;
    private FutureTask<Void> task;
    private Object buffered;

    AsyncResultsSource(TransformIterator<?,?> iterator, ExecutorService executor) {
        super(iterator);
        this.executor = executor;
    }

    private void start() {
        FutureTask<Void> submitted;
        synchronized (monitor) {
            if (state != State.NEW) {
                return;
            }
            state = State.RUNNING;
            task = new FutureTask<>(() -> {
                produce();
                return null;
            });
            submitted = task;
        }
        try {
            executor.execute(submitted);
        } catch (RuntimeException | Error e) {
            fail(e);
            throw e;
        }
    }

    private void produce() {
        synchronized (monitor) {
            // Cancellation before the executor starts this task must not touch the old iterator, even after reset.
            if (state != State.RUNNING) {
                return;
            }
            reader = Thread.currentThread();
        }
        try {
            while (true) {
                synchronized (monitor) {
                    while (state == State.RUNNING && buffered != null) {
                        monitor.wait();
                    }
                    if (state != State.RUNNING) {
                        return;
                    }
                }
                Attempt attempt = read();
                synchronized (monitor) {
                    if (state != State.RUNNING) {
                        return;
                    }
                    if (attempt.outcome == Outcome.EXHAUSTED) {
                        state = State.EXHAUSTED;
                        monitor.notifyAll();
                        return;
                    }
                    buffered = attempt.result;
                    monitor.notifyAll();
                }
            }
        } catch (Throwable t) {
            fail(t);
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            synchronized (monitor) {
                reader = null;
                monitor.notifyAll();
            }
        }
    }

    @Override
    Attempt poll(PageRequest request) throws Exception {
        start();
        synchronized (monitor) {
            while (true) {
                checkFailure();
                if (state == State.EXHAUSTED && buffered == null) {
                    return Attempt.EXHAUSTED;
                }
                Attempt control = pageControl(request);
                if (control != null) {
                    return control;
                }
                if (buffered != null) {
                    Object result = buffered;
                    buffered = null;
                    monitor.notifyAll();
                    return Attempt.result(result);
                }
                TimeUnit.NANOSECONDS.timedWait(monitor, request.remainingNanos());
            }
        }
    }

    @Override
    void cancel() {
        synchronized (monitor) {
            super.cancel();
            buffered = null;
            if (task != null) {
                // The reader is interrupted and awaited separately; also prevent a queued task from starting.
                task.cancel(false);
            }
        }
    }
}
