package datawave.webservice.query.runner;

import org.apache.commons.collections4.iterators.TransformIterator;

/**
 * Reads on the caller's thread. A deadline can be checked before a read, but cannot preempt a blocking synchronous iterator.
 */
final class DirectResultsSource extends ResultsSource {
    DirectResultsSource(TransformIterator<?,?> iterator) {
        super(iterator);
    }

    @Override
    Attempt poll(PageRequest request) throws Exception {
        synchronized (monitor) {
            checkFailure();
            if (state == State.EXHAUSTED) {
                return Attempt.EXHAUSTED;
            }
            Attempt control = pageControl(request);
            if (control != null) {
                return control;
            }
            state = State.RUNNING;
            reader = Thread.currentThread();
        }
        try {
            Attempt result = read();
            synchronized (monitor) {
                checkFailure();
                if (result.outcome == Outcome.EXHAUSTED) {
                    state = State.EXHAUSTED;
                }
                return result;
            }
        } catch (Throwable t) {
            fail(t);
            checkFailure();
            rethrow(t);
            throw new AssertionError("unreachable");
        } finally {
            synchronized (monitor) {
                reader = null;
                monitor.notifyAll();
            }
        }
    }
}
