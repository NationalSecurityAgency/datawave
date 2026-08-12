package datawave.webservice.query.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.tuple.ImmutableTriple;

/**
 * Implementation of {@link UncaughtExceptionHandler} that captures and retains the first exception only thrown during a query.
 */
public class QueryUncaughtExceptionHandler implements UncaughtExceptionHandler {

    private final AtomicLong sequenceGenerator = new AtomicLong();
    private final AtomicReference<ImmutableTriple<Long,Throwable,Thread>> atomicRef = new AtomicReference<>();
    private final Queue<String> messages = new ConcurrentLinkedQueue<>();

    /**
     * Add an uncaught exception to this {@link QueryUncaughtExceptionHandler}. Only the first non-null exception will be kept.
     *
     * @param thread
     *            the thread
     * @param throwable
     *            the throwable
     */
    @Override
    public void uncaughtException(Thread thread, Throwable throwable) {
        if (throwable == null) {
            return;
        }
        // Get a sequence as the very first action after the null check, so that it reflects call order as much as possible, not CAS-completion order.
        long sequence = sequenceGenerator.getAndIncrement();
        ImmutableTriple<Long,Throwable,Thread> candidate = new ImmutableTriple<>(sequence, throwable, thread);

        // Always retain whichever candidate has the lowest sequence number, regardless of which thread's update executes first. A later-sequenced value can
        // never beat an earlier one, even if it reaches this line first.
        atomicRef.accumulateAndGet(candidate, (current, next) -> (current == null || next.getLeft() < current.getLeft()) ? next : current);
    }

    /**
     * Return the thread of the first non-null throwable supplied to {@link #uncaughtException(Thread, Throwable)}.
     *
     * @return the thread, possibly null, even if {@link #getThrowable()} returns non-null
     */
    public Thread getThread() {
        ImmutableTriple<Long,Throwable,Thread> triple = atomicRef.get();
        return triple == null ? null : triple.getRight();
    }

    /**
     *
     * Returns the first non-null throwable supplied to {@link #uncaughtException(Thread, Throwable)}.
     *
     * @return the throwable, possibly null
     */
    public Throwable getThrowable() {
        ImmutableTriple<Long,Throwable,Thread> triple = atomicRef.get();
        return triple == null ? null : triple.getMiddle();
    }

    /**
     * Add a message to this {@link QueryUncaughtExceptionHandler}.
     *
     * @param message
     *            the message to add
     */
    public void addMessage(String message) {
        messages.add(message);
    }

    /**
     * Return a copy of the messages of this {@link QueryUncaughtExceptionHandler}.
     *
     * @return the messages
     */
    public List<String> getMessages() {
        return List.copyOf(messages);
    }

}
