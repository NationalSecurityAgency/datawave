package datawave.webservice.query.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Implementation of {@link UncaughtExceptionHandler} that captures and retains the first exception only thrown during a query.
 */
public class QueryUncaughtExceptionHandler implements UncaughtExceptionHandler {

    private final AtomicLong sequenceGenerator = new AtomicLong();
    private final AtomicReference<ImmutablePair<Long,ImmutablePair<Throwable,Thread>>> atomicRef = new AtomicReference<>();
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
        ImmutablePair<Long,ImmutablePair<Throwable,Thread>> candidate = ImmutablePair.of(sequence, ImmutablePair.of(throwable, thread));

        // Always retain whichever candidate has the lowest sequence number, regardless of which thread's update executes first. A later-sequenced value can
        // never beat an earlier one, even if it reaches this line first.
        atomicRef.accumulateAndGet(candidate, (current, next) -> (current == null || next.getLeft() < current.getLeft()) ? next : current);
    }

    /**
     * Return the thread of the first non-null throwable supplied to {@link #uncaughtException(Thread, Throwable)}. <strong>Warning:</strong>: this method can
     * possibly diverge from {@link #getThrowable()} in high-contention scenarios. {@link #getUncaughtException()} is recommended.
     *
     * @return the thread, possibly null, even if {@link #getThrowable()} returns non-null
     * @deprecated in favor of {@link #getUncaughtException()} that supports atomic fetching of an uncaught exception and its associated thread
     */
    @Deprecated
    public Thread getThread() {
        ImmutablePair<Long,ImmutablePair<Throwable,Thread>> ref = atomicRef.get();
        return ref == null ? null : ref.getRight().getRight();
    }

    /**
     * Return the first non-null throwable supplied to {@link #uncaughtException(Thread, Throwable)}. <strong>Warning:</strong> this method can possibly diverge
     * from {@link #getThread()} ()} in high-contention scenarios. {@link #getUncaughtException()} is recommended.
     *
     * @return the throwable, possibly null
     * @deprecated in favor of {@link #getUncaughtException()} that supports atomic fetching of an uncaught exception and its associated thread
     */
    @Deprecated
    public Throwable getThrowable() {
        ImmutablePair<Long,ImmutablePair<Throwable,Thread>> ref = atomicRef.get();
        return ref == null ? null : ref.getRight().getLeft();
    }

    /**
     * Return the first non-null uncaught exception supplied to {@link #uncaughtException(Thread, Throwable)} (never null), and its associated thread (possibly
     * null).
     *
     * @return a pair consisting of the uncaught exception and its associated thread
     */
    public ImmutablePair<Throwable,Thread> getUncaughtException() {
        ImmutablePair<Long,ImmutablePair<Throwable,Thread>> ref = atomicRef.get();
        return ref == null ? null : ref.getRight();
    }

    /**
     * Add a message to this {@link QueryUncaughtExceptionHandler} if the message is not null.
     *
     * @param message
     *            the message to add
     */
    public void addMessage(String message) {
        if (message != null) {
            messages.add(message);
        }
    }

    /**
     * Return a copy of the messages of this {@link QueryUncaughtExceptionHandler}. Possibly empty, but never null.
     *
     * @return the messages
     */
    public List<String> getMessages() {
        return List.copyOf(messages);
    }

}
