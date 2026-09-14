package datawave.webservice.query.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Implementation of {@link UncaughtExceptionHandler} that atomically captures and retains the chronologically first exception recorded to the handler.
 */
public class QueryUncaughtExceptionHandler implements UncaughtExceptionHandler {

    /**
     * Generates sequences for candidate calls to {@link #uncaughtException(Thread, Throwable)} to identify the chronologically first call.
     */
    private final AtomicLong sequenceGenerator = new AtomicLong();

    /**
     * A reference to the currently captured exception and its thread.
     */
    private final AtomicReference<ImmutablePair<Long,ImmutablePair<Throwable,Thread>>> atomicRef = new AtomicReference<>();

    /**
     * The recorded messages.
     */
    private final Queue<String> messages = new ConcurrentLinkedQueue<>();

    /**
     * Atomically add an uncaught exception to this {@link QueryUncaughtExceptionHandler}. Only the chronologically first non-null exception will be kept. Under
     * high contention, it is possible for an exception to be recorded for a chronologically later call to this method whose thread's CAS-operation completes
     * first. The captured exception will later be replaced by a call to the method that occurred earlier.
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
     * Return the first non-null uncaught exception supplied to {@link #uncaughtException(Thread, Throwable)}, or null if no exception has been caught. If a
     * non-null pair is returned, the throwable for the pair will never be null, but the thread of the pair can be null.
     * <p>
     * NOTE: it is possible for this method to return different pairs in the case when {@link #uncaughtException(Thread, Throwable)} is initially called
     * concurrently. This is only a concern when trying to call this method immediately after the contending threads supply exceptions to
     * {@link #uncaughtException(Thread, Throwable)}.
     *
     * @return a pair consisting of the uncaught exception and its associated thread
     */
    public ImmutablePair<Throwable,Thread> getUncaughtException() {
        ImmutablePair<Long,ImmutablePair<Throwable,Thread>> ref = atomicRef.get();
        return ref == null ? null : ref.getRight();
    }

    /**
     * Return whether an exception has been captured via {@link #uncaughtException(Thread, Throwable)}.
     *
     * @return true if an exception has been caught, or false otherwise
     */
    public boolean hasUncaughtException() {
        return atomicRef.get() != null;
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
