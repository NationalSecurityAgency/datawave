package datawave.webservice.query.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.junit.jupiter.api.Test;

/**
 * Pins the current, thread-unsafe behavior of {@link QueryUncaughtExceptionHandler}.
 * <p>
 * The handler guards its write with <code>if (this.throwable == null)</code> evaluated <em>outside</em> the <code>synchronized</code> block, and exposes
 * <code>throwable</code> and <code>thread</code> through unsynchronized reads of non-volatile fields. That is a broken double-checked lock with two
 * consequences: the documented "keep only the first one" invariant is not enforced, and a reader thread has no happens-before edge guaranteeing it ever
 * observes a recorded failure.
 * <p>
 * The second consequence matters most in practice. {@link datawave.query.tables.ScannerSession} records scan failures here on its worker thread and checks them
 * on the consumer thread; a missed write turns a failed scan into a silently empty result set.
 * <p>
 * These tests assert the broken behavior on purpose so that a fix is forced to update them. When the handler is made safe, every assertion here should be
 * inverted rather than deleted.
 */
public class QueryUncaughtExceptionHandlerTest {

    private static final long TIMEOUT_MILLIS = 10_000L;

    /**
     * A writer that reads the null guard before any throwable is recorded goes on to overwrite whatever was recorded in the meantime, so the first throwable is
     * lost.
     * <p>
     * The race is made deterministic by holding the handler's own monitor: the writer thread clears the unsynchronized guard, then parks on the monitor this
     * thread already owns, which lets this thread record the first throwable before the writer is released.
     */
    @Test
    public void testFirstThrowableIsClobberedByAWriterThatAlreadyClearedTheGuard() throws Exception {
        QueryUncaughtExceptionHandler handler = new QueryUncaughtExceptionHandler();

        Throwable first = new RuntimeException("first failure");
        Throwable second = new RuntimeException("second failure");

        Thread writer = new Thread(() -> handler.uncaughtException(Thread.currentThread(), second), "clobbering-writer");

        synchronized (handler) {
            writer.start();

            // the writer has read a null throwable and is now parked on the monitor held here
            awaitBlocked(writer);

            // reentrant on the monitor, so this write completes while the writer is still parked
            handler.uncaughtException(Thread.currentThread(), first);
            assertSame(first, handler.getThrowable());
        }

        writer.join(TIMEOUT_MILLIS);
        assertFalse(writer.isAlive(), "writer thread did not finish");

        // the guard was cleared before the lock was taken, so the first throwable is overwritten
        assertSame(second, handler.getThrowable());
        assertSame(writer, handler.getThread());
    }

    /**
     * The failure state is written under a lock but read without one, from fields that are not volatile. Nothing establishes a happens-before edge between the
     * thread that records a failure and the thread that polls for it.
     */
    @Test
    public void testFailureStateIsPublishedWithoutVisibilityGuarantees() throws Exception {
        Field throwable = QueryUncaughtExceptionHandler.class.getDeclaredField("throwable");
        Field thread = QueryUncaughtExceptionHandler.class.getDeclaredField("thread");

        assertFalse(Modifier.isVolatile(throwable.getModifiers()), "throwable is volatile, the visibility hole is closed");
        assertFalse(Modifier.isVolatile(thread.getModifiers()), "thread is volatile, the visibility hole is closed");

        Method getThrowable = QueryUncaughtExceptionHandler.class.getDeclaredMethod("getThrowable");
        Method getThread = QueryUncaughtExceptionHandler.class.getDeclaredMethod("getThread");

        assertFalse(Modifier.isSynchronized(getThrowable.getModifiers()), "getThrowable is synchronized, the visibility hole is closed");
        assertFalse(Modifier.isSynchronized(getThread.getModifiers()), "getThread is synchronized, the visibility hole is closed");
    }

    /**
     * The recorded thread and throwable are read through two separate unsynchronized accessors, so a reader can observe one without the other rather than a
     * single consistent pair.
     */
    @Test
    public void testThreadAndThrowableAreNotReadAsAPair() throws Exception {
        for (Method method : QueryUncaughtExceptionHandler.class.getDeclaredMethods()) {
            if (method.getReturnType().equals(Throwable.class) && method.getParameterCount() == 0) {
                assertFalse(Modifier.isSynchronized(method.getModifiers()), method.getName() + " is synchronized");
            }
        }

        QueryUncaughtExceptionHandler handler = new QueryUncaughtExceptionHandler();
        Throwable failure = new RuntimeException("failure");
        Thread recorded = new Thread(() -> handler.uncaughtException(Thread.currentThread(), failure), "recording-thread");
        recorded.start();
        recorded.join(TIMEOUT_MILLIS);

        // no accessor returns both halves together, they can only be read one at a time
        assertSame(failure, handler.getThrowable());
        assertSame(recorded, handler.getThread());
    }

    /**
     * Wait for a thread to park on a monitor.
     *
     * @param thread
     *            the thread to wait on
     * @throws InterruptedException
     *             if this thread is interrupted while waiting
     */
    private void awaitBlocked(Thread thread) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TIMEOUT_MILLIS;
        while (thread.getState() != Thread.State.BLOCKED) {
            if (System.currentTimeMillis() > deadline) {
                fail("timed out waiting for " + thread.getName() + " to block, state was " + thread.getState());
            }
            Thread.sleep(1);
        }
    }
}
