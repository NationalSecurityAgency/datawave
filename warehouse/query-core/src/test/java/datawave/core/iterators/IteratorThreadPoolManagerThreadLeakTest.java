package datawave.core.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrates the thread-leak bug in {@link IteratorThreadPoolManager}.
 *
 * <p>
 * The {@link IteratorThreadPoolManager} constructor creates three {@link ScheduledExecutorService} instances (two via
 * {@code Executors.newSingleThreadScheduledExecutor()} and one via {@code Executors.newScheduledThreadPool(int)}) to run periodic monitoring tasks. The
 * references to these scheduled executors are discarded immediately — each is created inline as the receiver of {@code scheduleWithFixedDelay(...)} and never
 * stored in a field. As a result there is no way to shut them down: when the {@link IteratorThreadPoolManager} singleton is replaced, or when the surrounding
 * query lifecycle ends, the scheduled executor threads keep running indefinitely because they are non-daemon threads created by
 * {@code Executors.defaultThreadFactory()}.
 * </p>
 *
 * <p>
 * The structural tests ({@link #scheduledExecutorsFieldExists()} and {@link #shutdownMethodExists()}) assert the contract directly and fail deterministically
 * on the unfixed code. The behavioral test ({@link #scheduledExecutorsAreRetainedAndShutDown()}) verifies that, after construction, the manager retains
 * references to all of its scheduled executors in a list and that {@code shutdown} terminates every one of them.
 * </p>
 */
public class IteratorThreadPoolManagerThreadLeakTest {

    private static final Logger log = LoggerFactory.getLogger(IteratorThreadPoolManagerThreadLeakTest.class);

    private IteratorThreadPoolManager originalInstance;

    @Before
    public void snapshotSingleton() throws Exception {
        Field instanceField = IteratorThreadPoolManager.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        originalInstance = (IteratorThreadPoolManager) instanceField.get(null);
    }

    @After
    public void restoreSingleton() throws Exception {
        Field instanceField = IteratorThreadPoolManager.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, originalInstance);
    }

    /**
     * Asserts that {@link IteratorThreadPoolManager} declares a field named {@code scheduledExecutors} of type {@link List} (of {@link ScheduledExecutorService}
     * ). Without this field the references to the scheduled executors created in the constructor are discarded and the executors can never be shut down.
     */
    @Test
    public void scheduledExecutorsFieldExists() throws Exception {
        Field field = IteratorThreadPoolManager.class.getDeclaredField("scheduledExecutors");
        field.setAccessible(true);
        assertTrue("IteratorThreadPoolManager.scheduledExecutors must be a List so the manager retains references to the ScheduledExecutorService instances "
                        + "it creates in its constructor. Found type: " + field.getType(),
                        List.class.isAssignableFrom(field.getType()));
    }

    /**
     * Asserts that {@link IteratorThreadPoolManager} declares a {@code shutdown} method that can be called to release all of its thread pools and scheduled
     * executors. Without this method there is no way to stop the periodic monitoring threads created in the constructor, and they leak across query lifecycle
     * cycles.
     */
    @Test
    public void shutdownMethodExists() throws Exception {
        Method shutdown = findShutdownMethod();
        assertNotNull("IteratorThreadPoolManager must declare a shutdown method (either static shutdown(IteratorEnvironment) or instance shutdown()) "
                        + "so callers can release the thread pools and scheduled executors it creates.", shutdown);
    }

    /**
     * Verifies that, after construction, the manager retains references to all of its scheduled executors in the {@code scheduledExecutors} list and that
     * calling {@code shutdown} terminates every one of them.
     * <p>
     * On the unfixed code this test cannot pass: the {@code scheduledExecutors} field does not exist, so the list of retained executors is empty (or the field
     * is absent), and the {@code shutdown} method either does not exist or does not shut down the scheduled executors — so the periodic monitoring threads
     * keep running.
     * </p>
     */
    @Test
    public void scheduledExecutorsAreRetainedAndShutDown() throws Exception {
        IteratorThreadPoolManager manager = newManagerInstance();

        @SuppressWarnings("unchecked")
        List<ScheduledExecutorService> retained = (List<ScheduledExecutorService>) readField(manager, "scheduledExecutors");
        assertNotNull("scheduledExecutors field is null after construction", retained);
        assertTrue("scheduledExecutors list is empty after construction — the manager is not retaining references to the ScheduledExecutorService instances "
                        + "it creates in its constructor. Expected at least 3 entries (two single-thread monitors and one thread-pool monitor).",
                        retained.size() >= 3);

        // Every retained scheduled executor must be running (not yet shut down) right after construction.
        for (int i = 0; i < retained.size(); i++) {
            assertTrue("scheduledExecutors[" + i + "] is already shut down immediately after construction", !retained.get(i).isShutdown());
        }

        // The tracked ThreadPoolExecutors must also be running.
        @SuppressWarnings("unchecked")
        java.util.Map<String,ThreadPoolExecutor> threadPools = (java.util.Map<String,ThreadPoolExecutor>) readField(manager, "threadPools");
        assertTrue("threadPools map is empty after construction", !threadPools.isEmpty());
        for (java.util.Map.Entry<String,ThreadPoolExecutor> e : threadPools.entrySet()) {
            assertTrue("threadPools[" + e.getKey() + "] is already shut down immediately after construction", !e.getValue().isShutdown());
        }

        // Invoke shutdown.
        invokeShutdown(manager);

        // After shutdown, every retained scheduled executor must be shut down.
        for (int i = 0; i < retained.size(); i++) {
            assertTrue("scheduledExecutors[" + i + "] is not shut down after shutdown() was called — the manager is leaking scheduled executor threads.",
                            retained.get(i).isShutdown());
            // And they should terminate promptly (they have no long-running work, only periodic tasks that we just cancelled).
            assertTrue("scheduledExecutors[" + i + "] did not terminate within 5 seconds after shutdown() was called.",
                            retained.get(i).awaitTermination(5, TimeUnit.SECONDS));
        }

        // And every tracked ThreadPoolExecutor must be shut down.
        for (java.util.Map.Entry<String,ThreadPoolExecutor> e : threadPools.entrySet()) {
            assertTrue("threadPools[" + e.getKey() + "] is not shut down after shutdown() was called — the manager is leaking thread pool threads.",
                            e.getValue().isShutdown());
        }
    }

    private IteratorThreadPoolManager newManagerInstance() throws Exception {
        // The constructor is private; invoke it via reflection with a null env (the constructor tolerates null env by
        // falling back to DefaultConfiguration).
        java.lang.reflect.Constructor<IteratorThreadPoolManager> ctor = IteratorThreadPoolManager.class.getDeclaredConstructor(
                        org.apache.accumulo.core.iterators.IteratorEnvironment.class);
        ctor.setAccessible(true);
        IteratorThreadPoolManager mgr = ctor.newInstance((Object) null);

        // The static shutdown(IteratorEnvironment) method operates on the static 'instance' singleton field, so we
        // must install our freshly-constructed manager as the singleton for shutdown() to find it.
        Field instanceField = IteratorThreadPoolManager.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, mgr);

        return mgr;
    }

    private static Object readField(Object target, String name) throws Exception {
        Field f = IteratorThreadPoolManager.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }

    private static Method findShutdownMethod() {
        // Prefer the static shutdown(IteratorEnvironment) form used by the fix; fall back to an instance shutdown().
        try {
            return IteratorThreadPoolManager.class.getDeclaredMethod("shutdown", org.apache.accumulo.core.iterators.IteratorEnvironment.class);
        } catch (NoSuchMethodException e) {
            // fall through
        }
        try {
            return IteratorThreadPoolManager.class.getDeclaredMethod("shutdown");
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static void invokeShutdown(IteratorThreadPoolManager manager) throws Exception {
        Method m = findShutdownMethod();
        assertNotNull("shutdown method not found", m);
        m.setAccessible(true);
        if (java.lang.reflect.Modifier.isStatic(m.getModifiers())) {
            m.invoke(null, (Object) null);
        } else {
            m.invoke(manager);
        }
    }
}
