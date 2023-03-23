package datawave.core.iterators;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 *
 */
public class IteratorThreadPoolManager {
    private static final Logger log = Logger.getLogger(IteratorThreadPoolManager.class);
    private static final String IVARATOR_THREAD_PROP = "tserver.datawave.ivarator.threads";
    private static final String IVARATOR_THREAD_NAME = "DATAWAVE Ivarator";
    private static final String EVALUATOR_THREAD_PROP = "tserver.datawave.evaluation.threads";
    private static final String EVALUATOR_THREAD_NAME = "DATAWAVE Evaluation";
    private static final int DEFAULT_THREAD_POOL_SIZE = 100;
    private static final String IVARATOR_MAX_TIME_AFTER_ACCESS_PROP = "tserver.datawave.ivarator.maxTimeAfterAccess";
    private static final String IVARATOR_MAX_SCAN_TIMEOUT_PROP = "tserver.datawave.ivarator.maxScanTimeout";
    private static final long DEFAULT_IVARATOR_MAX_TIME_AFTER_ACCESS = TimeUnit.HOURS.toMillis(1);
    private static final long DEFAULT_IVARATOR_MAX_SCAN_TIMEOUT = TimeUnit.HOURS.toMillis(1);

    private Map<String,ExecutorService> threadPools = new TreeMap<>();
    private Cache<String,IvaratorFuture> ivaratorFutures;
    // If an IvaratorFuture is not accessed within this time, then it will be removed from the cache
    private long ivaratorMaxTimeAfterAccess;
    // Each Ivarator has a scanTimeout. This is a system-wide limit which could be
    // useful in causing all Ivarators to terminate if necessary
    private long ivaratorMaxScanTimeout;
    private static final Object instanceSemaphore = new Object();
    private static final String instanceId = Integer.toHexString(instanceSemaphore.hashCode());
    private static volatile IteratorThreadPoolManager instance;

    private IteratorThreadPoolManager(IteratorEnvironment env) {
        final AccumuloConfiguration config;
        if (env != null) {
            config = env.getConfig();
        } else {
            config = DefaultConfiguration.getInstance();
        }
        // create the thread pools
        createExecutorService(IVARATOR_THREAD_PROP, IVARATOR_THREAD_NAME, config);
        createExecutorService(EVALUATOR_THREAD_PROP, EVALUATOR_THREAD_NAME, config);
        ivaratorMaxTimeAfterAccess = getLongPropertyValue(IVARATOR_MAX_TIME_AFTER_ACCESS_PROP, DEFAULT_IVARATOR_MAX_TIME_AFTER_ACCESS, config);
        log.info("Using " + ivaratorMaxTimeAfterAccess + " ms for " + IVARATOR_MAX_TIME_AFTER_ACCESS_PROP);
        ivaratorMaxScanTimeout = getLongPropertyValue(IVARATOR_MAX_SCAN_TIMEOUT_PROP, DEFAULT_IVARATOR_MAX_SCAN_TIMEOUT, config);
        log.info("Using " + ivaratorMaxScanTimeout + " ms for " + IVARATOR_MAX_SCAN_TIMEOUT_PROP);
        // This thread will check for changes to ivaratorMaxTimeAfterAccess and ivaratorMaxScanTimeout
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(config).scheduleWithFixedDelay(() -> {
            try {
                long max = getLongPropertyValue(IVARATOR_MAX_TIME_AFTER_ACCESS_PROP, DEFAULT_IVARATOR_MAX_TIME_AFTER_ACCESS, config);
                if (ivaratorMaxTimeAfterAccess != max) {
                    log.info("Changing " + IVARATOR_MAX_TIME_AFTER_ACCESS_PROP + " to " + max + " ms");
                    ivaratorMaxTimeAfterAccess = max;
                }
            } catch (Throwable t) {
                log.error(t, t);
            }
            try {
                long max = getLongPropertyValue(IVARATOR_MAX_SCAN_TIMEOUT_PROP, DEFAULT_IVARATOR_MAX_SCAN_TIMEOUT, config);
                if (ivaratorMaxScanTimeout != max) {
                    log.info("Changing " + IVARATOR_MAX_SCAN_TIMEOUT_PROP + " to " + max + " ms");
                    ivaratorMaxScanTimeout = max;
                }
            } catch (Throwable t) {
                log.error(t, t);
            }
        }, 1, 10, TimeUnit.SECONDS);

        // If the IvaratorFuture has been not been accessed in ivaratorMaxTimeAfterAccess, then cancel and remove from the cache
        ivaratorFutures = Caffeine.newBuilder().expireAfterAccess(ivaratorMaxTimeAfterAccess, TimeUnit.MILLISECONDS).evictionListener((t, f, removalCause) -> {
            ((IvaratorFuture) f).cancel(true);
        }).build();

        // If Ivarator has been been running for a time greater than either its scanTimeout or the maxScanTimeout,
        // then stop the Ivarator and remove the future from the cache
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(config).scheduleWithFixedDelay(() -> {
            Set<String> tasks = new HashSet<>(ivaratorFutures.asMap().keySet());
            long now = System.currentTimeMillis();
            for (String taskName : tasks) {
                IvaratorFuture future = getIvaratorFuture(taskName, env);
                if (future != null) {
                    DatawaveFieldIndexCachingIteratorJexl ivarator = future.getIvarator();
                    long elapsed = now - ivarator.getStartTime();
                    long ivaratorScanTimeout = ivarator.getScanTimeout();
                    if (!ivarator.isRunning() && ((elapsed > ivaratorScanTimeout) || (elapsed > ivaratorMaxScanTimeout))) {
                        removeIvaratorFuture(taskName, env);
                    }
                }
            }
        }, 1, 60, TimeUnit.SECONDS);
    }

    private ThreadPoolExecutor createExecutorService(final String prop, final String name, final AccumuloConfiguration config) {
        final ThreadPoolExecutor service = createExecutorService(getIntPropertyValue(prop, DEFAULT_THREAD_POOL_SIZE, config), name + " (" + instanceId + ')');
        threadPools.put(name, service);
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(config).scheduleWithFixedDelay(() -> {
            try {
                int max = getIntPropertyValue(prop, DEFAULT_THREAD_POOL_SIZE, config);
                if (service.getMaximumPoolSize() != max) {
                    log.info("Changing " + prop + " to " + max);
                    service.setMaximumPoolSize(max);
                    service.setCorePoolSize(max);
                }
            } catch (Throwable t) {
                log.error(t, t);
            }
        }, 1, 10, TimeUnit.SECONDS);
        return service;
    }

    private ThreadPoolExecutor createExecutorService(int maxThreads, String name) {
        ThreadPoolExecutor pool = ThreadPools.getServerThreadPools().createThreadPool(maxThreads, maxThreads, 5 * 60, TimeUnit.SECONDS, name,
                        new LinkedBlockingQueue<>(), false);
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    private int getIntPropertyValue(final String prop, int defaultValue, AccumuloConfiguration config) {
        if (config != null) {
            Map<String,String> properties = new TreeMap<>();
            config.getProperties(properties, k -> Objects.equals(k, prop));
            if (properties.containsKey(prop)) {
                return Integer.parseInt(properties.get(prop));
            }
        }
        return defaultValue;
    }

    private long getLongPropertyValue(final String prop, long defaultValue, AccumuloConfiguration config) {
        String valueStr = config.get(prop);
        if (valueStr != null) {
            return Long.parseLong(valueStr);
        }
        return defaultValue;
    }

    private static IteratorThreadPoolManager instance(IteratorEnvironment env) {
        if (instance == null) {
            synchronized (instanceSemaphore) {
                if (instance == null) {
                    instance = new IteratorThreadPoolManager(env);
                }
            }
        }
        return instance;
    }

    private Future<?> execute(String name, final Runnable task, final String taskName) {
        return threadPools.get(name).submit(() -> {
            String oldName = Thread.currentThread().getName();
            Thread.currentThread().setName(oldName + " -> " + taskName);
            try {
                task.run();
            } finally {
                Thread.currentThread().setName(oldName);
            }
        });
    }

    public static IvaratorFuture getIvaratorFuture(String taskName, IteratorEnvironment env) {
        return instance(env).ivaratorFutures.getIfPresent(taskName);
    }

    public static void removeIvaratorFuture(String taskName, IteratorEnvironment env) {
        IvaratorFuture future = instance(env).ivaratorFutures.getIfPresent(taskName);
        if (future != null) {
            if (future.getIvarator().isRunning()) {
                future.cancel(true);
                future.getIvarator().waitUntilComplete();
            }
            instance(env).ivaratorFutures.invalidate(taskName);
        }
    }

    public static IvaratorFuture executeIvarator(DatawaveFieldIndexCachingIteratorJexl ivarator, Runnable task, String taskName, IteratorEnvironment env) {
        IvaratorFuture future = instance(env).ivaratorFutures.getIfPresent(taskName);
        if (future == null) {
            future = new IvaratorFuture(instance(env).execute(IVARATOR_THREAD_NAME, task, taskName), ivarator);
            instance(env).ivaratorFutures.put(taskName, future);
        }
        return future;
    }

    public static Future<?> executeEvaluation(Runnable task, String taskName, IteratorEnvironment env) {
        return instance(env).execute(EVALUATOR_THREAD_NAME, task, taskName);
    }
}
