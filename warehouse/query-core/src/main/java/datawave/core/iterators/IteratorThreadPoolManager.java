package datawave.core.iterators;

import static datawave.core.iterators.IvaratorRunnable.Status.CREATED;
import static datawave.core.iterators.IvaratorRunnable.Status.RUNNING;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.log4j.Logger;
import org.checkerframework.checker.index.qual.NonNegative;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
    private static final String IVARATOR_RUNNABLE_TIMEOUT_MINUTES_PROP = "tserver.datawave.ivarator.runnableTimeoutMinutes";
    private static final long DEFAULT_IVARATOR_RUNNABLE_TIMEOUT_MINUTES = 60;

    private Map<String,ThreadPoolExecutor> threadPools = new TreeMap<>();
    private Cache<String,IvaratorFuture> ivaratorFutures;
    // Each Ivarator has a scanTimeout. This is a system-wide limit which could be useful in terminating
    // all Ivarators if necessary. It is also used to ensure that abandoned IvaratorFutures are removed.
    private long ivaratorRunnableTimeoutMinutes = DEFAULT_IVARATOR_RUNNABLE_TIMEOUT_MINUTES;
    private static final Object instanceSemaphore = new Object();
    private static final String instanceId = Integer.toHexString(instanceSemaphore.hashCode());
    private static volatile IteratorThreadPoolManager instance;

    private IteratorThreadPoolManager(IteratorEnvironment env) {
        final AccumuloConfiguration accumuloConfiguration;
        final PluginEnvironment pluginEnv;
        if (env != null) {
            pluginEnv = env.getPluginEnv();
            accumuloConfiguration = env.getConfig();
        } else {
            pluginEnv = null;
            accumuloConfiguration = DefaultConfiguration.getInstance();
        }
        // create the thread pools
        createExecutorService(IVARATOR_THREAD_PROP, IVARATOR_THREAD_NAME, env);
        createExecutorService(EVALUATOR_THREAD_PROP, EVALUATOR_THREAD_NAME, env);
        ivaratorRunnableTimeoutMinutes = getLongPropertyValue(IVARATOR_RUNNABLE_TIMEOUT_MINUTES_PROP, DEFAULT_IVARATOR_RUNNABLE_TIMEOUT_MINUTES, pluginEnv);
        log.info("Using " + ivaratorRunnableTimeoutMinutes + " minutes for " + IVARATOR_RUNNABLE_TIMEOUT_MINUTES_PROP);
        // This thread will check for changes to ivaratorRunnableTimeoutMinutes
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(accumuloConfiguration).scheduleWithFixedDelay(() -> {
            try {
                long value = getLongPropertyValue(IVARATOR_RUNNABLE_TIMEOUT_MINUTES_PROP, DEFAULT_IVARATOR_RUNNABLE_TIMEOUT_MINUTES, pluginEnv);
                if (ivaratorRunnableTimeoutMinutes != value) {
                    log.info("Changing " + IVARATOR_RUNNABLE_TIMEOUT_MINUTES_PROP + " to " + value + " minutes");
                    ivaratorRunnableTimeoutMinutes = value;
                }
            } catch (Throwable t) {
                log.error(t, t);
            }
        }, 1, 10, TimeUnit.SECONDS);

        // If the IvaratorFuture has not been created, modified, or read in ivaratorRunnableTimeoutMinutes*1.1, then remove
        // the IvaratorFuture from the cache and if running, suspend the IvaratorRunnable with an IvaratorException
        ivaratorFutures = Caffeine.newBuilder().expireAfter(new Expiry<String,IvaratorFuture>() {
            // This allows us to change ivaratorRunnableTimeoutMinutes on an existing cache
            // instead of only setting expiration times when the cache is created
            // Any expiration should happen after the IvaratorRunnable times out after ivaratorRunnableTimeout

            @Override
            public long expireAfterCreate(String key, IvaratorFuture ivaratorFuture, long currentTime) {
                // currentTime is the current time in nanoseconds. We will return a
                // duration such that this entry will expire that many nanoseconds from now
                return TimeUnit.MINUTES.toNanos((long) (ivaratorRunnableTimeoutMinutes * 1.1));
            }

            @Override
            public long expireAfterUpdate(String key, IvaratorFuture ivaratorFuture, long currentTime, @NonNegative long currentDuration) {
                // currentDuration is the current time in nanoseconds that needs to elapse from now until the entry will be expired.
                // We will return an updated duration such that this entry will expire that many nanoseconds from now
                return TimeUnit.MINUTES.toNanos((long) (ivaratorRunnableTimeoutMinutes * 1.1));
            }

            @Override
            public long expireAfterRead(String key, IvaratorFuture ivaratorFuture, long currentTime, @NonNegative long currentDuration) {
                // currentDuration is the current time in nanoseconds that needs to elapse from now until the entry will be expired.
                // We will return an updated duration such that this entry will expire that many nanoseconds from now
                return TimeUnit.MINUTES.toNanos((long) (ivaratorRunnableTimeoutMinutes * 1.1));
            }
        }).evictionListener((t, f, removalCause) -> {
            // ensure that the task does not get executed if it has not started
            boolean removedBeforeExecution = instance(env).threadPools.get(IVARATOR_THREAD_NAME).remove((Runnable) f.getFuture());
            if (!removedBeforeExecution) {
                // If the IvaratorRunnable is still running, then send an IvaratorException into suspend()
                // just in case the IvaratorFuture is being used. This should not happen, but if it does,
                // any call to IvaratorFuture.get() will throw the Exception
                f.getIvaratorRunnable().suspend(new IvaratorException("IvaratorFuture evicted from the cache"));
            }
            log.info("IvaratorFuture for queryId:" + f.getIvaratorRunnable().getQueryId() + " evicted from the cache");
        }).build();

        // If Ivarator has been running for a time greater than either its scanTimeout or the ivaratorRunnableTimeoutMinutes,
        // then stop the Ivarator and remove the future from the cache
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(accumuloConfiguration).scheduleWithFixedDelay(() -> {
            Map<String,Integer> queryToTaskMap = new TreeMap<>();
            long now = System.currentTimeMillis();
            ivaratorFutures.asMap().forEach((String taskName, IvaratorFuture future) -> {
                DatawaveFieldIndexCachingIteratorJexl ivarator = future.getIvarator();
                IvaratorRunnable ivaratorRunnable = future.getIvaratorRunnable();
                long timeInIvaratorFillSortedSet = now - ivarator.getStartTime();
                long timeInIvaratorRunnable = now - ivaratorRunnable.getCurrentStartTime();
                long scanTimeout = ivarator.getScanTimeout();
                if (ivaratorRunnable.getStatus().equals(RUNNING)) {
                    if (timeInIvaratorFillSortedSet > scanTimeout) {
                        ivarator.setTimedOut(true);
                        timeoutIvarator(future, env, new IvaratorException("Ivarator query timed out, exceeded scanTimeout=" + scanTimeout));
                    } else if (timeInIvaratorRunnable > (ivaratorRunnableTimeoutMinutes * 60 * 1000)) {
                        ivarator.setTimedOut(true);
                        timeoutIvarator(future, env, new IvaratorException(
                                        "Ivarator query timed out, exceeded ivaratorRunnableTimeoutMinutes=" + ivaratorRunnableTimeoutMinutes));
                    } else {
                        String queryId = ivaratorRunnable.getQueryId();
                        Integer numTasks = queryToTaskMap.get(queryId);
                        if (numTasks == null) {
                            numTasks = 0;
                        }
                        queryToTaskMap.put(queryId, numTasks + 1);
                    }
                }
            });
            ThreadPoolExecutor exec = threadPools.get(IVARATOR_THREAD_NAME);
            log.info(String.format("Ivarator threadPool max:%d, running:%d, waiting:%d, queryId/numRunning:%s", exec.getMaximumPoolSize(),
                            exec.getActiveCount(), exec.getQueue().size(), queryToTaskMap));
        }, 1, 60, TimeUnit.SECONDS);
    }

    private ThreadPoolExecutor createExecutorService(final String prop, final String name, IteratorEnvironment env) {
        final PluginEnvironment pluginEnv;
        if (env != null) {
            pluginEnv = env.getPluginEnv();
        } else {
            pluginEnv = null;
        }
        int maxThreads = getIntPropertyValue(prop, DEFAULT_THREAD_POOL_SIZE, pluginEnv);
        final ThreadPoolExecutor service = createExecutorService(maxThreads, name + " (" + instanceId + ')');
        threadPools.put(name, service);
        Executors.newScheduledThreadPool(maxThreads).scheduleWithFixedDelay(() -> {
            try {
                // Very important to not use the accumuloConfiguration in this thread and instead use the pluginEnv
                // The accumuloConfiguration caches table ids which may no longer exist down the road.
                int max = getIntPropertyValue(prop, DEFAULT_THREAD_POOL_SIZE, pluginEnv);
                if (service.getMaximumPoolSize() != max) {
                    log.info("Changing " + prop + " to " + max);
                    // if raising the max size, then we need to set the max first before the core
                    // otherwise we get an exception. Same in the reverse.
                    if (service.getMaximumPoolSize() < max) {
                        service.setMaximumPoolSize(max);
                        service.setCorePoolSize(max);
                    } else {
                        service.setCorePoolSize(max);
                        service.setMaximumPoolSize(max);
                    }
                }
            } catch (Throwable t) {
                log.error(t, t);
            }
        }, 1, 10, TimeUnit.SECONDS);
        return service;
    }

    private ThreadPoolExecutor createExecutorService(int maxThreads, String name) {
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(maxThreads, maxThreads, 5, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), tf);
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    private int getIntPropertyValue(final String prop, int defaultValue, PluginEnvironment pluginEnv) {
        if (pluginEnv != null && pluginEnv.getConfiguration() != null) {
            String value = pluginEnv.getConfiguration().get(prop);
            try {
                if (value != null) {
                    return Integer.parseInt(value);
                }
            } catch (Exception e) {
                log.error("property:" + prop + " value:" + value + " failed to parse, using default value:" + defaultValue, e);
            }
        }
        return defaultValue;
    }

    private long getLongPropertyValue(final String prop, long defaultValue, PluginEnvironment pluginEnv) {
        if (pluginEnv != null && pluginEnv.getConfiguration() != null) {
            String value = pluginEnv.getConfiguration().get(prop);
            try {
                if (value != null) {
                    return Long.parseLong(value);
                }
            } catch (Exception e) {
                log.error("property:" + prop + " value:" + value + " failed to parse, using default value:" + defaultValue, e);
            }
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

    public static void removeIvarator(String taskName, IteratorEnvironment env) {
        instance(env).ivaratorFutures.invalidate(taskName);
    }

    public static void suspendIvarator(IvaratorFuture future, boolean assumeExecuting, boolean removeFuture, IteratorEnvironment env) {
        if (future != null) {
            IvaratorRunnable ivaratorRunnable = future.getIvaratorRunnable();
            // ensure that the task does not get executed if it has not started
            boolean removedBeforeExecution = instance(env).threadPools.get(IVARATOR_THREAD_NAME).remove((Runnable) future.getFuture());
            if (!removedBeforeExecution) {
                if (assumeExecuting && ivaratorRunnable.getStatus().equals(CREATED)) {
                    // if the task was not in the workQueue, then wait for it to start
                    // the duration is used to prevent the Thread from waiting indefinitely
                    ivaratorRunnable.waitUntilStarted(60, TimeUnit.SECONDS);
                }
                // this will cause the IvaratorRunnable to stop in a controlled manner
                ivaratorRunnable.suspend(null);
            }
            if (removeFuture) {
                removeIvarator(ivaratorRunnable.getTaskName(), env);
            }
        }
    }

    public static void timeoutIvarator(IvaratorFuture future, IteratorEnvironment env, Exception e) {
        if (future != null) {
            // this will cause the IvaratorRunnable to stop (if it is running) in a controlled manner
            future.getIvaratorRunnable().suspend(e);
            removeIvarator(future.getIvaratorRunnable().getTaskName(), env);
        }
    }

    public static IvaratorFuture executeIvarator(IvaratorRunnable ivaratorRunnable, String taskName, IteratorEnvironment env) {
        IvaratorFuture future = instance(env).ivaratorFutures.getIfPresent(taskName);
        if (future == null) {
            future = new IvaratorFuture(instance(env).execute(IVARATOR_THREAD_NAME, ivaratorRunnable, taskName), ivaratorRunnable);
            instance(env).ivaratorFutures.put(taskName, future);
        }
        return future;
    }

    public static Future<?> executeEvaluation(Runnable task, String taskName, IteratorEnvironment env) {
        return instance(env).execute(EVALUATOR_THREAD_NAME, task, taskName);
    }
}
