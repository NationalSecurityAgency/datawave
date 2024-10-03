package datawave.core.iterators;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
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
import org.apache.log4j.Logger;

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

    private Map<String,ExecutorService> threadPools = new TreeMap<>();

    private static final Object instanceSemaphore = new Object();
    private static final String instanceId = Integer.toHexString(instanceSemaphore.hashCode());
    private static volatile IteratorThreadPoolManager instance;

    private IteratorThreadPoolManager(IteratorEnvironment env) {
        // create the thread pools
        createExecutorService(IVARATOR_THREAD_PROP, IVARATOR_THREAD_NAME, env);
        createExecutorService(EVALUATOR_THREAD_PROP, EVALUATOR_THREAD_NAME, env);
    }

    private ThreadPoolExecutor createExecutorService(final String prop, final String name, final IteratorEnvironment env) {
        final PluginEnvironment pluginEnv;
        if (env != null) {
            pluginEnv = env.getPluginEnv();
        } else {
            pluginEnv = null;
        }
        final ThreadPoolExecutor service = createExecutorService(getMaxThreads(prop, pluginEnv), name + " (" + instanceId + ')');
        threadPools.put(name, service);
        Executors.newScheduledThreadPool(getMaxThreads(prop, pluginEnv)).scheduleWithFixedDelay(() -> {
            try {
                // Very important to not use the accumuloConfiguration in this thread and instead use the pluginEnv
                // The accumuloConfiguration caches table ids which may no longer exist down the road.
                int max = getMaxThreads(prop, pluginEnv);
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

    private int getMaxThreads(final String prop, PluginEnvironment pluginEnv) {
        if (pluginEnv != null && pluginEnv.getConfiguration() != null) {
            String value = pluginEnv.getConfiguration().get(prop);
            if (value != null) {
                return Integer.parseInt(value);
            }
        }
        return DEFAULT_THREAD_POOL_SIZE;
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

    public static Future<?> executeIvarator(Runnable task, String taskName, IteratorEnvironment env) {
        return instance(env).execute(IVARATOR_THREAD_NAME, task, taskName);
    }

    public static Future<?> executeEvaluation(Runnable task, String taskName, IteratorEnvironment env) {
        return instance(env).execute(EVALUATOR_THREAD_NAME, task, taskName);
    }

}
