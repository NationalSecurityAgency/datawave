package datawave.core.iterators;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.log4j.Logger;

/**
 * 
 */
public class IteratorThreadPoolManager {
    private static final Logger log = Logger.getLogger(IteratorThreadPoolManager.class);
    private static final String IVARATOR_THREAD_PROP = "tserver.datawave.ivarator.threads";
    private static final String IVARATOR_THREAD_NAME = "DATAWAVE Ivarator";
    private static final String EVALUATOR_THREAD_PROP = "tserver.datawave.evaluation.threads";
    private static final String EVALUATOR_THREAD_NAME = "DATAWAVE Evaluation";
    private ExecutorService ivaratorThreadPool;
    private ExecutorService evaluationThreadPool;
    private static final int DEFAULT_THREAD_POOL_SIZE = 100;
    
    private Map<String,ExecutorService> threadPools = new TreeMap<>();
    
    private ServerConfigurationFactory confFactory;
    
    private static final Object instanceSemaphore = new Object();
    private static final String instanceId = Integer.toHexString(instanceSemaphore.hashCode());
    private static volatile IteratorThreadPoolManager instance;
    
    private IteratorThreadPoolManager() {
        // create the thread pools\
        try {
            this.confFactory = new ServerConfigurationFactory(HdfsZooInstance.getInstance());
        } catch (Throwable e) {
            log.error("Unable to get the accumulo configuration, using default thread pool sizes (" + DEFAULT_THREAD_POOL_SIZE + " per pool)");
        }
        this.ivaratorThreadPool = createExecutorService(IVARATOR_THREAD_PROP, IVARATOR_THREAD_NAME);
        this.evaluationThreadPool = createExecutorService(EVALUATOR_THREAD_PROP, EVALUATOR_THREAD_NAME);
    }
    
    private ThreadPoolExecutor createExecutorService(final String prop, final String name) {
        final ThreadPoolExecutor service = createExecutorService(getMaxThreads(prop), name + " (" + instanceId + ')');
        threadPools.put(name, service);
        SimpleTimer.getInstance(AccumuloConfiguration.getDefaultConfiguration()).schedule(() -> {
            try {
                
                int max = getMaxThreads(prop);
                if (service.getMaximumPoolSize() != max) {
                    log.info("Changing " + prop + " to " + max);
                    service.setCorePoolSize(max);
                    service.setMaximumPoolSize(max);
                }
            } catch (Throwable t) {
                log.error(t, t);
            }
        }, 1000, 10 * 1000);
        return service;
    }
    
    private ThreadPoolExecutor createExecutorService(int maxThreads, String name) {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(maxThreads, maxThreads, 5 * 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                        new NamingThreadFactory(name));
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }
    
    private int getMaxThreads(final String prop) {
        if (this.confFactory != null) {
            AccumuloConfiguration conf = this.confFactory.getConfiguration();
            Map<String,String> properties = new TreeMap<>();
            conf.getProperties(properties, new AccumuloConfiguration.MatchFilter(prop));
            if (properties.containsKey(prop)) {
                return Integer.parseInt(properties.get(prop));
            }
        }
        return DEFAULT_THREAD_POOL_SIZE;
    }
    
    private static IteratorThreadPoolManager instance() {
        if (instance == null) {
            synchronized (instanceSemaphore) {
                if (instance == null) {
                    instance = new IteratorThreadPoolManager();
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
    
    public static Future<?> executeIvarator(Runnable task, String taskName) {
        return instance().execute(IVARATOR_THREAD_NAME, task, taskName);
    }
    
    public static Future<?> executeEvaluation(Runnable task, String taskName) {
        return instance().execute(EVALUATOR_THREAD_NAME, task, taskName);
    }
    
}
