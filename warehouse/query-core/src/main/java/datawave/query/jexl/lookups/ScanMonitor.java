package datawave.query.jexl.lookups;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

/**
 * A simple runnable that registers futures and cancels them when the timeout is exceeded. Monitoring happens in an internal executor.
 * <p>
 * No further configuration is necessary after calling {@link ScanMonitor#of(String, QueryUncaughtExceptionHandler)}
 */
public class ScanMonitor implements Runnable, Closeable {

    private static final Logger log = LoggerFactory.getLogger(ScanMonitor.class);

    private static final long DEFAULT_INTERVAL_MILLIS = 25L;

    private int taskID = 0;
    private final Map<String,IndexScanTask> tasks = new HashMap<>();

    private final long monitorIntervalMillis;
    private final ExecutorService executor;

    /**
     * Static entrypoint that creates a {@link ScanMonitor} for a given query id and exception handler. This constructor uses the
     * {@link #DEFAULT_INTERVAL_MILLIS} of 25 milliseconds.
     *
     * @param id
     *            the query id
     * @param handler
     *            the uncaught exception handler
     * @return a {@link ScanMonitor}
     */
    public static ScanMonitor of(String id, QueryUncaughtExceptionHandler handler) {
        return new ScanMonitor(DEFAULT_INTERVAL_MILLIS, id, handler);
    }

    /**
     * Static entrypoint that creates a {@link ScanMonitor} for a given monitor interval, query id and exception handler
     *
     * @param monitorIntervalMillis
     *            the interval between monitoring checks in milliseconds
     * @param id
     *            the query id
     * @param handler
     *            the uncaught exception handler
     * @return a {@link ScanMonitor}
     */
    public static ScanMonitor of(long monitorIntervalMillis, String id, QueryUncaughtExceptionHandler handler) {
        return new ScanMonitor(monitorIntervalMillis, id, handler);
    }

    /**
     * Private constructor creates a thread factory, executor and submits this runnable
     *
     * @param monitorIntervalMillis
     *            the interval between monitoring checks
     * @param id
     *            the query id
     * @param handler
     *            the uncaught exception handler
     */
    private ScanMonitor(long monitorIntervalMillis, String id, QueryUncaughtExceptionHandler handler) {
        this.monitorIntervalMillis = monitorIntervalMillis;

        ScanMonitorThreadFactory threadFactory = new ScanMonitorThreadFactory(id, handler);
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.executor.submit(this);
    }

    /**
     * Registers the future along with the timeout
     *
     * @param future
     *            the future
     * @param timeout
     *            the timeout
     */
    public void registerTask(Future<?> future, long timeout) {
        log.info("registering task: {}", taskID);
        String id = String.valueOf(taskID++);
        IndexScanTask task = new IndexScanTask(future, timeout);

        synchronized (tasks) {
            tasks.put(id, task);
        }
    }

    @Override
    public void run() {
        while (true) {

            // always check for interrupts first
            if (currentThread().isInterrupted()) {
                log.info("thread interrupted, stopping");
                break;
            }

            synchronized (tasks) {
                long currentTime = System.currentTimeMillis();
                var iter = tasks.keySet().iterator();
                while (iter.hasNext()) {
                    String key = iter.next();
                    IndexScanTask task = tasks.get(key);
                    if (task.isDone(currentTime)) {
                        log.info("closing task {}", key);
                        task.cancelFuture();
                        iter.remove();
                    }
                }
            }

            try {
                sleep(monitorIntervalMillis);
            } catch (InterruptedException e) {
                log.info("thread interrupted, stopping");
                break;
            }
        }
    }

    @Override
    public void close() {
        if (executor != null && !executor.isShutdown()) {
            executor.shutdownNow();
        }
    }

    private static class IndexScanTask {

        private final Future<?> future;
        private final long start;
        private final long timeout;

        public IndexScanTask(Future<?> future, long timeout) {
            this.future = future;
            this.start = System.currentTimeMillis();
            this.timeout = timeout;
        }

        public boolean isDone(long current) {
            long elapsed = current - start;
            return elapsed >= timeout;
        }

        public void cancelFuture() {
            if (future != null && !future.isCancelled()) {
                future.cancel(true);
            }
        }
    }

    /**
     * A simple thread factory for the {@link ScanMonitor}
     */
    protected static class ScanMonitorThreadFactory implements ThreadFactory {
        private final String queryId;
        private final QueryUncaughtExceptionHandler uncaughtExceptionHandler;
        private final ThreadFactory threadFactory = Executors.defaultThreadFactory();

        public ScanMonitorThreadFactory(String queryId, QueryUncaughtExceptionHandler uncaughtExceptionHandler) {
            this.queryId = queryId;
            this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = threadFactory.newThread(r);
            thread.setName(queryId + " monitor");
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
            return thread;
        }
    }
}
