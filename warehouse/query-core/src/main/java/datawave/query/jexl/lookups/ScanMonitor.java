package datawave.query.jexl.lookups;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<String,IndexScanTask> tasks = new ConcurrentHashMap<>();

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
        if (log.isTraceEnabled()) {
            log.trace("registering task: {}", taskID);
        }
        String id = String.valueOf(taskID++);
        IndexScanTask task = new IndexScanTask(future, timeout);
        tasks.put(id, task);
    }

    @Override
    public void run() {
        while (true) {

            // always check for interrupts first
            if (currentThread().isInterrupted()) {
                if (log.isDebugEnabled()) {
                    log.debug("thread interrupted, stopping");
                }
                break;
            }

            long currentTime = System.currentTimeMillis();
            Iterator<String> iter = tasks.keySet().iterator();
            while (iter.hasNext()) {
                String key = iter.next();
                IndexScanTask task = tasks.get(key);
                if (task.isComplete(currentTime)) {
                    if (log.isDebugEnabled()) {
                        log.debug("closing task {}", key);
                    }
                    task.cancelFuture();
                    iter.remove();
                }
            }

            try {
                sleep(monitorIntervalMillis);
            } catch (InterruptedException e) {
                if (log.isDebugEnabled()) {
                    log.debug("thread interrupted, stopping");
                }
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

    /**
     * A utility class that associates a future with a timeout
     */
    private static class IndexScanTask {

        private final Future<?> future;
        private final long startMillis;
        private final long timeoutMillis;

        /**
         * Simple constructor that accepts a future and a timeout
         *
         * @param future
         *            the future
         * @param timeoutMillis
         *            the timeout in milliseconds
         */
        public IndexScanTask(Future<?> future, long timeoutMillis) {
            this.future = future;
            this.startMillis = System.currentTimeMillis();
            this.timeoutMillis = timeoutMillis;
        }

        /**
         * A task is complete if the future is done or canceled, or if the timeout is exceeded
         *
         * @param currentMillis
         *            the current time since the task was registered
         * @return true if the task is complete
         */
        public boolean isComplete(long currentMillis) {
            return future.isDone() || future.isCancelled() || (currentMillis - startMillis >= timeoutMillis);
        }

        /**
         * Cancels the future if it exists and is not already canceled
         */
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
