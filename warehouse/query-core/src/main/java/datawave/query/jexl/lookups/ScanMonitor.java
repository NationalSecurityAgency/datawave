package datawave.query.jexl.lookups;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple runnable that registers futures and cancels them when the timeout is exceeded.
 */
public class ScanMonitor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ScanMonitor.class);

    private final Map<String,IndexScanTask> tasks = new HashMap<>();

    private int taskID = 0;
    private final long monitorIntervalMillis;

    public ScanMonitor() {
        this(5L);
    }

    public ScanMonitor(long monitorIntervalMillis) {
        this.monitorIntervalMillis = monitorIntervalMillis;
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
        synchronized (tasks) {
            log.info("registering task: {}", taskID);
            tasks.put(String.valueOf(taskID++), new IndexScanTask(future, timeout));
        }
    }

    public void registerTask(Future<?> future, CountDownLatch latch, long timeout) {
        synchronized (tasks) {
            log.info("registering task: {}", taskID);
            tasks.put(String.valueOf(taskID++), new IndexScanTask(future, latch, timeout));
        }
    }

    @Override
    public void run() {
        while (true) {

            // always check for interrupts first
            if (Thread.currentThread().isInterrupted()) {
                log.info("thread interrupted, stopping");
                break;
            }

            synchronized (tasks) {
                long currentTime = System.currentTimeMillis();
                Set<String> keys = new HashSet<>(tasks.keySet());
                for (String key : keys) {
                    IndexScanTask task = tasks.get(key);
                    if (task.isDone(currentTime)) {
                        log.info("closing task {}", key);
                        task.cancelFuture();
                        synchronized (tasks) {
                            log.info("removing task {}", key);
                            tasks.remove(key);
                        }
                    }
                }
            }

            try {
                Thread.sleep(monitorIntervalMillis);
            } catch (InterruptedException e) {
                log.warn("interrupted");
                // break;
            }
        }
    }

    private static class IndexScanTask {

        private final Future<?> future;
        private final long start;
        private final long timeout;

        private CountDownLatch latch;

        public IndexScanTask(Future<?> future, long timeout) {
            this.future = future;
            this.start = System.currentTimeMillis();
            this.timeout = timeout;
        }

        public IndexScanTask(Future<?> future, CountDownLatch latch, long timeout) {
            this.future = future;
            this.start = System.currentTimeMillis();
            this.timeout = timeout;
            this.latch = latch;
        }

        public boolean isDone(long current) {
            long elapsed = current - start;
            return elapsed >= timeout;
        }

        public void cancelFuture() {
            if (future != null && !future.isCancelled()) {
                if (latch != null) {
                    latch.countDown();
                }
                future.cancel(true);
            }
        }
    }

}
