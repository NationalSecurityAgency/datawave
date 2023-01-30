package datawave.ingest.util;

import org.apache.log4j.Logger;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Utilities for working with ThreadPools.
 */
public class ThreadUtil {
    
    private static final Logger logger = Logger.getLogger(ThreadUtil.class);
    
    /**
     * Shuts down the executor and gives tasks that are still in progress the given amount of time before continuing.
     *
     * @param executor
     *            a executor
     * @param timeToWait
     *            the time to wait
     * @param unit
     *            the time unit
     * @return true if all tasks completed, false, if we interrupted and continued.
     */
    public static boolean shutdownAndWait(ThreadPoolExecutor executor, long timeToWait, TimeUnit unit) {
        executor.shutdown();
        try {
            executor.awaitTermination(timeToWait, unit);
            return true;
        } catch (InterruptedException e) {
            logger.warn("Closed thread pool but not all threads completed successfully.");
            return false;
        }
    }
    
    /**
     * Waits for all active threads in the thread pool to complete.
     *
     * @param log
     *            a logger
     * @param executor
     *            the thread executor
     * @param type
     *            the type
     * @param poolSize
     *            the pool size
     * @param workUnits
     *            the work time units
     * @param start
     *            the start time
     * @return time taken to complete all tasks
     */
    public static long waitForThreads(Logger log, ThreadPoolExecutor executor, String type, int poolSize, long workUnits, long start) {
        long cur = System.currentTimeMillis();
        int active = executor.getActiveCount();
        int qSize = executor.getQueue().size();
        long compl = executor.getCompletedTaskCount();
        long time = 0;
        while (((qSize > 0) || (active > 0) || (compl < workUnits)) && !executor.isTerminated()) {
            if (log != null && (time < (System.currentTimeMillis() - (1000L * 10L)))) {
                log.info(type + " running, T: " + active + "/" + poolSize + ", Completed: " + compl + "/" + workUnits + ", " + ", Remaining: " + qSize + ", "
                                + (cur - start) + " ms elapsed");
                time = System.currentTimeMillis();
            }
            cur = System.currentTimeMillis();
            active = executor.getActiveCount();
            qSize = executor.getQueue().size();
            compl = executor.getCompletedTaskCount();
        }
        if (log != null) {
            log.info("Finished Waiting for " + type + " running, T: " + active + "/" + poolSize + ", Completed: " + compl + "/" + workUnits + ", "
                            + ", Remaining: " + qSize + ", " + (cur - start) + " ms elapsed");
        }
        
        long stop = System.currentTimeMillis();
        return (stop - start);
    }
    
}
