package datawave.microservice.query.executor.task;

import datawave.microservice.query.executor.QueryExecutor;
import org.apache.log4j.Logger;

public class ExecutorStatusLogger {
    private Logger log = Logger.getLogger(ExecutorStatusLogger.class);
    private String lastThreadPoolStatus = "";
    private volatile long lastThreadPoolStatusUpdate = 0;
    private static final int TEN_MINUTES = 10 * 60 * 1000;
    
    private boolean hasUpdatedStatus(QueryExecutor executor) {
        return (!lastThreadPoolStatus.equals(executor.toString()) || ((System.currentTimeMillis() - lastThreadPoolStatusUpdate) > TEN_MINUTES));
    }
    
    public String getStatus(QueryExecutor executor) {
        lastThreadPoolStatus = executor.toString();
        lastThreadPoolStatusUpdate = System.currentTimeMillis();
        return lastThreadPoolStatus;
    }
    
    public void logStatus(QueryExecutor executor) {
        if (hasUpdatedStatus(executor)) {
            log.info("Executor status: " + getStatus(executor));
        }
    }
}
