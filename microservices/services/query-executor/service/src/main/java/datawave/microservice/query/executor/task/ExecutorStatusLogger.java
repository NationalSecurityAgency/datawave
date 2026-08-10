package datawave.microservice.query.executor.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.microservice.query.executor.QueryExecutor;

public class ExecutorStatusLogger {
    private Logger log = LoggerFactory.getLogger(ExecutorStatusLogger.class);
    private String lastThreadPoolStatus = "";
    private volatile long lastThreadPoolStatusUpdate = 0;

    private String getUpdatedStatus(QueryExecutor executor) {
        String newStatus = executor.toString();
        if (lastThreadPoolStatus.equals(newStatus)) {
            if ((System.currentTimeMillis() - lastThreadPoolStatusUpdate) < executor.getExecutorProperties().getLogStatusPeriodMs()) {
                newStatus = null;
            }
        } else {
            if ((System.currentTimeMillis() - lastThreadPoolStatusUpdate) < executor.getExecutorProperties().getLogStatusWhenChangedMs()) {
                newStatus = null;
            }
        }
        return newStatus;
    }

    public void logStatus(QueryExecutor executor) {
        String newStatus = getUpdatedStatus(executor);
        if (newStatus != null) {
            lastThreadPoolStatus = newStatus;
            lastThreadPoolStatusUpdate = System.currentTimeMillis();
            log.info("Executor status: " + newStatus);
        }
    }
}
