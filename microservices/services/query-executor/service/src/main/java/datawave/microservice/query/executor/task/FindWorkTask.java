package datawave.microservice.query.executor.task;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryStorageLock;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;

public class FindWorkTask implements Callable<Void> {
    private Logger log = LoggerFactory.getLogger(FindWorkTask.class);
    protected final QueryStorageCache queryStorageCache;
    protected final QueryExecutor executor;
    protected final ExecutorStatusLogger statusLogger;
    
    private final String originService;
    private final String destinationService;
    
    public FindWorkTask(QueryStorageCache queryStorageCache, QueryExecutor executor, String originService, String destinationService,
                    ExecutorStatusLogger statusLogger) {
        this.queryStorageCache = queryStorageCache;
        this.executor = executor;
        this.originService = originService;
        this.destinationService = destinationService;
        this.statusLogger = statusLogger;
    }
    
    @Override
    public Void call() throws Exception {
        statusLogger.logStatus(executor);
        for (QueryStatus queryStatus : queryStorageCache.getQueryStatus()) {
            // only process the queries that match this pool!!!
            if (queryStatus.getQueryKey().getQueryPool().equalsIgnoreCase(executor.getExecutorProperties().getPool())) {
                String queryId = queryStatus.getQueryKey().getQueryId();
                switch (queryStatus.getQueryState()) {
                    case CLOSE:
                        if (executor.isWorkingOn(queryId)) {
                            log.debug("Closing " + queryId);
                            executor.handleRemoteRequest(QueryRequest.close(queryId), originService, destinationService);
                        }
                        break;
                    case CANCEL:
                        if (executor.isWorkingOn(queryId)) {
                            log.debug("Cancelling " + queryId);
                            executor.handleRemoteRequest(QueryRequest.cancel(queryId), originService, destinationService);
                        }
                        break;
                    case CREATE:
                        // Should we create a new thread to handle the remote request instead so that we can return immediately?
                        // even if the next task is to plan, this will take care of it
                        switch (queryStatus.getCreateStage()) {
                            case CREATE:
                            case PLAN:
                                // recover orphaned tasks
                                recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.READY);
                                log.debug("Creating " + queryId);
                                executor.handleRemoteRequest(QueryRequest.create(queryId), originService, destinationService);
                                break;
                            case TASK:
                                // recover orphaned tasks, however create tasks should be failed as we were already creating tasks
                                recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.READY,
                                                Collections.singletonMap(QueryRequest.Method.CREATE, TaskStates.TASK_STATE.FAILED));
                                log.debug("Nexting " + queryId);
                                executor.handleRemoteRequest(QueryRequest.next(queryId), originService, destinationService);
                                break;
                            case RESULTS:
                                // recover orphaned tasks, however create tasks should be completed as all tasks have already been created
                                recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.READY,
                                                Collections.singletonMap(QueryRequest.Method.CREATE, TaskStates.TASK_STATE.COMPLETED));
                                log.debug("Nexting " + queryId);
                                executor.handleRemoteRequest(QueryRequest.next(queryId), originService, destinationService);
                                break;
                        }
                        break;
                    case PLAN:
                        log.debug("Planning " + queryId);
                        recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.READY);
                        // Should we create a new thread to handle the remote request instead so that we can return immediately?
                        // even if the next task is to plan, this will take care of it
                        executor.handleRemoteRequest(QueryRequest.plan(queryId), originService, destinationService);
                        break;
                    case PREDICT:
                        log.debug("Predicting " + queryId);
                        recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.READY);
                        // Should we create a new thread to handle the remote request instead so that we can return immediately?
                        // even if the next task is to plan, this will take care of it
                        executor.handleRemoteRequest(QueryRequest.predict(queryId), originService, destinationService);
                        break;
                    case DEFINE:
                    case FAIL:
                        // noop
                        break;
                }
            }
        }
        return null;
    }
    
    /**
     * For the specified query id, find tasks that are orphaned and reset their state
     *
     * @param queryId
     *            The query id
     * @param state
     *            The state to reset orphaned tasks to
     */
    public void recoverOrphanedTasks(String queryId, TaskStates.TASK_STATE state) {
        recoverOrphanedTasks(queryId, state, false);
    }
    
    /**
     * For the specified query id, find tasks that are orphaned and reset their state
     *
     * @param queryId
     *            The query id
     * @param state
     *            The state to reset orphaned tasks to
     * @param all
     *            If true then all of the running tasks will be addressed instead of maxOrphanedTasksToCheck
     */
    public void recoverOrphanedTasks(String queryId, TaskStates.TASK_STATE state, boolean all) {
        recoverOrphanedTasks(queryId, state, Collections.emptyMap(), all);
    }
    
    /**
     * For the specified query id, find tasks that are orphaned and reset their state
     *
     * @param queryId
     *            The query id
     * @param state
     *            The state to reset orphaned tasks to
     * @param overrides
     *            The state to reset orphaned tasks for a specific method
     */
    public void recoverOrphanedTasks(String queryId, TaskStates.TASK_STATE state, Map<QueryRequest.Method,TaskStates.TASK_STATE> overrides) {
        recoverOrphanedTasks(queryId, state, overrides, false);
    }
    
    /**
     * For the specified query id, find tasks that are orphaned and reset their state
     * 
     * @param queryId
     *            The query id
     * @param state
     *            The state to reset orphaned tasks to
     * @param overrides
     *            The state to reset orphaned tasks for a specific method
     * @param all
     *            If true then all of the running tasks will be addressed instead of maxOrphanedTasksToCheck
     */
    public void recoverOrphanedTasks(String queryId, TaskStates.TASK_STATE state, Map<QueryRequest.Method,TaskStates.TASK_STATE> overrides, boolean all) {
        QueryStorageLock lock = queryStorageCache.getTaskStatesLock(queryId);
        lock.lock();
        try {
            // get the query states from the cache
            TaskStates taskStates = queryStorageCache.getTaskStates(queryId);
            
            if (taskStates != null) {
                log.debug("Searching for orphaned tasks for " + queryId);
                
                for (TaskKey taskKey : taskStates.getTasksForState(TaskStates.TASK_STATE.RUNNING,
                                all ? -1 : executor.getExecutorProperties().getMaxOrphanedTasksToCheck())) {
                    QueryTask task = queryStorageCache.getTask(taskKey);
                    if ((System.currentTimeMillis() - task.getLastUpdatedMillis()) > executor.getExecutorProperties().getOrphanThresholdMs()) {
                        if (overrides.containsKey(task.getAction())) {
                            log.info("Resetting orphaned task " + taskKey.getTaskId() + " for " + queryId + " to " + overrides.get(task.getAction()));
                            taskStates.setState(taskKey.getTaskId(), overrides.get(task.getAction()));
                        } else {
                            log.info("Resetting orphaned task " + taskKey.getTaskId() + " for " + queryId + " to " + state);
                            taskStates.setState(taskKey.getTaskId(), state);
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
