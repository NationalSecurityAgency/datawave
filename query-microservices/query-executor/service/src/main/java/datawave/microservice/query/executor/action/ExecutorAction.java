package datawave.microservice.query.executor.action;

import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.logic.QueryCheckpoint;
import datawave.microservice.query.logic.QueryKey;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.Result;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.logic.CheckpointableQueryLogic;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.UUID;

public abstract class ExecutorAction implements Runnable {
    
    private static final Logger log = Logger.getLogger(ExecutorAction.class);
    
    protected final Connector connector;
    protected final QueryStorageCache cache;
    protected final QueryQueueManager queues;
    protected final QueryLogicFactory queryLogicFactory;
    protected final ExecutorProperties executorProperties;
    protected final QueryTask task;
    protected boolean interrupted = false;
    
    public ExecutorAction(ExecutorProperties executorProperties, Connector connector, QueryStorageCache cache, QueryQueueManager queues,
                    QueryLogicFactory queryLogicFactory, QueryTask task) {
        this.executorProperties = executorProperties;
        this.cache = cache;
        this.queues = queues;
        this.connector = connector;
        this.queryLogicFactory = queryLogicFactory;
        this.task = task;
    }
    
    public TaskKey getTaskKey() {
        return task.getTaskKey();
    }
    
    /**
     * Execute the task
     * 
     * @return True if the task was completed, false otherwise.
     * @throws Exception
     *             is the task failed
     */
    public abstract boolean executeTask() throws Exception;
    
    /**
     * Interrupt this execution
     */
    public void interrupt() {
        interrupted = true;
    }
    
    @Override
    public void run() {
        
        boolean taskComplete = false;
        boolean taskFailed = false;
        
        TaskKey taskKey = task.getTaskKey();
        boolean gotLock = cache.updateTaskState(taskKey, TaskStates.TASK_STATE.RUNNING);
        if (gotLock) {
            log.debug("Got lock for task " + taskKey);
            try {
                taskComplete = executeTask();
            } catch (Exception e) {
                log.error("Failed to process task " + taskKey, e);
                taskFailed = true;
                cache.updateFailedQueryStatus(taskKey.getQueryId(), e);
            } finally {
                if (taskComplete) {
                    cache.updateTaskState(taskKey, TaskStates.TASK_STATE.COMPLETED);
                    try {
                        cache.deleteTask(taskKey);
                    } catch (IOException e) {
                        log.error("We may be leaving an orphaned task: " + taskKey, e);
                    }
                } else if (taskFailed) {
                    cache.updateTaskState(taskKey, TaskStates.TASK_STATE.FAILED);
                } else {
                    cache.updateTaskState(taskKey, TaskStates.TASK_STATE.READY);
                }
            }
        } else {
            log.warn("Unable to get lock for task " + taskKey);
        }
        
    }
    
    /**
     * Checkpoint a query logic
     *
     * @param queryKey
     * @param cpQueryLogic
     * @throws IOException
     */
    protected void checkpoint(QueryKey queryKey, CheckpointableQueryLogic cpQueryLogic) throws IOException {
        boolean createdTask = false;
        for (QueryCheckpoint cp : cpQueryLogic.checkpoint(queryKey)) {
            cache.createTask(QueryTask.QUERY_ACTION.NEXT, cp);
            createdTask = true;
        }
        if (createdTask) {
            // TODO : post next event
        }
    }
    
    protected QueryLogic<?> getQueryLogic(Query query) throws QueryException, CloneNotSupportedException {
        return queryLogicFactory.getQueryLogic(query.getQueryLogicName());
    }
    
    private boolean shouldGenerateMoreResults(boolean exhaust, TaskKey taskKey, int maxPageSize, long maxResults, CachedQueryStatus queryStatus) {
        QueryStatus.QUERY_STATE state = queryStatus.getQueryState();
        if (state == QueryStatus.QUERY_STATE.CANCELED || state == QueryStatus.QUERY_STATE.CLOSED || state == QueryStatus.QUERY_STATE.FAILED) {
            return false;
        }
        // if we are to exhaust the iterator, then continue generating results
        if (exhaust) {
            return true;
        }
        if (maxResults > 0 && queryStatus.getNumResultsGenerated() >= maxResults) {
            return false;
        }
        // get the queue size
        long queueSize;
        if (executorProperties.isPollQueueSize()) {
            queueSize = queues.getQueueSize(taskKey.getQueryId());
        } else {
            queueSize = queryStatus.getNumResultsGenerated() - queryStatus.getNumResultsReturned();
        }
        // we should return results if
        return (queueSize < (executorProperties.getAvailableResultsPageMultiplier() * maxPageSize));
    }
    
    protected boolean pullResults(TaskKey taskKey, QueryLogic queryLogic, CachedQueryStatus queryStatus, boolean exhaustIterator) throws Exception {
        // start the timer on the query status to ensure we flush numResultsGenerated updates periodically
        queryStatus.startTimer();
        try {
            TransformIterator iter = queryLogic.getTransformIterator(queryStatus.getQuery());
            long maxResults = queryLogic.getResultLimit(queryStatus.getQuery().getDnList());
            if (maxResults != queryLogic.getMaxResults()) {
                log.info("Maximum results set to " + maxResults + " instead of default " + queryLogic.getMaxResults() + ", user "
                                + queryStatus.getQuery().getUserDN() + " has a DN configured with a different limit");
            }
            if (queryStatus.getQuery().isMaxResultsOverridden()) {
                maxResults = Math.max(maxResults, queryStatus.getQuery().getMaxResultsOverride());
            }
            int pageSize = queryStatus.getQuery().getPagesize();
            if (queryLogic.getMaxPageSize() != 0) {
                pageSize = Math.min(pageSize, queryLogic.getMaxPageSize());
            }
            boolean running = shouldGenerateMoreResults(exhaustIterator, taskKey, pageSize, maxResults, queryStatus);
            while (running && iter.hasNext()) {
                Object result = iter.next();
                queues.sendMessage(taskKey.getQueryId(), new Result(UUID.randomUUID().toString(), new Object[] {result}));
                queryStatus.incrementNumResultsGenerated(1);
                
                // regardless whether the transform iterator returned a result, it may have updated the metrics (next/seek calls etc.)
                // TODO
                // if (iter.getTransformer() instanceof WritesQueryMetrics) {
                // ((WritesQueryMetrics) iter.getTransformer()).writeQueryMetrics(this.getMetric());
                // }
                
                running = shouldGenerateMoreResults(exhaustIterator, taskKey, pageSize, maxResults, queryStatus);
            }
            
            return !iter.hasNext();
        } finally {
            queryStatus.stopTimer();
            queryStatus.forceCacheUpdateIfDirty();
        }
    }
    
}
