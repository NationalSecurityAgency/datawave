package datawave.microservice.query.executor;

import datawave.microservice.common.storage.QueryCheckpoint;
import datawave.microservice.common.storage.QueryKey;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryStatus;
import datawave.microservice.common.storage.CachedQueryStatus;
import datawave.microservice.common.storage.QueryStorageCache;
import datawave.microservice.common.storage.QueryTask;
import datawave.microservice.common.storage.QueryTaskNotification;
import datawave.microservice.common.storage.Result;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskLockException;
import datawave.microservice.common.storage.TaskStates;
import datawave.microservice.common.storage.remote.QueryTaskNotificationHandler;
import datawave.microservice.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.logic.CheckpointableQueryLogic;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.UUID;

/**
 * This class holds the business logic for handling a task notification
 *
 * TODO: Query Metrics
 **/
public class QueryExecutor implements QueryTaskNotificationHandler {
    private static final Logger log = Logger.getLogger(QueryExecutor.class);
    
    private final Connector connector;
    private final QueryStorageCache cache;
    private final QueryQueueManager queues;
    private final QueryLogicFactory queryLogicFactory;
    private final ExecutorProperties executorProperties;
    
    public QueryExecutor(ExecutorProperties executorProperties, Connector connector, QueryStorageCache cache, QueryQueueManager queues,
                    QueryLogicFactory queryLogicFactory) {
        this.executorProperties = executorProperties;
        this.cache = cache;
        this.queues = queues;
        this.connector = connector;
        this.queryLogicFactory = queryLogicFactory;
    }
    
    @Override
    public void handleQueryTaskNotification(QueryTaskNotification taskNotification) {
        boolean gotLock = false;
        boolean taskComplete = false;
        boolean taskFailed = false;
        TaskKey taskKey = taskNotification.getTaskKey();
        UUID queryId = taskKey.getQueryId();
        try {
            // pull the task out of the cache, locking it in the process
            QueryTask task = cache.getTask(taskKey, executorProperties.getLockWaitTimeMillis(), executorProperties.getLockLeaseTimeMillis());
            if (task != null) {
                // check the states to see if we can run this now
                gotLock = cache.updateTaskState(taskKey, TaskStates.TASK_STATE.RUNNING);
                
                // only proceed if we got the lock
                if (gotLock) {
                    // pull the query from the cache
                    CachedQueryStatus queryStatus = new CachedQueryStatus(cache, queryId, executorProperties.getMaxQueryStatusAge());
                    QueryLogic<?> queryLogic;
                    switch (task.getAction()) {
                        case PLAN:
                            queryLogic = queryLogicFactory.getQueryLogic(queryStatus.getQuery().getQueryLogicName());
                            // by default we will expand the fields but not the values.
                            boolean expandFields = true;
                            boolean expandValues = false;
                            Query query = queryStatus.getQuery();
                            for (QueryImpl.Parameter p : query.getParameters()) {
                                if (p.getParameterName().equals(QueryTask.EXPAND_FIELDS)) {
                                    expandFields = Boolean.valueOf(p.getParameterValue());
                                } else if (p.getParameterName().equals(QueryTask.EXPAND_VALUES)) {
                                    expandValues = Boolean.valueOf(p.getParameterValue());
                                }
                            }
                            String plan = queryLogic.getPlan(connector, queryStatus.getQuery(), queryStatus.getCalculatedAuthorizations(), expandFields,
                                            expandValues);
                            queryStatus.setPlan(plan);
                            break;
                        case CREATE:
                        case NEXT:
                            queryLogic = queryLogicFactory.getQueryLogic(queryStatus.getQuery().getQueryLogicName());
                            GenericQueryConfiguration config = queryLogic.initialize(connector, queryStatus.getQuery(),
                                            queryStatus.getCalculatedAuthorizations());
                            
                            // update the query status plan
                            if (task.getAction() != QueryTask.QUERY_ACTION.NEXT) {
                                queryStatus.setPlan(config.getQueryString());
                            }
                            
                            if (queryLogic instanceof CheckpointableQueryLogic && ((CheckpointableQueryLogic) queryLogic).isCheckpointable()) {
                                CheckpointableQueryLogic cpQueryLogic = (CheckpointableQueryLogic) queryLogic;
                                cpQueryLogic.setupQuery(connector, task.getQueryCheckpoint());
                                
                                if (task.getAction() == QueryTask.QUERY_ACTION.NEXT) {
                                    taskComplete = pullResults(taskKey, queryLogic, queryStatus, false);
                                    if (!taskComplete) {
                                        checkpoint(taskKey.getQueryKey(), cpQueryLogic);
                                        taskComplete = true;
                                    }
                                } else {
                                    queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATED);
                                    checkpoint(taskKey.getQueryKey(), cpQueryLogic);
                                    taskComplete = true;
                                }
                            } else {
                                queryLogic.setupQuery(config);
                                taskComplete = pullResults(taskKey, queryLogic, queryStatus, true);
                                if (!taskComplete) {
                                    throw new IllegalStateException("Expected to have exhausted results.  Something went wrong here");
                                }
                            }
                            break;
                        case CLOSE:
                            taskComplete = true;
                            break;
                        case TEST:
                            // we can ignore this one
                        default:
                            throw new IllegalStateException("Unknown task action: " + task.getAction() + " for " + taskKey);
                    }
                }
            }
        } catch (TaskLockException tle) {
            // somebody is already processing this one
        } catch (Exception e) {
            log.error("Failed to process task " + taskKey, e);
            taskFailed = true;
            cache.updateFailedQueryStatus(taskKey.getQueryId(), e);
        } finally {
            if (gotLock) {
                if (taskComplete) {
                    cache.updateTaskState(taskKey, TaskStates.TASK_STATE.COMPLETED);
                    try {
                        cache.deleteTask(taskKey);
                    } catch (IOException e) {
                        log.error("We may be leaving an orphaned task: " + taskKey, e);
                    }
                } else if (taskFailed) {
                    cache.updateTaskState(taskKey, TaskStates.TASK_STATE.FAILED);
                    cache.getTaskLock(taskKey).unlock();
                } else {
                    cache.updateTaskState(taskKey, TaskStates.TASK_STATE.READY);
                    cache.getTaskLock(taskKey).unlock();
                }
            } else {
                cache.post(taskNotification);
            }
        }
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
        if (queryStatus.getNumResultsGenerated() >= maxResults) {
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
    
    private QueryStatus.QUERY_STATE getQueryState(TaskKey taskKey) {
        QueryStatus status = cache.getQueryStatus(taskKey.getQueryId());
        if (status != null) {
            return status.getQueryState();
        } else {
            return QueryStatus.QUERY_STATE.CLOSED;
        }
    }
    
    private boolean pullResults(TaskKey taskKey, QueryLogic queryLogic, CachedQueryStatus queryStatus, boolean exhaustIterator) throws Exception {
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
                maxResults = Math.min(maxResults, queryStatus.getQuery().getMaxResultsOverride());
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
    
    /**
     * Checkpoint a query logic
     * 
     * @param queryKey
     * @param cpQueryLogic
     * @throws IOException
     */
    private void checkpoint(QueryKey queryKey, CheckpointableQueryLogic cpQueryLogic) throws IOException {
        for (QueryCheckpoint cp : cpQueryLogic.checkpoint(queryKey)) {
            cache.checkpointTask(new TaskKey(UUID.randomUUID(), queryKey), cp);
        }
    }
    
}
