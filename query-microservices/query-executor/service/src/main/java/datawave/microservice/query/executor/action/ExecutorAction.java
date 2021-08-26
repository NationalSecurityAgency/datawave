package datawave.microservice.query.executor.action;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.logic.CheckpointableQueryLogic;
import datawave.microservice.query.logic.QueryCheckpoint;
import datawave.microservice.query.logic.QueryKey;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.Result;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.runner.AccumuloConnectionRequestMap;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public abstract class ExecutorAction implements Runnable {
    
    private static final Logger log = Logger.getLogger(ExecutorAction.class);
    
    protected final QueryExecutor source;
    protected final AccumuloConnectionRequestMap connectionMap;
    protected final AccumuloConnectionFactory connectionFactory;
    protected final QueryStorageCache cache;
    protected final QueryQueueManager queues;
    protected final QueryLogicFactory queryLogicFactory;
    protected final BusProperties busProperties;
    protected final QueryProperties queryProperties;
    protected final ExecutorProperties executorProperties;
    protected final ApplicationEventPublisher publisher;
    protected final QueryTask task;
    protected boolean interrupted = false;
    
    public ExecutorAction(QueryExecutor source, ExecutorProperties executorProperties, QueryProperties queryProperties, BusProperties busProperties,
                    AccumuloConnectionRequestMap connectionMap, AccumuloConnectionFactory connectionFactory, QueryStorageCache cache, QueryQueueManager queues,
                    QueryLogicFactory queryLogicFactory, ApplicationEventPublisher publisher, QueryTask task) {
        this.source = source;
        this.executorProperties = executorProperties;
        this.queryProperties = queryProperties;
        this.busProperties = busProperties;
        this.cache = cache;
        this.queues = queues;
        this.connectionMap = connectionMap;
        this.connectionFactory = connectionFactory;
        this.queryLogicFactory = queryLogicFactory;
        this.publisher = publisher;
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
    public abstract boolean executeTask(CachedQueryStatus status, Connector connector) throws Exception;
    
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
        String queryId = taskKey.getQueryId();
        boolean gotLock = cache.updateTaskState(taskKey, TaskStates.TASK_STATE.RUNNING);
        if (gotLock) {
            log.debug("Got lock for task " + taskKey);
            
            Connector connector = null;
            
            try {
                CachedQueryStatus queryStatus = new CachedQueryStatus(cache, queryId, executorProperties.getQueryStatusExpirationMs());
                connector = getConnector(queryStatus, AccumuloConnectionFactory.Priority.LOW);
                taskComplete = executeTask(queryStatus, connector);
            } catch (Exception e) {
                log.error("Failed to process task " + taskKey, e);
                taskFailed = true;
                cache.updateFailedQueryStatus(taskKey.getQueryId(), e);
            } finally {
                if (connector != null) {
                    try {
                        connectionFactory.returnConnection(connector);
                    } catch (Exception e) {
                        log.error("Failed to return connection for " + taskKey);
                    }
                }
                
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
     *            The query key
     * @param cpQueryLogic
     *            The checkpointable query logic
     * @throws IOException
     *             if checkpointing fails
     */
    protected void checkpoint(QueryKey queryKey, CheckpointableQueryLogic cpQueryLogic) throws IOException {
        boolean createdTask = false;
        for (QueryCheckpoint cp : cpQueryLogic.checkpoint(queryKey)) {
            cache.createTask(QueryRequest.Method.NEXT, cp);
            createdTask = true;
        }
        if (createdTask) {
            publishExecutorEvent(QueryRequest.next(queryKey.getQueryId()), queryKey.getQueryPool());
        }
    }
    
    protected Connector getConnector(QueryStatus status, AccumuloConnectionFactory.Priority priority) throws Exception {
        Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        Query q = status.getQuery();
        if (q.getOwner() != null) {
            trackingMap.put(AccumuloConnectionFactory.QUERY_USER, q.getOwner());
        }
        if (q.getId() != null) {
            trackingMap.put(AccumuloConnectionFactory.QUERY_ID, q.getId().toString());
        }
        if (q.getQuery() != null) {
            trackingMap.put(AccumuloConnectionFactory.QUERY, q.getQuery());
        }
        connectionMap.requestBegin(q.getId().toString(), q.getUserDN(), trackingMap);
        try {
            return connectionFactory.getConnection(q.getUserDN(), q.getDnList(), status.getQueryKey().getQueryPool(), priority, trackingMap);
        } finally {
            connectionMap.requestEnd(q.getId().toString());
        }
    }
    
    protected QueryLogic<?> getQueryLogic(Query query) throws QueryException, CloneNotSupportedException {
        return queryLogicFactory.getQueryLogic(query.getQueryLogicName());
    }
    
    protected boolean shouldGenerateMoreResults(boolean exhaust, TaskKey taskKey, int maxPageSize, long maxResults, QueryStatus queryStatus) {
        QueryStatus.QUERY_STATE state = queryStatus.getQueryState();
        int concurrentNextCalls = queryStatus.getActiveNextCalls();
        float bufferMultiplier = executorProperties.getAvailableResultsPageMultiplier();
        long numResultsGenerated = queryStatus.getNumResultsGenerated();
        
        // if the state is closed AND we don't have any ongoing next calls, then stop
        if (state == QueryStatus.QUERY_STATE.CLOSED) {
            if (concurrentNextCalls == 0) {
                return false;
            } else {
                // we know these are the last next calls, so cap the buffer multiplier to 1
                bufferMultiplier = 1.0f;
            }
        }
        
        // if the state is canceled or failed, then stop
        if (state == QueryStatus.QUERY_STATE.CANCELED || state == QueryStatus.QUERY_STATE.FAILED) {
            return false;
        }
        
        // if we have reached the max results for this query, then stop
        if (maxResults > 0 && queryStatus.getNumResultsGenerated() >= maxResults) {
            return false;
        }
        
        // if we are to exhaust the iterator, then continue generating results
        if (exhaust) {
            return true;
        }
        
        // get the queue size
        long queueSize;
        if (executorProperties.isPollQueueSize()) {
            queueSize = queues.getQueueSize(taskKey.getQueryId());
        } else {
            queueSize = queryStatus.getNumResultsGenerated() - queryStatus.getNumResultsReturned();
        }
        
        // calculate a result buffer size (pagesize * multiplier) adjusting for concurrent next calls
        long bufferSize = (long) (maxPageSize * Math.max(1, concurrentNextCalls) * bufferMultiplier);
        
        // cap the buffer size by max results
        if (maxResults > 0) {
            bufferSize = Math.min(bufferSize, maxResults - numResultsGenerated);
        }
        
        // we should return results if we have less than what we want to have buffered
        return (queueSize < bufferSize);
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
                queues.sendMessage(taskKey.getQueryId(), new Result(UUID.randomUUID().toString(), result));
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
    
    private void publishExecutorEvent(QueryRequest queryRequest, String queryPool) {
        // @formatter:off
        publisher.publishEvent(
                new RemoteQueryRequestEvent(
                        source,
                        busProperties.getId(),
                        getPooledExecutorName(queryPool),
                        queryRequest));
        // @formatter:on
    }
    
    protected String getPooledExecutorName(String poolName) {
        return String.join("-", Arrays.asList(queryProperties.getExecutorServiceName(), poolName));
    }
    
}
