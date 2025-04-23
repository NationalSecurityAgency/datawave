package datawave.microservice.query.executor.action;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationEventPublisher;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.CheckpointableQueryLogic;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.core.query.logic.WritesQueryMetrics;
import datawave.core.query.runner.AccumuloConnectionRequestMap;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.query.util.QueryStatusUpdateUtil;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.webservice.query.exception.QueryException;

public abstract class ExecutorTask implements Runnable {
    
    private static final Logger log = Logger.getLogger(ExecutorTask.class);
    
    protected final QueryExecutor source;
    protected final AccumuloConnectionRequestMap connectionMap;
    protected final AccumuloConnectionFactory connectionFactory;
    protected final QueryStorageCache cache;
    protected final QueryStatusUpdateUtil queryStatusUpdateUtil;
    protected final QueryResultsManager resultsManager;
    protected final QueryLogicFactory queryLogicFactory;
    protected final BusProperties busProperties;
    protected final QueryProperties queryProperties;
    protected final ExecutorProperties executorProperties;
    protected final ApplicationEventPublisher publisher;
    protected final QueryMetricFactory metricFactory;
    protected final QueryMetricClient metricClient;
    protected QueryTask task;
    protected boolean interrupted = false;
    protected final QueryTaskUpdater queryTaskUpdater;
    protected boolean running = false;
    boolean taskComplete = false;
    boolean taskFailed = false;
    
    public ExecutorTask(QueryExecutor source, QueryTask task) {
        this(source, source.getExecutorProperties(), source.getQueryProperties(), source.getBusProperties(), source.getConnectionRequestMap(),
                        source.getConnectionFactory(), source.getCache(), source.getQueues(), source.getQueryLogicFactory(), source.getPublisher(),
                        source.getMetricFactory(), source.getMetricClient(), task);
    }
    
    public ExecutorTask(QueryExecutor source, ExecutorProperties executorProperties, QueryProperties queryProperties, BusProperties busProperties,
                    AccumuloConnectionRequestMap connectionMap, AccumuloConnectionFactory connectionFactory, QueryStorageCache cache,
                    QueryResultsManager resultsManager, QueryLogicFactory queryLogicFactory, ApplicationEventPublisher publisher,
                    QueryMetricFactory metricFactory, QueryMetricClient metricClient, QueryTask task) {
        this.source = source;
        this.executorProperties = executorProperties;
        this.queryProperties = queryProperties;
        this.busProperties = busProperties;
        this.cache = cache;
        this.queryStatusUpdateUtil = new QueryStatusUpdateUtil(queryProperties, cache);
        this.resultsManager = resultsManager;
        this.connectionMap = connectionMap;
        this.connectionFactory = connectionFactory;
        this.queryLogicFactory = queryLogicFactory;
        this.publisher = publisher;
        this.metricFactory = metricFactory;
        this.metricClient = metricClient;
        this.task = task;
        this.queryTaskUpdater = new QueryTaskUpdater();
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
    public abstract boolean executeTask(QueryStatus queryStatus) throws Exception;
    
    /**
     * Interrupt this execution
     */
    public void interrupt() {
        interrupted = true;
    }
    
    /**
     * It is presumed that a lock for this task has already been obtained by the QueryExecutor
     */
    @Override
    public void run() {
        running = true;
        
        queryTaskUpdater.start();
        
        AccumuloClient client = null;
        TaskKey taskKey = task.getTaskKey();
        
        try {
            log.debug("Running " + taskKey);
            
            QueryStatus queryStatus = cache.getQueryStatus(taskKey.getQueryId());
            
            log.debug("Executing task for " + taskKey);
            taskComplete = executeTask(queryStatus);
        } catch (Exception e) {
            log.error("Failed to process task " + taskKey, e);
            taskFailed = true;
            cache.updateFailedQueryStatus(taskKey.getQueryId(), e);
        } finally {
            queryTaskUpdater.close();
            
            completeTask(taskComplete, taskFailed);
            
            running = false;
        }
    }
    
    /**
     * Complete the task by updating its state appropriately
     * 
     * @param taskComplete
     * @param taskFailed
     */
    public void completeTask(boolean taskComplete, boolean taskFailed) {
        TaskKey taskKey = task.getTaskKey();
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
            // more work to do on this task, lets notify
            switch (task.getAction()) {
                case CREATE:
                    publishExecutorEvent(QueryRequest.create(taskKey.getQueryId()), taskKey.getQueryPool());
                    break;
                case PLAN:
                    publishExecutorEvent(QueryRequest.plan(taskKey.getQueryId()), taskKey.getQueryPool());
                    break;
                case PREDICT:
                    publishExecutorEvent(QueryRequest.predict(taskKey.getQueryId()), taskKey.getQueryPool());
                    break;
                case NEXT:
                    publishExecutorEvent(QueryRequest.next(taskKey.getQueryId()), taskKey.getQueryPool());
                    break;
                default:
                    throw new UnsupportedOperationException(task.getTaskKey().toString());
            }
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
        for (QueryCheckpoint cp : cpQueryLogic.checkpoint(queryKey)) {
            log.debug("Storing a query checkpoint: " + cp);
            cache.createTask(QueryRequest.Method.NEXT, cp);
            publishExecutorEvent(QueryRequest.next(queryKey.getQueryId()), queryKey.getQueryPool());
        }
    }
    
    protected AccumuloClient borrowClient(QueryStatus queryStatus, String poolName, AccumuloConnectionFactory.Priority priority) throws Exception {
        log.debug("Getting connector for " + getTaskKey());
        
        Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        Query q = queryStatus.getQuery();
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
            return connectionFactory.getClient(q.getUserDN(), q.getDnList(), poolName, priority, trackingMap);
        } finally {
            connectionMap.requestEnd(q.getId().toString());
        }
    }
    
    protected void returnClient(AccumuloClient client) {
        if (client != null) {
            try {
                connectionFactory.returnClient(client);
            } catch (Exception e) {
                log.error("Failed to return connection for " + getTaskKey());
            }
        }
    }
    
    protected QueryLogic<?> getQueryLogic(Query query, DatawaveUserDetails currentUser) throws QueryException, CloneNotSupportedException {
        log.debug("Getting query logic for " + query.getQueryLogicName());
        return queryLogicFactory.getQueryLogic(query.getQueryLogicName(), currentUser);
    }
    
    public enum RESULTS_ACTION {
        PAUSE, GENERATE, COMPLETE;
    }
    
    protected RESULTS_ACTION shouldGenerateMoreResults(boolean exhaust, TaskKey taskKey, int maxPageSize, long maxResults) {
        QueryStatus queryStatus = cache.getQueryStatus(taskKey.getQueryId());
        QueryStatus.QUERY_STATE state = queryStatus.getQueryState();
        int concurrentNextCalls = queryStatus.getActiveNextCalls();
        float bufferMultiplier = executorProperties.getAvailableResultsPageMultiplier();
        long numResultsGenerated = queryStatus.getNumResultsGenerated();
        
        // if the state is closed AND we don't have any ongoing next calls, then stop
        if (state == QueryStatus.QUERY_STATE.CLOSE) {
            if (concurrentNextCalls == 0) {
                log.debug("Not getting results for closed query " + taskKey);
                return RESULTS_ACTION.COMPLETE;
            } else {
                // we know these are the last next calls, so cap the buffer multiplier to 1
                bufferMultiplier = 1.0f;
            }
        }
        
        // if the state is canceled or failed, then stop
        if (state == QueryStatus.QUERY_STATE.CANCEL || state == QueryStatus.QUERY_STATE.FAIL) {
            log.debug("Not getting results for canceled or failed query " + taskKey);
            return RESULTS_ACTION.COMPLETE;
        }
        
        // if we have reached the max results for this query, then stop
        if (maxResults > 0 && queryStatus.getNumResultsGenerated() >= maxResults) {
            log.debug("max resuilts reached for " + taskKey);
            return RESULTS_ACTION.COMPLETE;
        }
        
        // if we are to exhaust the iterator, then continue generating results
        if (exhaust) {
            return RESULTS_ACTION.GENERATE;
        }
        
        // get the queue size
        long queueSize = resultsManager.getNumResultsRemaining(taskKey.getQueryId());
        
        // calculate a result buffer size (pagesize * multiplier) adjusting for concurrent next calls
        long bufferSize = (long) (maxPageSize * Math.max(1, concurrentNextCalls) * bufferMultiplier);
        
        // cap the buffer size by max results
        if (maxResults > 0) {
            bufferSize = Math.min(bufferSize, maxResults - numResultsGenerated);
        }
        
        // we should return results if we have less than what we want to have buffered
        log.debug("Getting results if " + queueSize + " < " + bufferSize);
        if (queueSize < bufferSize) {
            return RESULTS_ACTION.GENERATE;
        } else {
            return RESULTS_ACTION.PAUSE;
        }
    }
    
    protected boolean pullResults(QueryLogic queryLogic, Query query, boolean exhaustIterator) throws Exception {
        if (queryLogic instanceof CheckpointableQueryLogic && ((CheckpointableQueryLogic) queryLogic).isCheckpointable()) {
            queryTaskUpdater.setQueryLogic((CheckpointableQueryLogic) queryLogic);
        }
        
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        TransformIterator iter = queryLogic.getTransformIterator(query);
        long maxResults = queryLogic.getResultLimit(query);
        if (maxResults != queryLogic.getMaxResults()) {
            log.info("Maximum results set to " + maxResults + " instead of default " + queryLogic.getMaxResults() + ", user " + query.getUserDN()
                            + " has a DN configured with a different limit");
        }
        if (query.isMaxResultsOverridden()) {
            maxResults = Math.max(maxResults, query.getMaxResultsOverride());
        }
        int pageSize = query.getPagesize();
        
        QueryResultsPublisher publisher = resultsManager.createPublisher(queryId);
        RESULTS_ACTION running = shouldGenerateMoreResults(exhaustIterator, taskKey, pageSize, maxResults);
        int count = 0;
        QueryStatusMetrics metrics = new QueryStatusMetrics();
        while (running == RESULTS_ACTION.GENERATE && iter.hasNext()) {
            count++;
            try {
                Object result = iter.next();
                if (log.isTraceEnabled()) {
                    log.trace("Generated result for " + taskKey + ": " + result);
                }
                if (result != null) {
                    publisher.publish(new Result(UUID.randomUUID().toString(), result));
                    queryTaskUpdater.resultPublished();
                    metrics.incrementNumResultsGenerated();
                    updateMetrics(queryId, query, metrics, iter);
                    updateQueryStatusMetrics(metrics);
                }
            } catch (EmptyObjectException eoe) {
                if (log.isTraceEnabled()) {
                    log.trace("Generated empty object exception for " + taskKey);
                }
            }
            running = shouldGenerateMoreResults(exhaustIterator, taskKey, pageSize, maxResults);
        }
        log.debug("Generated " + count + " results for " + taskKey);
        
        // a final metrics update
        if (updateMetrics(queryId, query, metrics, iter)) {
            updateQueryStatusMetrics(metrics);
        }
        
        // if the query is complete or we have no more results to generate, then the task is complete
        return (running == RESULTS_ACTION.COMPLETE || !iter.hasNext());
    }
    
    protected void updateQueryStatusMetrics(QueryStatusMetrics metrics) throws QueryException, InterruptedException {
        String queryId = getTaskKey().getQueryId();
        
        // update the query status
        QueryStatus queryStatus = queryStatusUpdateUtil.lockedUpdate(queryId, (newQueryStatus) -> {
            // sum up the counts
            newQueryStatus.incrementNextCount(metrics.nextCount);
            newQueryStatus.incrementSeekCount(metrics.seekCount);
            newQueryStatus.incrementNumResultsGenerated(metrics.numResultsGenerated);
        });
        
        log.debug("Updating summed results (" + queryStatus.getNumResultsReturned() + ") for " + queryId);
        
        // clear the counts in the local instance
        metrics.clear();
    }
    
    /**
     * Update the metrics in the query status. This method will NOT update the storage cache.
     * 
     * @param queryId
     * @param metrics
     * @param iter
     * @return true if metrics were found and updated
     */
    protected boolean updateMetrics(String queryId, Query query, QueryStatusMetrics metrics, TransformIterator iter) {
        try {
            QueryLogic<?> logic = queryLogicFactory.getQueryLogic(query.getQueryLogicName());
            // regardless whether the transform iterator returned a result, it may have updated the metrics (next/seek calls etc.)
            if (logic.getCollectQueryMetrics() && iter.getTransformer() instanceof WritesQueryMetrics) {
                WritesQueryMetrics metricsWriter = ((WritesQueryMetrics) iter.getTransformer());
                if (metricsWriter.hasMetrics()) {
                    BaseQueryMetric baseQueryMetric = metricFactory.createMetric();
                    baseQueryMetric.setQueryId(queryId);
                    baseQueryMetric.setSourceCount(metricsWriter.getSourceCount());
                    metrics.incrementNextCount(metricsWriter.getNextCount());
                    baseQueryMetric.setNextCount(metricsWriter.getNextCount());
                    metrics.incrementSeekCount(metricsWriter.getSeekCount());
                    baseQueryMetric.setSeekCount(metricsWriter.getSeekCount());
                    baseQueryMetric.setYieldCount(metricsWriter.getYieldCount());
                    baseQueryMetric.setDocRanges(metricsWriter.getDocRanges());
                    baseQueryMetric.setFiRanges(metricsWriter.getFiRanges());
                    baseQueryMetric.setLastUpdated(new Date(System.currentTimeMillis()));
                    try {
                        // @formatter:off
                        metricClient.submit(
                                new QueryMetricClient.Request.Builder()
                                        .withUser((DatawaveUserDetails)logic.getServerUser())
                                        .withMetric(baseQueryMetric)
                                        .withMetricType(QueryMetricType.DISTRIBUTED)
                                        .build());
                        // @formatter:on
                        metricsWriter.resetMetrics();
                    } catch (Exception e) {
                        log.error("Error updating query metric", e);
                    }
                    return true;
                }
            }
        } catch (QueryException | CloneNotSupportedException e) {
            log.warn("Could not determine whether the query logic supports metrics");
        }
        return false;
    }
    
    protected void publishExecutorEvent(QueryRequest queryRequest, String queryPool) {
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
    
    /**
     * The default query task updater that will update the QueryTask in the cache periodically based on the number of results returned or the time that has
     * passed.
     */
    private class QueryTaskUpdater implements Runnable {
        protected volatile int resultsCount = 0;
        protected volatile int resultsThreshold;
        protected volatile long lastRefresh;
        
        protected volatile boolean closed = false;
        // the refresh object is used to notify that a checkpoint update may be required or we just closed
        protected final Object refresh = new Object();
        protected Thread workThread;
        protected CheckpointableQueryLogic queryLogic;
        
        public QueryTaskUpdater() {
            resultsThreshold = executorProperties.getCheckpointFlushResults();
            lastRefresh = System.currentTimeMillis();
        }
        
        public void start() {
            refreshTask();
            workThread = new Thread(this, "Checkpoint Updater for " + task.getTaskKey().toString());
            workThread.setDaemon(true);
            workThread.start();
        }
        
        @Override
        public void run() {
            synchronized (refresh) {
                while (!closed) {
                    try {
                        refresh.wait(executorProperties.getCheckpointFlushMs());
                    } catch (InterruptedException e) {
                        // time to potentially refresh and loop
                    }
                    if (!closed && isRefreshTime()) {
                        refreshTask();
                        updateCounters();
                    }
                }
            }
        }
        
        public void close() {
            closed = true;
            while (workThread.isAlive()) {
                synchronized (refresh) {
                    refresh.notifyAll();
                }
            }
        }
        
        public void setQueryLogic(CheckpointableQueryLogic queryLogic) {
            this.queryLogic = queryLogic;
        }
        
        protected boolean isRefreshTime() {
            return (resultsCount >= resultsThreshold || lastRefresh + executorProperties.getCheckpointFlushMs() < System.currentTimeMillis());
        }
        
        protected void refreshTask() {
            try {
                if (queryLogic != null) {
                    // update the task checkpoint and its last update millis
                    task = cache.checkpointTask(task.getTaskKey(), queryLogic.updateCheckpoint(task.getQueryCheckpoint()));
                } else {
                    // update the task last update millis
                    task = cache.updateTask(task);
                }
            } catch (IllegalStateException e) {
                // in this case the task no longer exists, probably because the query was deleted
                log.warn("Attempted to refresh task " + task.getTaskKey() + " but it no longer exists.  Query was probably deleted");
            }
        }
        
        protected void updateCounters() {
            lastRefresh = System.currentTimeMillis();
            if (resultsCount >= resultsThreshold) {
                resultsThreshold = resultsCount + executorProperties.getCheckpointFlushResults();
            }
        }
        
        public void resultPublished() {
            resultsCount++;
            if (resultsCount >= resultsThreshold) {
                synchronized (refresh) {
                    refresh.notifyAll();
                }
            }
        }
    }
    
    private static class QueryStatusMetrics {
        private int numResultsGenerated = 0;
        private long seekCount = 0;
        private long nextCount = 0;
        
        public int getNumResultsGenerated() {
            return numResultsGenerated;
        }
        
        public void incrementNumResultsGenerated() {
            numResultsGenerated++;
        }
        
        public long getSeekCount() {
            return seekCount;
        }
        
        public void incrementSeekCount(long count) {
            seekCount += count;
        }
        
        public long getNextCount() {
            return nextCount;
        }
        
        public void incrementNextCount(long count) {
            nextCount += count;
        }
        
        public void clear() {
            numResultsGenerated = 0;
            seekCount = nextCount = 0;
        }
    }
    
    public boolean isInterrupted() {
        return interrupted;
    }
    
    public boolean isRunning() {
        return running;
    }
    
    public boolean isTaskComplete() {
        return taskComplete;
    }
    
    public boolean isTaskFailed() {
        return taskFailed;
    }
    
    public QueryTask getTask() {
        return task;
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("taskKey", task.getTaskKey()).build();
    }
    
}
