package datawave.microservice.query.executor.action;

import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageLock;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.services.query.configuration.GenericQueryConfiguration;
import datawave.services.query.logic.CheckpointableQueryLogic;
import datawave.services.query.logic.QueryLogic;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;

import java.util.Date;

public class CreateTask extends ExecutorTask {
    private static final Logger log = Logger.getLogger(CreateTask.class);
    
    private final String originService;
    
    public CreateTask(QueryExecutor source, QueryTask task, String originService) {
        super(source, task);
        this.originService = originService;
    }
    
    @Override
    public boolean executeTask(CachedQueryStatus queryStatus, Connector connector) throws Exception {
        assert (QueryRequest.Method.CREATE.equals(task.getAction()));
        
        boolean taskComplete = false;
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        
        QueryLogic<?> queryLogic = getQueryLogic(queryStatus.getQuery());
        log.debug("Initializing query logic for " + queryId);
        GenericQueryConfiguration config = queryLogic.initialize(connector, queryStatus.getQuery(), queryStatus.getCalculatedAuthorizations());
        
        // update the query status plan
        log.debug("Setting plan for " + queryId);
        queryStatus.setPlan(config.getQueryString());
        
        // update the query metrics with the plan
        BaseQueryMetric baseQueryMetric = metricFactory.createMetric();
        baseQueryMetric.setQueryId(taskKey.getQueryId());
        baseQueryMetric.setPlan(config.getQueryString());
        baseQueryMetric.setLastUpdated(new Date(queryStatus.getLastUpdatedMillis()));
        try {
            // @formatter:off
            metricClient.submit(
                    new QueryMetricClient.Request.Builder()
                            .withMetric(baseQueryMetric)
                            .withMetricType(QueryMetricType.DISTRIBUTED)
                            .build());
            // @formatter:on
        } catch (Exception e) {
            log.error("Error updating query metric", e);
        }
        
        log.debug("Setup query logic for " + queryId);
        queryLogic.setupQuery(config);
        
        if (queryLogic instanceof CheckpointableQueryLogic && ((CheckpointableQueryLogic) queryLogic).isCheckpointable()) {
            log.debug("Checkpointing " + queryId);
            CheckpointableQueryLogic cpQueryLogic = (CheckpointableQueryLogic) queryLogic;
            queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATED);
            
            notifyOriginOfCreation(queryId);
            
            checkpoint(task.getTaskKey().getQueryKey(), cpQueryLogic);
            
            // update the task states to indicate that all tasks are created
            taskCreationComplete(queryId);
            
            taskComplete = true;
        } else {
            queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATED);
            
            notifyOriginOfCreation(queryId);
            
            // update the task states to indicate that all tasks are created
            taskCreationComplete(queryId);
            
            log.debug("Exhausting results for " + queryId);
            taskComplete = pullResults(task.getTaskKey(), queryLogic, queryStatus, true);
            if (!taskComplete) {
                Exception e = new IllegalStateException("Expected to have exhausted results.  Something went wrong here");
                cache.updateFailedQueryStatus(queryId, e);
                throw e;
            }
        }
        
        return taskComplete;
    }
    
    private void notifyOriginOfCreation(String queryId) {
        if (originService != null) {
            log.debug("Publishing a create request to the originating service: " + originService);
            // @formatter:off
            publisher.publishEvent(
                    new RemoteQueryRequestEvent(
                            this,
                            busProperties.getId(),
                            originService,
                            QueryRequest.create(queryId)));
            // @formatter:on
        }
    }
    
    private void taskCreationComplete(String queryId) {
        // update the task states to indicate that all tasks are created
        QueryStorageLock lock = cache.getTaskStatesLock(queryId);
        lock.lock();
        try {
            TaskStates taskStates = cache.getTaskStates(queryId);
            taskStates.setCreatingTasks(false);
            cache.updateTaskStates(taskStates);
        } finally {
            lock.unlock();
        }
    }
}
