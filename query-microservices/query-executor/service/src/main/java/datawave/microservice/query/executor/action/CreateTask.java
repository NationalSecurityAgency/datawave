package datawave.microservice.query.executor.action;

import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.services.query.configuration.CheckpointableQueryConfiguration;
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
    private volatile boolean originNotified = false;
    
    public CreateTask(QueryExecutor source, QueryTask task, String originService) {
        super(source, task);
        this.originService = originService;
    }
    
    /**
     * It is presumed that a lock for this task has already been obtained by the QueryExecutor
     */
    @Override
    public void run() {
        try {
            super.run();
        } finally {
            // in the case of the create, don't leave em hanging in case we failed somewhere.
            TaskKey taskKey = task.getTaskKey();
            String queryId = taskKey.getQueryId();
            notifyOriginOfCreation(queryId);
        }
    }
    
    @Override
    public boolean executeTask(CachedQueryStatus queryStatus, Connector connector) throws Exception {
        assert (QueryRequest.Method.CREATE.equals(task.getAction()));
        
        boolean taskComplete = false;
        
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        
        QueryLogic<?> queryLogic = getQueryLogic(queryStatus.getQuery());
        try {
            // start with the planning stage
            queryStatus.setCreateStage(QueryStatus.CREATE_STAGE.PLAN);
            
            log.debug("Initializing query logic for " + queryId);
            GenericQueryConfiguration config = queryLogic.initialize(connector, queryStatus.getQuery(), queryStatus.getCalculatedAuthorizations());
            
            // update the query status plan
            log.debug("Setting plan for " + queryId);
            queryStatus.setPlan(config.getQueryString());
            
            // update the query status configuration, but only if we configured the query logic to be checkpointable
            if (config instanceof CheckpointableQueryConfiguration && ((CheckpointableQueryLogic) queryLogic).isCheckpointable()) {
                queryStatus.setConfig(((CheckpointableQueryConfiguration) config).checkpoint());
            } else {
                queryStatus.setConfig(config);
            }
            
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
            
            // now we move into the tasking stage
            queryStatus.setCreateStage(QueryStatus.CREATE_STAGE.TASK);
            
            // notify the origin that the creation stage is complete
            notifyOriginOfCreation(queryId);
            
            if (queryLogic instanceof CheckpointableQueryLogic && ((CheckpointableQueryLogic) queryLogic).isCheckpointable()) {
                log.debug("Checkpointing " + queryId);
                CheckpointableQueryLogic cpQueryLogic = (CheckpointableQueryLogic) queryLogic;
                
                // create the tasks
                checkpoint(task.getTaskKey().getQueryKey(), cpQueryLogic);
                
                // Now that the checkpoints are created, we can start the results stage
                queryStatus.setCreateStage(QueryStatus.CREATE_STAGE.RESULTS);
                
                taskComplete = true;
            } else {
                // for non checkpointable queries we go immediately into the results stage since no tasks will be generated
                queryStatus.setCreateStage(QueryStatus.CREATE_STAGE.RESULTS);
                
                log.debug("Setup query logic for " + queryId);
                queryLogic.setupQuery(config);
                
                log.debug("Exhausting results for " + queryId);
                taskComplete = pullResults(queryLogic, queryStatus, true);
                
                if (!taskComplete) {
                    Exception e = new IllegalStateException("Expected to have exhausted results.  Something went wrong here");
                    cache.updateFailedQueryStatus(queryId, e);
                    throw e;
                }
            }
        } finally {
            try {
                queryLogic.close();
            } catch (Exception e) {
                log.error("Failed to close query logic", e);
            }
        }
        
        return taskComplete;
    }
    
    private void notifyOriginOfCreation(String queryId) {
        if (originService != null && !originNotified) {
            log.debug("Publishing a create request to the originating service: " + originService);
            // @formatter:off
            publisher.publishEvent(
                    new RemoteQueryRequestEvent(
                            this,
                            busProperties.getId(),
                            originService,
                            QueryRequest.create(queryId)));
            // @formatter:on
            originNotified = true;
        }
    }
    
}
