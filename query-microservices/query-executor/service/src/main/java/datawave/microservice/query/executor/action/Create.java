package datawave.microservice.query.executor.action;

import datawave.microservice.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.logic.CheckpointableQueryLogic;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricType;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;

import java.util.Date;

public class Create extends ExecutorAction {
    private static final Logger log = Logger.getLogger(Create.class);
    
    public Create(QueryExecutor source, QueryTask task) {
        super(source, task);
    }
    
    @Override
    public boolean executeTask(CachedQueryStatus queryStatus, Connector connector) throws Exception {
        assert (QueryRequest.Method.CREATE.equals(task.getAction()));
        
        boolean taskComplete = false;
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        
        QueryLogic<?> queryLogic = getQueryLogic(queryStatus.getQuery());
        GenericQueryConfiguration config = queryLogic.initialize(connector, queryStatus.getQuery(), queryStatus.getCalculatedAuthorizations());
        
        // update the query status plan
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
        
        queryLogic.setupQuery(config);
        
        if (queryLogic instanceof CheckpointableQueryLogic && ((CheckpointableQueryLogic) queryLogic).isCheckpointable()) {
            CheckpointableQueryLogic cpQueryLogic = (CheckpointableQueryLogic) queryLogic;
            queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATED);
            checkpoint(task.getTaskKey().getQueryKey(), cpQueryLogic);
            taskComplete = true;
        } else {
            queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATED);
            taskComplete = pullResults(task.getTaskKey(), queryLogic, queryStatus, true);
            if (!taskComplete) {
                Exception e = new IllegalStateException("Expected to have exhausted results.  Something went wrong here");
                cache.updateFailedQueryStatus(queryId, e);
                throw e;
            }
        }
        
        return taskComplete;
    }
    
}
