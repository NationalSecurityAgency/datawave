package datawave.microservice.query.executor.action;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.logic.CheckpointableQueryLogic;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.ApplicationEventPublisher;

public class Create extends ExecutorAction {
    private static final Logger log = Logger.getLogger(Create.class);
    
    public Create(ExecutorProperties executorProperties, QueryProperties queryProperties, BusProperties busProperties, Connector connector,
                    QueryStorageCache cache, QueryQueueManager queues, QueryLogicFactory queryLogicFactory, ApplicationEventPublisher publisher,
                    QueryTask task) {
        super(executorProperties, queryProperties, busProperties, connector, cache, queues, queryLogicFactory, publisher, task);
    }
    
    @Override
    public boolean executeTask() throws Exception {
        assert (QueryRequest.Method.CREATE.equals(task.getAction()));
        
        boolean taskComplete = false;
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        
        CachedQueryStatus queryStatus = new CachedQueryStatus(cache, queryId, executorProperties.getQueryStatusExpirationMs());
        QueryLogic<?> queryLogic = getQueryLogic(queryStatus.getQuery());
        GenericQueryConfiguration config = queryLogic.initialize(connector, queryStatus.getQuery(), queryStatus.getCalculatedAuthorizations());
        
        // update the query status plan
        queryStatus.setPlan(config.getQueryString());
        
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
