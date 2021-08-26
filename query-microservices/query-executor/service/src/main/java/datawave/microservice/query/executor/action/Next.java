package datawave.microservice.query.executor.action;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.logic.CheckpointableQueryLogic;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestMap;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.ApplicationEventPublisher;

public class Next extends ExecutorAction {
    private static final Logger log = Logger.getLogger(Next.class);
    
    public Next(QueryExecutor source, ExecutorProperties executorProperties, QueryProperties queryProperties, BusProperties busProperties,
                    AccumuloConnectionRequestMap connectionMap, AccumuloConnectionFactory connectionFactory, QueryStorageCache cache, QueryQueueManager queues,
                    QueryLogicFactory queryLogicFactory, ApplicationEventPublisher publisher, QueryTask task) {
        super(source, executorProperties, queryProperties, busProperties, connectionMap, connectionFactory, cache, queues, queryLogicFactory, publisher, task);
    }
    
    @Override
    public boolean executeTask(CachedQueryStatus queryStatus, Connector connector) throws Exception {
        
        assert (QueryRequest.Method.NEXT.equals(task.getAction()));
        
        boolean taskComplete = false;
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        
        QueryLogic<?> queryLogic = getQueryLogic(queryStatus.getQuery());
        if (queryLogic instanceof CheckpointableQueryLogic && ((CheckpointableQueryLogic) queryLogic).isCheckpointable()) {
            CheckpointableQueryLogic cpQueryLogic = (CheckpointableQueryLogic) queryLogic;
            
            cpQueryLogic.setupQuery(connector, task.getQueryCheckpoint());
            
            taskComplete = pullResults(taskKey, queryLogic, queryStatus, false);
            if (!taskComplete) {
                checkpoint(taskKey.getQueryKey(), cpQueryLogic);
                taskComplete = true;
            }
        } else {
            Exception e = new IllegalStateException("Attempting to get results for an uninitialized, non-checkpointable query logic");
            cache.updateFailedQueryStatus(queryId, e);
            throw e;
        }
        
        return taskComplete;
    }
    
}
