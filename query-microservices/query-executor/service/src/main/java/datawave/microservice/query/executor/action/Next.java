package datawave.microservice.query.executor.action;

import datawave.microservice.common.storage.CachedQueryStatus;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryStatus;
import datawave.microservice.common.storage.QueryStorageCache;
import datawave.microservice.common.storage.QueryTask;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskStates;
import datawave.microservice.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.logic.CheckpointableQueryLogic;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.UUID;

public class Next extends ExecutorAction {
    private static final Logger log = Logger.getLogger(Next.class);
    
    public Next(ExecutorProperties executorProperties, Connector connector, QueryStorageCache cache, QueryQueueManager queues,
                    QueryLogicFactory queryLogicFactory, QueryTask task) {
        super(executorProperties, connector, cache, queues, queryLogicFactory, task);
    }
    
    @Override
    public boolean executeTask() throws Exception {
        
        assert (QueryTask.QUERY_ACTION.NEXT.equals(task.getAction()));
        
        boolean taskComplete = false;
        TaskKey taskKey = task.getTaskKey();
        UUID queryId = taskKey.getQueryId();
        CachedQueryStatus queryStatus = new CachedQueryStatus(cache, queryId, executorProperties.getQueryStatusExpirationMs());
        
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
