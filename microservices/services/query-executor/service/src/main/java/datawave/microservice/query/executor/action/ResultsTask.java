package datawave.microservice.query.executor.action;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.log4j.Logger;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.CheckpointableQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;

public class ResultsTask extends ExecutorTask {
    private static final Logger log = Logger.getLogger(ResultsTask.class);
    
    public ResultsTask(QueryExecutor source, QueryTask task) {
        super(source, task);
    }
    
    @Override
    public boolean executeTask(QueryStatus queryStatus) throws Exception {
        
        assert (QueryRequest.Method.NEXT.equals(task.getAction()));
        
        AccumuloClient client = null;
        
        boolean taskComplete = false;
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        Query query = queryStatus.getQuery();
        
        QueryLogic<?> queryLogic = getQueryLogic(query, queryStatus.getCurrentUser());
        try {
            if (queryLogic instanceof CheckpointableQueryLogic && ((CheckpointableQueryLogic) queryLogic).isCheckpointable()) {
                CheckpointableQueryLogic cpQueryLogic = (CheckpointableQueryLogic) queryLogic;
                
                client = borrowClient(queryStatus, queryLogic.getConnPoolName(), AccumuloConnectionFactory.Priority.LOW);
                
                cpQueryLogic.setupQuery(client, queryStatus.getConfig(), task.getQueryCheckpoint());
                
                log.debug("Pulling results for  " + task.getTaskKey() + ": " + task.getQueryCheckpoint());
                taskComplete = pullResults(queryLogic, query, false);
                if (!taskComplete) {
                    checkpoint(taskKey.getQueryKey(), cpQueryLogic);
                    taskComplete = true;
                }
            } else {
                Exception e = new IllegalStateException("Attempting to get results for an uninitialized, non-checkpointable query logic");
                cache.updateFailedQueryStatus(queryId, e);
                throw e;
            }
        } finally {
            returnClient(client);
            
            try {
                queryLogic.close();
            } catch (Exception e) {
                log.error("Failed to close query logic", e);
            }
        }
        
        return taskComplete;
    }
    
}
