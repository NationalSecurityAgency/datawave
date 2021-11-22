package datawave.microservice.query.executor.action;

import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.services.query.logic.QueryLogic;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;

public class PredictTask extends ExecutorTask {
    private static final Logger log = Logger.getLogger(PredictTask.class);
    
    private final String originService;
    
    public PredictTask(QueryExecutor source, QueryTask task, String originService) {
        super(source, task);
        this.originService = originService;
    }
    
    @Override
    public boolean executeTask(CachedQueryStatus queryStatus, Connector connector) throws Exception {
        
        assert (QueryRequest.Method.PREDICT.equals(task.getAction()));
        
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        QueryLogic<?> queryLogic = getQueryLogic(queryStatus.getQuery());
        
        // TODO: Implement the prediction logic
        
        notifyOriginOfPrediction(queryId);
        
        return true;
    }
    
    private void notifyOriginOfPrediction(String queryId) {
        if (originService != null) {
            log.debug("Publishing a prediction request to the originating service: " + originService);
            // @formatter:off
            publisher.publishEvent(
                    new RemoteQueryRequestEvent(
                            this,
                            busProperties.getId(),
                            originService,
                            QueryRequest.predict(queryId)));
            // @formatter:on
        }
    }
}
