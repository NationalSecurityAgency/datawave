package datawave.microservice.query.executor.action;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;

import datawave.core.query.predict.QueryPredictor;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetric;

public class PredictTask extends ExecutorTask {
    private static final Logger log = Logger.getLogger(PredictTask.class);
    
    private final String originService;
    private final QueryPredictor predictor;
    
    public PredictTask(QueryExecutor source, QueryTask task, String originService) {
        super(source, task);
        this.predictor = source.getPredictor();
        this.originService = originService;
    }
    
    @Override
    public boolean executeTask(QueryStatus queryStatus) throws Exception {
        
        assert (QueryRequest.Method.PREDICT.equals(task.getAction()));
        
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        
        QueryMetric metric = new QueryMetric();
        metric.populate(queryStatus.getQuery());
        
        Set<BaseQueryMetric.Prediction> predictions = predictor.predict(metric);
        
        log.debug("Setting predictions for " + queryId);
        queryStatusUpdateUtil.lockedUpdate(queryId, (newQueryStatus) -> newQueryStatus.setPredictions(predictions));
        
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
