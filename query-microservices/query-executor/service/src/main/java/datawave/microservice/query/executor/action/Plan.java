package datawave.microservice.query.executor.action;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.ApplicationEventPublisher;

public class Plan extends ExecutorAction {
    private static final Logger log = Logger.getLogger(Plan.class);
    
    public Plan(QueryExecutor source, ExecutorProperties executorProperties, QueryProperties queryProperties, BusProperties busProperties, Connector connector,
                    QueryStorageCache cache, QueryQueueManager queues, QueryLogicFactory queryLogicFactory, ApplicationEventPublisher publisher,
                    QueryTask task) {
        super(source, executorProperties, queryProperties, busProperties, connector, cache, queues, queryLogicFactory, publisher, task);
    }
    
    @Override
    public boolean executeTask() throws Exception {
        
        assert (QueryRequest.Method.PLAN.equals(task.getAction()));
        
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        CachedQueryStatus queryStatus = new CachedQueryStatus(cache, queryId, executorProperties.getQueryStatusExpirationMs());
        QueryLogic<?> queryLogic = getQueryLogic(queryStatus.getQuery());
        // by default we will expand the fields but not the values.
        boolean expandFields = true;
        boolean expandValues = false;
        Query query = queryStatus.getQuery();
        for (QueryImpl.Parameter p : query.getParameters()) {
            if (p.getParameterName().equals(QueryTask.EXPAND_FIELDS)) {
                expandFields = Boolean.valueOf(p.getParameterValue());
            } else if (p.getParameterName().equals(QueryTask.EXPAND_VALUES)) {
                expandValues = Boolean.valueOf(p.getParameterValue());
            }
        }
        String plan = queryLogic.getPlan(connector, queryStatus.getQuery(), queryStatus.getCalculatedAuthorizations(), expandFields, expandValues);
        queryStatus.setPlan(plan);
        
        return true;
    }
    
}
