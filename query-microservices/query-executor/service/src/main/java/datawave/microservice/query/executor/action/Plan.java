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
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.UUID;

public class Plan extends ExecutorAction {
    private static final Logger log = Logger.getLogger(Plan.class);
    
    public Plan(ExecutorProperties executorProperties, Connector connector, QueryStorageCache cache, QueryQueueManager queues,
                    QueryLogicFactory queryLogicFactory, QueryTask task) {
        super(executorProperties, connector, cache, queues, queryLogicFactory, task);
    }
    
    @Override
    public boolean executeTask() throws Exception {
        
        assert (QueryTask.QUERY_ACTION.PLAN.equals(task.getAction()));
        
        TaskKey taskKey = task.getTaskKey();
        UUID queryId = taskKey.getQueryId();
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
