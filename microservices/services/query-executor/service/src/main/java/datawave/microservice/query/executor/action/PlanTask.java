package datawave.microservice.query.executor.action;

import static datawave.microservice.query.QueryParameters.QUERY_PLAN_EXPAND_FIELDS;
import static datawave.microservice.query.QueryParameters.QUERY_PLAN_EXPAND_VALUES;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;

public class PlanTask extends ExecutorTask {
    private static final Logger log = Logger.getLogger(PlanTask.class);
    
    private final String originService;
    
    public PlanTask(QueryExecutor source, QueryTask task, String originService) {
        super(source, task);
        this.originService = originService;
    }
    
    @Override
    public boolean executeTask(QueryStatus queryStatus) throws Exception {
        
        assert (QueryRequest.Method.PLAN.equals(task.getAction()));
        
        AccumuloClient client = null;
        
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        Query query = queryStatus.getQuery();
        QueryLogic<?> queryLogic = getQueryLogic(query, queryStatus.getCurrentUser());
        try {
            // by default we will expand the fields but not the values.
            boolean expandFields = true;
            boolean expandValues = false;
            for (QueryImpl.Parameter p : query.getParameters()) {
                if (p.getParameterName().equals(QUERY_PLAN_EXPAND_FIELDS)) {
                    expandFields = Boolean.parseBoolean(p.getParameterValue());
                } else if (p.getParameterName().equals(QUERY_PLAN_EXPAND_VALUES)) {
                    expandValues = Boolean.parseBoolean(p.getParameterValue());
                }
            }
            
            client = borrowClient(queryStatus, queryLogic.getConnPoolName(), AccumuloConnectionFactory.Priority.LOW);
            
            String plan = queryLogic.getPlan(client, query, queryStatus.getCalculatedAuthorizations(), expandFields, expandValues);
            
            log.debug("Setting plan for " + queryId);
            queryStatusUpdateUtil.lockedUpdate(queryId, (newQueryStatus) -> newQueryStatus.setPlan(plan));
            
            notifyOriginOfPlan(queryId);
        } finally {
            returnClient(client);
            
            try {
                queryLogic.close();
            } catch (Exception e) {
                log.error("Failed to close query logic", e);
            }
        }
        
        return true;
    }
    
    private void notifyOriginOfPlan(String queryId) {
        if (originService != null) {
            log.debug("Publishing a plan request to the originating service: " + originService);
            // @formatter:off
            publisher.publishEvent(
                    new RemoteQueryRequestEvent(
                            this,
                            busProperties.getId(),
                            originService,
                            QueryRequest.plan(queryId)));
            // @formatter:on
        }
    }
}
