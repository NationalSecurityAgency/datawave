package datawave.microservice.query.executor.action;

import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.services.query.logic.QueryLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;

import static datawave.microservice.query.QueryParameters.QUERY_PLAN_EXPAND_FIELDS;
import static datawave.microservice.query.QueryParameters.QUERY_PLAN_EXPAND_VALUES;

public class PlanTask extends ExecutorTask {
    private static final Logger log = Logger.getLogger(PlanTask.class);
    
    private final String originService;
    
    public PlanTask(QueryExecutor source, QueryTask task, String originService) {
        super(source, task);
        this.originService = originService;
    }
    
    @Override
    public boolean executeTask(CachedQueryStatus queryStatus, Connector connector) throws Exception {
        
        assert (QueryRequest.Method.PLAN.equals(task.getAction()));
        
        TaskKey taskKey = task.getTaskKey();
        String queryId = taskKey.getQueryId();
        QueryLogic<?> queryLogic = getQueryLogic(queryStatus.getQuery(), queryStatus.getCurrentUser().getPrimaryUser().getRoles());
        try {
            // by default we will expand the fields but not the values.
            boolean expandFields = true;
            boolean expandValues = false;
            Query query = queryStatus.getQuery();
            for (QueryImpl.Parameter p : query.getParameters()) {
                if (p.getParameterName().equals(QUERY_PLAN_EXPAND_FIELDS)) {
                    expandFields = Boolean.parseBoolean(p.getParameterValue());
                } else if (p.getParameterName().equals(QUERY_PLAN_EXPAND_VALUES)) {
                    expandValues = Boolean.parseBoolean(p.getParameterValue());
                }
            }
            String plan = queryLogic.getPlan(connector, queryStatus.getQuery(), queryStatus.getCalculatedAuthorizations(), expandFields, expandValues);
            queryStatus.setPlan(plan);
            
            notifyOriginOfPlan(queryId);
        } finally {
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
