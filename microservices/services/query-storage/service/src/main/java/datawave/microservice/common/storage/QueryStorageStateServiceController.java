package datawave.microservice.common.storage;

import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStatusCache;
import datawave.microservice.query.storage.TaskCache;
import datawave.microservice.query.storage.TaskDescription;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.query.storage.TaskStatesCache;
import io.swagger.annotations.ApiOperation;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * The QueryStorageController presents the REST endpoints for the query storage service.
 */
@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryStorageStateServiceController implements QueryStorageStateService {
    private final QueryStatusCache queryStatusCache;
    private final TaskStatesCache taskStatesCache;
    private final TaskCache taskCache;
    
    public QueryStorageStateServiceController(QueryStatusCache queryStatusCache, TaskStatesCache taskStatesCache, TaskCache taskCache) {
        this.queryStatusCache = queryStatusCache;
        this.taskStatesCache = taskStatesCache;
        this.taskCache = taskCache;
    }
    
    @ApiOperation(value = "Get the running queries.")
    @Secured({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/queries", method = RequestMethod.GET)
    @Override
    public List<datawave.microservice.query.storage.QueryState> getRunningQueries() {
        List<datawave.microservice.query.storage.QueryState> queries = new ArrayList<>();
        for (QueryStatus query : queryStatusCache.getQueryStatus()) {
            if (query.getQueryState() == QueryStatus.QUERY_STATE.CREATE) {
                TaskStates taskStates = taskStatesCache.getTaskStates(query.getQueryKey().getQueryId());
                queries.add(new datawave.microservice.query.storage.QueryState(query, taskStates));
            }
        }
        return queries;
    }
    
    @ApiOperation(value = "Get the query and task states for a query")
    @Secured({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/query/{id}", method = RequestMethod.GET)
    @Override
    public datawave.microservice.query.storage.QueryState getQuery(@PathVariable("id") String queryId) {
        QueryStatus query = queryStatusCache.getQueryStatus(queryId);
        if (query != null) {
            return new datawave.microservice.query.storage.QueryState(query, taskStatesCache.getTaskStates(queryId));
        }
        return null;
    }
    
    @ApiOperation(value = "Get the list of tasks for a query")
    @Secured({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/tasks/{id}", method = RequestMethod.GET)
    @Override
    public List<TaskDescription> getTasks(@PathVariable("id") String queryId) {
        return taskCache.getTaskDescriptions(queryId);
    }
}
