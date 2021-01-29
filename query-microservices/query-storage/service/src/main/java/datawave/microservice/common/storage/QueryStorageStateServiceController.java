package datawave.microservice.common.storage;

import datawave.microservice.common.storage.config.QueryStorageProperties;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;
import java.util.List;
import java.util.UUID;

/**
 * The QueryStorageController presents the REST endpoints for the query storage service.
 */
@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryStorageStateServiceController implements QueryStorageStateService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final QueryStorageProperties storageProperties;
    
    private final QueryStorageCache cache;
    
    public QueryStorageStateServiceController(QueryStorageProperties storageProperties, QueryStorageCache cache) {
        this.storageProperties = storageProperties;
        this.cache = cache;
    }
    
    @ApiOperation(value = "Get the list of running queries.")
    @RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/queries", method = RequestMethod.GET)
    @Override
    public List<QueryState> getRunningQueries() {
        return cache.getQueries();
    }
    
    @ApiOperation(value = "Get the list of tasks for a query")
    @RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/query/{id}", method = RequestMethod.GET)
    @Override
    public QueryState getQuery(@PathVariable("id") String queryId) {
        return cache.getQuery(UUID.fromString(queryId));
    }
    
    @ApiOperation(value = "Get the list of tasks for a query")
    @RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/tasks/{id}", method = RequestMethod.GET)
    @Override
    public List<TaskDescription> getTasks(@PathVariable("id") String queryId) {
        return cache.getTaskDescriptions(UUID.fromString(queryId));
    }
    
    @ApiOperation(value = "Get the list of running task ids.")
    @RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/tasks/{type}", method = RequestMethod.GET)
    @Override
    public List<QueryState> getRunningQueries(@PathVariable("type") String type) {
        return cache.getQueries(new QueryType(type));
    }
}
