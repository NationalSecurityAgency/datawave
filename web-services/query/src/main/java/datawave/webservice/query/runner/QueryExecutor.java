package datawave.webservice.query.runner;

import java.util.Date;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import datawave.webservice.query.QueryPersistence;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;

public interface QueryExecutor {
    
    /**
     * List QueryLogic types that are currently available
     *
     * @return base response
     */
    QueryLogicResponse listQueryLogic();
    
    /**
     * @param logicName
     * @param queryParameters
     * @return
     */
    GenericResponse<String> predictQuery(String logicName, MultivaluedMap<String,String> queryParameters);
    
    /**
     * @param logicName
     * @param queryParameters
     * @return
     */
    GenericResponse<String> defineQuery(String logicName, MultivaluedMap<String,String> queryParameters);
    
    /**
     * @param logicName
     * @param queryParameters
     * @return
     */
    GenericResponse<String> createQuery(String logicName, MultivaluedMap<String,String> queryParameters);
    
    /**
     * @param logicName
     * @param queryParameters
     * @return
     */
    BaseQueryResponse createQueryAndNext(String logicName, MultivaluedMap<String,String> queryParameters);
    
    /**
     * Resets the query named by {@code id}. If the query is not alive, meaning that the current session has expired (due to either timeout, or server failure),
     * then this will reload the query and start it over. If the query is alive, it closes it and starts the query over.
     *
     * @param id
     *            the ID of the query to reload/reset
     * @return an empty response
     */
    VoidResponse reset(String id);
    
    /**
     * 
     * @param uuid
     * @param uuidType
     * @param uriInfo
     * @param httpHeaders
     * @return content results, either as a paged BaseQueryResponse or StreamingOutput
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 202 if asynch is true - see Location response header for the job URI location
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    <T> T lookupContentByUUID(String uuidType, String uuid, UriInfo uriInfo, HttpHeaders httpHeaders);
    
    /**
     * 
     * @param queryParameters
     * @param httpHeaders
     * @return content results, either as a paged BaseQueryResponse or StreamingOutput
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    <T> T lookupContentByUUIDBatch(MultivaluedMap<String,String> queryParameters, HttpHeaders httpHeaders);
    
    /**
     * 
     * @param uuidType
     * @param uuid
     * @param uriInfo
     * @param httpHeaders
     * @return
     * @return event results, either as a paged BaseQueryResponse (automatically closed upon return) or StreamingOutput
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    <T> T lookupUUID(String uuidType, String uuid, UriInfo uriInfo, HttpHeaders httpHeaders);
    
    /**
     * 
     * @param queryParameters
     * @param httpHeaders
     * @return event results, either as a paged BaseQueryResponse or StreamingOutput
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    <T> T lookupUUIDBatch(MultivaluedMap<String,String> queryParameters, HttpHeaders httpHeaders);
    
    /**
     * Gets the plan from the query object. If the object is no longer alive, meaning that the current session has expired, then this will fail.
     *
     * @param id
     * @return the plan
     */
    GenericResponse<String> plan(String id);
    
    /**
     * Gets the latest query predictions from the query object. If the object is no longer alive, meaning that the current session has expired, then this will
     * fail.
     *
     * @param id
     * @return the predictions
     */
    GenericResponse<String> predictions(String id);
    
    /**
     * Gets the next page of results from the query object. If the object is no longer alive, meaning that the current session has expired, then this will fail.
     * 
     * @param id
     * @return set of Row objects
     */
    BaseQueryResponse next(String id);
    
    /**
     * Locates queries for the current user by name.
     *
     * @param name
     *            the name of the query to locate
     * @return the query named {@code name}, if any
     */
    QueryImplListResponse list(String name);
    
    /**
     *
     * @return list of users queries.
     */
    QueryImplListResponse listUserQueries();
    
    /**
     * Updates a query object
     *
     * @param id
     *            - the ID of the query to update (required)
     * @param queryLogicName
     *            - name of class that this query should be run with (optional)
     * @param newQuery
     *            - query string (optional, auditing required if changed)
     * @param newColumnVisibility
     *            - query string column visibility (required if any auditable properties are updated)
     * @param newBeginDate
     *            - begin date range for the query (optional, auditing required if changed)
     * @param newEndDate
     *            - end date range for the query (optional, auditing required if changed)
     * @param newQueryAuthorizations
     *            - authorizations for use in the query (optional, auditing required if changed)
     * @param newExpirationDate
     *            - defaults to 1 day, meaningless if transient (optional)
     * @param newPagesize
     *            - number of results to return on each call to next() (optional)
     * @param newPageTimeout
     *            - specify timeout (in minutes) for each call to next(), default to -1 for disabled (optional)
     * @param newMaxResultsOverride
     *            - max results (optional)
     * @param newPersistenceMode
     *            - indicates whether or not the query is persistent (optional)
     * @param newParameters
     *            - optional parameters to the query, a semi-colon separated list name=value pairs (optional, auditing required if changed)
     * @return base response
     */
    GenericResponse<String> updateQuery(String id, String queryLogicName, String newQuery, String newColumnVisibility, Date newBeginDate, Date newEndDate,
                    String newQueryAuthorizations, Date newExpirationDate, Integer newPagesize, Integer newPageTimeout, Long newMaxResultsOverride,
                    QueryPersistence newPersistenceMode, String newParameters);
    
    /**
     * Duplicates a query and allows modification of optional properties
     *
     * @param id
     *            - the ID of the query to copy (required)
     * @param newQueryName
     *            - query name
     * @param newQueryLogicName
     *            - name of class that this query should be run with (optional)
     * @param newQuery
     *            - query string (optional, auditing required if changed)
     * @param newColumnVisibility
     *            - query string column visibility (required if any auditable properties are updated)
     * @param newBeginDate
     *            - begin date range for the query (optional, auditing required if changed)
     * @param newEndDate
     *            - end date range for the query (optional, auditing required if changed)
     * @param newQueryAuthorizations
     *            - authorizations for use in the query (optional, auditing required if changed)
     * @param newExpirationDate
     *            - defaults to 1 day, meaningless if transient (optional)
     * @param newPagesize
     *            - number of results to return on each call to next() (optional)
     * @param newPageTimeout
     *            - specify timeout (in minutes) for each call to next(), default to -1 for disabled (optional)
     * @param newMaxResultsOverride
     *            - max results (optional)
     * @param newPersistenceMode
     *            - indicates whether or not the query is persistent (optional)
     * @param newParameters
     *            - optional parameters to the query, a semi-colon separated list name=value pairs (optional, auditing required if changed)
     * @param trace
     *            - optional (defaults to {@code false}) indication of whether or not the query should be traced using the distributed tracing mechanism
     * @return the new query ID in the result element
     */
    GenericResponse<String> duplicateQuery(String id, String newQueryName, String newQueryLogicName, String newQuery, String newColumnVisibility,
                    Date newBeginDate, Date newEndDate, String newQueryAuthorizations, Date newExpirationDate, Integer newPagesize, Integer newPageTimeout,
                    Long newMaxResultsOverride, QueryPersistence newPersistenceMode, String newParameters, boolean trace);
    
    /**
     * Release the resources associated with this query
     * 
     * @param id
     * @return base response
     */
    VoidResponse close(String id);
    
    VoidResponse adminClose(String id);
    
    /**
     * Cancels the current query and releases the resources associated with it.
     * 
     * @param id
     * @return base response
     */
    VoidResponse cancel(String id);
    
    VoidResponse adminCancel(String id);
    
    /**
     * remove query
     * 
     * @return base response
     * @param id
     */
    VoidResponse remove(String id);
    
    /**
     * <strong>Administrator credentials required.</strong>
     * <p>
     * Enables tracing for all queries whose query string matches a regular expression and/or are submitted by a named user. Note that at least one of
     * {@code queryRegex} or {@code user} must be specified. If both are specified, then queries submitted by {@code user} that match {@code queryRegex} are
     * traced.
     * <p>
     * All traces are stored under the query UUID.
     *
     * @param queryRegex
     *            (optional) the query regular expression defining queries to trace
     * @param user
     *            (optional) the user name for which to trace queries
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 400 if neither queryRegex nor user are specified
     * @HTTP 401 if the user does not have Administrative credentials
     */
    VoidResponse enableTracing(String queryRegex, String user);
    
    /**
     * <strong>Administrator credentials required.</strong>
     * <p>
     * Disables tracing that was previously enabled using the {@link #enableTracing(String, String)} method.
     *
     * @param queryRegex
     *            (optional) the query regular expression defining queries to disable tracing
     * @param user
     *            (optional) the user name for which to disable query tracing
     * @return datawave.webservice.result.VoidResponse
     *
     * @HTTP 200 success
     * @HTTP 400 if neither queryRegex nor user are specified
     * @HTTP 401 if the user does not have Administrative credentials
     */
    VoidResponse disableTracing(String queryRegex, String user);
    
    /**
     * <strong>Administrator credentials required.</strong>
     * <p>
     * Disables all tracing that was enabled using the {@link #enableTracing(String, String)} method. Note that this does not prevent individual queries that
     * are created with the trace parameter specified.
     *
     * @return datawave.webservice.result.VoidResponse
     *
     * @HTTP 200 success
     * @HTTP 401 if the user does not have Administrative credentials
     */
    VoidResponse disableAllTracing();
    
    /**
     * Creates a query object for the user and returns all of the pages in a stream. When done, closes the query. This method is a convenience for users so that
     * they don't have to call create/next/next/next/.../close. Callers should utilize the max.override.results parameter to limit the number of results that
     * they receive.
     * 
     * @param logicName
     * @param queryParameters
     * @param httpHeaders
     *            HttpHeaders object injected by the JAX-RS layer
     * @return
     */
    StreamingOutput execute(String logicName, MultivaluedMap<String,String> queryParameters, HttpHeaders httpHeaders);
    
}
