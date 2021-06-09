package datawave.microservice.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.marking.SecurityMarking;
import datawave.microservice.audit.AuditClient;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.common.audit.PrivateAuditConstants;
import datawave.microservice.common.storage.QueryPool;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryStatus;
import datawave.microservice.common.storage.QueryStorageCache;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.remote.QueryRequestHandler;
import datawave.microservice.query.runner.NextCall;
import datawave.microservice.query.status.QueryStatusUpdateHelper;
import datawave.microservice.query.util.QueryUtil;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.cache.QueryMetricFactory;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.TimeoutQueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.result.logic.QueryLogicDescription;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static datawave.microservice.common.storage.QueryStatus.QUERY_STATE.CANCELED;
import static datawave.microservice.common.storage.QueryStatus.QUERY_STATE.CLOSED;
import static datawave.microservice.common.storage.QueryStatus.QUERY_STATE.CREATED;
import static datawave.microservice.common.storage.QueryStatus.QUERY_STATE.DEFINED;
import static datawave.microservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static io.undertow.util.StatusCodes.BAD_REQUEST;
import static io.undertow.util.StatusCodes.INTERNAL_SERVER_ERROR;

@Service
public class QueryManagementService implements QueryRequestHandler {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private final QueryProperties queryProperties;
    
    private final ApplicationContext appCtx;
    private final BusProperties busProperties;
    
    // Note: QueryParameters need to be request scoped
    private final QueryParameters queryParameters;
    // Note: SecurityMarking needs to be request scoped
    private final SecurityMarking securityMarking;
    
    private final QueryLogicFactory queryLogicFactory;
    private final QueryMetricFactory queryMetricFactory;
    private final ResponseObjectFactory responseObjectFactory;
    private final QueryStorageCache queryStorageCache;
    private final QueryQueueManager queryQueueManager;
    private final AuditClient auditClient;
    private final ThreadPoolTaskExecutor nextCallExecutor;
    private final String identifier;
    
    private final QueryStatusUpdateHelper queryStatusUpdateHelper;
    private final MultiValueMap<String,NextCall> nextCallMap = new LinkedMultiValueMap<>();
    
    public QueryManagementService(QueryProperties queryProperties, ApplicationContext appCtx, BusProperties busProperties, QueryParameters queryParameters,
                    SecurityMarking securityMarking, QueryLogicFactory queryLogicFactory, QueryMetricFactory queryMetricFactory,
                    ResponseObjectFactory responseObjectFactory, QueryStorageCache queryStorageCache, QueryQueueManager queryQueueManager,
                    AuditClient auditClient, ThreadPoolTaskExecutor nextCallExecutor) {
        this.queryProperties = queryProperties;
        this.appCtx = appCtx;
        this.busProperties = busProperties;
        this.queryParameters = queryParameters;
        this.securityMarking = securityMarking;
        this.queryLogicFactory = queryLogicFactory;
        this.queryMetricFactory = queryMetricFactory;
        this.responseObjectFactory = responseObjectFactory;
        this.queryStorageCache = queryStorageCache;
        this.queryQueueManager = queryQueueManager;
        this.auditClient = auditClient;
        this.nextCallExecutor = nextCallExecutor;
        
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "UNKNOWN";
        }
        this.identifier = hostname;
        
        this.queryStatusUpdateHelper = new QueryStatusUpdateHelper(this.queryProperties, this.queryStorageCache);
    }
    
    /**
     * List QueryLogic types that are currently available
     *
     * @return
     */
    public QueryLogicResponse listQueryLogic() {
        QueryLogicResponse response = new QueryLogicResponse();
        List<QueryLogic<?>> queryLogicList = queryLogicFactory.getQueryLogicList();
        List<QueryLogicDescription> logicConfigurationList = new ArrayList<>();
        
        // reference query necessary to avoid NPEs in getting the Transformer and BaseResponse
        Query q = new QueryImpl();
        Date now = new Date();
        q.setExpirationDate(now);
        q.setQuery("test");
        q.setQueryAuthorizations("ALL");
        
        for (QueryLogic<?> queryLogic : queryLogicList) {
            try {
                QueryLogicDescription logicDesc = new QueryLogicDescription(queryLogic.getLogicName());
                logicDesc.setAuditType(queryLogic.getAuditType(null).toString());
                logicDesc.setLogicDescription(queryLogic.getLogicDescription());
                
                Set<String> optionalQueryParameters = queryLogic.getOptionalQueryParameters();
                if (optionalQueryParameters != null) {
                    logicDesc.setSupportedParams(new ArrayList<>(optionalQueryParameters));
                }
                Set<String> requiredQueryParameters = queryLogic.getRequiredQueryParameters();
                if (requiredQueryParameters != null) {
                    logicDesc.setRequiredParams(new ArrayList<>(requiredQueryParameters));
                }
                Set<String> exampleQueries = queryLogic.getExampleQueries();
                if (exampleQueries != null) {
                    logicDesc.setExampleQueries(new ArrayList<>(exampleQueries));
                }
                Set<String> requiredRoles = queryLogic.getRequiredRoles();
                if (requiredRoles != null) {
                    List<String> requiredRolesList = new ArrayList<>(queryLogic.getRequiredRoles());
                    logicDesc.setRequiredRoles(requiredRolesList);
                }
                
                try {
                    logicDesc.setResponseClass(queryLogic.getResponseClass(q));
                } catch (QueryException e) {
                    log.error("Unable to get response class for query logic: " + queryLogic.getLogicName(), e);
                    response.addException(e);
                    logicDesc.setResponseClass("unknown");
                }
                
                List<String> querySyntax = new ArrayList<>();
                try {
                    Method m = queryLogic.getClass().getMethod("getQuerySyntaxParsers");
                    Object result = m.invoke(queryLogic);
                    if (result instanceof Map<?,?>) {
                        Map<?,?> map = (Map<?,?>) result;
                        for (Object o : map.keySet())
                            querySyntax.add(o.toString());
                    }
                } catch (Exception e) {
                    log.warn("Unable to get query syntax for query logic: " + queryLogic.getClass().getCanonicalName());
                }
                if (querySyntax.isEmpty()) {
                    querySyntax.add("CUSTOM");
                }
                logicDesc.setQuerySyntax(querySyntax);
                
                logicConfigurationList.add(logicDesc);
            } catch (Exception e) {
                log.error("Error setting query logic description", e);
            }
        }
        logicConfigurationList.sort(Comparator.comparing(QueryLogicDescription::getName));
        response.setQueryLogicList(logicConfigurationList);
        
        return response;
    }
    
    /**
     * Defines a datawave query.
     * <p>
     * Validates the query parameters using the base validation, query logic-specific validation, and markings validation. If the parameters are valid, the
     * query will be stored in the query storage cache where it can be acted upon in a subsequent call.
     *
     * @param queryLogicName
     * @param parameters
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public GenericResponse<String> define(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        try {
            TaskKey taskKey = storeQuery(queryLogicName, parameters, currentUser, false);
            GenericResponse<String> response = new GenericResponse<>();
            response.setResult(taskKey.getQueryId().toString());
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error defining query", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error defining query.");
        }
    }
    
    /**
     * Creates a datawave query.
     * <p>
     * Validates the query parameters using the base validation, query logic-specific validation, and markings validation. If the parameters are valid, the
     * query will be stored in the query storage cache where it can be acted upon in a subsequent call.
     *
     * @param queryLogicName
     * @param parameters
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public GenericResponse<String> create(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        try {
            TaskKey taskKey = storeQuery(queryLogicName, parameters, currentUser, true);
            GenericResponse<String> response = new GenericResponse<>();
            response.setResult(taskKey.getQueryId().toString());
            response.setHasResults(true);
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error creating query", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error creating query.");
        }
    }
    
    private TaskKey storeQuery(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser, boolean isCreateRequest)
                    throws QueryException {
        return storeQuery(queryLogicName, parameters, currentUser, isCreateRequest, null);
    }
    
    private TaskKey storeQuery(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser, boolean isCreateRequest,
                    String queryId) throws QueryException {
        // validate query and get a query logic
        QueryLogic<?> queryLogic = validateQuery(queryLogicName, parameters, currentUser);
        
        String userId = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        log.trace(userId + " has authorizations " + currentUser.getPrimaryUser().getAuths());
        
        // set some audit parameters which are used internally
        String userDn = currentUser.getPrimaryUser().getDn().subjectDN();
        setInternalAuditParameters(queryLogicName, userDn, parameters);
        
        Query query = createQuery(queryLogicName, parameters, userDn, currentUser.getDNs(), queryId);
        
        // if this is a create request, send an audit record to the auditor
        if (isCreateRequest) {
            audit(query, queryLogic, parameters, currentUser);
        }
        
        try {
            // persist the query w/ query id in the query storage cache
            TaskKey taskKey;
            if (isCreateRequest) {
                // @formatter:off
                taskKey = queryStorageCache.createQuery(
                        new QueryPool(getPoolName()),
                        query,
                        AuthorizationsUtil.getDowngradedAuthorizations(queryParameters.getAuths(), currentUser),
                        getMaxConcurrentTasks(queryLogic));
                // @formatter:on
                
                // publish a create event to the executor pool
                publishExecutorEvent(QueryRequest.create(taskKey.getQueryId().toString()), getPoolName());
            } else {
                // @formatter:off
                taskKey = queryStorageCache.defineQuery(
                        new QueryPool(getPoolName()),
                        query,
                        AuthorizationsUtil.getDowngradedAuthorizations(queryParameters.getAuths(), currentUser),
                        getMaxConcurrentTasks(queryLogic));
                // @formatter:on
            }
            
            // TODO: JWO: Figure out how to make query tracing work with our new architecture. Datawave issue #1155
            
            // TODO: JWO: Figure out how to make query metrics work with our new architecture. Datawave issue #1156
            
            return taskKey;
        } catch (Exception e) {
            log.error("Unknown error storing query", e);
            throw new BadRequestQueryException(DatawaveErrorCode.RUNNING_QUERY_CACHE_ERROR, e);
        }
    }
    
    /**
     * Creates a datawave query and calls next.
     * <p>
     * Validates the query parameters using the base validation, query logic-specific validation, and markings validation. If the parameters are valid, the
     * query will be stored in the query storage cache where it can be acted upon.
     *
     * Once created, gets the next page of results from the query object. The response object type is dynamic, see the listQueryLogic operation to determine
     * what the response type object will be.
     *
     * @param queryLogicName
     * @param parameters
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public BaseQueryResponse createAndNext(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        try {
            String queryId = create(queryLogicName, parameters, currentUser).getResult();
            return next(queryId, currentUser.getPrimaryUser().getRoles());
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error creating and nexting query", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error creating and nexting query query.");
        }
    }
    
    /**
     * Gets the next page of results from the query object. If the object is no longer alive, meaning that the current session has expired, then this fail. The
     * response object type is dynamic, see the listQueryLogic operation to determine what the response type object will be.
     *
     * @param queryId
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public BaseQueryResponse next(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            // make sure the state is created
            if (queryStatus.getQueryState() == CREATED) {
                return next(queryId, currentUser.getPrimaryUser().getRoles());
            } else {
                throw new QueryException("Cannot call next on a query with state: " + queryStatus.getQueryState().name(), BAD_REQUEST + "-1");
            }
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error getting next page for query " + queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Unknown error getting next page for query " + queryId);
        }
    }
    
    /**
     * Gets the next page of results from the query object. If the object is no longer alive, meaning that the current session has expired, then this fail. The
     * response object type is dynamic, see the listQueryLogic operation to determine what the response type object will be.
     *
     * @param queryId
     * @param userRoles
     * @return
     * @throws QueryException
     */
    private BaseQueryResponse next(String queryId, Collection<String> userRoles) throws Exception {
        UUID queryUUID = UUID.fromString(queryId);
        
        // before we spin up a separate thread, make sure we are allowed to call next
        boolean success = false;
        QueryStatus queryStatus = queryStatusUpdateHelper.lockedUpdate(queryUUID, queryStatusUpdateHelper::claimConcurrentNext);
        try {
            // publish a next event to the executor pool
            publishNextEvent(queryId, queryStatus.getQueryKey().getQueryPool().getName());
            
            // get the query logic
            String queryLogicName = queryStatus.getQuery().getQueryLogicName();
            QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic(queryStatus.getQuery().getQueryLogicName(), userRoles);
            
            // @formatter:off
            final NextCall nextCall = new NextCall.Builder()
                    .setQueryProperties(queryProperties)
                    .setQueryQueueManager(queryQueueManager)
                    .setQueryStorageCache(queryStorageCache)
                    .setQueryMetricFactory(queryMetricFactory)
                    .setQueryId(queryId)
                    .setQueryLogic(queryLogic)
                    .setIdentifier(identifier)
                    .build();
            // @formatter:on
            
            nextCallMap.add(queryId, nextCall);
            try {
                // submit the next call to the executor
                nextCall.setFuture(nextCallExecutor.submit(nextCall));
                
                // wait for the results to be ready
                ResultsPage<Object> resultsPage = nextCall.getFuture().get();
                
                // format the response
                if (!resultsPage.getResults().isEmpty()) {
                    BaseQueryResponse response = queryLogic.getTransformer(queryStatus.getQuery()).createResponse(resultsPage);
                    
                    // after all of our work is done, perform our final query status update for this next call
                    queryStatus = queryStatusUpdateHelper.lockedUpdate(queryUUID, status -> {
                        queryStatusUpdateHelper.releaseConcurrentNext(status);
                        status.setLastPageNumber(status.getLastPageNumber() + 1);
                        status.setNumResultsReturned(status.getNumResultsReturned() + resultsPage.getResults().size());
                    });
                    success = true;
                    
                    response.setHasResults(true);
                    response.setPageNumber(queryStatus.getLastPageNumber());
                    response.setLogicName(queryLogicName);
                    response.setQueryId(queryId);
                    return response;
                } else {
                    if (nextCall.getMetric().getLifecycle() == BaseQueryMetric.Lifecycle.NEXTTIMEOUT) {
                        throw new TimeoutQueryException(DatawaveErrorCode.QUERY_TIMEOUT, MessageFormat.format("{0}", queryId));
                    } else {
                        throw new NoResultsQueryException(DatawaveErrorCode.NO_QUERY_RESULTS_FOUND, MessageFormat.format("{0}", queryId));
                    }
                }
            } catch (TaskRejectedException e) {
                throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Next task rejected by the executor for query " + queryId);
            } catch (ExecutionException e) {
                // try to unwrap the execution exception
                Throwable cause = e.getCause();
                if (cause instanceof Exception) {
                    throw (Exception) e.getCause();
                } else {
                    throw e;
                }
            } finally {
                // remove this next call from the map, and decrement the next count for this query
                nextCallMap.get(queryId).remove(nextCall);
            }
        } finally {
            // update query status if we failed
            if (!success) {
                queryStatusUpdateHelper.lockedUpdate(queryUUID, queryStatusUpdateHelper::releaseConcurrentNext);
            }
        }
    }
    
    /**
     * Releases the resources associated with this query. Any currently running calls to 'next' on the query will be stopped. Calls to 'next' after a 'cancel'
     * will start over at page 1.
     *
     * @param queryId
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public VoidResponse cancel(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        return cancel(queryId, currentUser, false);
    }
    
    /**
     * Releases the resources associated with this query. Any currently running calls to 'next' on the query will be stopped. Calls to 'next' after a 'cancel'
     * will start over at page 1.
     *
     * @param queryId
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public VoidResponse adminCancel(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        return cancel(queryId, currentUser, true);
    }
    
    private VoidResponse cancel(String queryId, ProxiedUserDetails currentUser, boolean adminOverride) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser, adminOverride);
            
            VoidResponse response = new VoidResponse();
            switch (queryStatus.getQueryState()) {
                case DEFINED:
                case CREATED:
                    // close the query
                    cancel(queryId, true);
                    response.addMessage(queryId + " canceled.");
                    break;
                // TODO: Should we throw an exception for these cases?
                case CLOSED:
                case CANCELED:
                case FAILED:
                    response.addMessage(queryId + " was not canceled because it is not running.");
                    break;
                default:
                    throw new IllegalStateException("Unexpected query state: " + queryStatus.getQueryState());
            }
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.CANCELLATION_ERROR, e, "query_id: " + queryId);
            log.error("Unable to cancel query with id " + queryId, queryException);
            throw queryException;
        }
    }
    
    /**
     * Once the cancel request is validated, this method is called to actually cancel the query.
     *
     * @param queryId
     *            The id of the query to cancel
     * @param publishEvent
     *            If true, the cancel request will be published on the bus
     * @throws InterruptedException
     * @throws QueryException
     */
    public void cancel(String queryId, boolean publishEvent) throws InterruptedException, QueryException {
        // if we have an active next call for this query locally, cancel it
        List<NextCall> nextCalls = nextCallMap.get(queryId);
        if (nextCalls != null) {
            nextCalls.forEach(NextCall::cancel);
        }
        
        if (publishEvent) {
            // only the initial event publisher should update the status
            QueryStatus queryStatus = queryStatusUpdateHelper.lockedUpdate(UUID.fromString(queryId), status -> {
                // update query state to CANCELED
                status.setQueryState(CANCELED);
            });
            
            QueryRequest cancelRequest = QueryRequest.cancel(queryId);
            
            // publish a cancel event to all of the query services
            publishSelfEvent(cancelRequest);
            
            // publish a cancel event to the executor pool
            publishExecutorEvent(cancelRequest, queryStatus.getQueryKey().getQueryPool().getName());
        }
    }
    
    /**
     * Releases the resources associated with this query. Any currently running calls to 'next' on the query will continue until they finish. Calls to 'next'
     * after a 'close' will start over at page 1.
     *
     * @param queryId
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public VoidResponse close(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        return close(queryId, currentUser, false);
    }
    
    /**
     * Releases the resources associated with this query. Any currently running calls to 'next' on the query will continue until they finish. Calls to 'next'
     * after a 'close' will start over at page 1.
     *
     * @param queryId
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public VoidResponse adminClose(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        return close(queryId, currentUser, true);
    }
    
    private VoidResponse close(String queryId, ProxiedUserDetails currentUser, boolean adminOverride) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser, adminOverride);
            
            VoidResponse response = new VoidResponse();
            switch (queryStatus.getQueryState()) {
                case DEFINED:
                case CREATED:
                    // close the query
                    close(queryId);
                    response.addMessage(queryId + " closed.");
                    break;
                // TODO: Should we throw an exception for these cases?
                case CLOSED:
                case CANCELED:
                case FAILED:
                    response.addMessage(queryId + " was not closed because it is not running.");
                    break;
                default:
                    throw new IllegalStateException("Unexpected query state: " + queryStatus.getQueryState());
            }
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.CLOSE_ERROR, e, "query_id: " + queryId);
            log.error("Unable to close query with id " + queryId, queryException);
            throw queryException;
        }
    }
    
    /**
     * Once the close request is validated, this method is called to actually close the query.
     *
     * @param queryId
     *            The id of the query to close
     * @throws InterruptedException
     * @throws QueryException
     */
    public void close(String queryId) throws InterruptedException, QueryException {
        QueryStatus queryStatus = queryStatusUpdateHelper.lockedUpdate(UUID.fromString(queryId), status -> {
            // update query state to CLOSED
            status.setQueryState(CLOSED);
        });
        
        // publish a close event to the executor pool
        publishExecutorEvent(QueryRequest.close(queryId), queryStatus.getQueryKey().getQueryPool().getName());
    }
    
    /**
     * Resets the query named by {@code queryId}. If the query is not alive, meaning that the current session has expired (due to either timeout, or server
     * failure), then this will reload the query and start it over. If the query is alive, it closes it and starts the query over.
     *
     * @param queryId
     * @return
     * @throws QueryException
     */
    public GenericResponse<String> reset(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            // cancel the query if it is running
            if (queryStatus.getQueryState() == CREATED) {
                cancel(queryStatus.getQueryKey().getQueryId().toString(), true);
            }
            
            // create a new query which is an exact copy of the specified query
            TaskKey taskKey = duplicate(queryStatus, new LinkedMultiValueMap<>(), currentUser);
            
            GenericResponse<String> response = new GenericResponse<>();
            response.addMessage(queryId + " reset.");
            response.setResult(taskKey.getQueryId().toString());
            response.setHasResults(true);
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error resetting query " + queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Unknown error resetting query " + queryId);
        }
    }
    
    /**
     * Remove (delete) the query
     *
     * @param queryId
     * @param currentUser
     * @return
     */
    public VoidResponse remove(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            if (!remove(queryStatus)) {
                throw new QueryException("Failed to remove " + queryId, INTERNAL_SERVER_ERROR + "-1");
            }
            
            VoidResponse response = new VoidResponse();
            response.addMessage(queryId + " removed.");
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error removing query " + queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Unknown error removing query " + queryId);
        }
    }
    
    private boolean remove(QueryStatus queryStatus) throws IOException {
        boolean success = false;
        // remove the query from the cache if it is not running
        if (queryStatus.getQueryState() != CREATED) {
            success = queryStorageCache.deleteQuery(queryStatus.getQueryKey().getQueryId());
        }
        return success;
    }
    
    public GenericResponse<String> update(String queryId, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            GenericResponse<String> response = new GenericResponse<>();
            if (!parameters.isEmpty()) {
                // TODO: Are we losing anything by recreating the parameters this way?
                // recreate the query parameters
                MultiValueMap<String,String> currentParams = new LinkedMultiValueMap<>();
                currentParams.addAll(queryStatus.getQuery().getOptionalQueryParameters());
                queryStatus.getQuery().getParameters().forEach(x -> currentParams.add(x.getParameterName(), x.getParameterValue()));
                
                boolean updated = false;
                if (queryStatus.getQueryState() == DEFINED) {
                    // update all parameters if the state is defined
                    updated = updateParameters(parameters, currentParams);
                    
                    // redefine the query
                    if (updated) {
                        storeQuery(queryStatus.getQuery().getQueryLogicName(), currentParams, currentUser, false, queryId);
                    }
                } else if (queryStatus.getQueryState() == CREATED) {
                    // if the query is created/running, update safe parameters only
                    List<String> ignoredParams = new ArrayList<>(parameters.keySet());
                    List<String> safeParams = new ArrayList<>(queryProperties.getUpdatableParams());
                    safeParams.retainAll(parameters.keySet());
                    ignoredParams.removeAll(safeParams);
                    
                    // only update the safe parameters if the query is running
                    updated = updateParameters(safeParams, parameters, currentParams);
                    
                    if (updated) {
                        // validate the update
                        String queryLogicName = queryStatus.getQuery().getQueryLogicName();
                        validateQuery(queryLogicName, parameters, currentUser);
                        
                        // create a new query object
                        String userDn = currentUser.getPrimaryUser().getDn().subjectDN();
                        Query query = createQuery(queryLogicName, parameters, userDn, currentUser.getDNs(), queryId);
                        
                        // save the new query object in the cache
                        queryStatusUpdateHelper.lockedUpdate(UUID.fromString(queryId), status -> status.setQuery(query));
                    }
                    
                    if (!ignoredParams.isEmpty()) {
                        response.addMessage("The following parameters cannot be updated for a running query: " + String.join(",", ignoredParams));
                    }
                } else {
                    throw new QueryException("Cannot update a query unless it is defined or running.", BAD_REQUEST + "-1");
                }
                
                if (updated) {
                    response.addMessage(queryId + " updated.");
                } else {
                    response.addMessage(queryId + " unchanged.");
                }
            } else {
                throw new QueryException("No parameters specified for update.", BAD_REQUEST + "-1");
            }
            
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error updating query " + queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Unknown error updating query " + queryId);
        }
    }
    
    public GenericResponse<String> duplicate(String queryId, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            // define a duplicate query from the existing query
            TaskKey taskKey = duplicate(queryStatus, parameters, currentUser);
            
            GenericResponse<String> response = new GenericResponse<>();
            response.addMessage(queryId + " duplicated.");
            response.setResult(taskKey.getQueryId().toString());
            response.setHasResults(true);
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error resetting query " + queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Unknown error resetting query " + queryId);
        }
    }
    
    private TaskKey duplicate(QueryStatus queryStatus, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        // TODO: Are we losing anything by recreating the parameters this way?
        // recreate the query parameters
        MultiValueMap<String,String> currentParams = new LinkedMultiValueMap<>();
        currentParams.addAll(queryStatus.getQuery().getOptionalQueryParameters());
        queryStatus.getQuery().getParameters().forEach(x -> {
            currentParams.add(x.getParameterName(), x.getParameterValue());
        });
        
        // updated all of the passed in parameters
        updateParameters(parameters, currentParams);
        
        // define a duplicate query
        return storeQuery(queryStatus.getQuery().getQueryLogicName(), currentParams, currentUser, true);
    }
    
    /**
     * Updates the current params with the new params.
     *
     * @param newParameters
     * @param currentParams
     * @return true if current params was modified
     */
    private boolean updateParameters(MultiValueMap<String,String> newParameters, MultiValueMap<String,String> currentParams) throws QueryException {
        return updateParameters(newParameters.keySet(), newParameters, currentParams);
    }
    
    /**
     * Updates the current params with the new params for the given parameter names.
     *
     * @param parameterNames
     * @param newParameters
     * @param currentParams
     * @return true if current params was modified
     */
    private boolean updateParameters(Collection<String> parameterNames, MultiValueMap<String,String> newParameters, MultiValueMap<String,String> currentParams)
                    throws QueryException {
        boolean paramsUpdated = false;
        for (String paramName : parameterNames) {
            if (newParameters.get(paramName) != null && !newParameters.get(paramName).isEmpty()) {
                if (!newParameters.get(paramName).get(0).equals(currentParams.getFirst(paramName))) {
                    // if the new value differs from the old value, update the old value
                    currentParams.put(paramName, newParameters.remove(paramName));
                    paramsUpdated = true;
                }
            } else {
                throw new QueryException("Cannot update a query parameter without a value: " + paramName, BAD_REQUEST + "-1");
            }
        }
        return paramsUpdated;
    }
    
    private QueryStatus validateRequest(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        return validateRequest(queryId, currentUser, false);
    }
    
    private QueryStatus validateRequest(String queryId, ProxiedUserDetails currentUser, boolean adminOverride) throws QueryException {
        // does the query exist?
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(UUID.fromString(queryId));
        if (queryStatus == null) {
            throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, MessageFormat.format("{0}", queryId));
        }
        
        // admins requests can operate on any query, regardless of ownership
        if (!adminOverride) {
            // does the current user own this query?
            String userId = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
            Query query = queryStatus.getQuery();
            if (!query.getOwner().equals(userId)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", userId, query.getOwner()));
            }
        }
        
        return queryStatus;
    }
    
    @Override
    public void handleRemoteRequest(QueryRequest queryRequest) {
        try {
            if (queryRequest.getMethod() == QueryRequest.Method.CANCEL) {
                log.trace("Received remote cancel request.");
                cancel(queryRequest.getQueryId(), false);
            } else {
                log.debug("No handling specified for remote query request method: {}", queryRequest.getMethod());
            }
        } catch (Exception e) {
            log.error("Remote request failed:" + queryRequest);
        }
    }
    
    protected void audit(Query query, QueryLogic<?> queryLogic, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        Auditor.AuditType auditType = queryLogic.getAuditType(query);
        
        parameters.add(PrivateAuditConstants.AUDIT_TYPE, auditType.name());
        if (auditType != Auditor.AuditType.NONE) {
            // audit the query before execution
            try {
                try {
                    List<String> selectors = queryLogic.getSelectors(query);
                    if (selectors != null && !selectors.isEmpty()) {
                        parameters.put(PrivateAuditConstants.SELECTORS, selectors);
                    }
                } catch (Exception e) {
                    log.error("Error accessing query selector", e);
                }
                
                // is the user didn't set an audit id, use the query id
                if (!parameters.containsKey(AuditParameters.AUDIT_ID)) {
                    parameters.set(AuditParameters.AUDIT_ID, query.getId().toString());
                }
                
                // TODO: Write a test to ensure that the audit id we set is used
                // @formatter:off
                auditClient.submit(new AuditClient.Request.Builder()
                        .withParams(parameters)
                        .withQueryExpression(query.getQuery())
                        .withProxiedUserDetails(currentUser)
                        .withMarking(securityMarking)
                        .withAuditType(auditType)
                        .withQueryLogic(queryLogic.getLogicName())
                        .build());
                // @formatter:on
            } catch (IllegalArgumentException e) {
                log.error("Error validating audit parameters", e);
                throw new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER, e);
            } catch (Exception e) {
                log.error("Error auditing query", e);
                throw new BadRequestQueryException(DatawaveErrorCode.QUERY_AUDITING_ERROR, e);
            }
        }
    }
    
    protected String getPoolName() {
        return (queryParameters.getPool() != null) ? queryParameters.getPool() : queryProperties.getDefaultParams().getPool();
    }
    
    protected String getPooledExecutorName(String poolName) {
        return String.join("-", Arrays.asList(queryProperties.getExecutorServiceName(), poolName));
    }
    
    public void publishNextEvent(String queryId, String queryPool) {
        publishExecutorEvent(QueryRequest.next(queryId), queryPool);
    }
    
    private void publishExecutorEvent(QueryRequest queryRequest, String queryPool) {
        // @formatter:off
        appCtx.publishEvent(
                new RemoteQueryRequestEvent(
                        this,
                        busProperties.getId(),
                        getPooledExecutorName(queryPool),
                        queryRequest));
        // @formatter:on
    }
    
    private void publishSelfEvent(QueryRequest queryRequest) {
        // @formatter:off
        appCtx.publishEvent(
                new RemoteQueryRequestEvent(
                        this,
                        busProperties.getId(),
                        appCtx.getApplicationName(),
                        queryRequest));
        // @formatter:on
    }
    
    protected int getMaxConcurrentTasks(QueryLogic<?> queryLogic) {
        // if there's an override, use it
        if (queryParameters.isMaxConcurrentTasksOverridden()) {
            return queryParameters.getMaxConcurrentTasks();
        }
        // if the query logic has a limit, use it
        else if (queryLogic.getMaxConcurrentTasks() > 0) {
            return queryLogic.getMaxConcurrentTasks();
        }
        // otherwise, use the configuration default
        else {
            return queryProperties.getDefaultParams().getMaxConcurrentTasks();
        }
    }
    
    protected Query createQuery(String queryLogicName, MultiValueMap<String,String> parameters, String userDn, List<String> dnList, String queryId) {
        Query q = responseObjectFactory.getQueryImpl();
        q.initialize(userDn, dnList, queryLogicName, queryParameters, queryParameters.getUnknownParameters(parameters));
        q.setColumnVisibility(securityMarking.toColumnVisibilityString());
        q.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
        Thread.currentThread().setUncaughtExceptionHandler(q.getUncaughtExceptionHandler());
        if (queryId != null) {
            q.setId(UUID.fromString(queryId));
        }
        return q;
    }
    
    /**
     * This method will provide some initial query validation for the define and create query calls.
     */
    protected QueryLogic<?> validateQuery(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        // validate the query parameters
        validateParameters(queryLogicName, parameters);
        
        // create the query logic, and perform query logic parameter validation
        QueryLogic<?> queryLogic = createQueryLogic(queryLogicName, currentUser);
        validateQueryLogic(queryLogic, parameters, currentUser);
        
        // validate the security markings
        validateSecurityMarkings(parameters);
        
        return queryLogic;
    }
    
    protected void validateParameters(String queryLogicName, MultiValueMap<String,String> parameters) throws QueryException {
        // add query logic name to parameters
        parameters.add(QUERY_LOGIC_NAME, queryLogicName);
        
        log.debug(writeValueAsString(parameters));
        
        // Pull "params" values into individual query parameters for validation on the query logic.
        // This supports the deprecated "params" value (both on the old and new API). Once we remove the deprecated
        // parameter, this code block can go away.
        if (parameters.get(QueryParameters.QUERY_PARAMS) != null)
            parameters.get(QueryParameters.QUERY_PARAMS).stream().map(QueryUtil::parseParameters).forEach(parameters::addAll);
        
        parameters.remove(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ);
        parameters.remove(AuditParameters.USER_DN);
        parameters.remove(AuditParameters.QUERY_AUDIT_TYPE);
        
        // Ensure that all required parameters exist prior to validating the values.
        queryParameters.validate(parameters);
        
        // The pageSize and expirationDate checks will always be false when called from the RemoteQueryExecutor.
        // Leaving for now until we can test to ensure that is always the case.
        if (queryParameters.getPagesize() <= 0) {
            log.error("Invalid page size: " + queryParameters.getPagesize());
            throw new BadRequestQueryException(DatawaveErrorCode.INVALID_PAGE_SIZE);
        }
        
        long pageMinTimeoutMillis = queryProperties.getExpiration().getPageMinTimeoutMillis();
        long pageMaxTimeoutMillis = queryProperties.getExpiration().getPageMaxTimeoutMillis();
        long pageTimeoutMillis = TimeUnit.MINUTES.toMillis(queryParameters.getPageTimeout());
        if (queryParameters.getPageTimeout() != -1 && (pageTimeoutMillis < pageMinTimeoutMillis || pageTimeoutMillis > pageMaxTimeoutMillis)) {
            log.error("Invalid page timeout: " + queryParameters.getPageTimeout());
            throw new BadRequestQueryException(DatawaveErrorCode.INVALID_PAGE_TIMEOUT);
        }
        
        if (System.currentTimeMillis() >= queryParameters.getExpirationDate().getTime()) {
            log.error("Invalid expiration date: " + queryParameters.getExpirationDate());
            throw new BadRequestQueryException(DatawaveErrorCode.INVALID_EXPIRATION_DATE);
        }
        
        // Ensure begin date does not occur after the end date (if dates are not null)
        if ((queryParameters.getBeginDate() != null && queryParameters.getEndDate() != null)
                        && queryParameters.getBeginDate().after(queryParameters.getEndDate())) {
            log.error("Invalid begin and/or end date: " + queryParameters.getBeginDate() + " - " + queryParameters.getEndDate());
            throw new BadRequestQueryException(DatawaveErrorCode.BEGIN_DATE_AFTER_END_DATE);
        }
    }
    
    protected QueryLogic<?> createQueryLogic(String queryLogicName, ProxiedUserDetails currentUser) throws QueryException {
        // will throw IllegalArgumentException if not defined
        try {
            return queryLogicFactory.getQueryLogic(queryLogicName, currentUser.getPrimaryUser().getRoles());
        } catch (Exception e) {
            log.error("Failed to get query logic for " + queryLogicName, e);
            throw new BadRequestQueryException(DatawaveErrorCode.QUERY_LOGIC_ERROR, e);
        }
    }
    
    protected void validateQueryLogic(QueryLogic<?> queryLogic, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        queryLogic.validate(parameters);
        
        // always check against the max
        if (queryLogic.getMaxPageSize() > 0 && queryParameters.getPagesize() > queryLogic.getMaxPageSize()) {
            log.error("Invalid page size: " + queryParameters.getPagesize() + " vs " + queryLogic.getMaxPageSize());
            throw new BadRequestQueryException(DatawaveErrorCode.PAGE_SIZE_TOO_LARGE, MessageFormat.format("Max = {0}.", queryLogic.getMaxPageSize()));
        }
        
        // If the user is not privileged, make sure they didn't exceed the limits for the following parameters
        if (!currentUser.getPrimaryUser().getRoles().contains(queryProperties.getPrivilegedRole())) {
            // validate the max results override relative to the max results on a query logic
            // privileged users however can set whatever they want
            if (queryParameters.isMaxResultsOverridden() && queryLogic.getMaxResults() >= 0) {
                if (queryParameters.getMaxResultsOverride() < 0 || (queryLogic.getMaxResults() < queryParameters.getMaxResultsOverride())) {
                    log.error("Invalid max results override: " + queryParameters.getMaxResultsOverride() + " vs " + queryLogic.getMaxResults());
                    throw new BadRequestQueryException(DatawaveErrorCode.INVALID_MAX_RESULTS_OVERRIDE);
                }
            }
            
            // validate the max concurrent tasks override relative to the max concurrent tasks on a query logic
            // privileged users however can set whatever they want
            if (queryParameters.isMaxConcurrentTasksOverridden() && queryLogic.getMaxConcurrentTasks() >= 0) {
                if (queryParameters.getMaxConcurrentTasks() < 0 || (queryLogic.getMaxConcurrentTasks() < queryParameters.getMaxConcurrentTasks())) {
                    log.error("Invalid max concurrent tasks override: " + queryParameters.getMaxConcurrentTasks() + " vs "
                                    + queryLogic.getMaxConcurrentTasks());
                    throw new BadRequestQueryException(DatawaveErrorCode.INVALID_MAX_CONCURRENT_TASKS_OVERRIDE);
                }
            }
        }
        
        // Verify that the calling principal has access to the query logic.
        List<String> dnList = currentUser.getDNs();
        if (!queryLogic.containsDNWithAccess(dnList)) {
            throw new UnauthorizedQueryException("None of the DNs used have access to this query logic: " + dnList, 401);
        }
    }
    
    protected void validateSecurityMarkings(MultiValueMap<String,String> parameters) throws QueryException {
        try {
            securityMarking.clear();
            securityMarking.validate(parameters);
        } catch (IllegalArgumentException e) {
            log.error("Failed security markings validation", e);
            throw new BadRequestQueryException(DatawaveErrorCode.SECURITY_MARKING_CHECK_ERROR, e);
        }
    }
    
    protected void setInternalAuditParameters(String queryLogicName, String userDn, MultiValueMap<String,String> parameters) {
        // Set private audit-related parameters, stripping off any that the user might have passed in first.
        // These are parameters that aren't passed in by the user, but rather are computed from other sources.
        PrivateAuditConstants.stripPrivateParameters(parameters);
        parameters.add(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        parameters.set(PrivateAuditConstants.COLUMN_VISIBILITY, securityMarking.toColumnVisibilityString());
        parameters.add(PrivateAuditConstants.USER_DN, userDn);
    }
    
    private String writeValueAsString(Object object) {
        String stringValue;
        try {
            stringValue = mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            stringValue = String.valueOf(object);
        }
        return stringValue;
    }
}
