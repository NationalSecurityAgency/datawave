package datawave.microservice.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.marking.SecurityMarking;
import datawave.microservice.audit.AuditClient;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.common.audit.PrivateAuditConstants;
import datawave.microservice.common.storage.QueryPool;
import datawave.microservice.common.storage.QueryQueueListener;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryStatus;
import datawave.microservice.common.storage.QueryStorageCache;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.remote.QueryRequestHandler;
import datawave.microservice.query.util.QueryUtil;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

import static datawave.microservice.common.storage.QueryStatus.QUERY_STATE.CANCELED;
import static datawave.microservice.common.storage.QueryStatus.QUERY_STATE.CLOSED;

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
    private final ResponseObjectFactory responseObjectFactory;
    private final QueryStorageCache queryStorageCache;
    private final QueryQueueManager queryQueueManager;
    private final AuditClient auditClient;
    private final ThreadPoolTaskExecutor nextExecutor;
    
    private final String identifier;
    
    private MultiValueMap<String,Future<List<Object>>> nextTaskMap = new LinkedMultiValueMap<>();
    
    // TODO: JWO: Pull these from configuration instead
    private final int PAGE_TIMEOUT_MIN = 1;
    private final int PAGE_TIMEOUT_MAX = QueryExpirationProperties.PAGE_TIMEOUT_MIN_DEFAULT;
    
    public QueryManagementService(QueryProperties queryProperties, ApplicationContext appCtx, BusProperties busProperties, QueryParameters queryParameters,
                    SecurityMarking securityMarking, QueryLogicFactory queryLogicFactory, ResponseObjectFactory responseObjectFactory,
                    QueryStorageCache queryStorageCache, QueryQueueManager queryQueueManager, AuditClient auditClient, ThreadPoolTaskExecutor nextExecutor) {
        this.queryProperties = queryProperties;
        this.appCtx = appCtx;
        this.busProperties = busProperties;
        this.queryParameters = queryParameters;
        this.securityMarking = securityMarking;
        this.queryLogicFactory = queryLogicFactory;
        this.responseObjectFactory = responseObjectFactory;
        this.queryStorageCache = queryStorageCache;
        this.queryQueueManager = queryQueueManager;
        this.auditClient = auditClient;
        this.nextExecutor = nextExecutor;
        
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "UNKNOWN";
        }
        this.identifier = hostname;
    }
    
    /**
     * Defines a datawave query.
     *
     * Validates the query parameters using the base validation, query logic-specific validation, and markings validation. If the parameters are valid, the
     * query will be stored in the query storage cache where it can be acted upon in a subsequent call.
     *
     * @param queryLogicName
     * @param parameters
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public TaskKey define(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        // validate query and get a query logic
        QueryLogic<?> queryLogic = validateQuery(queryLogicName, parameters, currentUser);
        
        String userId = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        log.trace(userId + " has authorizations " + currentUser.getPrimaryUser().getAuths());
        
        // set some audit parameters which are used internally
        String userDn = currentUser.getPrimaryUser().getDn().subjectDN();
        setInternalAuditParameters(queryLogicName, userDn, parameters);
        
        try {
            // persist the query w/ query id in the query storage cache
            // TODO: JWO: storeQuery assumes that this is a 'create' call, but this is a 'define' call.
            // @formatter:off
            TaskKey taskKey = queryStorageCache.defineQuery(
                    new QueryPool(getPoolName()),
                    createQuery(queryLogicName, parameters, userDn, currentUser.getDNs()),
                    AuthorizationsUtil.getDowngradedAuthorizations(queryParameters.getAuths(), currentUser),
                    getMaxConcurrentTasks(queryLogic));
            // @formatter:on
            
            // TODO: JWO: Figure out how to make query tracing work with our new architecture. Datawave issue #1155
            
            // TODO: JWO: Figure out how to make query metrics work with our new architecture. Datawave issue #1156
            
            return taskKey;
        } catch (Exception e) {
            log.error("Unknown error storing query", e);
            throw new BadRequestQueryException(DatawaveErrorCode.RUNNING_QUERY_CACHE_ERROR, e);
        }
    }
    
    /**
     * Creates a datawave query.
     *
     * Validates the query parameters using the base validation, query logic-specific validation, and markings validation. If the parameters are valid, the
     * query will be stored in the query storage cache where it can be acted upon in a subsequent call.
     *
     * @param queryLogicName
     * @param parameters
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public TaskKey create(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        try {
            // validate query and get a query logic
            QueryLogic<?> queryLogic = validateQuery(queryLogicName, parameters, currentUser);
            
            String userId = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
            log.trace(userId + " has authorizations " + currentUser.getPrimaryUser().getAuths());
            
            // set some audit parameters which are used internally
            String userDn = currentUser.getPrimaryUser().getDn().subjectDN();
            setInternalAuditParameters(queryLogicName, userDn, parameters);
            
            // send an audit record to the auditor
            Query query = createQuery(queryLogicName, parameters, userDn, currentUser.getDNs());
            audit(query, queryLogic, parameters, currentUser);
            
            try {
                // persist the query w/ query id in the query storage cache
                // @formatter:off
                TaskKey taskKey = queryStorageCache.createQuery(
                        new QueryPool(getPoolName()),
                        query,
                        AuthorizationsUtil.getDowngradedAuthorizations(queryParameters.getAuths(), currentUser),
                        getMaxConcurrentTasks(queryLogic));
                // @formatter:on
                
                // TODO: JWO: Figure out how to make query tracing work with our new architecture. Datawave issue #1155
                
                // TODO: JWO: Figure out how to make query metrics work with our new architecture. Datawave issue #1156
                
                return taskKey;
            } catch (Exception e) {
                log.error("Unknown error storing query", e);
                throw new BadRequestQueryException(DatawaveErrorCode.RUNNING_QUERY_CACHE_ERROR, e);
            }
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error creating query", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error creating query.");
        }
    }
    
    public List<Object> next(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        UUID queryUUID = UUID.fromString(queryId);
        try {
            // make sure the query is valid, and the user can act on it
            validateRequest(queryId, currentUser);
            
            boolean decrementNext = false;
            try {
                // before we spin up a separate thread, make sure we are allowed to call next
                incrementConcurrentNextCount(queryUUID);
                decrementNext = true;
                
                Future<List<Object>> future;
                try {
                    future = nextExecutor.submit(() -> nextCall(queryId));
                } catch (TaskRejectedException e) {
                    throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Next task rejected by the executor for query " + queryId);
                }
                
                // add this future to the next task map
                nextTaskMap.add(queryId, future);
                
                // wait for the results to be ready
                List<Object> results = future.get();
                
                // remote this future from the next task map
                nextTaskMap.remove(queryId, future);
                
                return results;
            } finally {
                if (decrementNext) {
                    decrementConcurrentNextCount(queryUUID);
                }
            }
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error getting next page for query " + queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Unknown error getting next page for query " + queryId);
        }
    }
    
    private void incrementConcurrentNextCount(UUID queryUUID) throws InterruptedException, QueryException {
        if (queryStorageCache.getQueryStatusLock(queryUUID).tryLock(queryProperties.getLockWaitTimeMillis(), queryProperties.getLockLeaseTimeMillis())) {
            try {
                // increment the concurrent next
                QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryUUID);
                if (queryStatus != null && queryStatus.getConcurrentNextCount() < queryProperties.getConcurrentNextLimit()) {
                    queryStatus.setConcurrentNextCount(queryStatus.getConcurrentNextCount() + 1);
                    queryStorageCache.updateQueryStatus(queryStatus);
                } else {
                    throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR,
                                    "Concurrent next call limit reached: " + queryProperties.getConcurrentNextLimit());
                }
            } finally {
                queryStorageCache.getQueryStatusLock(queryUUID).unlock();
            }
        } else {
            throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR, "Unable to acquire lock on query.");
        }
    }
    
    private void decrementConcurrentNextCount(UUID queryUUID) throws InterruptedException, QueryException {
        if (queryStorageCache.getQueryStatusLock(queryUUID).tryLock(queryProperties.getLockWaitTimeMillis(), queryProperties.getLockLeaseTimeMillis())) {
            try {
                // increment the concurrent next
                QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryUUID);
                if (queryStatus != null && queryStatus.getConcurrentNextCount() < queryProperties.getConcurrentNextLimit()) {
                    queryStatus.setConcurrentNextCount(queryStatus.getConcurrentNextCount() - 1);
                    queryStorageCache.updateQueryStatus(queryStatus);
                } else {
                    throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR,
                                    "Concurrent next call limit reached: " + queryProperties.getConcurrentNextLimit());
                }
            } finally {
                queryStorageCache.getQueryStatusLock(queryUUID).unlock();
            }
        } else {
            throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR, "Unable to acquire lock on query.");
        }
    }
    
    private List<Object> nextCall(String queryId) throws Exception {
        List<Object> resultList = new ArrayList<>();
        QueryQueueListener resultListener = queryQueueManager.createListener(identifier, queryId);
        
        // keep waiting for results until we're finished
        while (!isFinished(queryId)) {
            Object[] results = getObjectResults(resultListener.receive(queryProperties.getResultQueueIntervalMillis()).getPayload());
            if (results != null) {
                resultList.addAll(Arrays.asList(results));
            }
        }
        
        return resultList;
    }
    
    private boolean isFinished(String queryId) {
        // get the query stats from the cache
        // TODO: It may be more efficient to broadcast a canceled query to all query services vs. hitting the cache each time
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(UUID.fromString(queryId));
        
        // conditions we need to check for
        // 1) was this query canceled?
        // 2) have we hit the user's results-per-page limit?
        // 3) have we hit the query logic's results-per-page limit?
        // 4) have we hit the query logic's bytes-per-page limit?
        // 5) have we hit the max results (or the max results override)?
        // 6) have we reached the "max work" limit? (i.e. next count + seek count)
        // 7) are we going to timeout before getting a full page? if so, return partial results
        return false;
    }
    
    // TODO: This is totally bogus and should be removed once QueryQueueListener is updated to return objects
    private Object[] getObjectResults(byte[] bytes) {
        Object[] objects = null;
        if (bytes != null) {
            objects = new Object[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                objects[i] = bytes[i];
            }
        }
        return objects;
    }
    
    public void cancel(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            validateRequest(queryId, currentUser);
            
            // cancel the query
            cancel(queryId, true);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.CANCELLATION_ERROR, e, "query_id: " + queryId);
            log.error("Unable to cancel query with id " + queryId, queryException);
            throw queryException;
        }
    }
    
    private void cancel(String queryId, boolean publishEvent) throws InterruptedException, QueryException {
        UUID queryUUID = UUID.fromString(queryId);
        
        // if we have an active next call for this query locally, cancel it
        List<Future<List<Object>>> futures = nextTaskMap.get(queryId);
        if (futures != null) {
            for (Future<List<Object>> future : futures) {
                future.cancel(true);
            }
            
            // TODO: lock the cache entry and change state to canceled
            // try to be nice and acquire the lock before updating the status
            if (queryStorageCache.getQueryStatusLock(queryUUID).tryLock(queryProperties.getLockWaitTimeMillis(), queryProperties.getLockLeaseTimeMillis())) {
                try {
                    // update query state to CANCELED
                    QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryUUID);
                    queryStatus.setQueryState(CANCELED);
                    queryStorageCache.updateQueryStatus(queryStatus);
                } finally {
                    queryStorageCache.getQueryStatusLock(queryUUID).unlock();
                }
            } else {
                // TODO: Instead of throwing an exception, do we want to be mean here and update the state to canceled without acquiring a lock? Probably yes...
                throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR, "Unable to acquire lock on query");
            }
        } else {
            if (publishEvent) {
                // broadcast the cancel request to all of the query services
                appCtx.publishEvent(new RemoteQueryRequestEvent(this, busProperties.getId(), appCtx.getApplicationName(), QueryRequest.cancel(queryId)));
            }
        }
        
        if (publishEvent) {
            // broadcast the cancel request to all of the executor services
            appCtx.publishEvent(
                            new RemoteQueryRequestEvent(this, busProperties.getId(), queryProperties.getExecutorServiceName(), QueryRequest.cancel(queryId)));
        }
    }
    
    public void close(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            validateRequest(queryId, currentUser);
            
            // close the query
            close(queryId, true);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.CLOSE_ERROR, e, "query_id: " + queryId);
            log.error("Unable to close query with id " + queryId, queryException);
            throw queryException;
        }
    }
    
    private void close(String queryId, boolean publishEvent) throws InterruptedException, QueryException {
        UUID queryUUID = UUID.fromString(queryId);
        
        if (queryStorageCache.getQueryStatusLock(queryUUID).tryLock(queryProperties.getLockWaitTimeMillis(), queryProperties.getLockLeaseTimeMillis())) {
            try {
                // update query state to CLOSED
                QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryUUID);
                queryStatus.setQueryState(CLOSED);
                queryStorageCache.updateQueryStatus(queryStatus);
            } finally {
                queryStorageCache.getQueryStatusLock(queryUUID).unlock();
            }
        } else {
            // TODO: Instead of throwing an exception, do we want to be mean here and update the state to canceled without acquiring a lock? Probably yes...
            throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR, "Unable to acquire lock on query");
        }
        
        if (publishEvent) {
            // broadcast the close request to all of the executor services
            appCtx.publishEvent(
                            new RemoteQueryRequestEvent(this, busProperties.getId(), queryProperties.getExecutorServiceName(), QueryRequest.close(queryId)));
        }
    }
    
    private void validateRequest(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        // does the query exist?
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(UUID.fromString(queryId));
        if (queryStatus == null) {
            throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, MessageFormat.format("{0}", queryId));
        }
        
        // TODO: Check to see if this is an admin user
        // does the current user own this query?
        String userId = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        Query query = queryStatus.getQuery();
        if (!query.getOwner().equals(userId)) {
            throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", userId, query.getOwner()));
        }
    }
    
    @Override
    public void handleRemoteRequest(QueryRequest queryRequest) {
        try {
            switch (queryRequest.getMethod()) {
                case CANCEL:
                    log.trace("Received remote cancel request.");
                    cancel(queryRequest.getQueryId(), false);
                    break;
                case CLOSE:
                    log.trace("Received remote close request.");
                    close(queryRequest.getQueryId(), false);
                    break;
                default:
                    log.debug("Unknown remote query request method: {}", queryRequest.getMethod());
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
    
    protected Query createQuery(String queryLogicName, MultiValueMap<String,String> parameters, String userDn, List<String> dnList) {
        Query q = responseObjectFactory.getQueryImpl();
        q.initialize(userDn, dnList, queryLogicName, queryParameters, queryParameters.getUnknownParameters(parameters));
        q.setColumnVisibility(securityMarking.toColumnVisibilityString());
        q.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
        Thread.currentThread().setUncaughtExceptionHandler(q.getUncaughtExceptionHandler());
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
        parameters.add(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        
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
        
        // The pagesize and expirationDate checks will always be false when called from the RemoteQueryExecutor.
        // Leaving for now until we can test to ensure that is always the case.
        if (queryParameters.getPagesize() <= 0) {
            log.error("Invalid page size: " + queryParameters.getPagesize());
            throw new BadRequestQueryException(DatawaveErrorCode.INVALID_PAGE_SIZE);
        }
        
        if (queryParameters.getPageTimeout() != -1
                        && (queryParameters.getPageTimeout() < PAGE_TIMEOUT_MIN || queryParameters.getPageTimeout() > PAGE_TIMEOUT_MAX)) {
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
        String stringValue = "";
        try {
            stringValue = mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            stringValue = String.valueOf(object);
        }
        return stringValue;
    }
}
