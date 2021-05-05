package datawave.microservice.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.marking.SecurityMarking;
import datawave.microservice.audit.AuditClient;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.common.audit.PrivateAuditConstants;
import datawave.microservice.common.storage.QueryPool;
import datawave.microservice.common.storage.QueryStorageCache;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.lock.LockManager;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.microservice.query.util.QueryUtil;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.text.MessageFormat;
import java.util.List;

@Service
public class QueryManagementService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private QueryProperties queryProperties;
    
    // Note: QueryParameters need to be request scoped
    private QueryParameters queryParameters;
    // Note: SecurityMarking needs to be request scoped
    private SecurityMarking securityMarking;
    private QueryLogicFactory queryLogicFactory;
    private ResponseObjectFactory responseObjectFactory;
    private QueryStorageCache queryStorageCache;
    private AuditClient auditClient;
    private LockManager lockManager;
    
    // TODO: JWO: Pull these from configuration instead
    private final int PAGE_TIMEOUT_MIN = 1;
    private final int PAGE_TIMEOUT_MAX = QueryExpirationProperties.PAGE_TIMEOUT_MIN_DEFAULT;
    
    public QueryManagementService(QueryProperties queryProperties, QueryParameters queryParameters, SecurityMarking securityMarking,
                    QueryLogicFactory queryLogicFactory, ResponseObjectFactory responseObjectFactory, QueryStorageCache queryStorageCache, AuditClient auditClient, LockManager lockManager) {
        this.queryProperties = queryProperties;
        this.queryParameters = queryParameters;
        this.securityMarking = securityMarking;
        this.queryLogicFactory = queryLogicFactory;
        this.responseObjectFactory = responseObjectFactory;
        this.queryStorageCache = queryStorageCache;
        this.auditClient = auditClient;
        this.lockManager = lockManager;
    }

    /**
     * Defines a datawave query.
     *
     * Validates the query parameters using the base validation, query logic-specific validation, and markings validation.
     * If the parameters are valid, the query will be stored in the query storage cache where it can be acted upon in a subsequent call.
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
            TaskKey taskKey = queryStorageCache.storeQuery(
                    new QueryPool(getPoolName()),
                    createQuery(queryLogicName, parameters, userDn, currentUser.getDNs()),
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
     * Validates the query parameters using the base validation, query logic-specific validation, and markings validation.
     * If the parameters are valid, the query will be stored in the query storage cache where it can be acted upon in a subsequent call.
     *
     * @param queryLogicName
     * @param parameters
     * @param currentUser
     * @return
     * @throws QueryException
     */
    public TaskKey create(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
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
            TaskKey taskKey = queryStorageCache.storeQuery(
                    new QueryPool(getPoolName()),
                    query,
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
                if (!parameters.containsKey(AuditParameters.AUDIT_ID)){
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
