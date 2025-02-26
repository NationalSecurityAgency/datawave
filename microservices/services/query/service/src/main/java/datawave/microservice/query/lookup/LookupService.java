package datawave.microservice.query.lookup;

import static datawave.core.query.logic.lookup.LookupQueryLogic.LOOKUP_KEY_VALUE_DELIMITER;
import static datawave.microservice.query.QueryParameters.QUERY_AUTHORIZATIONS;
import static datawave.microservice.query.QueryParameters.QUERY_BEGIN;
import static datawave.microservice.query.QueryParameters.QUERY_END;
import static datawave.microservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.microservice.query.QueryParameters.QUERY_NAME;
import static datawave.microservice.query.QueryParameters.QUERY_PARAMS;
import static datawave.microservice.query.QueryParameters.QUERY_STRING;
import static datawave.query.QueryParameters.QUERY_SYNTAX;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang.time.DateUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.common.collect.Iterables;

import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.core.query.logic.lookup.LookupQueryLogic;
import datawave.core.query.util.QueryUtil;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.stream.StreamingService;
import datawave.microservice.query.stream.listener.StreamingResponseListener;
import datawave.query.data.UUIDType;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.TimeoutQueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

@Service
public class LookupService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String LOOKUP_UUID_PAIRS = "uuidPairs";
    public static final String LUCENE_UUID_SYNTAX = "LUCENE-UUID";
    public static final String LOOKUP_STREAMING = "streaming";
    public static final String LOOKUP_CONTEXT = "context";
    
    public static final String PARAM_HIT_LIST = "hit.list";
    protected static final String EMPTY_STRING = "";
    private static final String SPACE = " ";
    private static final String REGEX_GROUPING_CHARS = "[()]";
    private static final String REGEX_NONWORD_CHARS = "[\\W&&[^:_\\.\\s-]]";
    private static final String REGEX_OR_OPERATOR = "[\\s][oO][rR][\\s]";
    private static final String REGEX_WHITESPACE_CHARS = "\\s";
    
    private static final String CONTENT_QUERY_TERM_DELIMITER = ":";
    private static final String CONTENT_QUERY_VALUE_DELIMITER = "/";
    private static final String CONTENT_QUERY_TERM_SEPARATOR = " ";
    private static final String DOCUMENT_FIELD_PREFIX = "DOCUMENT" + CONTENT_QUERY_TERM_DELIMITER;
    
    private final LookupProperties lookupProperties;
    
    private final QueryLogicFactory queryLogicFactory;
    private final QueryManagementService queryManagementService;
    private final StreamingService streamingService;
    
    public LookupService(LookupProperties lookupProperties, QueryLogicFactory queryLogicFactory, QueryManagementService queryManagementService,
                    StreamingService streamingService) {
        this.lookupProperties = lookupProperties;
        this.queryLogicFactory = queryLogicFactory;
        this.queryManagementService = queryManagementService;
        this.streamingService = streamingService;
    }
    
    /**
     * Creates a batch event lookup query using the query logic associated with the given uuid type(s) and parameters, and returns the first page of results.
     * <p>
     * Lookup queries will start running immediately. <br>
     * Auditing is performed before the query is started. <br>
     * Each of the uuid pairs must map to the same query logic. <br>
     * After the first page is returned, the query will be closed.
     *
     * @param parameters
     *            the query parameters, not null
     * @param pool
     *            the pool to target, may be null
     * @param currentUser
     *            the user who called this method, not null
     * @param listener
     *            the listener which will handle the result pages, not null
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws TimeoutQueryException
     *             if the next call times out
     * @throws NoResultsQueryException
     *             if no query results are found
     * @throws QueryException
     *             if this next task is rejected by the executor
     * @throws QueryException
     *             if there is an unknown error
     */
    public void lookupUUID(MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser, StreamingResponseListener listener)
                    throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupUUID from {} with params: {}", user, parameters);
        } else {
            log.info("Request: lookupUUID from {}", user);
        }
        
        try {
            lookup(parameters, pool, currentUser, listener);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID.");
        }
    }
    
    /**
     * Creates a batch event lookup query using the query logic associated with the given uuid type(s) and parameters, and returns the first page of results.
     * <p>
     * Lookup queries will start running immediately. <br>
     * Auditing is performed before the query is started. <br>
     * Each of the uuid pairs must map to the same query logic. <br>
     * After the first page is returned, the query will be closed.
     *
     * @param parameters
     *            the query parameters, not null
     * @param pool
     *            the pool to target, may be null
     * @param currentUser
     *            the user who called this method, not null
     * @return a base query response containing the first page of results
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws TimeoutQueryException
     *             if the next call times out
     * @throws NoResultsQueryException
     *             if no query results are found
     * @throws QueryException
     *             if this next task is rejected by the executor
     * @throws QueryException
     *             if there is an unknown error
     */
    public BaseQueryResponse lookupUUID(MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupUUID from {} with params: {}", user, parameters);
        } else {
            log.info("Request: lookupUUID from {}", user);
        }
        
        try {
            return lookup(parameters, pool, currentUser, null);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID.");
        }
    }
    
    /**
     * Creates a batch content lookup query using the query logic associated with the given uuid type(s) and parameters, and returns the first page of results.
     * <p>
     * Lookup queries will start running immediately. <br>
     * Auditing is performed before the query is started. <br>
     * Each of the uuid pairs must map to the same query logic. <br>
     * After the first page is returned, the query will be closed.
     *
     * @param parameters
     *            the query parameters, not null
     * @param pool
     *            the pool to target, may be null
     * @param currentUser
     *            the user who called this method, not null
     * @param listener
     *            the listener which will handle the result pages, not null
     * @return a base query response containing the first page of results
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws TimeoutQueryException
     *             if the next call times out
     * @throws NoResultsQueryException
     *             if no query results are found
     * @throws QueryException
     *             if this next task is rejected by the executor
     * @throws QueryException
     *             if there is an unknown error
     */
    public <T> T lookupContentUUID(MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser, StreamingResponseListener listener)
                    throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupContentUUID from {} with params: {}", user, parameters);
        } else {
            log.info("Request: lookupContentUUID from {}", user);
        }
        
        try {
            // first lookup the UUIDs, then get the content for each UUID
            return lookupContent(parameters, pool, currentUser, listener);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID content", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID content.");
        }
    }
    
    /**
     * Creates a batch content lookup query using the query logic associated with the given uuid type(s) and parameters, and returns the first page of results.
     * <p>
     * Lookup queries will start running immediately. <br>
     * Auditing is performed before the query is started. <br>
     * Each of the uuid pairs must map to the same query logic. <br>
     * After the first page is returned, the query will be closed.
     *
     * @param parameters
     *            the query parameters, not null
     * @param pool
     *            the pool to target, may be null
     * @param currentUser
     *            the user who called this method, not null
     * @return a base query response containing the first page of results
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws TimeoutQueryException
     *             if the next call times out
     * @throws NoResultsQueryException
     *             if no query results are found
     * @throws QueryException
     *             if this next task is rejected by the executor
     * @throws QueryException
     *             if there is an unknown error
     */
    public BaseQueryResponse lookupContentUUID(MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupContentUUID from {} with params: {}", user, parameters);
        } else {
            log.info("Request: lookupContentUUID from {}", user);
        }
        
        try {
            // first lookup the UUIDs, then get the content for each UUID
            return lookupContent(parameters, pool, currentUser, null);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID content", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID content.");
        }
    }
    
    private <T> T lookup(MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser, StreamingResponseListener listener)
                    throws QueryException, AuthorizationException {
        List<String> lookupTerms = parameters.get(LOOKUP_UUID_PAIRS);
        if (lookupTerms == null || lookupTerms.isEmpty()) {
            log.error("Unable to validate lookupUUID parameters: No UUID Pairs");
            throw new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER);
        }
        
        String uuidTypeContext = parameters.getFirst(LOOKUP_CONTEXT);
        
        // flatten out the terms
        lookupTerms = lookupTerms.stream().flatMap(x -> Arrays.stream(reformatQuery(x).split(REGEX_WHITESPACE_CHARS))).collect(Collectors.toList());
        
        // validate the lookup terms
        LookupQueryLogic<?> lookupQueryLogic = validateLookupTerms(uuidTypeContext, lookupTerms);
        
        // perform the event lookup
        return lookupEvents(lookupQueryLogic, new LinkedMultiValueMap<>(parameters), pool, currentUser, listener);
    }
    
    private String reformatQuery(String query) {
        String reformattedQuery = EMPTY_STRING;
        if (query != null) {
            reformattedQuery = query;
            reformattedQuery = reformattedQuery.replaceAll(REGEX_GROUPING_CHARS, SPACE); // Replace grouping characters with whitespace
            reformattedQuery = reformattedQuery.replaceAll(REGEX_NONWORD_CHARS, EMPTY_STRING); // Remove most, but not all, non-word characters
            reformattedQuery = reformattedQuery.replaceAll(REGEX_OR_OPERATOR, SPACE); // Remove OR operators
        }
        return reformattedQuery;
    }
    
    private BaseQueryResponse lookupEvents(LookupQueryLogic<?> lookupQueryLogic, MultiValueMap<String,String> parameters, String pool,
                    DatawaveUserDetails currentUser) throws QueryException, AuthorizationException {
        return lookupEvents(lookupQueryLogic, parameters, pool, currentUser, null);
    }
    
    private <T> T lookupEvents(LookupQueryLogic<?> lookupQueryLogic, MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser,
                    StreamingResponseListener listener) throws QueryException, AuthorizationException {
        String queryId = null;
        try {
            // add the query logic name and query string to our parameters
            parameters.put(QUERY_LOGIC_NAME, Collections.singletonList(lookupQueryLogic.getLogicName()));
            parameters.put(QUERY_STRING, Collections.singletonList(parameters.getFirst(LOOKUP_UUID_PAIRS)));
            
            // update the parameters for query
            setupEventQueryParameters(parameters, lookupQueryLogic, currentUser);
            
            // create the query
            queryId = queryManagementService.create(parameters.getFirst(QUERY_LOGIC_NAME), parameters, pool, currentUser).getResult();
            
            if (listener != null) {
                // stream results to the listener
                streamingService.execute(queryId, currentUser, (DatawaveUserDetails) lookupQueryLogic.getServerUser(), listener);
                return null;
            } else {
                // get the first page of results
                // noinspection unchecked
                return (T) queryManagementService.next(queryId, currentUser);
            }
        } finally {
            // close the query if applicable
            if (listener == null && queryId != null) {
                queryManagementService.close(queryId, currentUser);
            }
        }
    }
    
    protected LookupQueryLogic<?> validateLookupTerms(String uuidTypeContext, List<String> lookupUUIDPairs) throws QueryException {
        return validateLookupTerms(uuidTypeContext, lookupUUIDPairs, null);
    }
    
    protected LookupQueryLogic<?> validateLookupTerms(String uuidTypeContext, List<String> lookupUUIDPairs, MultiValueMap<String,String> lookupUUIDMap)
                    throws QueryException {
        String queryLogicName = null;
        
        // make sure there aren't too many terms to lookup
        if (lookupProperties.getBatchLookupLimit() > 0 && lookupUUIDPairs.size() <= lookupProperties.getBatchLookupLimit()) {
            
            // validate each of the uuid pairs
            for (String uuidPair : lookupUUIDPairs) {
                String[] fieldValue = uuidPair.split(LOOKUP_KEY_VALUE_DELIMITER);
                
                // there should be a field and value present - no more, no less
                if (fieldValue.length == 2) {
                    String field = fieldValue[0];
                    String value = fieldValue[1];
                    
                    // neither the field or value should be empty
                    if (!field.isEmpty() && !value.isEmpty()) {
                        
                        // is this a supported uuid type/field?
                        UUIDType uuidType = lookupProperties.getTypes().get(field.toUpperCase());
                        if (uuidType != null) {
                            if (queryLogicName == null) {
                                queryLogicName = uuidType.getQueryLogic(uuidTypeContext);
                            }
                            // if we are mixing and matching query logics
                            else if (!queryLogicName.equals(uuidType.getQueryLogic(uuidTypeContext))) {
                                String message = "Multiple UUID types '" + queryLogicName + "' and '" + uuidType.getQueryLogic(uuidTypeContext)
                                                + "' not supported within the same lookup request";
                                log.error(message);
                                throw new BadRequestQueryException(new IllegalArgumentException(message), HttpStatus.SC_BAD_REQUEST + "-1");
                            }
                        }
                        // if uuid type is null
                        else {
                            String message = "Invalid type '" + field.toUpperCase() + "' for UUID " + value
                                            + " not supported with the LuceneToJexlUUIDQueryParser";
                            log.error(message);
                            throw new BadRequestQueryException(new IllegalArgumentException(message), HttpStatus.SC_BAD_REQUEST + "-1");
                        }
                        
                        if (lookupUUIDMap != null) {
                            lookupUUIDMap.add(field, value);
                        }
                    }
                    // if the field or value is empty
                    else {
                        String message = "Empty UUID type or value extracted from uuidPair " + uuidPair;
                        log.error(message);
                        throw new BadRequestQueryException(new IllegalArgumentException(message), HttpStatus.SC_BAD_REQUEST + "-1");
                    }
                }
                // if there isn't a field AND a value
                else {
                    String message = "Unable to determine UUID type and value from uuidPair " + uuidPair;
                    log.error(message);
                    throw new BadRequestQueryException(new IllegalArgumentException(message), HttpStatus.SC_BAD_REQUEST + "-1");
                }
            }
        }
        // too many terms to lookup
        else {
            String message = "The " + lookupUUIDPairs.size() + " specified UUIDs exceed the maximum number of " + lookupProperties.getBatchLookupLimit()
                            + " allowed for a given lookup request";
            log.error(message);
            throw new BadRequestQueryException(new IllegalArgumentException(message), HttpStatus.SC_BAD_REQUEST + "-1");
        }
        
        try {
            QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic(queryLogicName);
            
            if (queryLogic instanceof LookupQueryLogic) {
                return (LookupQueryLogic<?>) queryLogic;
            } else {
                log.error("Lookup UUID can only be run with a LookupQueryLogic");
                throw new BadRequestQueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, "Lookup UUID can only be run with a LookupQueryLogic");
            }
        } catch (CloneNotSupportedException e) {
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unable to create instance of the requested query logic " + queryLogicName);
        }
    }
    
    @SuppressWarnings("ConstantConditions")
    public Query createSettings(Map<String,List<String>> queryParameters) {
        log.debug("Initial query parameters: " + queryParameters);
        Query query = new QueryImpl();
        if (queryParameters != null) {
            MultiValueMap<String,String> expandedQueryParameters = new LinkedMultiValueMap<>();
            List<String> params = queryParameters.get(QueryParameters.QUERY_PARAMS);
            String delimitedParams = null;
            if (params != null && !params.isEmpty()) {
                delimitedParams = params.get(0);
            }
            if (delimitedParams != null) {
                for (QueryImpl.Parameter pm : QueryUtil.parseParameters(delimitedParams)) {
                    expandedQueryParameters.add(pm.getParameterName(), pm.getParameterValue());
                }
            }
            expandedQueryParameters.putAll(queryParameters);
            log.debug("Final query parameters: " + expandedQueryParameters);
            query.setOptionalQueryParameters(expandedQueryParameters);
            for (String key : expandedQueryParameters.keySet()) {
                if (expandedQueryParameters.get(key).size() == 1) {
                    query.addParameter(key, expandedQueryParameters.getFirst(key));
                }
            }
        }
        return query;
    }
    
    public String getAuths(MultiValueMap<String,String> queryParameters, QueryLogic<?> queryLogic, DatawaveUserDetails currentUser)
                    throws AuthorizationException {
        Query query = createSettings(queryParameters);
        
        String userAuths;
        try {
            String queryAuths = null;
            if (queryParameters.containsKey(QUERY_AUTHORIZATIONS)) {
                queryAuths = queryParameters.getFirst(QUERY_AUTHORIZATIONS);
                queryLogic.preInitialize(query, AuthorizationsUtil.buildAuthorizations(Collections.singleton(AuthorizationsUtil.splitAuths(queryAuths))));
            } else {
                // if no requested auths, then use the overall auths for any filtering of the query operations
                queryLogic.preInitialize(query, AuthorizationsUtil.buildAuthorizations(currentUser.getAuthorizations()));
            }
            
            // the query principal is our local principal unless the query logic has a different user operations
            ProxiedUserDetails queryPrincipal = ((queryLogic.getUserOperations() == null) ? currentUser
                            : queryLogic.getUserOperations().getRemoteUser(currentUser));
            
            if (queryAuths != null) {
                userAuths = AuthorizationsUtil.downgradeUserAuths(queryAuths, currentUser, queryPrincipal);
            } else {
                userAuths = AuthorizationsUtil.buildUserAuthorizationString(queryPrincipal);
            }
        } catch (Exception e) {
            log.error("Failed to get user query authorizations", e);
            throw new AuthorizationException("Failed to get user query authorizations", e);
        }
        
        return userAuths;
    }
    
    protected void setupEventQueryParameters(MultiValueMap<String,String> parameters, LookupQueryLogic<?> queryLogic, DatawaveUserDetails currentUser)
                    throws AuthorizationException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        final String queryName = user + "-" + UUID.randomUUID().toString();
        
        final String endDate;
        try {
            endDate = DefaultQueryParameters.formatDate(DateUtils.addDays(new Date(), 2));
        } catch (ParseException e) {
            throw new RuntimeException("Unable to format new query end date");
        }
        
        setOptionalQueryParameters(parameters);
        
        // Override the extraneous query details
        parameters.set(QUERY_SYNTAX, LUCENE_UUID_SYNTAX);
        parameters.set(QUERY_NAME, queryName);
        parameters.set(QUERY_BEGIN, lookupProperties.getBeginDate());
        parameters.set(QUERY_END, endDate);
        
        parameters.set(QUERY_AUTHORIZATIONS, getAuths(parameters, queryLogic, currentUser));
    }
    
    protected void setOptionalQueryParameters(MultiValueMap<String,String> parameters) {
        if (lookupProperties.getColumnVisibility() != null) {
            parameters.set(QueryParameters.QUERY_VISIBILITY, lookupProperties.getColumnVisibility());
        }
    }
    
    private <T> T lookupContent(MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser, StreamingResponseListener listener)
                    throws QueryException, AuthorizationException {
        List<String> lookupTerms = parameters.get(LOOKUP_UUID_PAIRS);
        if (lookupTerms == null || lookupTerms.isEmpty()) {
            log.error("Unable to validate lookupContentUUID parameters: No UUID Pairs");
            throw new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER);
        }
        
        String uuidTypeContext = parameters.getFirst(LOOKUP_CONTEXT);
        
        // flatten out the terms
        lookupTerms = lookupTerms.stream().flatMap(x -> Arrays.stream(reformatQuery(x).split(REGEX_WHITESPACE_CHARS))).collect(Collectors.toList());
        
        MultiValueMap<String,String> lookupTermMap = new LinkedMultiValueMap<>();
        
        // validate the lookup terms
        LookupQueryLogic<?> lookupQueryLogic = validateLookupTerms(uuidTypeContext, lookupTerms, lookupTermMap);
        
        BaseQueryResponse response = null;
        
        boolean isEventLookupRequired = lookupQueryLogic.isEventLookupRequired(lookupTermMap);
        
        // do the event lookup if necessary
        if (isEventLookupRequired) {
            response = lookupEvents(lookupQueryLogic, new LinkedMultiValueMap<>(parameters), pool, currentUser);
        }
        
        // perform the content lookup
        Set<String> contentLookupTerms;
        if (!isEventLookupRequired) {
            contentLookupTerms = lookupQueryLogic.getContentLookupTerms(lookupTermMap);
        } else {
            contentLookupTerms = getContentLookupTerms(response);
        }
        
        return lookupContent(contentLookupTerms, parameters, pool, currentUser, (DatawaveUserDetails) lookupQueryLogic.getServerUser(), listener);
    }
    
    private Set<String> getContentLookupTerms(BaseQueryResponse response) {
        Set<String> contentQueries = new HashSet<>();
        
        if (response instanceof EventQueryResponseBase) {
            ((EventQueryResponseBase) response).getEvents().forEach(e -> contentQueries.add(createContentLookupTerm(e.getMetadata())));
        }
        
        return contentQueries;
    }
    
    private String createContentLookupTerm(Metadata eventMetadata) {
        return DOCUMENT_FIELD_PREFIX
                        + String.join(CONTENT_QUERY_VALUE_DELIMITER, eventMetadata.getRow(), eventMetadata.getDataType(), eventMetadata.getInternalId());
    }
    
    private <T> T lookupContent(Set<String> contentLookupTerms, MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser,
                    DatawaveUserDetails serverUser, StreamingResponseListener listener) throws QueryException {
        // create queries from the content lookup terms
        List<String> contentQueries = createContentQueries(contentLookupTerms);
        
        // Required so that we can return identifiers alongside the content returned in the content lookup.
        String params = parameters.getFirst(QUERY_PARAMS) != null ? parameters.getFirst(QUERY_PARAMS) : "";
        params += ";" + PARAM_HIT_LIST + ":true";
        
        EventQueryResponseBase mergedResponse = null;
        for (String contentQuery : contentQueries) {
            MultiValueMap<String,String> queryParameters = new LinkedMultiValueMap<>(parameters);
            
            // set the content query string
            queryParameters.put(QUERY_STRING, Collections.singletonList(contentQuery));
            queryParameters.put(QUERY_PARAMS, Collections.singletonList(params));
            
            // update parameters for the query
            setContentQueryParameters(queryParameters, currentUser);
            
            if (listener != null) {
                streamingService.createAndExecute(queryParameters.getFirst(QUERY_LOGIC_NAME), queryParameters, pool, currentUser, serverUser, listener);
            } else {
                // run the query
                EventQueryResponseBase contentQueryResponse = runContentQuery(queryParameters, pool, currentUser);
                
                // merge the response
                if (contentQueryResponse != null) {
                    if (mergedResponse == null) {
                        mergedResponse = contentQueryResponse;
                    } else {
                        mergedResponse.merge(contentQueryResponse);
                    }
                }
            }
        }
        
        // noinspection unchecked
        return (T) mergedResponse;
    }
    
    private List<String> createContentQueries(Set<String> contentLookupTerms) {
        List<String> contentQueries = new ArrayList<>();
        
        Iterables.partition(contentLookupTerms, lookupProperties.getBatchLookupLimit())
                        .forEach(termBatch -> contentQueries.add(String.join(CONTENT_QUERY_TERM_SEPARATOR, termBatch)));
        
        return contentQueries;
    }
    
    protected void setContentQueryParameters(MultiValueMap<String,String> parameters, DatawaveUserDetails currentUser) {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        
        setOptionalQueryParameters(parameters);
        
        // all content queries use the same query logic
        parameters.put(QUERY_LOGIC_NAME, Collections.singletonList(lookupProperties.getContentQueryLogicName()));
        
        parameters.set(QUERY_NAME, user + '-' + UUID.randomUUID());
        
        parameters.set(QUERY_BEGIN, lookupProperties.getBeginDate());
        
        final Date endDate = new Date();
        try {
            parameters.set(QUERY_END, DefaultQueryParameters.formatDate(endDate));
        } catch (ParseException e1) {
            throw new RuntimeException("Error formatting end date: " + endDate);
        }
        
        final String userAuths = AuthorizationsUtil.buildUserAuthorizationString(currentUser);
        parameters.set(QUERY_AUTHORIZATIONS, userAuths);
    }
    
    protected EventQueryResponseBase runContentQuery(MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser) {
        EventQueryResponseBase mergedResponse = null;
        String queryId = null;
        boolean isQueryFinished = false;
        
        do {
            BaseQueryResponse nextResponse = null;
            try {
                if (queryId == null) {
                    nextResponse = queryManagementService.createAndNext(parameters.getFirst(QUERY_LOGIC_NAME), parameters, pool, currentUser);
                    queryId = nextResponse.getQueryId();
                } else {
                    nextResponse = queryManagementService.next(queryId, currentUser);
                }
            } catch (NoResultsQueryException e) {
                log.debug("No results found for content query '{}'", parameters.getFirst(QUERY_STRING));
            } catch (QueryException e) {
                log.info("Encountered error while getting results for content query '{}'", parameters.getFirst(QUERY_STRING));
            }
            
            if (nextResponse instanceof EventQueryResponseBase) {
                EventQueryResponseBase nextEventQueryResponse = (EventQueryResponseBase) nextResponse;
                
                // Prevent NPE due to attempted merge when total events is null
                if (nextEventQueryResponse.getTotalEvents() == null) {
                    final Long totalEvents = nextEventQueryResponse.getReturnedEvents();
                    nextEventQueryResponse.setTotalEvents((totalEvents != null) ? totalEvents : 0L);
                }
                
                // save or update the merged response
                if (mergedResponse == null) {
                    mergedResponse = nextEventQueryResponse;
                } else {
                    mergedResponse.merge(nextEventQueryResponse);
                }
            } else {
                isQueryFinished = true;
            }
        } while (!isQueryFinished);
        
        return mergedResponse;
    }
}
