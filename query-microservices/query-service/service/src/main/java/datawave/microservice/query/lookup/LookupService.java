package datawave.microservice.query.lookup;

import com.google.common.collect.Iterables;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.stream.StreamingService;
import datawave.microservice.query.stream.listener.StreamingResponseListener;
import datawave.query.data.UUIDType;
import datawave.security.util.ProxiedEntityUtils;
import datawave.services.query.logic.QueryLogic;
import datawave.services.query.logic.QueryLogicFactory;
import datawave.services.query.logic.lookup.LookupQueryLogic;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.TimeoutQueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.commons.lang.time.DateUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static datawave.microservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.microservice.query.QueryParameters.QUERY_STRING;
import static datawave.query.QueryParameters.QUERY_SYNTAX;
import static datawave.services.query.logic.lookup.LookupQueryLogic.LOOKUP_KEY_VALUE_DELIMITER;

@Service
public class LookupService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String LOOKUP_UUID_PAIRS = "uuidPairs";
    public static final String LUCENE_UUID_SYNTAX = "LUCENE-UUID";
    public static final String LOOKUP_STREAMING = "streaming";
    
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
    public void lookupUUID(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser, StreamingResponseListener listener) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupUUID from {} with params: {}", user, parameters);
        } else {
            log.info("Request: lookupUUID from {}", user);
        }
        
        try {
            lookup(parameters, currentUser, listener);
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
    public BaseQueryResponse lookupUUID(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupUUID from {} with params: {}", user, parameters);
        } else {
            log.info("Request: lookupUUID from {}", user);
        }
        
        try {
            return lookup(parameters, currentUser, null);
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
    public <T> T lookupContentUUID(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser, StreamingResponseListener listener)
                    throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupContentUUID from {} with params: {}", user, parameters);
        } else {
            log.info("Request: lookupContentUUID from {}", user);
        }
        
        try {
            // first lookup the UUIDs, then get the content for each UUID
            return lookupContent(parameters, currentUser, listener);
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
    public BaseQueryResponse lookupContentUUID(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupContentUUID from {} with params: {}", user, parameters);
        } else {
            log.info("Request: lookupContentUUID from {}", user);
        }
        
        try {
            // first lookup the UUIDs, then get the content for each UUID
            return lookupContent(parameters, currentUser, null);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID content", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID content.");
        }
    }
    
    private <T> T lookup(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser, StreamingResponseListener listener) throws QueryException {
        List<String> lookupTerms = parameters.get(LOOKUP_UUID_PAIRS);
        if (lookupTerms == null || lookupTerms.isEmpty()) {
            log.error("Unable to validate lookupUUID parameters: No UUID Pairs");
            throw new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER);
        }
        
        MultiValueMap<String,String> lookupTermMap = new LinkedMultiValueMap<>();
        
        // validate the lookup terms
        LookupQueryLogic<?> lookupQueryLogic = validateLookupTerms(lookupTerms, lookupTermMap);
        
        // perform the event lookup
        return lookupEvents(lookupTermMap, lookupQueryLogic, new LinkedMultiValueMap<>(parameters), currentUser, listener);
    }
    
    private BaseQueryResponse lookupEvents(MultiValueMap<String,String> lookupTermMap, LookupQueryLogic<?> lookupQueryLogic,
                    MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        return lookupEvents(lookupTermMap, lookupQueryLogic, parameters, currentUser, null);
    }
    
    private <T> T lookupEvents(MultiValueMap<String,String> lookupTermMap, LookupQueryLogic<?> lookupQueryLogic, MultiValueMap<String,String> parameters,
                    ProxiedUserDetails currentUser, StreamingResponseListener listener) throws QueryException {
        String queryId = null;
        try {
            // add the query logic name and query string to our parameters
            parameters.put(QUERY_LOGIC_NAME, Collections.singletonList(lookupQueryLogic.getLogicName()));
            parameters.put(QUERY_STRING, Collections.singletonList(lookupQueryLogic.createQueryFromLookupTerms(lookupTermMap)));
            
            // update the parameters for query
            setEventQueryParameters(parameters, currentUser);
            
            // create the query
            queryId = queryManagementService.create(parameters.getFirst(QUERY_LOGIC_NAME), parameters, currentUser).getResult();
            
            if (listener != null) {
                // stream results to the listener
                streamingService.execute(queryId, currentUser, listener);
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
    
    protected LookupQueryLogic<?> validateLookupTerms(List<String> lookupUUIDPairs, MultiValueMap<String,String> lookupUUIDMap) throws QueryException {
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
                                queryLogicName = uuidType.getQueryLogic();
                            }
                            // if we are mixing and matching query logics
                            else if (!queryLogicName.equals(uuidType.getQueryLogic())) {
                                String message = "Multiple UUID types '" + queryLogicName + "' and '" + uuidType.getQueryLogic()
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
                        
                        lookupUUIDMap.add(field, value);
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
    protected void setEventQueryParameters(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        
        setOptionalQueryParameters(parameters);
        
        parameters.set(QUERY_SYNTAX, LUCENE_UUID_SYNTAX);
        
        // Override the extraneous query details
        String userAuths;
        if (parameters.containsKey(QueryParameters.QUERY_AUTHORIZATIONS)) {
            userAuths = AuthorizationsUtil.downgradeUserAuths(currentUser, parameters.getFirst(QueryParameters.QUERY_AUTHORIZATIONS));
        } else {
            userAuths = AuthorizationsUtil.buildUserAuthorizationString(currentUser);
        }
        parameters.set(QueryParameters.QUERY_AUTHORIZATIONS, userAuths);
        
        final String queryName = user + "-" + UUID.randomUUID();
        parameters.set(QueryParameters.QUERY_NAME, queryName);
        
        parameters.set(QueryParameters.QUERY_BEGIN, lookupProperties.getBeginDate());
        
        final Date endDate = DateUtils.addDays(new Date(), 2);
        try {
            parameters.set(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        } catch (ParseException e) {
            throw new RuntimeException("Unable to format new query end date: " + endDate);
        }
    }
    
    protected void setOptionalQueryParameters(MultiValueMap<String,String> parameters) {
        if (lookupProperties.getColumnVisibility() != null) {
            parameters.set(QueryParameters.QUERY_VISIBILITY, lookupProperties.getColumnVisibility());
        }
    }
    
    private <T> T lookupContent(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser, StreamingResponseListener listener)
                    throws QueryException {
        List<String> lookupTerms = parameters.get(LOOKUP_UUID_PAIRS);
        if (lookupTerms == null || lookupTerms.isEmpty()) {
            log.error("Unable to validate lookupContentUUID parameters: No UUID Pairs");
            throw new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER);
        }
        
        MultiValueMap<String,String> lookupTermMap = new LinkedMultiValueMap<>();
        
        // validate the lookup terms
        LookupQueryLogic<?> lookupQueryLogic = validateLookupTerms(lookupTerms, lookupTermMap);
        
        BaseQueryResponse response = null;
        
        boolean isEventLookupRequired = lookupQueryLogic.isEventLookupRequired(lookupTermMap);
        
        // do the event lookup if necessary
        if (isEventLookupRequired) {
            response = lookupEvents(lookupTermMap, lookupQueryLogic, new LinkedMultiValueMap<>(parameters), currentUser);
        }
        
        // perform the content lookup
        Set<String> contentLookupTerms;
        if (!isEventLookupRequired) {
            contentLookupTerms = lookupQueryLogic.getContentLookupTerms(lookupTermMap);
        } else {
            contentLookupTerms = getContentLookupTerms(response);
        }
        
        return lookupContent(contentLookupTerms, parameters, currentUser, listener);
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
    
    private <T> T lookupContent(Set<String> contentLookupTerms, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser,
                    StreamingResponseListener listener) throws QueryException {
        // create queries from the content lookup terms
        List<String> contentQueries = createContentQueries(contentLookupTerms);
        
        EventQueryResponseBase mergedResponse = null;
        for (String contentQuery : contentQueries) {
            MultiValueMap<String,String> queryParameters = new LinkedMultiValueMap<>(parameters);
            
            // set the content query string
            queryParameters.put(QUERY_STRING, Collections.singletonList(contentQuery));
            
            // update parameters for the query
            setContentQueryParameters(queryParameters, currentUser);
            
            if (listener != null) {
                streamingService.createAndExecute(queryParameters.getFirst(QUERY_LOGIC_NAME), queryParameters, currentUser, listener);
            } else {
                // run the query
                EventQueryResponseBase contentQueryResponse = runContentQuery(queryParameters, currentUser);
                
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
    
    protected void setContentQueryParameters(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        
        setOptionalQueryParameters(parameters);
        
        // all content queries use the same query logic
        parameters.put(QUERY_LOGIC_NAME, Collections.singletonList(lookupProperties.getContentQueryLogicName()));
        
        parameters.set(QueryParameters.QUERY_NAME, user + '-' + UUID.randomUUID());
        
        parameters.set(QueryParameters.QUERY_BEGIN, lookupProperties.getBeginDate());
        
        final Date endDate = new Date();
        try {
            parameters.set(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        } catch (ParseException e1) {
            throw new RuntimeException("Error formatting end date: " + endDate);
        }
        
        final String userAuths = AuthorizationsUtil.buildUserAuthorizationString(currentUser);
        parameters.set(QueryParameters.QUERY_AUTHORIZATIONS, userAuths);
    }
    
    protected EventQueryResponseBase runContentQuery(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) {
        EventQueryResponseBase mergedResponse = null;
        String queryId = null;
        boolean isQueryFinished = false;
        
        do {
            BaseQueryResponse nextResponse = null;
            try {
                if (queryId == null) {
                    nextResponse = queryManagementService.createAndNext(parameters.getFirst(QUERY_LOGIC_NAME), parameters, currentUser);
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
