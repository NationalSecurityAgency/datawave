package datawave.microservice.query.uuid;

import com.google.common.collect.Iterables;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.QueryParameters;
import datawave.query.data.UUIDType;
import datawave.security.util.ProxiedEntityUtils;
import datawave.services.query.logic.QueryLogic;
import datawave.services.query.logic.QueryLogicFactory;
import datawave.services.query.logic.lookup.LookupQueryLogic;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.commons.lang.time.DateUtils;
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
    
    private static final String CONTENT_QUERY_TERM_DELIMITER = ":";
    private static final String CONTENT_QUERY_VALUE_DELIMITER = "/";
    private static final String CONTENT_QUERY_TERM_SEPARATOR = " ";
    private static final String DOCUMENT_FIELD_PREFIX = "DOCUMENT" + CONTENT_QUERY_TERM_DELIMITER;
    
    private final LookupUUIDProperties uuidProperties;
    
    private final QueryLogicFactory queryLogicFactory;
    private final QueryManagementService queryManagementService;
    
    public LookupService(LookupUUIDProperties uuidProperties, QueryLogicFactory queryLogicFactory, QueryManagementService queryManagementService) {
        this.uuidProperties = uuidProperties;
        this.queryLogicFactory = queryLogicFactory;
        this.queryManagementService = queryManagementService;
    }
    
    public BaseQueryResponse lookupUUID(String uuidType, String uuid, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupUUID/{}/{} from {} with params: {}", uuidType, uuid, user, parameters);
        } else {
            log.info("Request: lookupUUID/{}/{} from {}", uuidType, uuid, user);
        }
        
        parameters.add(LOOKUP_UUID_PAIRS, String.join(LOOKUP_KEY_VALUE_DELIMITER, uuidType, uuid));
        
        try {
            return lookup(parameters, currentUser, false);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID.");
        }
    }
    
    public BaseQueryResponse lookupUUID(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupUUID (batch) from {} with params: {}", user, parameters);
        } else {
            log.info("Request: lookupUUID (batch) from {}", user);
        }
        
        try {
            return lookup(parameters, currentUser, false);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID.");
        }
    }
    
    public BaseQueryResponse lookupContentUUID(String uuidType, String uuid, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupContentUUID/{}/{} from {} with params: {}", uuidType, uuid, user, parameters);
        } else {
            log.info("Request: lookupContentUUID/{}/{} from {}", uuidType, uuid, user);
        }
        
        parameters.add(LOOKUP_UUID_PAIRS, String.join(LOOKUP_KEY_VALUE_DELIMITER, uuidType, uuid));
        
        try {
            // first lookup the UUIDs, then get the content for each UUID
            return lookup(parameters, currentUser, true);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID content", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID content.");
        }
    }
    
    public BaseQueryResponse lookupContentUUID(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: lookupContentUUID (batch) from {} with params: {}", user, parameters);
        } else {
            log.info("Request: lookupContentUUID (batch) from {}", user);
        }
        
        try {
            // first lookup the UUIDs, then get the content for each UUID
            return lookup(parameters, currentUser, true);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID content", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID content.");
        }
    }
    
    private BaseQueryResponse lookup(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser, boolean isContentLookup) throws QueryException {
        List<String> lookupTerms = parameters.get(LOOKUP_UUID_PAIRS);
        if (lookupTerms == null || lookupTerms.isEmpty()) {
            log.error("Unable to validate lookupUUID parameters: No UUID Pairs");
            throw new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER);
        }
        
        MultiValueMap<String,String> lookupTermMap = new LinkedMultiValueMap<>();
        
        // validate the lookup terms
        LookupQueryLogic<?> lookupQueryLogic = validateLookupTerms(lookupTerms, lookupTermMap);
        
        BaseQueryResponse response = null;
        
        boolean isEventLookupRequired = lookupQueryLogic.isEventLookupRequired(lookupTermMap);
        
        // do the event lookup if necessary
        if (!isContentLookup || isEventLookupRequired) {
            response = lookupEvents(lookupTermMap, lookupQueryLogic, new LinkedMultiValueMap<>(parameters), currentUser);
        }
        
        // perform the content lookup if necessary
        if (isContentLookup) {
            Set<String> contentLookupTerms;
            if (!isEventLookupRequired) {
                contentLookupTerms = lookupQueryLogic.getContentLookupTerms(lookupTermMap);
            } else {
                contentLookupTerms = getContentLookupTerms(response);
            }
            
            response = lookupContent(contentLookupTerms, new LinkedMultiValueMap<>(parameters), currentUser);
        }
        
        return response;
    }
    
    private BaseQueryResponse lookupEvents(MultiValueMap<String,String> lookupTermMap, LookupQueryLogic<?> lookupQueryLogic,
                    MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        String queryId = null;
        try {
            // add the query logic name and query string to our parameters
            parameters.put(QUERY_LOGIC_NAME, Collections.singletonList(lookupQueryLogic.getLogicName()));
            parameters.put(QUERY_STRING, Collections.singletonList(lookupQueryLogic.createQueryFromLookupTerms(lookupTermMap)));
            
            // update the parameters for query
            updateParametersForEventQuery(parameters, currentUser);
            
            // run the query
            BaseQueryResponse nextResponse = queryManagementService.createAndNext(parameters.getFirst(QUERY_LOGIC_NAME), parameters, currentUser);
            
            // save the query id
            queryId = nextResponse.getQueryId();
            
            return nextResponse;
        } finally {
            // close the query if applicable
            if (queryId != null) {
                queryManagementService.close(queryId, currentUser);
            }
        }
    }
    
    protected LookupQueryLogic<?> validateLookupTerms(List<String> lookupUUIDPairs, MultiValueMap<String,String> lookupUUIDMap) throws QueryException {
        String queryLogicName = null;
        
        // make sure there aren't too many terms to lookup
        if (uuidProperties.getBatchLookupLimit() > 0 && lookupUUIDPairs.size() <= uuidProperties.getBatchLookupLimit()) {
            
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
                        UUIDType uuidType = uuidProperties.getTypes().get(field.toUpperCase());
                        if (uuidType != null) {
                            if (queryLogicName == null) {
                                queryLogicName = uuidType.getQueryLogic();
                            }
                            // if we are mixing and matching query logics
                            else if (!queryLogicName.equals(uuidType.getQueryLogic())) {
                                // TODO: This is a deal breaker
                            }
                        }
                        // if uuid type is null
                        else {
                            // TODO: This is a deal breaker
                        }
                        
                        lookupUUIDMap.add(field, value);
                    }
                    // if the field or value is empty
                    else {
                        // TODO: This is a deal breaker
                    }
                }
                // if there isn't a field AND a value
                else {
                    // TODO: This is a deal breaker
                }
            }
        }
        // too many terms to lookup
        else {
            // TODO: This is a deal breaker
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
    
    protected void updateParametersForEventQuery(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        
        if (uuidProperties.getColumnVisibility() != null) {
            parameters.set(QueryParameters.QUERY_VISIBILITY, uuidProperties.getColumnVisibility());
        }
        
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
        
        parameters.set(QueryParameters.QUERY_BEGIN, uuidProperties.getBeginDate());
        
        final Date endDate = DateUtils.addDays(new Date(), 2);
        try {
            parameters.set(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        } catch (ParseException e) {
            throw new RuntimeException("Unable to format new query end date: " + endDate);
        }
        
        // TODO: This may not be needed?
        final Date expireDate = new Date(endDate.getTime() + 1000 * 60 * 60);
        try {
            parameters.set(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expireDate));
        } catch (ParseException e) {
            throw new RuntimeException("Unable to format new query expr date: " + expireDate);
        }
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
    
    private BaseQueryResponse lookupContent(Set<String> contentLookupTerms, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        // create queries from the content lookup terms
        List<String> contentQueries = createContentQueries(contentLookupTerms);
        
        EventQueryResponseBase mergedResponse = null;
        for (String contentQuery : contentQueries) {
            // set the content query string
            parameters.put(QUERY_STRING, Collections.singletonList(contentQuery));
            
            // update parameters for the query
            updateParametersForContentQuery(parameters, currentUser);
            
            // run the query
            EventQueryResponseBase contentQueryResponse = runContentQuery(parameters, currentUser);
            
            if (contentQueryResponse != null) {
                if (mergedResponse == null) {
                    mergedResponse = contentQueryResponse;
                } else {
                    mergedResponse.merge(contentQueryResponse);
                }
            }
            
        }
        return mergedResponse;
    }
    
    private List<String> createContentQueries(Set<String> contentLookupTerms) {
        List<String> contentQueries = new ArrayList<>();
        
        Iterables.partition(contentLookupTerms, uuidProperties.getBatchLookupLimit())
                        .forEach(termBatch -> contentQueries.add(String.join(CONTENT_QUERY_TERM_SEPARATOR, termBatch)));
        
        return contentQueries;
    }
    
    protected void updateParametersForContentQuery(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        
        // all content queries use the same query logic
        parameters.put(QUERY_LOGIC_NAME, Collections.singletonList(uuidProperties.getContentQueryLogicName()));
        
        parameters.set(QueryParameters.QUERY_NAME, user + '-' + UUID.randomUUID());
        
        parameters.set(QueryParameters.QUERY_BEGIN, uuidProperties.getBeginDate());
        
        final Date endDate = new Date();
        try {
            parameters.set(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        } catch (ParseException e1) {
            throw new RuntimeException("Error formatting end date: " + endDate);
        }
        
        final String userAuths = AuthorizationsUtil.buildUserAuthorizationString(currentUser);
        parameters.set(QueryParameters.QUERY_AUTHORIZATIONS, userAuths);
        
        final Date expireDate = new Date(endDate.getTime() + 1000 * 60 * 60);
        try {
            parameters.set(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expireDate));
        } catch (ParseException e1) {
            throw new RuntimeException("Error formatting expr date: " + expireDate);
        }
    }
    
    protected EventQueryResponseBase runContentQuery(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
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
                log.info("No results found for content query '{}'", parameters.getFirst(QUERY_STRING));
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
