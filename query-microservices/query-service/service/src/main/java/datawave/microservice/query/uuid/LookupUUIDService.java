package datawave.microservice.query.uuid;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.QueryPersistence;
import datawave.microservice.query.config.QueryProperties;
import datawave.query.data.UUIDType;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static datawave.microservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.microservice.query.QueryParameters.QUERY_STRING;
import static datawave.query.QueryParameters.QUERY_SYNTAX;

@Service
public class LookupUUIDService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String LOOKUP_UUID_PAIRS = "uuidPairs";
    public static final String LUCENE_UUID_SYNTAX = "LUCENE-UUID";
    
    protected static final String UUID_TERM_DELIMITER = ":";
    
    private final QueryProperties queryProperties;
    private final LookupUUIDProperties uuidProperties;
    
    private final ResponseObjectFactory responseObjectFactory;
    private final QueryManagementService queryManagementService;
    
    public LookupUUIDService(QueryProperties queryProperties, LookupUUIDProperties uuidProperties, ResponseObjectFactory responseObjectFactory,
                    QueryManagementService queryManagementService) {
        this.queryProperties = queryProperties;
        this.uuidProperties = uuidProperties;
        this.responseObjectFactory = responseObjectFactory;
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
        
        parameters.add(LOOKUP_UUID_PAIRS, String.join(UUID_TERM_DELIMITER, uuidType, uuid));
        
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
        
        parameters.add(LOOKUP_UUID_PAIRS, String.join(UUID_TERM_DELIMITER, uuidType, uuid));
        
        try {
            return lookup(parameters, currentUser, true);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID.");
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
            return lookup(parameters, currentUser, true);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error looking up UUID", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error looking up UUID.");
        }
    }
    
    private BaseQueryResponse lookup(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser, boolean isContentLookup) throws QueryException {
        String queryId = null;
        
        try {
            // make sure the UUID lookup is valid
            validateLookupParameters(parameters, isContentLookup);
            
            // update the parameters for query
            updateQueryParameters(parameters, currentUser);
            
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
    
    protected void validateLookupParameters(MultiValueMap<String,String> parameters, boolean isContentLookup) throws BadRequestQueryException {
        // get the requested uuids
        List<String> lookupUUIDPairs = parameters.get(LOOKUP_UUID_PAIRS);
        if (lookupUUIDPairs != null && !lookupUUIDPairs.isEmpty()) {
            
            // make sure there aren't too many terms to lookup
            if (uuidProperties.getBatchLookupLimit() > 0 && lookupUUIDPairs.size() <= uuidProperties.getBatchLookupLimit()) {
                
                // map the uuid pairs by key and value
                MultiValueMap<String,String> lookupUUIDMap = new LinkedMultiValueMap<>(lookupUUIDPairs.size());
                
                String queryLogic = null;
                
                // validate each of the uuid pairs
                for (String uuidPair : lookupUUIDPairs) {
                    String[] fieldValue = uuidPair.split(UUID_TERM_DELIMITER);
                    
                    // there should be a field and value present - no more, no less
                    if (fieldValue.length == 2) {
                        String field = fieldValue[0];
                        String value = fieldValue[1];
                        
                        // neither the field or value should be empty
                        if (!field.isEmpty() && !value.isEmpty()) {
                            
                            // is this a supported uuid type/field?
                            UUIDType uuidType = uuidProperties.getTypes().get(field.toUpperCase());
                            if (uuidType != null) {
                                if (queryLogic == null) {
                                    queryLogic = uuidType.getQueryLogic();
                                }
                                // if we are mixing and matching query logics
                                else if (!queryLogic.equals(uuidType.getQueryLogic())) {
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
                
                // let's create the query
                String query = lookupUUIDMap.entrySet().stream()
                                .flatMap(entry -> entry.getValue().stream().map(value -> String.join(UUID_TERM_DELIMITER, entry.getKey(), value)))
                                .collect(Collectors.joining(" OR "));
                
                // add the query logic name and query string to our parameters
                parameters.put(QUERY_LOGIC_NAME, Collections.singletonList(queryLogic));
                parameters.put(QUERY_STRING, Collections.singletonList(query));
            }
            // too many terms to lookup
            else {
                // TODO: This is a deal breaker
            }
        }
        // if there are no lookup uuid pairs
        else {
            // TODO: This is a deal breaker
        }
    }
    
    protected void updateQueryParameters(MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        
        if (uuidProperties.getColumnVisibility() != null) {
            parameters.put(QueryParameters.QUERY_VISIBILITY, Collections.singletonList(uuidProperties.getColumnVisibility()));
        }
        
        parameters.put(QUERY_SYNTAX, Collections.singletonList(LUCENE_UUID_SYNTAX));
        
        // Override the extraneous query details
        String userAuths;
        if (parameters.containsKey(QueryParameters.QUERY_AUTHORIZATIONS)) {
            userAuths = AuthorizationsUtil.downgradeUserAuths(currentUser, parameters.getFirst(QueryParameters.QUERY_AUTHORIZATIONS));
        } else {
            userAuths = AuthorizationsUtil.buildUserAuthorizationString(currentUser);
        }
        parameters.put(QueryParameters.QUERY_AUTHORIZATIONS, Collections.singletonList(userAuths));
        
        final String queryName = user + "-" + UUID.randomUUID();
        parameters.put(QueryParameters.QUERY_NAME, Collections.singletonList(queryName));
        
        parameters.put(QueryParameters.QUERY_BEGIN, Collections.singletonList(uuidProperties.getBeginDate()));
        
        final Date endDate = DateUtils.addDays(new Date(), 2);
        try {
            parameters.put(QueryParameters.QUERY_END, Collections.singletonList(DefaultQueryParameters.formatDate(endDate)));
        } catch (ParseException e) {
            throw new RuntimeException("Unable to format new query end date: " + endDate);
        }
        
        final Date expireDate = new Date(endDate.getTime() + 1000 * 60 * 60);
        try {
            parameters.put(QueryParameters.QUERY_EXPIRATION, Collections.singletonList(DefaultQueryParameters.formatDate(expireDate)));
        } catch (ParseException e) {
            throw new RuntimeException("Unable to format new query expr date: " + expireDate);
        }
        parameters.put(QueryParameters.QUERY_PERSISTENCE, Collections.singletonList(QueryPersistence.TRANSIENT.name()));
        parameters.put(QueryParameters.QUERY_TRACE, Collections.singletonList("false"));
    }
}
