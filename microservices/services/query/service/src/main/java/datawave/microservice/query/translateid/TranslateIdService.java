package datawave.microservice.query.translateid;

import static datawave.microservice.query.QueryParameters.QUERY_AUTHORIZATIONS;
import static datawave.microservice.query.QueryParameters.QUERY_BEGIN;
import static datawave.microservice.query.QueryParameters.QUERY_END;
import static datawave.microservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.microservice.query.QueryParameters.QUERY_NAME;
import static datawave.microservice.query.QueryParameters.QUERY_STRING;
import static datawave.query.QueryParameters.QUERY_SYNTAX;
import static datawave.webservice.query.exception.DatawaveErrorCode.MISSING_REQUIRED_PARAMETER;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.QueryParameters;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.TimeoutQueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.result.BaseQueryResponse;

@Service
public class TranslateIdService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String TRANSLATE_ID = "id";
    public static final String TRANSLATE_TLD_ONLY = "TLDonly";
    public static final String LUCENE_SYNTAX = "LUCENE";
    
    private final TranslateIdProperties translateIdProperties;
    
    private final QueryManagementService queryManagementService;
    
    public TranslateIdService(TranslateIdProperties translateIdProperties, QueryManagementService queryManagementService) {
        this.translateIdProperties = translateIdProperties;
        this.queryManagementService = queryManagementService;
    }
    
    /**
     * Get one or more ID(s), if any, that correspond to the given ID. This method only returns the first page, so set pagesize appropriately. Since the
     * underlying query is automatically closed, callers are NOT expected to request additional pages or close the query.
     *
     * @param id
     *            the id to translate
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
    public BaseQueryResponse translateId(String id, MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser)
                    throws QueryException {
        String queryId = null;
        try {
            parameters.set(TRANSLATE_ID, id);
            
            BaseQueryResponse response = translateIds(parameters, pool, currentUser);
            queryId = response.getQueryId();
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error with translateId", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error with translateId.");
        } finally {
            // close the query if applicable
            if (queryId != null) {
                queryManagementService.close(queryId, currentUser);
            }
        }
    }
    
    /**
     * Get the ID(s), if any, associated with the specified IDs. Because the query created by this call may return multiple pages, callers are expected to
     * request additional pages and eventually close the query.
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
    public BaseQueryResponse translateIds(MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser) throws QueryException {
        if (!parameters.containsKey(TRANSLATE_ID)) {
            throw new BadRequestQueryException(MISSING_REQUIRED_PARAMETER, "Missing required parameter: " + TRANSLATE_ID);
        }
        
        try {
            MultiValueMap<String,String> queryParams = setupQueryParameters(parameters, currentUser);
            return queryManagementService.createAndNext(parameters.getFirst(QUERY_LOGIC_NAME), queryParams, pool, currentUser);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error with translateIds", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error with translateIds.");
        }
    }
    
    protected MultiValueMap<String,String> setupQueryParameters(MultiValueMap<String,String> parameters, DatawaveUserDetails currentUser) {
        MultiValueMap<String,String> queryParams = new LinkedMultiValueMap<>();
        
        // copy over any query parameters which are explicitly allowed to be set, ignoring ones that aren't
        for (String queryParam : translateIdProperties.getAllowedQueryParameters()) {
            if (parameters.containsKey(queryParam)) {
                queryParams.put(queryParam, parameters.get(queryParam));
            }
        }
        
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        String queryName = user + "-" + UUID.randomUUID().toString();
        
        String queryLogic;
        if (Boolean.parseBoolean(parameters.getFirst(TRANSLATE_TLD_ONLY))) {
            queryLogic = translateIdProperties.getTldQueryLogicName();
        } else {
            queryLogic = translateIdProperties.getQueryLogicName();
        }
        
        String endDate;
        try {
            endDate = DefaultQueryParameters.formatDate(DateUtils.addDays(new Date(), 2));
        } catch (ParseException e) {
            throw new RuntimeException("Unable to format new query end date");
        }
        
        setOptionalQueryParameters(queryParams);
        
        queryParams.set(QUERY_SYNTAX, LUCENE_SYNTAX);
        queryParams.add(QUERY_NAME, queryName);
        queryParams.add(QUERY_LOGIC_NAME, queryLogic);
        queryParams.add(QUERY_STRING, buildQuery(parameters.get(TRANSLATE_ID)));
        queryParams.set(QUERY_AUTHORIZATIONS, AuthorizationsUtil.buildUserAuthorizationString(currentUser));
        queryParams.add(QUERY_BEGIN, translateIdProperties.getBeginDate());
        queryParams.set(QUERY_END, endDate);
        
        return queryParams;
    }
    
    protected void setOptionalQueryParameters(MultiValueMap<String,String> parameters) {
        if (translateIdProperties.getColumnVisibility() != null) {
            parameters.set(QueryParameters.QUERY_VISIBILITY, translateIdProperties.getColumnVisibility());
        }
    }
    
    private String buildQuery(List<String> ids) {
        List<String> uuidTypes = new ArrayList<>();
        translateIdProperties.getTypes().keySet().forEach(uuidType -> uuidTypes.add(uuidType.toUpperCase()));
        
        // @formatter:off
        return ids.stream()
                .map(id -> "\"" + id + "\"")
                .flatMap(id -> uuidTypes.stream().map(uuidType -> uuidType + ":" + id))
                .collect(Collectors.joining(" OR "));
        // @formatter:on
    }
}
