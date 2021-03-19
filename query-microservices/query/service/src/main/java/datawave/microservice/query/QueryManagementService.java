package datawave.microservice.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.marking.SecurityMarking;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.util.QueryUtil;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import javax.ws.rs.core.HttpHeaders;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Service
public class QueryManagementService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private QueryProperties queryProperties;
    private QueryParameters queryParameters;
    private SecurityMarking securityMarking;
    
    // TODO: Pull these from configuration instead
    private final int PAGE_TIMEOUT_MIN = 1;
    private final int PAGE_TIMEOUT_MAX = QueryExpirationProperties.PAGE_TIMEOUT_MIN_DEFAULT;
    
    public QueryManagementService(QueryProperties queryProperties, QueryParameters queryParameters, SecurityMarking securityMarking) {
        this.queryProperties = queryProperties;
        this.queryParameters = queryParameters;
        this.securityMarking = securityMarking;
    }
    
    // A few items that are cached by the validateQuery call
    private static class QueryData {
        QueryLogic<?> logic = null;
        Principal p = null;
        Set<String> proxyServers = null;
        String userDn = null;
        String userid = null;
        List<String> dnList = null;
    }
    
    /**
     * This method will provide some initial query validation for the define and create query calls.
     */
    private void validateQuery(String queryLogicName, MultiValueMap<String,String> parameters, HttpHeaders httpHeaders) throws QueryException {
        
        // add query logic name to parameters
        parameters.add(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        
        log.debug(writeValueAsString(parameters));
        
        // Pull "params" values into individual query parameters for validation on the query logic.
        // This supports the deprecated "params" value (both on the old and new API). Once we remove the deprecated
        // parameter, this code block can go away.
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
        
        // // TODO: Get this working!
        // // will throw IllegalArgumentException if not defined
        // try {
        // qd.logic = queryLogicFactory.getQueryLogic(queryLogicName, ctx.getCallerPrincipal());
        // } catch (Exception e) {
        // log.error("Failed to get query logic for " + queryLogicName, e);
        // throw new BadRequestQueryException(DatawaveErrorCode.QUERY_LOGIC_ERROR, e);
        // }
        // qd.logic.validate(parameters);
        //
        // try {
        // securityMarking.clear();
        // securityMarking.validate(parameters);
        // } catch (IllegalArgumentException e) {
        // log.error("Failed security markings validation", e);
        // throw new BadRequestQueryException(DatawaveErrorCode.SECURITY_MARKING_CHECK_ERROR, e);
        // }
        // // Find out who/what called this method
        // qd.proxyServers = null;
        // qd.p = ctx.getCallerPrincipal();
        // qd.userDn = qd.p.getName();
        // qd.userid = qd.userDn;
        // qd.dnList = Collections.singletonList(qd.userid);
        // if (qd.p instanceof DatawavePrincipal) {
        // DatawavePrincipal dp = (DatawavePrincipal) qd.p;
        // qd.userid = dp.getShortName();
        // qd.userDn = dp.getUserDN().subjectDN();
        // String[] dns = dp.getDNs();
        // Arrays.sort(dns);
        // qd.dnList = Arrays.asList(dns);
        // qd.proxyServers = dp.getProxyServers();
        // }
        // log.trace(qd.userid + " has authorizations " + ((qd.p instanceof DatawavePrincipal) ? ((DatawavePrincipal) qd.p).getAuthorizations() : ""));
        //
        // // always check against the max
        // if (qd.logic.getMaxPageSize() > 0 && queryParameters.getPagesize() > qd.logic.getMaxPageSize()) {
        // log.error("Invalid page size: " + queryParameters.getPagesize() + " vs " + qd.logic.getMaxPageSize());
        // throw new BadRequestQueryException(DatawaveErrorCode.PAGE_SIZE_TOO_LARGE, MessageFormat.format("Max = {0}.", qd.logic.getMaxPageSize()));
        // }
        //
        // // validate the max results override relative to the max results on a query logic
        // // privileged users however can set whatever they want
        // if (queryParameters.isMaxResultsOverridden() && qd.logic.getMaxResults() >= 0) {
        // if (!ctx.isCallerInRole(PRIVILEGED_USER)) {
        // if (queryParameters.getMaxResultsOverride() < 0 || (qd.logic.getMaxResults() < queryParameters.getMaxResultsOverride())) {
        // log.error("Invalid max results override: " + queryParameters.getMaxResultsOverride() + " vs " + qd.logic.getMaxResults());
        // throw new BadRequestQueryException(DatawaveErrorCode.INVALID_MAX_RESULTS_OVERRIDE);
        // }
        // }
        // }
        //
        // // Set private audit-related parameters, stripping off any that the user might have passed in first.
        // // These are parameters that aren't passed in by the user, but rather are computed from other sources.
        // PrivateAuditConstants.stripPrivateParameters(parameters);
        // parameters.add(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        // parameters.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, marking.toColumnVisibilityString());
        // parameters.add(PrivateAuditConstants.USER_DN, qd.userDn);
        //
        // return qd;
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
