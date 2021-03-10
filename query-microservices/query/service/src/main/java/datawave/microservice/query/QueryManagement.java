package datawave.microservice.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.query.util.QueryUtil;
import datawave.webservice.common.audit.AuditParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import javax.ws.rs.core.HttpHeaders;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;

@Component
public class QueryManagement {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * This method will provide some initial query validation for the define and create query calls.
     */
    private void validateQuery(String logicName, MultiValueMap<String, String> parameters, HttpHeaders httpHeaders) {

        // add query logic name to parameters
        parameters.add(QueryParameters.QUERY_LOGIC_NAME, logicName);

        log.debug(writeValueAsString(parameters));

        // Pull "params" values into individual query parameters for validation on the query logic.
        // This supports the deprecated "params" value (both on the old and new API). Once we remove the deprecated
        // parameter, this code block can go away.
        parameters.get(QueryParameters.QUERY_PARAMS).stream().map(QueryUtil::parseParameters).forEach(parameters::addAll);

        parameters.remove(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ);
        parameters.remove(AuditParameters.USER_DN);
        parameters.remove(AuditParameters.QUERY_AUDIT_TYPE);

        // Ensure that all required parameters exist prior to validating the values.
        qp.validate(parameters);

        // The pagesize and expirationDate checks will always be false when called from the RemoteQueryExecutor.
        // Leaving for now until we can test to ensure that is always the case.
        if (qp.getPagesize() <= 0) {
            log.error("Invalid page size: " + qp.getPagesize());
            GenericResponse<String> response = new GenericResponse<>();
            throwBadRequest(DatawaveErrorCode.INVALID_PAGE_SIZE, response);
        }

        if (qp.getPageTimeout() != -1 && (qp.getPageTimeout() < PAGE_TIMEOUT_MIN || qp.getPageTimeout() > PAGE_TIMEOUT_MAX)) {
            log.error("Invalid page timeout: " + qp.getPageTimeout());
            GenericResponse<String> response = new GenericResponse<>();
            throwBadRequest(DatawaveErrorCode.INVALID_PAGE_TIMEOUT, response);
        }

        if (System.currentTimeMillis() >= qp.getExpirationDate().getTime()) {
            log.error("Invalid expiration date: " + qp.getExpirationDate());
            GenericResponse<String> response = new GenericResponse<>();
            throwBadRequest(DatawaveErrorCode.INVALID_EXPIRATION_DATE, response);
        }

        // Ensure begin date does not occur after the end date (if dates are not null)
        if ((qp.getBeginDate() != null && qp.getEndDate() != null) && qp.getBeginDate().after(qp.getEndDate())) {
            log.error("Invalid begin and/or end date: " + qp.getBeginDate() + " - " + qp.getEndDate());
            GenericResponse<String> response = new GenericResponse<>();
            throwBadRequest(DatawaveErrorCode.BEGIN_DATE_AFTER_END_DATE, response);
        }

        // will throw IllegalArgumentException if not defined
        try {
            qd.logic = queryLogicFactory.getQueryLogic(queryLogicName, ctx.getCallerPrincipal());
        } catch (Exception e) {
            log.error("Failed to get query logic for " + queryLogicName, e);
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.QUERY_LOGIC_ERROR, e);
            GenericResponse<String> response = new GenericResponse<>();
            response.addException(qe.getBottomQueryException());
            throw new BadRequestException(qe, response);
        }
        qd.logic.validate(parameters);

        try {
            marking.clear();
            marking.validate(parameters);
        } catch (IllegalArgumentException e) {
            log.error("Failed security markings validation", e);
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.SECURITY_MARKING_CHECK_ERROR, e);
            GenericResponse<String> response = new GenericResponse<>();
            response.addException(qe);
            throw new BadRequestException(qe, response);
        }
        // Find out who/what called this method
        qd.proxyServers = null;
        qd.p = ctx.getCallerPrincipal();
        qd.userDn = qd.p.getName();
        qd.userid = qd.userDn;
        qd.dnList = Collections.singletonList(qd.userid);
        if (qd.p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) qd.p;
            qd.userid = dp.getShortName();
            qd.userDn = dp.getUserDN().subjectDN();
            String[] dns = dp.getDNs();
            Arrays.sort(dns);
            qd.dnList = Arrays.asList(dns);
            qd.proxyServers = dp.getProxyServers();
        }
        log.trace(qd.userid + " has authorizations " + ((qd.p instanceof DatawavePrincipal) ? ((DatawavePrincipal) qd.p).getAuthorizations() : ""));

        // always check against the max
        if (qd.logic.getMaxPageSize() > 0 && qp.getPagesize() > qd.logic.getMaxPageSize()) {
            log.error("Invalid page size: " + qp.getPagesize() + " vs " + qd.logic.getMaxPageSize());
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.PAGE_SIZE_TOO_LARGE, MessageFormat.format("Max = {0}.",
                    qd.logic.getMaxPageSize()));
            GenericResponse<String> response = new GenericResponse<>();
            response.addException(qe);
            throw new BadRequestException(qe, response);
        }

        // validate the max results override relative to the max results on a query logic
        // privileged users however can set whatever they want
        if (qp.isMaxResultsOverridden() && qd.logic.getMaxResults() >= 0) {
            if (!ctx.isCallerInRole(PRIVILEGED_USER)) {
                if (qp.getMaxResultsOverride() < 0 || (qd.logic.getMaxResults() < qp.getMaxResultsOverride())) {
                    log.error("Invalid max results override: " + qp.getMaxResultsOverride() + " vs " + qd.logic.getMaxResults());
                    GenericResponse<String> response = new GenericResponse<>();
                    throwBadRequest(DatawaveErrorCode.INVALID_MAX_RESULTS_OVERRIDE, response);
                }
            }
        }

        // Set private audit-related parameters, stripping off any that the user might have passed in first.
        // These are parameters that aren't passed in by the user, but rather are computed from other sources.
        PrivateAuditConstants.stripPrivateParameters(parameters);
        parameters.add(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        parameters.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, marking.toColumnVisibilityString());
        parameters.add(PrivateAuditConstants.USER_DN, qd.userDn);

        return qd;
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
