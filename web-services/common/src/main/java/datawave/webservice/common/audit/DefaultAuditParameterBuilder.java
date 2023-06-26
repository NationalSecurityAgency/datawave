package datawave.webservice.common.audit;

import datawave.core.common.audit.PrivateAuditConstants;
import datawave.microservice.query.QueryParameters;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;

import javax.ws.rs.core.MultivaluedMap;
import java.util.Map;

public class DefaultAuditParameterBuilder implements AuditParameterBuilder {
    private Logger log = LoggerFactory.getLogger(getClass().getName());

    @Override
    public Map<String,String> convertAndValidate(MultiValueMap<String,String> queryParameters) {
        AuditParameters validatedParams = new AuditParameters();

        MultivaluedMapImpl<String,String> auditParams = new MultivaluedMapImpl<>();
        // Pull parameters that are specified as query parameters (potentially under a different name) into the
        // audit parameters.
        if (queryParameters.containsKey(QueryParameters.QUERY_AUTHORIZATIONS))
            auditParams.put(AuditParameters.QUERY_AUTHORIZATIONS, queryParameters.get(AuditParameters.QUERY_AUTHORIZATIONS));
        if (queryParameters.containsKey(QueryParameters.QUERY_STRING))
            auditParams.put(AuditParameters.QUERY_STRING, queryParameters.get(AuditParameters.QUERY_STRING));

        // Put additional values passed by the caller (because these values were computed programmatically and not
        // directly supplied in the query call) into the audit parameters.
        if (queryParameters.containsKey(PrivateAuditConstants.AUDIT_TYPE))
            auditParams.putSingle(AuditParameters.QUERY_AUDIT_TYPE, queryParameters.getFirst(PrivateAuditConstants.AUDIT_TYPE));
        if (queryParameters.containsKey(PrivateAuditConstants.COLUMN_VISIBILITY))
            auditParams.putSingle(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, queryParameters.getFirst(PrivateAuditConstants.COLUMN_VISIBILITY));
        if (queryParameters.containsKey(PrivateAuditConstants.USER_DN))
            auditParams.putSingle(AuditParameters.USER_DN, queryParameters.getFirst(PrivateAuditConstants.USER_DN));
        if (queryParameters.containsKey(PrivateAuditConstants.LOGIC_CLASS))
            auditParams.putSingle(AuditParameters.QUERY_LOGIC_CLASS, queryParameters.getFirst(PrivateAuditConstants.LOGIC_CLASS));
        if (queryParameters.containsKey(PrivateAuditConstants.SELECTORS))
            validatedParams.setSelectors(queryParameters.get(PrivateAuditConstants.SELECTORS));
        if (queryParameters.containsKey(AuditParameters.AUDIT_ID)) {
            validatedParams.setAuditId(queryParameters.getFirst(AuditParameters.AUDIT_ID));
        }

        // Now validate the audit parameters and convert to a map.
        validatedParams.validate(auditParams);

        log.debug("generated audit parameters: " + validatedParams);
        return validatedParams.toMap();
    }

    @Override
    public Map<String,String> validate(MultivaluedMap<String,String> auditParameters) {
        AuditParameters validatedParams = new AuditParameters();
        validatedParams.validate(auditParameters);
        log.debug("generated audit parameters: " + validatedParams);
        return validatedParams.toMap();
    }
}
