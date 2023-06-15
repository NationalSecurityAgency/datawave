package datawave.webservice.common.audit;

import javax.ws.rs.core.MultivaluedMap;
import java.util.Map;

/**
 * A utility to extract parameters from a REST call and convert them, as necessary, into parameters that are required by the auditor.
 */
public interface AuditParameterBuilder {
    /**
     * Extracts parameters from {@code queryParameters}, converts to the parameters required by the audit microservice, and then validates the parameters before
     * returning them in a {@link Map}.
     *
     * @param queryParameters
     *            the query parameters
     * @return validated parameters
     */
    Map<String,String> convertAndValidate(MultivaluedMap<String,String> queryParameters);

    /**
     * Builds validated audit parameters for a direct call to the audit service. That is, the parameters passed in are expected to be those used by the audit
     * service.
     *
     * @param auditParameters
     *            the parameters to a REST call intended for the audit service
     * @return a {@link Map} containing the parameter names and values necessary to pass to the audit service
     */
    Map<String,String> validate(MultivaluedMap<String,String> auditParameters);
}
