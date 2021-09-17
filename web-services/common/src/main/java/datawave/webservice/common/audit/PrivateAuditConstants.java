package datawave.webservice.common.audit;

import javax.ws.rs.core.MultivaluedMap;

/**
 * Constants marking private parameters that are computed internally and then added at runtime to the incoming query parameters for the purposes of passing them
 * along to the audit service.
 */
public class PrivateAuditConstants extends datawave.services.common.audit.PrivateAuditConstants {
    @Deprecated
    public static void stripPrivateParameters(MultivaluedMap<String,String> queryParameters) {
        queryParameters.entrySet().removeIf(entry -> entry.getKey().startsWith(PREFIX));
    }
}
