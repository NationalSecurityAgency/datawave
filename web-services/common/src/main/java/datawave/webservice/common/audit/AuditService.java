package datawave.webservice.common.audit;

import java.util.Map;

/**
 * A facade for sending an audit to the auditing service (typically a remote micro service). This will take care of extracting required parameters from the
 * query parameters and converting as necessary to those parameter values required by the auditing service.
 */
public interface AuditService {
    String audit(Map<String,String> queryParameters) throws Exception;
}
