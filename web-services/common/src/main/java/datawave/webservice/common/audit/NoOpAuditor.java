package datawave.webservice.common.audit;

import java.util.Map;
import java.util.UUID;

import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.interceptor.Interceptor;

import datawave.configuration.RefreshableScope;

/**
 * This auditor is injected when remote auditing is disabled.
 */
@RefreshableScope
@Alternative
// Make this alternative active for the entire application per the CDI 1.2 specification
@Priority(Interceptor.Priority.APPLICATION - 1)
public class NoOpAuditor implements AuditService {
    @Override
    public String audit(Map<String,String> parameters) throws Exception {
        // do nothing
        return UUID.randomUUID().toString();
    }
}
