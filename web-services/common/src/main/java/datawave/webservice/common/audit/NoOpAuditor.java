package datawave.webservice.common.audit;

import datawave.configuration.RefreshableScope;

import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.interceptor.Interceptor;

/**
 * This auditor is injected when remote auditing is disabled.
 */
@RefreshableScope
@Alternative
// Make this alternative active for the entire application per the CDI 1.2 specification
@Priority(Interceptor.Priority.APPLICATION - 1)
public class NoOpAuditor implements Auditor {
    @Override
    public void audit(AuditParameters msg) throws Exception {
        // do nothing
    }
}
