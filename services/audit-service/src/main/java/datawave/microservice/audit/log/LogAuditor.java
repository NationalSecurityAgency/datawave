package datawave.microservice.audit.log;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogAuditor implements Auditor {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Override
    public void audit(AuditParameters am) throws Exception {
        if (!am.getAuditType().equals(AuditType.NONE)) {
            log.info(am.toString());
        }
    }
}
