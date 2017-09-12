package datawave.microservice.audit.log;

import datawave.microservice.audit.common.AuditParameters;
import datawave.microservice.audit.common.Auditor;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

public class LogAuditor implements Auditor {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Override
    public void audit(AuditParameters am) throws Exception {
        if (!am.getAuditType().equals(AuditType.NONE)) {
            log.info(am.toString());
        }
    }
}
