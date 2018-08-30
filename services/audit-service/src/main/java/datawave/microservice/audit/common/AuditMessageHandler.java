package datawave.microservice.audit.common;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.audit.Auditor.AuditType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditMessageHandler {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private AuditParameters msgHandlerAuditParams;
    
    private Auditor auditor;
    
    public AuditMessageHandler(AuditParameters auditParameters, Auditor auditor) {
        this.msgHandlerAuditParams = auditParameters;
        this.auditor = auditor;
    }
    
    public void onMessage(AuditMessage msg) throws Exception {
        try {
            AuditParameters ap = msgHandlerAuditParams.fromMap(msg.getAuditParameters());
            if (!ap.getAuditType().equals(AuditType.NONE)) {
                auditor.audit(ap);
            }
        } catch (Exception e) {
            log.error("Error processing audit message: " + e.getMessage());
            throw e;
        }
    }
}
