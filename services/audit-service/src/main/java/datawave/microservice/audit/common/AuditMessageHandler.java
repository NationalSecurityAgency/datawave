package datawave.microservice.audit.common;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.audit.Auditor.AuditType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the base Audit Message Handler which is paired with the given Auditor to process audit messages. The common convention is for the auditor to override
 * the onMessage method in it's associated configuration class in order to add the appropriate StreamListener annotation.
 */
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
            // log the audit message if the type is anything except NONE (even null)
            if (!(ap.getAuditType() != null && ap.getAuditType().equals(AuditType.NONE))) {
                auditor.audit(ap);
            }
        } catch (Exception e) {
            log.error("Error processing audit message: " + e.getMessage());
            throw e;
        }
    }
}
