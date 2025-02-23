package datawave.microservice.audit.common;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;

public class AuditMessageConsumer implements Consumer<AuditMessage> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private AuditParameters msgHandlerAuditParams;
    
    private Auditor auditor;
    
    public AuditMessageConsumer(AuditParameters auditParameters, Auditor auditor) {
        this.msgHandlerAuditParams = auditParameters;
        this.auditor = auditor;
    }
    
    @Override
    public void accept(AuditMessage auditMessage) {
        try {
            AuditParameters ap = msgHandlerAuditParams.fromMap(auditMessage.getAuditParameters());
            // log the audit message if the type is anything except NONE (even null)
            if (!(ap.getAuditType() != null && ap.getAuditType().equals(Auditor.AuditType.NONE))) {
                auditor.audit(ap);
            }
        } catch (Exception e) {
            log.error("Error processing audit message: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
