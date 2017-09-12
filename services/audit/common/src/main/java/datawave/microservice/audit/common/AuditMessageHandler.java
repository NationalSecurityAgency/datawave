package datawave.microservice.audit.common;

import datawave.microservice.audit.common.Auditor.AuditType;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.inject.Inject;
import java.util.Map;

public class AuditMessageHandler {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    public static String LISTENER_METHOD = "onMessage";
    
    private Auditor auditor;
    
    @Autowired
    private AuditParameters auditParameters;
    
    public AuditMessageHandler(Auditor auditor) {
        this.auditor = auditor;
    }
    
    public void onMessage(Map<String,Object> msg) {
        try {
            auditParameters.clear();
            AuditParameters ap = auditParameters.fromMap(msg);
            if (!ap.getAuditType().equals(AuditType.NONE)) {
                auditor.audit(ap);
            }
        } catch (Exception e) {
            log.error("Error processing audit message: " + e.getMessage());
        }
    }
}
