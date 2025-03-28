package datawave.microservice.audit.common;

import java.util.Map;

import datawave.webservice.common.audit.AuditParameters;

/**
 * This class represents a message sent from the audit service via messaging to the various queues that are receiving audit events (e.g., log auditor, Accumulo
 * auditor, etc).
 */
public class AuditMessage {
    private Map<String,String> auditParameters;
    
    public static AuditMessage fromParams(AuditParameters auditParameters) {
        return new AuditMessage(auditParameters.toMap());
    }
    
    public AuditMessage() {
        // empty constructor provided for serialization
    }
    
    public AuditMessage(Map<String,String> auditParameters) {
        this.auditParameters = auditParameters;
    }
    
    public Map<String,String> getAuditParameters() {
        return auditParameters;
    }
    
    public void setAuditParameters(Map<String,String> auditParameters) {
        this.auditParameters = auditParameters;
    }
}
