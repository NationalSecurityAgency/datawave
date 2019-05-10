package datawave.microservice.audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Top-level properties for the remote audit service
 */
@ConfigurationProperties(prefix = "audit-client")
public class AuditServiceProperties {
    
    private String serviceId = "audit";
    
    private String uri = "http://localhost/audit";
    
    /**
     * By default we're suppressing requests having audit level of AuditType.NONE, as they're likely to be dropped at the remote audit service anyway
     */
    private boolean suppressAuditTypeNone = true;
    
    public String getUri() {
        return uri;
    }
    
    public void setUri(String uri) {
        this.uri = uri;
    }
    
    public String getServiceId() {
        return serviceId;
    }
    
    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }
    
    public boolean isSuppressAuditTypeNone() {
        return suppressAuditTypeNone;
    }
    
    public void setSuppressAuditTypeNone(boolean suppressAuditTypeNone) {
        this.suppressAuditTypeNone = suppressAuditTypeNone;
    }
}
