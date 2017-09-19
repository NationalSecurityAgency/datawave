package datawave.webservice.operations.configuration;

import datawave.webservice.common.audit.Auditor.AuditType;

import java.util.ArrayList;
import java.util.List;

public class LookupBeanConfiguration {
    
    private AuditType defaultAuditType = AuditType.ACTIVE;
    private List<LookupAuditConfiguration> lookupAuditConfiguration = new ArrayList<>();
    
    public List<LookupAuditConfiguration> getLookupAuditConfiguration() {
        return lookupAuditConfiguration;
    }
    
    public void setLookupAuditConfiguration(List<LookupAuditConfiguration> lookupAuditConfiguration) {
        this.lookupAuditConfiguration = lookupAuditConfiguration;
    }
    
    public AuditType getDefaultAuditType() {
        return defaultAuditType;
    }
    
    public void setDefaultAuditType(AuditType defaultAuditType) {
        this.defaultAuditType = defaultAuditType;
    }
}
