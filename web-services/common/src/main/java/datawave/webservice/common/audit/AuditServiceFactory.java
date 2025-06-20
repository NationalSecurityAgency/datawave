package datawave.webservice.common.audit;

import java.util.HashMap;
import java.util.Map;

public class AuditServiceFactory {

    private Map<String,AuditService> services = new HashMap<>();
    private String activeService;

    public AuditService createService() {
        AuditService service = services.get(activeService);
        if (service == null) {
            throw new IllegalStateException("No AuditService found for name: " + activeService);
        }
        return service;
    }

    public void setServices(Map<String,AuditService> services) {
        this.services = services;
    }

    public void setActiveService(String activeService) {
        this.activeService = activeService;
    }

    public String getActiveService() {
        return activeService;
    }
}
