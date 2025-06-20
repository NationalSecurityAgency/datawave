package datawave.security.authorization;

import java.util.Map;

public class DatawaveUserServiceFactory {
    private Map<String,DatawaveUserService> services;
    private String activeService;

    public DatawaveUserService createDatawaveUserService() {
        DatawaveUserService service = services.get(activeService);
        if (service == null) {
            throw new IllegalStateException("No DatawaveUserService found for name: " + activeService);
        }
        return service;
    }

    public void setServices(Map<String,DatawaveUserService> services) {
        this.services = services;
    }

    public Map<String,DatawaveUserService> getServices() {
        return services;
    }

    public void setActiveService(String activeService) {
        this.activeService = activeService;
    }

    public String getActiveService() {
        return activeService;
    }
}
