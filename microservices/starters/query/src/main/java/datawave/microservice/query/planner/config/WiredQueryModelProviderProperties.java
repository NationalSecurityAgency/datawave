package datawave.microservice.query.planner.config;

import java.util.HashMap;
import java.util.Map;

public class WiredQueryModelProviderProperties {
    private Map<String,String> wiredQueryModelProviderQueryModel = new HashMap<>();
    
    public Map<String,String> getWiredQueryModelProviderQueryModel() {
        return wiredQueryModelProviderQueryModel;
    }
    
    public void setWiredQueryModelProviderQueryModel(Map<String,String> wiredQueryModelProviderQueryModel) {
        this.wiredQueryModelProviderQueryModel = wiredQueryModelProviderQueryModel;
    }
}
