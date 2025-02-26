package datawave.microservice.query.planner.config;

import java.util.ArrayList;
import java.util.List;

public class DefaultQueryPlannerProperties {
    public static final String PREFIX = "datawave.query.planner.planners.default-query-planner";
    
    private List<String> requestedTransformRules = new ArrayList<>();
    
    public List<String> getRequestedTransformRules() {
        return requestedTransformRules;
    }
    
    public void setRequestedTransformRules(List<String> requestedTransformRules) {
        this.requestedTransformRules = requestedTransformRules;
    }
}
