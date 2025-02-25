package datawave.microservice.query.logic.config;

import java.util.HashMap;
import java.util.Map;

public class EdgeQueryLogicProperties {
    private Map<String,String> querySyntaxParsers = new HashMap<>();
    
    public Map<String,String> getQuerySyntaxParsers() {
        return querySyntaxParsers;
    }
    
    public void setQuerySyntaxParsers(Map<String,String> querySyntaxParsers) {
        this.querySyntaxParsers = querySyntaxParsers;
    }
}
