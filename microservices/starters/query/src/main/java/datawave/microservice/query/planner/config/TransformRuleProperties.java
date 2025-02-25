package datawave.microservice.query.planner.config;

import java.util.ArrayList;
import java.util.List;

public class TransformRuleProperties {
    private List<String> regexPatterns = new ArrayList<>();
    
    public List<String> getRegexPatterns() {
        return regexPatterns;
    }
    
    public void setRegexPatterns(List<String> regexPatterns) {
        this.regexPatterns = regexPatterns;
    }
}
