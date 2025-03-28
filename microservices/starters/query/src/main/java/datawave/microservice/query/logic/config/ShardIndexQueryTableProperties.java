package datawave.microservice.query.logic.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Additional configuration for complex types used to configure ShardIndexQueryTable instances.
 */
public class ShardIndexQueryTableProperties {
    private List<String> realmSuffixExclusionPatterns = new ArrayList<>();
    
    public List<String> getRealmSuffixExclusionPatterns() {
        return realmSuffixExclusionPatterns;
    }
    
    public void setRealmSuffixExclusionPatterns(List<String> realmSuffixExclusionPatterns) {
        this.realmSuffixExclusionPatterns = realmSuffixExclusionPatterns;
    }
}
