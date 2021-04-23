package datawave.microservice.query.config;

import java.util.List;
import java.util.Map;

/**
 * Additional configuration for complex types used to configure ShardQueryLogic instances.
 */
public class ShardQueryLogicProperties {
    
    // passed as ref, but only assigned in BaseEventQuery
    private Map<String,String> hierarchyFieldOptions;
    
    private List<String> contentFieldNames;
    private List<String> realmSuffixExclusionPatterns;
    private List<String> enricherClassNames;
    private List<String> filterClassNames;
    
    // passed as ref, but only assigned in BaseEventQuery
    private Map<String,String> filterOptions;
    
    // passed as ref, and assigned in BaseEventQuery and HitHighlights
    private List<IvaratorCacheDirConfigProperties> ivaratorCacheDirConfigs;
    
    private EventQueryDataDecoratorTransformerProperties eventQueryDataDecoratorTransformer;
    
    // list defined in
    // querySyntaxParsers
    
    public static class IvaratorCacheDirConfigProperties {
        private String basePathUri;
        private int priority;
        private long minAvailableStorageMiB;
        private double minAvailableStoragePercent;
    }
    
    public static class EventQueryDataDecoratorTransformerProperties {
        private Map<String,EventQueryDataDecoratorProperties> dataDecorators;
        private List<String> requestedDecorators;
    }
    
    public static class EventQueryDataDecoratorProperties {
        private String fieldName;
        private Map<String,String> patternMap;
    }
    
    public static class QueryParserProperties {
        
    }
}
