package datawave.microservice.query.logic.config;

import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Additional configuration for complex types used to configure ShardQueryLogic instances.
 */
public class ShardQueryLogicProperties {
    private Map<String,String> hierarchyFieldOptions = new HashMap<>();
    private List<String> contentFieldNames = new ArrayList<>();
    private List<String> realmSuffixExclusionPatterns = new ArrayList<>();
    private List<String> enricherClassNames = new ArrayList<>();
    private List<String> filterClassNames = new ArrayList<>();
    private Map<String,String> filterOptions = new HashMap<>();
    private List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs = new ArrayList<>();
    private DataDecoratorTransformerConfig dataDecoratorTransformerConfig = new DataDecoratorTransformerConfig();
    private Map<String,String> querySyntaxParsers = new HashMap<>();
    
    public static class DataDecoratorTransformerConfig {
        private List<String> requestedDecorators = new ArrayList<>();
        private Map<String,Map<String,String>> dataDecorators = new HashMap<>();
        
        public List<String> getRequestedDecorators() {
            return requestedDecorators;
        }
        
        public void setRequestedDecorators(List<String> requestedDecorators) {
            this.requestedDecorators = requestedDecorators;
        }
        
        public Map<String,Map<String,String>> getDataDecorators() {
            return dataDecorators;
        }
        
        public void setDataDecorators(Map<String,Map<String,String>> dataDecorators) {
            this.dataDecorators = dataDecorators;
        }
    }
    
    public Map<String,String> getHierarchyFieldOptions() {
        return hierarchyFieldOptions;
    }
    
    public void setHierarchyFieldOptions(Map<String,String> hierarchyFieldOptions) {
        this.hierarchyFieldOptions = hierarchyFieldOptions;
    }
    
    public List<String> getContentFieldNames() {
        return contentFieldNames;
    }
    
    public void setContentFieldNames(List<String> contentFieldNames) {
        this.contentFieldNames = contentFieldNames;
    }
    
    public List<String> getRealmSuffixExclusionPatterns() {
        return realmSuffixExclusionPatterns;
    }
    
    public void setRealmSuffixExclusionPatterns(List<String> realmSuffixExclusionPatterns) {
        this.realmSuffixExclusionPatterns = realmSuffixExclusionPatterns;
    }
    
    public List<String> getEnricherClassNames() {
        return enricherClassNames;
    }
    
    public void setEnricherClassNames(List<String> enricherClassNames) {
        this.enricherClassNames = enricherClassNames;
    }
    
    public List<String> getFilterClassNames() {
        return filterClassNames;
    }
    
    public void setFilterClassNames(List<String> filterClassNames) {
        this.filterClassNames = filterClassNames;
    }
    
    public Map<String,String> getFilterOptions() {
        return filterOptions;
    }
    
    public void setFilterOptions(Map<String,String> filterOptions) {
        this.filterOptions = filterOptions;
    }
    
    public List<IvaratorCacheDirConfig> getIvaratorCacheDirConfigs() {
        return ivaratorCacheDirConfigs;
    }
    
    public void setIvaratorCacheDirConfigs(List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs) {
        this.ivaratorCacheDirConfigs = ivaratorCacheDirConfigs;
    }
    
    public DataDecoratorTransformerConfig getDataDecoratorTransformerConfig() {
        return dataDecoratorTransformerConfig;
    }
    
    public void setDataDecoratorTransformerConfig(DataDecoratorTransformerConfig dataDecoratorTransformerConfig) {
        this.dataDecoratorTransformerConfig = dataDecoratorTransformerConfig;
    }
    
    public Map<String,String> getQuerySyntaxParsers() {
        return querySyntaxParsers;
    }
    
    public void setQuerySyntaxParsers(Map<String,String> querySyntaxParsers) {
        this.querySyntaxParsers = querySyntaxParsers;
    }
}
