package datawave.microservice.query.logic.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.data.type.Type;
import datawave.query.cardinality.CardinalityConfiguration;
import datawave.query.config.IndexHole;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;

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
    private Set<String> mandatoryQuerySyntax = new HashSet<>();
    private Set<String> requiredRoles = new HashSet<>();
    private List<String> documentPermutations = new ArrayList<>();
    private Map<String,String> queryMacroFunction = new HashMap<>();
    private List<IndexHole> indexHoles = new ArrayList<>();
    private Set<String> whindexMappingFields = new HashSet<>();
    private Map<String,Map<String,String>> whindexFieldMappings = new HashMap<>();
    private Map<String,Long> dnResultLimits = new HashMap<>();
    private Set<String> disallowlistedFields = new HashSet<>();
    private CardinalityConfiguration cardinalityConfiguration = new CardinalityConfiguration();
    private List<Type> dataTypes = new ArrayList<>();
    private List<String> indexFilteringClassNames = new ArrayList<>();
    
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
    
    public Set<String> getMandatoryQuerySyntax() {
        return mandatoryQuerySyntax;
    }
    
    public void setMandatoryQuerySyntax(Set<String> mandatoryQuerySyntax) {
        this.mandatoryQuerySyntax = mandatoryQuerySyntax;
    }
    
    public Set<String> getRequiredRoles() {
        return requiredRoles;
    }
    
    public void setRequiredRoles(Set<String> requiredRoles) {
        this.requiredRoles = requiredRoles;
    }
    
    public List<String> getDocumentPermutations() {
        return documentPermutations;
    }
    
    public void setDocumentPermutations(List<String> documentPermutations) {
        this.documentPermutations = documentPermutations;
    }
    
    public Map<String,String> getQueryMacroFunction() {
        return queryMacroFunction;
    }
    
    public void setQueryMacroFunction(Map<String,String> queryMacroFunction) {
        this.queryMacroFunction = queryMacroFunction;
    }
    
    public List<IndexHole> getIndexHoles() {
        return indexHoles;
    }
    
    public void setIndexHoles(List<IndexHole> indexHoles) {
        this.indexHoles = indexHoles;
    }
    
    public Set<String> getWhindexMappingFields() {
        return whindexMappingFields;
    }
    
    public void setWhindexMappingFields(Set<String> whindexMappingFields) {
        this.whindexMappingFields = whindexMappingFields;
    }
    
    public Map<String,Map<String,String>> getWhindexFieldMappings() {
        return whindexFieldMappings;
    }
    
    public void setWhindexFieldMappings(Map<String,Map<String,String>> whindexFieldMappings) {
        this.whindexFieldMappings = whindexFieldMappings;
    }
    
    public Map<String,Long> getDnResultLimits() {
        return dnResultLimits;
    }
    
    public void setDnResultLimits(Map<String,Long> dnResultLimits) {
        this.dnResultLimits = dnResultLimits;
    }
    
    public Set<String> getDisallowlistedFields() {
        return disallowlistedFields;
    }
    
    public void setDisallowlistedFields(Set<String> disallowlistedFields) {
        this.disallowlistedFields = disallowlistedFields;
    }
    
    public CardinalityConfiguration getCardinalityConfiguration() {
        return cardinalityConfiguration;
    }
    
    public void setCardinalityConfiguration(CardinalityConfiguration cardinalityConfiguration) {
        this.cardinalityConfiguration = cardinalityConfiguration;
    }
    
    public List<Type> getDataTypes() {
        return dataTypes;
    }
    
    public void setDataTypes(List<Type> dataTypes) {
        this.dataTypes = dataTypes;
    }
    
    public List<String> getIndexFilteringClassNames() {
        return indexFilteringClassNames;
    }
    
    public void setIndexFilteringClassNames(List<String> indexFilteringClassNames) {
        this.indexFilteringClassNames = indexFilteringClassNames;
    }
}
