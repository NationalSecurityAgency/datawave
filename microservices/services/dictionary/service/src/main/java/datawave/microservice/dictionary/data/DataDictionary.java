package datawave.microservice.dictionary.data;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Multimap;

import datawave.microservice.Connection;
import datawave.webservice.dictionary.data.DescriptionBase;
import datawave.webservice.dictionary.data.DictionaryFieldBase;
import datawave.webservice.metadata.MetadataFieldBase;

public interface DataDictionary<META extends MetadataFieldBase<META,DESC>,DESC extends DescriptionBase<DESC>,FIELD extends DictionaryFieldBase<FIELD,DESC>> {
    
    Map<String,String> getNormalizationMap();
    
    void setNormalizationMap(Map<String,String> normalizationMap);
    
    Collection<META> getFields(Connection connectionConfig, Collection<String> dataTypeFilters, int numThreads) throws Exception;
    
    void setDescription(Connection connectionConfig, FIELD description) throws Exception;
    
    void setDescription(Connection connectionConfig, String fieldName, String datatype, DESC description) throws Exception;
    
    void setDescriptions(Connection connectionConfig, String fieldName, String datatype, Set<DESC> descriptions) throws Exception;
    
    Multimap<Entry<String,String>,DESC> getDescriptions(Connection connectionConfig) throws Exception;
    
    Multimap<Entry<String,String>,DESC> getDescriptions(Connection connectionConfig, String datatype) throws Exception;
    
    Set<DESC> getDescriptions(Connection connectionConfig, String fieldName, String datatype) throws Exception;
    
    void deleteDescription(Connection connectionConfig, String fieldName, String datatype, DESC description) throws Exception;
}
