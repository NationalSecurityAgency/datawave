package datawave.webservice.datadictionary;

import com.google.common.collect.Multimap;
import datawave.webservice.query.result.metadata.MetadataFieldBase;
import datawave.webservice.results.datadictionary.DescriptionBase;
import datawave.webservice.results.datadictionary.DictionaryFieldBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 
 */
public interface DatawaveDataDictionary {
    Collection<MetadataFieldBase> getFields(String modelName, String modelTableName, String metadataTableName, Collection<String> dataTypeFilters,
                    Connector connector, Set<Authorizations> auths, int numThreads) throws Exception;
    
    Map<String,String> getNormalizerMapping();
    
    void setNormalizerMapping(Map<String,String> normalizerMapping);
    
    void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName,
                    DictionaryFieldBase description) throws Exception;
    
    void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName, String fieldName,
                    String datatype, DescriptionBase description) throws Exception;
    
    void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName, String fieldName,
                    String datatype, Set<DescriptionBase> descriptions) throws Exception;
    
    Multimap<Entry<String,String>,? extends DescriptionBase> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths,
                    String modelName, String modelTableName) throws Exception;
    
    Multimap<Entry<String,String>,? extends DescriptionBase> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths,
                    String modelName, String modelTableName, String datatype) throws Exception;
    
    Set<? extends DescriptionBase> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName,
                    String modelTableName, String fieldName, String datatype) throws Exception;
    
    void deleteDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName, String fieldName,
                    String datatype, DescriptionBase description) throws Exception;
}
