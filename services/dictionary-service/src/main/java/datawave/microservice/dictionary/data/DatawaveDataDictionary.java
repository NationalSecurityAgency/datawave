package datawave.microservice.dictionary.data;

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

public interface DatawaveDataDictionary<META extends MetadataFieldBase<META,DESC>,DESC extends DescriptionBase<DESC>,FIELD extends DictionaryFieldBase<FIELD,DESC>> {
    Collection<META> getFields(String modelName, String modelTableName, String metadataTableName, Collection<String> dataTypeFilters, Connector connector,
                    Set<Authorizations> auths, int numThreads) throws Exception;
    
    Map<String,String> getNormalizerMapping();
    
    void setNormalizerMapping(Map<String,String> normalizerMapping);
    
    void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName, FIELD description)
                    throws Exception;
    
    void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName, String fieldName,
                    String datatype, DESC description) throws Exception;
    
    void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName, String fieldName,
                    String datatype, Set<DESC> descriptions) throws Exception;
    
    Multimap<Entry<String,String>,DESC> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName,
                    String modelTableName) throws Exception;
    
    Multimap<Entry<String,String>,DESC> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName,
                    String modelTableName, String datatype) throws Exception;
    
    Set<DESC> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName,
                    String fieldName, String datatype) throws Exception;
    
    void deleteDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName, String fieldName,
                    String datatype, DESC description) throws Exception;
}
