package nsa.datawave.ingest.data.config.ingest;

import java.util.List;
import java.util.Map;
import java.util.Set;

import nsa.datawave.data.type.Type;
import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.config.DataTypeHelper;
import nsa.datawave.ingest.data.config.DataTypeHelperImpl;
import nsa.datawave.ingest.data.config.MaskedFieldHelper;
import nsa.datawave.ingest.data.config.NormalizedContentInterface;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

/**
 * Specialization of the Helper type that validates the configuration for Ingest purposes. These helper classes also have the logic to parse the field names and
 * fields values from the datatypes that they represent.
 * 
 * 
 * 
 */
public interface IngestHelperInterface extends DataTypeHelper {
    
    void setup(Configuration conf);
    
    Set<String> getShardExclusions();
    
    /**
     * Fully parse the raw record and return a map of field names and values.
     * 
     * @param value
     * @return
     */
    Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer value);
    
    Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> fields);
    
    Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields);
    
    List<Type<?>> getDataTypes(String fieldName);
    
    Map<String,String> getNormalizedMaskedValues();
    
    Map<String,String> getMaskedValues();
    
    boolean getDeleteMode();
    
    boolean getReplaceMalformedUTF8();
    
    boolean isEmbeddedHelperMaskedFieldHelper();
    
    MaskedFieldHelper getEmbeddedHelperAsMaskedFieldHelper();
    
    DataTypeHelperImpl getEmbeddedHelper();
    
    boolean isIndexedField(String fieldName);
    
    boolean isReverseIndexedField(String fieldName);
    
    boolean isIndexOnlyField(String fieldName);
    
    void addIndexedField(String fieldName);
    
    void addReverseIndexedField(String fieldName);
    
    void addIndexOnlyField(String fieldName);
    
    boolean isCompositeField(String fieldName);
    
    void addCompositeField(String fieldName);
    
    boolean isNormalizedField(String fieldName);
    
    void addNormalizedField(String fieldName);
    
    boolean isDataTypeField(String fieldName);
    
    Map<String,String[]> getCompositeNameAndIndex(String fieldName);
    
    boolean isVirtualIndexedField(String fieldName);
    
    Map<String,String[]> getVirtualNameAndIndex(String fieldName);
    
    // if a field is know to be indexed by some datasource other than our own
    boolean shouldHaveBeenIndexed(String fieldName);
    
    // if a field is know to be reverse indexed by some datasource other than our own
    boolean shouldHaveBeenReverseIndexed(String fieldName);
}
