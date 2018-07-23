package datawave.ingest.data.config.ingest;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.data.type.Type;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.NormalizedContentInterface;

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
    
    /**
     * @deprecated use isShardExcluded(..) instead
     */
    @Deprecated
    Set<String> getShardExclusions();
    
    /**
     * @param fieldName
     * @return true if the filed should be excluded from the shard table.
     */
    @SuppressWarnings("deprecation")
    default boolean isShardExcluded(String fieldName) {
        return getShardExclusions().contains(fieldName);
    }
    
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
    
    String getNormalizedMaskedValue(final String key);
    
    /**
     * @return true if there exists any mappings
     */
    boolean hasMappings();
    
    /**
     * @param key
     *            field name for which to retrieve the mapping
     * @return true if there exists a mapping for this key else false
     */
    boolean contains(final String key);
    
    /**
     * @param key
     *            field name for which to retrieve the mapping
     * @return masked value (NOT NORMALIZED)
     */
    String get(final String key);
    
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
    
    boolean isFixedLengthCompositeField(String fieldName);
    
    boolean isTransitionedCompositeField(String fieldName);
    
    Date getCompositeFieldTransitionDate(String fieldName);
    
    boolean isOverloadedCompositeField(String fieldName);
    
    void addCompositeField(String fieldName);
    
    boolean isNormalizedField(String fieldName);
    
    void addNormalizedField(String fieldName);
    
    boolean isAliasedIndexField(String fieldName);
    
    HashSet<String> getAliasesForIndexedField(String fieldName);
    
    boolean isDataTypeField(String fieldName);
    
    Map<String,String[]> getCompositeFieldDefinitions();
    
    boolean isVirtualIndexedField(String fieldName);
    
    Map<String,String[]> getVirtualNameAndIndex(String fieldName);
    
    // if a field is know to be indexed by some datasource other than our own
    boolean shouldHaveBeenIndexed(String fieldName);
    
    // if a field is know to be reverse indexed by some datasource other than our own
    boolean shouldHaveBeenReverseIndexed(String fieldName);
}
