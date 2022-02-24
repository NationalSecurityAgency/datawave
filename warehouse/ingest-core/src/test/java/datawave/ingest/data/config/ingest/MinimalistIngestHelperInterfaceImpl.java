package datawave.ingest.data.config.ingest;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.policy.IngestPolicyEnforcer;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// For testing only, this implementation fulfills the interface contract with methods that throw UnsupportedOperationException.
// It is is extendable when implementations are needed for specific tests.
public class MinimalistIngestHelperInterfaceImpl implements IngestHelperInterface {
    @Override
    public Type getType() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public IngestPolicyEnforcer getPolicyEnforcer() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setup(Configuration conf) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Set<String> getShardExclusions() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isShardExcluded(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer value) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> fields) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public List<datawave.data.type.Type<?>> getDataTypes(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String getNormalizedMaskedValue(String key) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean hasMappings() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean contains(String key) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String get(String key) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean getDeleteMode() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean getReplaceMalformedUTF8() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isEmbeddedHelperMaskedFieldHelper() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public MaskedFieldHelper getEmbeddedHelperAsMaskedFieldHelper() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public DataTypeHelperImpl getEmbeddedHelper() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isIndexedField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isReverseIndexedField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isIndexOnlyField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void addIndexedField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void addShardExclusionField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void addReverseIndexedField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void addIndexOnlyField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isCompositeField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isOverloadedCompositeField(String fieldName) {
        return true;
    }
    
    @Override
    public boolean isNormalizedField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void addNormalizedField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isAliasedIndexField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public HashSet<String> getAliasesForIndexedField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isDataTypeField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Multimap<String,String> getCompositeFieldDefinitions() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Map<String,String> getCompositeFieldSeparators() {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isVirtualIndexedField(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Map<String,String[]> getVirtualNameAndIndex(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean shouldHaveBeenIndexed(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean shouldHaveBeenReverseIndexed(String fieldName) {
        // override this method, as needed
        throw new UnsupportedOperationException();
    }
}
