package datawave.ingest.mapreduce.job.reindex;

import com.google.common.collect.Multimap;
import datawave.data.type.Type;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.policy.IngestPolicyEnforcer;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ForwardIndexOnlyIngestHelper implements IngestHelperInterface {
    private final IngestHelperInterface delegate;

    public ForwardIndexOnlyIngestHelper(IngestHelperInterface delegate) {
        this.delegate = delegate;
    }

    // only non-delegated method
    @Override
    public boolean isReverseIndexedField(String fieldName) {
        return false;
    }


    @Override
    public void setup(Configuration conf) {
        delegate.setup(conf);
    }

    @Override
    public datawave.ingest.data.Type getType() {
        return delegate.getType();
    }

    @Override
    public IngestPolicyEnforcer getPolicyEnforcer() {
        return delegate.getPolicyEnforcer();
    }

    @Override
    public Set<String> getShardExclusions() {
        return delegate.getShardExclusions();
    }

    @Override
    public Multimap<String, NormalizedContentInterface> getEventFields(RawRecordContainer value) {
        return delegate.getEventFields(value);
    }

    @Override
    public Multimap<String, NormalizedContentInterface> normalizeMap(Multimap<String, NormalizedContentInterface> fields) {
        return delegate.normalizeMap(fields);
    }

    @Override
    public Multimap<String, NormalizedContentInterface> normalize(Multimap<String, String> fields) {
        return delegate.normalize(fields);
    }

    @Override
    public List<Type<?>> getDataTypes(String fieldName) {
        return delegate.getDataTypes(fieldName);
    }

    @Override
    public String getNormalizedMaskedValue(String key) {
        return delegate.getNormalizedMaskedValue(key);
    }

    @Override
    public boolean hasMappings() {
        return delegate.hasMappings();
    }

    @Override
    public boolean contains(String key) {
        return delegate.contains(key);
    }

    @Override
    public String get(String key) {
        return delegate.get(key);
    }

    @Override
    public boolean getDeleteMode() {
        return delegate.getDeleteMode();
    }

    @Override
    public boolean getReplaceMalformedUTF8() {
        return delegate.getReplaceMalformedUTF8();
    }

    @Override
    public boolean isEmbeddedHelperMaskedFieldHelper() {
        return delegate.isEmbeddedHelperMaskedFieldHelper();
    }

    @Override
    public MaskedFieldHelper getEmbeddedHelperAsMaskedFieldHelper() {
        return delegate.getEmbeddedHelperAsMaskedFieldHelper();
    }

    @Override
    public DataTypeHelperImpl getEmbeddedHelper() {
        return delegate.getEmbeddedHelper();
    }

    @Override
    public boolean isIndexedField(String fieldName) {
        return delegate.isIndexedField(fieldName);
    }

    @Override
    public boolean isIndexOnlyField(String fieldName) {
        return delegate.isIndexOnlyField(fieldName);
    }

    @Override
    public void addIndexedField(String fieldName) {
        delegate.addIndexedField(fieldName);
    }

    @Override
    public void addShardExclusionField(String fieldName) {
        delegate.addShardExclusionField(fieldName);
    }

    @Override
    public void addReverseIndexedField(String fieldName) {
        delegate.addReverseIndexedField(fieldName);
    }

    @Override
    public void addIndexOnlyField(String fieldName) {
        delegate.addIndexOnlyField(fieldName);
    }

    @Override
    public boolean isCompositeField(String fieldName) {
        return delegate.isCompositeField(fieldName);
    }

    @Override
    public boolean isOverloadedCompositeField(String fieldName) {
        return delegate.isOverloadedCompositeField(fieldName);
    }

    @Override
    public boolean isNormalizedField(String fieldName) {
        return delegate.isNormalizedField(fieldName);
    }

    @Override
    public void addNormalizedField(String fieldName) {
        delegate.addNormalizedField(fieldName);
    }

    @Override
    public boolean isAliasedIndexField(String fieldName) {
        return delegate.isAliasedIndexField(fieldName);
    }

    @Override
    public HashSet<String> getAliasesForIndexedField(String fieldName) {
        return delegate.getAliasesForIndexedField(fieldName);
    }

    @Override
    public boolean isDataTypeField(String fieldName) {
        return delegate.isDataTypeField(fieldName);
    }

    @Override
    public Multimap<String, String> getCompositeFieldDefinitions() {
        return delegate.getCompositeFieldDefinitions();
    }

    @Override
    public Map<String, String> getCompositeFieldSeparators() {
        return delegate.getCompositeFieldSeparators();
    }

    @Override
    public boolean isVirtualIndexedField(String fieldName) {
        return delegate.isVirtualIndexedField(fieldName);
    }

    @Override
    public Map<String, String[]> getVirtualNameAndIndex(String fieldName) {
        return delegate.getVirtualNameAndIndex(fieldName);
    }

    @Override
    public boolean shouldHaveBeenIndexed(String fieldName) {
        return delegate.shouldHaveBeenIndexed(fieldName);
    }

    @Override
    public boolean shouldHaveBeenReverseIndexed(String fieldName) {
        return delegate.shouldHaveBeenReverseIndexed(fieldName);
    }
}
