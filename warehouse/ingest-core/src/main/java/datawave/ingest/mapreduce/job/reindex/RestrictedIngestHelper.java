package datawave.ingest.mapreduce.job.reindex;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

import datawave.data.type.Type;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.policy.IngestPolicyEnforcer;

/**
 * Special class used to restrict how metadata is used when generating EventMetadata from fi/tf keys.
 */
public class RestrictedIngestHelper extends AbstractContentIngestHelper implements IngestHelperInterface {
    private final IngestHelperInterface delegate;

    private final boolean restrictShard;
    private final boolean restrictReverseIndex;
    private final boolean restrictTf;

    public RestrictedIngestHelper(IngestHelperInterface delegate, boolean restrictShard, boolean restrictReverseIndex, boolean restrictTf) {
        this.delegate = delegate;

        this.restrictShard = restrictShard;
        this.restrictReverseIndex = restrictReverseIndex;
        this.restrictTf = restrictTf;
    }

    // non-delegated methods
    @Override
    public boolean isReverseIndexedField(String fieldName) {
        return !this.restrictReverseIndex && delegate.isReverseIndexedField(fieldName);
    }

    @Override
    public boolean isShardExcluded(String fieldName) {
        return this.restrictShard || delegate.isShardExcluded(fieldName);
    }

    // this is a bit funny for tf keys
    // the field list will set the tf flag on the field that WILL be tokenized
    // the field list will NOT set the tf flag on that WAS tokenized
    // the metadata will set the tf flag on a field that WAS tokenized
    // the metadata will NOT set the tf flag on a field that will be tokenized

    @Override
    public boolean isContentIndexField(String field) {
        validateTypeAbstractIngestHelper();
        String tokenDesignator = ((AbstractContentIngestHelper) delegate).getTokenFieldNameDesignator();

        if (!field.endsWith(tokenDesignator)) {
            return false;
        }

        // strip the token designator
        field = field.substring(0, field.length() - tokenDesignator.length());

        return !this.restrictTf && ((AbstractContentIngestHelper) delegate).isContentIndexField(field);
    }

    @Override
    public boolean isReverseContentIndexField(String field) {
        validateTypeAbstractIngestHelper();
        String tokenDesignator = ((AbstractContentIngestHelper) delegate).getTokenFieldNameDesignator();

        if (!field.endsWith(tokenDesignator)) {
            return false;
        }

        // strip the token designator
        field = field.substring(0, field.length() - tokenDesignator.length());

        return !this.restrictTf && ((AbstractContentIngestHelper) delegate).isReverseContentIndexField(field);
    }

    // normally at ingest time the designator is used to create datawave metadata for the tokenized fields
    // in this case, we only want to create datawave metadata for the actual field seen. So never apply a
    // designator for the sake of reindexing
    @Override
    public String getTokenFieldNameDesignator() {
        validateTypeAbstractIngestHelper();

        // always strip this value
        return "";
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
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer value) {
        return delegate.getEventFields(value);
    }

    @Override
    public Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> fields) {
        return delegate.normalizeMap(fields);
    }

    @Override
    public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
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

    private void validateTypeAbstractIngestHelper() {
        if (!(delegate instanceof AbstractContentIngestHelper)) {
            throw new UnsupportedOperationException("cannot call method not defined by delegate");
        }
    }

    @Override
    public boolean isIndexListField(String field) {
        validateTypeAbstractIngestHelper();
        return ((AbstractContentIngestHelper) delegate).isIndexListField(field);
    }

    @Override
    public boolean isReverseIndexListField(String field) {
        validateTypeAbstractIngestHelper();
        return ((AbstractContentIngestHelper) delegate).isReverseIndexListField(field);
    }

    @Override
    public String getListDelimiter() {
        validateTypeAbstractIngestHelper();
        return ((AbstractContentIngestHelper) delegate).getListDelimiter();
    }

    @Override
    public boolean getSaveRawDataOption() {
        validateTypeAbstractIngestHelper();
        return ((AbstractContentIngestHelper) delegate).getSaveRawDataOption();
    }

    @Override
    public String getRawDocumentViewName() {
        validateTypeAbstractIngestHelper();
        return ((AbstractContentIngestHelper) delegate).getRawDocumentViewName();
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
    public Multimap<String,String> getCompositeFieldDefinitions() {
        return delegate.getCompositeFieldDefinitions();
    }

    @Override
    public Map<String,String> getCompositeFieldSeparators() {
        return delegate.getCompositeFieldSeparators();
    }

    @Override
    public boolean isVirtualIndexedField(String fieldName) {
        return delegate.isVirtualIndexedField(fieldName);
    }

    @Override
    public Map<String,String[]> getVirtualNameAndIndex(String fieldName) {
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
