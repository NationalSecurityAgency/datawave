package datawave.ingest.data.config.ingest;

import datawave.data.normalizer.NormalizationException;
import datawave.data.type.Type;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.data.config.MaskedFieldHelper;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Specialization of the Helper type that validates the configuration for Ingest purposes. These helper classes also have the logic to parse the field names and
 * fields values from the datatypes that they represent.
 */
public abstract class AbstractIngestHelper extends DataTypeHelperImpl implements IngestHelperInterface {
    private static final Logger log = Logger.getLogger(AbstractIngestHelper.class);

    protected boolean deleteMode = false;
    protected boolean replaceMalformedUTF8 = false;
    protected DataTypeHelperImpl embeddedHelper = null;

    /* Map of field names to normalizers, null key is the default normalizer */
    protected MaskedFieldHelper mfHelper = null;
    protected Set<String> shardExclusions = new HashSet<>();
    protected boolean hasIndexBlacklist = false;
    protected boolean hasReverseIndexBlacklist = false;

    public boolean getReplaceMalformedUTF8() {
        return replaceMalformedUTF8;
    }

    public boolean getDeleteMode() {
        return deleteMode;
    }

    public DataTypeHelperImpl getEmbeddedHelper() {
        return embeddedHelper;
    }

    public void setEmbeddedHelper(DataTypeHelperImpl embeddedHelper) {
        this.embeddedHelper = embeddedHelper;
    }

    /**
     * Get the normalized masked value for the provided field
     *
     * @param key a key in the {@link MaskedFieldHelper}
     * @return the normalized masked value
     */
    public String getNormalizedMaskedValue(final String key) {
        if (mfHelper != null && mfHelper.contains(key)) {
            final String value = mfHelper.get(key);
            if (value.isEmpty()) {
                return value;
            }

            final String fieldName = aliaser.normalizeAndAlias(key);
            try {
                final Set<String> normalizedValues = normalizeFieldValue(fieldName.toUpperCase(), value);
                return normalizedValues.iterator().next();
            } catch (final Exception ex) {
                log.warn(this.getType().typeName() + ": Unable to normalize masked value of '" + value + "' for " + fieldName, ex);
                return value;
            }
        }
        return null;
    }

    @Override
    public boolean hasMappings() {
        if (mfHelper == null) {
            return false;
        }
        return mfHelper.hasMappings();
    }

    @Override
    public boolean contains(final String key) {
        if (mfHelper == null) {
            return false;
        }
        return mfHelper.contains(key);
    }

    @Override
    public String get(final String key) {
        if (mfHelper == null) {
            return null;
        }
        return mfHelper.get(key);
    }

    /**
     * @return true if EmbeddedHelper is an instance of MaskedFieldHelper
     */
    public boolean isEmbeddedHelperMaskedFieldHelper() {
        return (null != mfHelper);
    }

    /**
     * @return EmbeddedHelper as a MaskedFieldHelper object
     */
    public MaskedFieldHelper getEmbeddedHelperAsMaskedFieldHelper() {
        return mfHelper;
    }

    /**
     * @deprecated use isShardExcluded(..) instead
     */
    @Deprecated
    public Set<String> getShardExclusions() {
        return shardExclusions;
    }

    @Override
    public boolean isShardExcluded(String fieldName) {
        return shardExclusions.contains(fieldName);
    }

    protected void setHasIndexDisallowlist(boolean hasIndexBlacklist) {
        this.hasIndexBlacklist = hasIndexBlacklist;
    }

    protected boolean hasIndexDisallowlist() {
        return this.hasIndexBlacklist;
    }

    protected boolean hasReverseIndexDisallowlist() {
        return this.hasReverseIndexBlacklist;
    }

    protected void setHasReverseIndexDisallowlist(boolean hasReverseIndexBlacklist) {
        this.hasReverseIndexBlacklist = hasReverseIndexBlacklist;
    }

    public void upperCaseSetEntries(Set<String> input, String warnMessage) {
        Set<String> removeList = new TreeSet<>();
        Set<String> addList = new TreeSet<>();
        for (String s : input) {
            if (!s.toUpperCase().equals(s)) {
                removeList.add(s);
                addList.add(s.toUpperCase());
                log.warn(" has a value " + s + "that was converted to uppercase.");
            }
        }
        input.removeAll(removeList);
        input.addAll(addList);
    }

    /**
     * This is a helper routine that will return a normalized field value using the configured normalizer
     *
     * @param fieldName  the field name
     * @param fieldValue the field value
     * @return the normalized field values
     * @throws NormalizationException if there is an issue with the normalization process
     */
    protected Set<String> normalizeFieldValue(final String fieldName, final String fieldValue) throws NormalizationException {
        final Collection<Type<?>> dataTypes = getDataTypes(fieldName);
        final HashSet<String> values = new HashSet<>(dataTypes.size());
        for (final datawave.data.type.Type<?> dataType : dataTypes) {
            final String normalized = dataType.normalize(fieldValue);
            values.add(normalized);
        }
        return values;
    }

}
