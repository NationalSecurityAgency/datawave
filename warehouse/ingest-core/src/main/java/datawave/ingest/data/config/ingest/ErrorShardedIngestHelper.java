package datawave.ingest.data.config.ingest;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 *
 */
public class ErrorShardedIngestHelper extends BaseIngestHelper {

    private static final Logger log = LoggerFactory.getLogger(ErrorShardedIngestHelper.class);


    /**
     * <p>
     * Configuration inserted between the <datatype> and the <field-constant> for explicit error parsing.
     * </p>
     * <p>
     * For example:
     * </p>
     * <p>
     * {@code String target = <some-datatype> + DATATYPE_ERROR + INDEX_FIELDS;}
     * </p>
     */
    private static final String DATATYPE_ERROR = ".error";

    /*
     * SETH NOTE Pulled these from the 2 chunks above. Only grabbed the first var, but you may need to grab the other 2 for each. Not sure if this will be
     * necessary though.
     * UPDATE 1: Seems like I will have to add them.
     */

//    // Same usage as the variables above, but for the error variants.
//    protected Set<String> errorIndexedFields = Sets.newHashSet();
//    protected Map<String, Pattern> errorIndexedPatterns = Maps.newHashMap();
//    protected Set<String> errorUnindexedFields = Sets.newHashSet();
//
//    // Same usage as the variables above, but for the error reverse variants.
//    protected Set<String> errorReverseIndexedFields.get(configProperty) = Sets.newHashSet();
//    protected Map<String,Pattern> errorReverseIndexedPatterns = Maps.newHashMap();
//    protected Set<String> errorReverseUnindexedFields = Sets.newHashSet();

    /*\

        final String t = ConfigurationHelper.isNull(config, <Properties.DATA_NAME>, String.class);
        TypeRegistry.getInstance(config);
        type = TypeRegistry.getType(t);

     */
    private Map<Type, IndexedFields> errorIndexedFields;
    private Map<Type, IndexedFields> errorReverseIndexedFields;


    /*\

    citrus.data.category.index=ORANGE,LEMON

    error.apple.data.category.index=FUJI,HONEYCRISP,GRANNY_SMITH
    error.cherry.data.category.index=SWEET,SOUR

    we see fields for the datatypes apple, cherry, and citrus



    - apple fields given isIndexed/isReversedIndexed should reference error indexed fields configured for apple (Meaning it'll use isErrorIndexed/isErrorReversedIndexed)
    - cherry fields given isIndexed/isReversedIndexed should reference error indexed fields configured for cherry
    - citrus fields given to isIndexed/isReversedIndexed should reference super.isIndexed/super.isReversedIndexed because no datatype specific error fields were specified for citrus

     */


    private static class IndexedFields {
        private Set<String> indexedFields;
        private Map<String, Pattern> patterns;
        private Set<String> unindexedFields;
    }

    /* SETH NOTE

        tests for the setup method
        https://github.com/NationalSecurityAgency/datawave/pull/2864/files#diff-86d9d0c6cfcff5b686be27e01ab927c38f6c347f59faeebdedd2ccbf96834d70
        1. Verify if no global or datatype specific i/ri configs given, setup does not throw exception.
        2. Verify if global i/ri given, setup does not throw exception. Verify datatype specific is still parsed.
        3. Verify if global i/ri given, but not datatype specific, setup does not throw exception.
        4. Verify that if both allow list and disallow list given for datatype specific, error is thrown.
     */

    @Override
    public void setup(Configuration config) {
        // we are error
        config.set(Properties.DATA_NAME, "error");
        String configProperty = null;

        // === BEGIN SETH'S ERROR FIELDS ===

        // --- ERROR INDEX_FIELDS ---

        /*
         * SETH NOTE This is most likely the start of the chunk that needs to be cloned for the error index stuff.
         */

        // Process the error indexed fields in the same way as the normal index fields, but with DATATYPE_ERROR appended to the datatype.
        if (config.get(this.getType().typeName() + DATATYPE_ERROR + DISALLOWLIST_INDEX_FIELDS) != null) {
            if (log.isDebugEnabled()) {
                log.debug("Disallowlist specified for: {}", this.getType().typeName() + DATATYPE_ERROR + DISALLOWLIST_INDEX_FIELDS);
            }
            super.setHasErrorIndexDisallowlist(true);
            configProperty = DATATYPE_ERROR + DISALLOWLIST_INDEX_FIELDS;
        } else if (config.get(this.getType().typeName() + DATATYPE_ERROR + INDEX_FIELDS) != null) {
            log.debug("ErrorIndexedFields specified.");
            super.setHasErrorIndexDisallowlist(false);
            configProperty = DATATYPE_ERROR + INDEX_FIELDS;
        }

        // Load the proper list of fields to (not) index
        if (fieldConfigHelper != null && log.isInfoEnabled()) {
            log.info("Using error field config helper for {}", this.getType().typeName());
        } else if (configProperty == null && log.isWarnEnabled()) {
            log.warn("No error index fields or error disallowlist fields specified, not generating index fields for {}", this.getType().typeName());
        } else {
            this.errorIndexedFields.get(configProperty).indexedFields = Sets.newHashSet();
            Collection<String> errorIndexedStrings = config.getStringCollection(this.getType().typeName() + DATATYPE_ERROR + configProperty);
            if (null != errorIndexedStrings && !errorIndexedStrings.isEmpty()) {
                for (String errorIndexedString : errorIndexedStrings) {
                    this.errorIndexedFields.get(configProperty).indexedFields.add(errorIndexedString.trim());
                }
                this.moveToPatternMap(this.errorIndexedFields.get(configProperty).indexedFields, this.errorIndexedFields.get(configProperty).patterns);
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("{} not specified.", this.getType().typeName() + DATATYPE_ERROR + configProperty);
                }
            }
        }

        // --- ERROR REVERSE INDEX FIELDS ---

        /*
         * SETH NOTE This is what Laura was talking about-- the Allow/Disallow is mutually exclusive. I haven't seen this same block above for the non-reverse
         * index fields. Maybe I need to take another look.
         */

        // Ensure that we have only an allowlist or a disallowlist of fields to
        // error-reverse-index
        if (config.get(this.getType().typeName() + DATATYPE_ERROR + DISALLOWLIST_REVERSE_INDEX_FIELDS) != null
                && config.get(this.getType().typeName() + DATATYPE_ERROR + REVERSE_INDEX_FIELDS) != null) {
            throw new RuntimeException(
                    "Configuration contains Disallowlist and Allowlist for error indexed fields, it specifies both.  Type: " + this.getType().typeName()
                            + ", parameters: " + config.get(this.getType().typeName() + DATATYPE_ERROR + DISALLOWLIST_REVERSE_INDEX_FIELDS)
                            + "  " + config.get(this.getType().typeName() + DATATYPE_ERROR + REVERSE_INDEX_FIELDS));
        }

        configProperty = null;

        // Process the error reverse index fields
        if (config.get(this.getType().typeName() + DATATYPE_ERROR + DISALLOWLIST_REVERSE_INDEX_FIELDS) != null) {
            if (log.isDebugEnabled()) {
                log.debug("Disallowlist specified for: {}", this.getType().typeName() + DATATYPE_ERROR + DISALLOWLIST_REVERSE_INDEX_FIELDS);
            }

            this.setHasErrorReverseIndexDisallowlist(true);

            configProperty = DATATYPE_ERROR + DISALLOWLIST_REVERSE_INDEX_FIELDS;
        } else if (config.get(this.getType().typeName() + DATATYPE_ERROR + REVERSE_INDEX_FIELDS) != null) {
            if (log.isDebugEnabled()) {
                log.debug("Reverse Index specified for: {}", this.getType().typeName() + DATATYPE_ERROR + REVERSE_INDEX_FIELDS);
            }
            this.setHasErrorReverseIndexDisallowlist(false);
            configProperty = DATATYPE_ERROR + REVERSE_INDEX_FIELDS;
        }

        // Load the proper list of fields to (not) error-reverse-index
        if (configProperty == null && log.isWarnEnabled()) {
            log.warn("No error reverse index fields or error disallowlist reverse index fields specified, not generating reverse index fields for {}",
                    this.getType().typeName());
        } else {
            errorReverseIndexedFields.get(configProperty).indexedFields = Sets.newHashSet();
            Collection<String> errorReverseIndexedStrings = config.getStringCollection(this.getType().typeName() + DATATYPE_ERROR + configProperty);
            if (null != errorReverseIndexedStrings && !errorReverseIndexedStrings.isEmpty()) {
                for (String errorReverseIndexedString : errorReverseIndexedStrings) {
                    errorReverseIndexedFields.get(configProperty).indexedFields.add(errorReverseIndexedString.trim());
                }
                this.moveToPatternMap(this.errorReverseIndexedFields.get(configProperty).indexedFields, this.errorReverseIndexedFields.get(configProperty).patterns);
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("{} not specified", this.getType().typeName() + DATATYPE_ERROR + configProperty);
                }
            }

        }
        for (Type type : TypeRegistry.getTypes()) {
            Collection<String> indexedStrings = config.getStringCollection(type.typeName() + DATATYPE_ERROR + INDEX_FIELDS);
            if (null != indexedStrings && !indexedStrings.isEmpty()) {
                for (String indexedString : indexedStrings) {
                    String indexedTrimmedString = indexedString.trim();
                    allIndexFields.add(indexedTrimmedString);
                }
            }
            Collection<String> reverseIndexedStrings = config.getStringCollection(type.typeName() + DATATYPE_ERROR + REVERSE_INDEX_FIELDS);
            if (null != reverseIndexedStrings && !reverseIndexedStrings.isEmpty()) {
                for (String reverseIndexedString : reverseIndexedStrings) {
                    String reverseIndexedTrimmedString = reverseIndexedString.trim();
                    allReverseIndexFields.add(reverseIndexedTrimmedString);
                }
            }
        }
        super.setup(config);
    }

    private IngestHelperInterface delegate = null;

    public void setDelegateHelper(IngestHelperInterface delegate) {
        this.delegate = delegate;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.AbstractIngestHelper#getEventFields(datawave.ingest.data.Event)
     */
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        // we need to do this safely, make our best attempt to get some fields
        try {
            return delegate.getEventFields(event);
        } catch (Exception e) {
            return HashMultimap.create();
        }
    }

    /**
     * Override to provide access to the data type handler
     */
    @Override
    public Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> fields) {
        return super.normalizeMap(fields);
    }

    @Override
    public Multimap<String, NormalizedContentInterface> normalize(Multimap<String, String> fields) {
        return null;
    }

    @Override
    public boolean isDataTypeRequiredForIndexedCheck() {
        return true;
    }

    /**
     * Checks if error-index-fields have been initialized yet.
     *
     * @return FALSE if errorIndexedFields is empty, TRUE if it's not.
     */
    private boolean hasErrorIndexConfig() {
        return !errorIndexedFields.isEmpty();
    }

    /**
     * Checks if error-reverse-index-fields have been initialized yet.
     *
     * @return FALSE if errorReverseIndexedFields.get(configProperty) is empty, TRUE if it's not.
     */
    private boolean hasErrorReverseIndexConfig() {
        return !errorReverseIndexedFields.get(configProperty).isEmpty();

    }

    /* SETH NOTE
    test cases for these,
    1. original isIndexedField /reverse returns unsupported,
    the new ones should not return unsupported. they should instead make sure that the dt passed references the super indexedFields configs from property:
     2. if datatype given that does not have datatype specific error index/ri config, then should call super and use configuration passed to error.data.category.index, etc. <-- global error index/ri configurations
     3. if datatype given that has datatype specific error i/ri config, then should determine based on fields in error.<datatype>.data.category.index and related properties
     4. For sanity check, add test to make sure isDataTypeRequiredForIndexCheck returns true.
     */

    /**
     * Checks if the {@code fieldName} has been indexed in either the index-field map or the error-index-field map.
     *
     * @return TRUE if {@code fieldName} has been indexed, FALSE if not.
     */
    @Override
    public boolean isIndexedField(String fieldName) {
        throw new UnsupportedOperationException("Use isIndexedFields(Type dataType, String field) instead");
    }


    public boolean isIndexedField(Type dataType, String fieldName) {
        IndexedFields indexedFields = errorIndexedFields.get(dataType);
        if (indexedFields != null) {
            // Determine if indexed based on IndexedFields.
        } else {
            return super.isIndexedField(fieldName);
        }
    }

    public Set<String> getIndexedFields(Type dataType) {
        return errorIndexedFields.containsKey(dataType) ? errorIndexedFields.get(dataType).indexedFields : Set.of();
    }


    /**
     * Checks if the {@code fieldName} has been indexed in either the reverse-index-field map or the error-reverse-index-field map.
     *
     * @return TRUE if {@code fieldName} has been indexed, FALSE if not.
     */
    @Override
    public boolean isReverseIndexedField(String fieldName) {
        if (hasErrorReverseIndexConfig()) {
            return isErrorReverseIndexedField(fieldName);
        } else {
            return super.isIndexedField(fieldName);
        }
    }


    /**
     * Checks if the fieldName is an error-reverse-index field. Fields are marked in the {@code setup()} method.
     *
     * @param fieldName
     *            the fieldName to test
     * @return TRUE if the fieldName provided has been identified as an error-reverse-index field, FALSE if not.
     */
    public boolean isErrorReverseIndexedField(String fieldName) {
        if (fieldConfigHelper != null) {
            return fieldConfigHelper.isErrorReverseIndexedField(fieldName);
        }
        return this.hasErrorReverseIndexDisallowlist() ? !isErrorReverseIndexed(fieldName) : isErrorReverseIndexed(fieldName);
    }

    /**
     * Helper method for {@link this.isErrorReverseIndexedField()}.
     *
     * @param fieldName
     *            the fieldName to test
     * @return TRUE if the fieldName provided has been identified as an error-reverse-index field, FALSE if not.
     */
    private boolean isErrorReverseIndexed(String fieldName) {
        if (fieldConfigHelper != null && fieldConfigHelper.isErrorReverseIndexedField(fieldName)) {
            return true;
        } else if (this.errorReverseIndexedFields.contains(fieldName)) {
            return true;
        } else if (this.errorReverseUnindexedFields.contains(fieldName)) {
            return false;
        } else if (this.errorReverseIndexedPatterns.isEmpty()) { // avoids filling errorReverseUnindexedFields if not necessary
            return false;
        } else {
            for (Pattern pattern : this.errorReverseIndexedPatterns.values()) {
                if (pattern.matcher(fieldName).matches()) {
                    this.errorReverseIndexedFields.get(configProperty).add(fieldName); // update so we don't need to match the next time we see it
                    return true;
                }
            }
            this.errorReverseUnindexedFields.add(fieldName);
            return false;
        }
    }

    /**
     * Checks if the fieldName is an error-index field. Fields are marked in the {@code setup()} method.
     *
     * @param fieldName
     *            the fieldName to test
     * @return TRUE if the fieldName provided has been identified as an error-index field, FALSE if not.
     */
    public boolean isErrorIndexedField(String fieldName) {
        if (fieldConfigHelper != null) {
            return fieldConfigHelper.isErrorIndexedField(fieldName);
        }
        return this.hasErrorIndexDisallowlist() ? !isErrorIndexed(fieldName) : isErrorIndexed(fieldName);
    }

    /**
     * Helper method for {@link this.isErrorIndexedField()}.
     *
     * @param fieldName
     *            the fieldName to test
     * @return TRUE if the fieldName provided has been identified as an error-index field, FALSE if not.
     */
    private boolean isErrorIndexed(String fieldName) {
        if (fieldConfigHelper != null && fieldConfigHelper.isErrorIndexedField(fieldName)) {
            return true;
        } else if (this.errorIndexedFields.contains(fieldName)) {
            return true;
        } else if (this.errorUnindexedFields.contains(fieldName)) {
            return false;
        } else if (this.errorIndexedPatterns.isEmpty()) { // avoids filling unindexedFields if not necessary
            return false;
        } else {
            for (Pattern pattern : this.errorIndexedPatterns.values()) {
                if (pattern.matcher(fieldName).matches()) {
                    this.errorIndexedFields.add(fieldName); // update so we don't need to match the next time we see it
                    return true;
                }
            }
            this.errorUnindexedFields.add(fieldName);
            return false;
        }
    }

}
