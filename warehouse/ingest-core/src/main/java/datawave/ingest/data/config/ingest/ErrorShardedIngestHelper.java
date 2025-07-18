package datawave.ingest.data.config.ingest;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;

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
    private IngestHelperInterface delegate = null;

    /*
     * \
     *
     * final String t = ConfigurationHelper.isNull(config, <Properties.DATA_NAME>, String.class); TypeRegistry.getInstance(config); type =
     * TypeRegistry.getType(t);
     *
     */
    private Map<Type,IndexedFields> errorIndexedFields = new HashMap<>();
    private Map<Type,IndexedFields> errorReverseIndexedFields = new HashMap<>();

    protected boolean hasErrorIndexDisallowlist = false;
    protected boolean hasErrorReverseIndexDisallowlist = false;
    /*
     * \
     *
     * citrus.data.category.index=ORANGE,LEMON
     *
     * error.apple.data.category.index=FUJI,HONEYCRISP,GRANNY_SMITH error.cherry.data.category.index=SWEET,SOUR
     *
     * we see fields for the datatypes apple, cherry, and citrus
     *
     *
     *
     * - apple fields given isIndexed/isReversedIndexed should reference error indexed fields configured for apple (Meaning it'll use
     * isErrorIndexed/isErrorReversedIndexed) - cherry fields given isIndexed/isReversedIndexed should reference error indexed fields configured for cherry -
     * citrus fields given to isIndexed/isReversedIndexed should reference super.isIndexed/super.isReversedIndexed because no datatype specific error fields
     * were specified for citrus
     *
     */

    private static class IndexedFields {
        private Set<String> indexedFields = new HashSet<>();
        private Map<String,Pattern> patterns = new HashMap<>();
        private Set<String> unindexedFields = new HashSet<>();
    }

    /*
     * SETH NOTE
     *
     * tests for the setup method
     * https://github.com/NationalSecurityAgency/datawave/pull/2864/files#diff-86d9d0c6cfcff5b686be27e01ab927c38f6c347f59faeebdedd2ccbf96834d70 1. Verify if no
     * global or datatype specific i/ri configs given, setup does not throw exception. 2. Verify if global i/ri given, setup does not throw exception. Verify
     * datatype specific is still parsed. 3. Verify if global i/ri given, but not datatype specific, setup does not throw exception. 4. Verify that if both
     * allow list and disallow list given for datatype specific, error is thrown.
     */

    @Override
    public void setup(Configuration config) {
        // we are error
        super.setup(config);

        config.set(Properties.DATA_NAME, "error");

        String configProperty = null;

        // --- ERROR INDEX_FIELDS ---

        // Process the error indexed fields in the same way as the normal index fields, but with DATATYPE_ERROR appended to the datatype.
        if (config.get(this.getType().typeName() + DATATYPE_ERROR + DISALLOWLIST_INDEX_FIELDS) != null) {
            if (log.isDebugEnabled()) {
                log.debug("Disallowlist specified for: {}", this.getType().typeName() + DATATYPE_ERROR + DISALLOWLIST_INDEX_FIELDS);
            }
            setHasErrorIndexDisallowlist(true);
            configProperty = DATATYPE_ERROR + DISALLOWLIST_INDEX_FIELDS;
        } else if (config.get(this.getType().typeName() + DATATYPE_ERROR + INDEX_FIELDS) != null) {
            log.debug("ErrorIndexedFields specified.");
            setHasErrorIndexDisallowlist(false);
            configProperty = DATATYPE_ERROR + INDEX_FIELDS;
        }

        // Load the proper list of fields to (not) index
        if (fieldConfigHelper != null && log.isInfoEnabled()) {
            log.info("Using error field config helper for {}", this.getType().typeName());
        } else if (configProperty == null && log.isWarnEnabled()) {
            log.warn("No error index fields or error disallowlist fields specified, not generating index fields for {}", this.getType().typeName());
        } else {
            this.errorIndexedFields.putIfAbsent(TypeRegistry.getType(configProperty), new IndexedFields());
            Collection<String> errorIndexedStrings = config.getStringCollection(this.getType().typeName() + DATATYPE_ERROR + configProperty); // todo: this
                                                                                                                                              // needs to be
                                                                                                                                              // updated based
                                                                                                                                              // on inclusive or
                                                                                                                                              // exclusive
                                                                                                                                              // dtErrors
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
                this.moveToPatternMap(this.errorReverseIndexedFields.get(configProperty).indexedFields,
                                this.errorReverseIndexedFields.get(configProperty).patterns);
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("{} not specified", this.getType().typeName() + DATATYPE_ERROR + configProperty);
                }
            }

        }
        //use below
        //config.getPropsWithPrefix("error.")

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

    }

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
    public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
        return null;
    }
    //
    // public boolean isDataTypeRequiredForIndexedCheck() {
    // return true;
    // }

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
        return !errorReverseIndexedFields.isEmpty();

    }

    /*
     * SETH NOTE test cases for these, 1. original isIndexedField /reverse returns unsupported, the new ones should not return unsupported. they should instead
     * make sure that the dt passed references the super indexedFields configs from property: 2. if datatype given that does not have datatype specific error
     * index/ri config, then should call super and use configuration passed to error.data.category.index, etc. <-- global error index/ri configurations 3. if
     * datatype given that has datatype specific error i/ri config, then should determine based on fields in error.<datatype>.data.category.index and related
     * properties 4. For sanity check, add test to make sure isDataTypeRequiredForIndexCheck returns true.
     */

    /**
     * Checks if the {@code fieldName} has been indexed in either the index-field map or the error-index-field map.
     *
     * @return TRUE if {@code fieldName} has been indexed, FALSE if not.
     */

    private Type activeDataType;

    public void setActiveDataType(Type dataType) {
        activeDataType = dataType;
    }

    public Type getActiveDataType() {
        return activeDataType;
    }

    @Override
    public boolean isIndexedField(String fieldName) {
        if (getActiveDataType() == null) {
            log.error("activeDataType has not been set. Call setActiveDatatype(Type) at least once before running this.");
            return false;
        }
        IndexedFields dataTypeIndexFields = errorIndexedFields.get(getActiveDataType());
        if (dataTypeIndexFields != null) {
            // Must either be explicitly indexed, or not explicitly unindexed.
            return dataTypeIndexFields.indexedFields.contains(fieldName) || !dataTypeIndexFields.unindexedFields.contains(fieldName);
        } else {
            return super.isIndexedField(fieldName);
        }
    }

    public Set<String> getIndexedFields(Type dataType) {
        return errorIndexedFields.containsKey(dataType) ? errorIndexedFields.get(dataType).indexedFields : Set.of();
    }

    public Set<String> getReverseIndexedFields(Type dataType) {
        return errorReverseIndexedFields.containsKey(dataType) ? errorReverseIndexedFields.get(dataType).indexedFields : Set.of();
    }

    /**
     * Checks if the {@code fieldName} has been indexed in either the reverse-index-field map or the error-reverse-index-field map.
     *
     * @return TRUE if {@code fieldName} has been indexed, FALSE if not.
     */

    @Override
    public boolean isReverseIndexedField(String fieldName) {
        if (getActiveDataType() == null) {
            log.error("activeDataType has not been set. Call setActiveDatatype(Type) at least once before running this.");
            return false;
        }
        IndexedFields dataTypeIndexFields = errorReverseIndexedFields.get(getActiveDataType());
        if (dataTypeIndexFields != null) {
            // Must either be explicitly indexed, or not explicitly unindexed.
            return dataTypeIndexFields.indexedFields.contains(fieldName) || !dataTypeIndexFields.unindexedFields.contains(fieldName);
        } else {
            return super.isIndexedField(fieldName);
        }
    }

    /**
     * Setter for {@code hasErrorIndexDisallowList}.
     */
    protected void setHasErrorIndexDisallowlist(boolean hasErrorIndexDisallowlist) {
        this.hasErrorIndexDisallowlist = hasErrorIndexDisallowlist;
    }

    /**
     * Setter for {@code hasErrorIndexDisallowList}.
     */
    protected void setHasErrorReverseIndexDisallowlist(boolean hasErrorReverseIndexDisallowlist) {
        this.hasErrorReverseIndexDisallowlist = hasErrorReverseIndexDisallowlist;
    }

    /**
     * Getter for {@code hasErrorIndexDisallowList}.
     */
    protected boolean hasErrorIndexDisallowlist() {
        return this.hasErrorIndexDisallowlist;
    }

    /**
     * Getter for {@code hasErrorReverseIndexDisallowList}.
     */
    protected boolean hasErrorReverseIndexDisallowlist() {
        return this.hasErrorReverseIndexDisallowlist;
    }
}
