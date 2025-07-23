package datawave.ingest.data.config.ingest;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    private static final String ERROR = "error";
    private IngestHelperInterface delegate = null;

    private Map<Type,IndexedFields> errorIndexedFields = new HashMap<>();
    private Map<Type,IndexedFields> errorReverseIndexedFields = new HashMap<>();

    protected boolean hasErrorIndexDisallowlist = false;
    protected boolean hasErrorReverseIndexDisallowlist = false;

    private Type activeDataType;

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

        super.setup(config);

        // we are error
        config.set(Properties.DATA_NAME, "error");

        // get all config properties that start with "error."
        Map<String, String> errorProps = config.getPropsWithPrefix(ERROR);

        // handle the index methods specified in the configuration properties for each datatype
        for (var entry : errorProps.entrySet()){

            String propertyKey = entry.getKey();

            // get the property's datatype
            // "error.<datatype>.<etc>..."
            List<String> keyParts = List.of(propertyKey.split("\\."));
            int datatypeIndex = keyParts.indexOf(ERROR) + 1;

            String dataTypeString = keyParts.get(datatypeIndex);
            String errorDataTypeString = ERROR + "." + dataTypeString;

            activeDataType = TypeRegistry.getType(dataTypeString);

            // --- PROBLEM CASES ---
            // these should never run if everything is working as intended

            // case: the datatype found in the configuration was not found in the TypeRegistry (defaulting to null)
            if(activeDataType == null) {
                throw new RuntimeException(
                        "Error Datatype found in configuration does not exist in TypeRegistry." +
                                " Type: " + dataTypeString
                );
            }

            // case: contains both allow and disallow for error index fields
            if(errorProps.containsKey(errorDataTypeString + INDEX_FIELDS) && errorProps.containsKey(errorDataTypeString + DISALLOWLIST_INDEX_FIELDS)){
                throw new RuntimeException(
                        "Configuration contains Disallowlist and Allowlist for error indexed fields, it specifies both." +
                                " Type: " + dataTypeString + ", " +
                                " Parameters: " + config.get(errorDataTypeString + INDEX_FIELDS) + " | " + config.get(errorDataTypeString + DISALLOWLIST_INDEX_FIELDS)
                );
            }

            // case: contains both allow and disallow for error reverse index fields
            if(errorProps.containsKey(errorDataTypeString + REVERSE_INDEX_FIELDS) && errorProps.containsKey(errorDataTypeString + DISALLOWLIST_REVERSE_INDEX_FIELDS)){
                throw new RuntimeException(
                        "Configuration contains Disallowlist and Allowlist for error reverse indexed fields, it specifies both." +
                                " Type: " + dataTypeString + ", " +
                                " Parameters: " + config.get(errorDataTypeString + REVERSE_INDEX_FIELDS) + " | " + config.get(errorDataTypeString + DISALLOWLIST_REVERSE_INDEX_FIELDS)
                );
            }

            // check if the property relates to index fields
            // if they do, they will always end with either INDEX_FIELDS (inclusive) or DISALLOWLIST_INDEX_FIELDS (exclusive)
            // "error.<datatype>.<possible-index-information>"
            if(propertyKey.endsWith(INDEX_FIELDS)){

                handleIndexFields(config.getStringCollection(errorDataTypeString + INDEX_FIELDS));

            } else if (propertyKey.endsWith(DISALLOWLIST_INDEX_FIELDS)) {

                handleDisallowListIndexFields(config.getStringCollection(errorDataTypeString + DISALLOWLIST_INDEX_FIELDS));

            } else{
                log.warn("No error index fields or error disallowlist fields specified, not generating index fields for {}", dataTypeString);
            }

            // same thing, but for reverse
            if (propertyKey.endsWith(REVERSE_INDEX_FIELDS)) {

                handleReverseIndexFields(config.getStringCollection(errorDataTypeString + REVERSE_INDEX_FIELDS));


            } else if (propertyKey.endsWith(DISALLOWLIST_REVERSE_INDEX_FIELDS)) {

                handleDisallowListReverseIndexFields(config.getStringCollection(errorDataTypeString + DISALLOWLIST_REVERSE_INDEX_FIELDS));

            } else {
                log.warn("No error reverse index fields or error disallowlist reverse index fields specified, not generating reverse index fields for {}", dataTypeString);
            }

        }

        // add the trimmed indexed fields to the main index field lists.
        for (Type type : TypeRegistry.getTypes()) {
            Collection<String> indexedStrings = config.getStringCollection(ERROR + "." + type.typeName() + INDEX_FIELDS);
            if (null != indexedStrings && !indexedStrings.isEmpty()) {
                for (String indexedString : indexedStrings) {
                    String indexedTrimmedString = indexedString.trim();
                    allIndexFields.add(indexedTrimmedString);
                }
            }
            Collection<String> reverseIndexedStrings = config.getStringCollection(ERROR + "." + type.typeName() + REVERSE_INDEX_FIELDS);
            if (null != reverseIndexedStrings && !reverseIndexedStrings.isEmpty()) {
                for (String reverseIndexedString : reverseIndexedStrings) {
                    String reverseIndexedTrimmedString = reverseIndexedString.trim();
                    allReverseIndexFields.add(reverseIndexedTrimmedString);
                }
            }
        }

    }

    private void handleIndexFields(Collection<String> errorIndexedStrings){

        // if we're using fieldConfigHelper, we don't need to do anything here.
        // SETH NOTE: Not sure if this needs to be in all of them or not. question to ask!
        if (fieldConfigHelper != null && log.isInfoEnabled()) {
            log.info("Using error field config helper for {}", activeDataType);
            return;
        }

        log.debug("ErrorIndexedFields specified.");
        setHasErrorIndexDisallowlist(false);

        // create an instance of the ErrorIndexFields for this datatype
        this.errorIndexedFields.putIfAbsent(activeDataType, new IndexedFields());

        // if something wonky happened with the errorIndexedStrings, drop a warning.
        if(errorIndexedStrings == null || errorIndexedStrings.isEmpty()){
            log.warn("{} not specified.", ERROR + "." + activeDataType + "." + INDEX_FIELDS);
            return;
        }

        // add the indexed fields to this datatype's ErrorIndexFields instance
        for (String entry : errorIndexedStrings) {
            this.errorIndexedFields.get(activeDataType).indexedFields.add(entry.trim());
        }

        // move em to the pattern map!!
        this.moveToPatternMap(this.errorIndexedFields.get(activeDataType).indexedFields, this.errorIndexedFields.get(activeDataType).patterns);

    }

    private void handleDisallowListIndexFields(Collection<String> errorUnindexedStrings){

        log.debug("ErrorDisallowListIndexedFields specified.");
        setHasErrorIndexDisallowlist(true);

        // create an instance of the ErrorIndexFields for this datatype
        this.errorIndexedFields.putIfAbsent(activeDataType, new IndexedFields());

        // if something wonky happened with the errorDisallowIndexedStrings, drop a warning.
        if(errorUnindexedStrings == null || errorUnindexedStrings.isEmpty()){
            log.warn("{} not specified.", ERROR + "." + activeDataType + "." + DISALLOWLIST_INDEX_FIELDS);
            return;
        }

        // add the indexed fields to this datatype's ErrorIndexFields instance
        for (String entry : errorUnindexedStrings) {
            this.errorIndexedFields.get(activeDataType).unindexedFields.add(entry.trim());
        }

        // move em to the pattern map!!
        this.moveToPatternMap(this.errorIndexedFields.get(activeDataType).unindexedFields, this.errorIndexedFields.get(activeDataType).patterns);

    }

    private void handleReverseIndexFields(Collection<String> errorReverseIndexedStrings){

        log.debug("ErrorReverseIndexedFields specified.");
        setHasErrorReverseIndexDisallowlist(false);

        // create an instance of the ErrorIndexFields for this datatype
        this.errorReverseIndexedFields.putIfAbsent(activeDataType, new IndexedFields());

        // if something wonky happened with the errorIndexedStrings, drop a warning.
        if(errorReverseIndexedStrings == null || errorReverseIndexedStrings.isEmpty()){
            log.warn("{} not specified.", ERROR + "." + activeDataType + "." + REVERSE_INDEX_FIELDS);
            return;
        }

        // add the indexed fields to this datatype's ErrorReverseIndexFields instance
        for (String entry : errorReverseIndexedStrings) {
            this.errorReverseIndexedFields.get(activeDataType).indexedFields.add(entry.trim());
        }

        // move em to the pattern map!!
        this.moveToPatternMap(this.errorReverseIndexedFields.get(activeDataType).indexedFields, this.errorReverseIndexedFields.get(activeDataType).patterns);

    }

    private void handleDisallowListReverseIndexFields(Collection<String> errorReverseUnindexedStrings){

        log.debug("ErrorDisallowListReverseIndexedFields specified.");
        setHasErrorReverseIndexDisallowlist(true);

        // create an instance of the ErrorIndexFields for this datatype
        this.errorReverseIndexedFields.putIfAbsent(activeDataType, new IndexedFields());

        // if something wonky happened with the errorDisallowIndexedStrings, drop a warning.
        if(errorReverseUnindexedStrings == null || errorReverseUnindexedStrings.isEmpty()){
            log.warn("{} not specified.", ERROR + "." + activeDataType + "." + DISALLOWLIST_REVERSE_INDEX_FIELDS);
            return;
        }

        // add the indexed fields to this datatype's ErrorIndexFields instance
        for (String entry : errorReverseUnindexedStrings) {
            this.errorReverseIndexedFields.get(activeDataType).unindexedFields.add(entry.trim());
        }

        // move em to the pattern map!!
        this.moveToPatternMap(this.errorReverseIndexedFields.get(activeDataType).unindexedFields, this.errorReverseIndexedFields.get(activeDataType).patterns);

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

    public void setActiveDataType(Type dataType) {
        activeDataType = dataType;
    }

    public Type getActiveDataType() {
        return activeDataType;
    }
}
