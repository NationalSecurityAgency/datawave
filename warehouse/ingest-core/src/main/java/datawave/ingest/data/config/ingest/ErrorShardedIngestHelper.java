package datawave.ingest.data.config.ingest;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

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

    private Type activeDataType;

    private static class IndexedFields {
        private Set<String> indexedFields = new HashSet<>();
        private Map<String,Pattern> patterns = new HashMap<>();
        private Set<String> unindexedFields = new HashSet<>();
        public boolean hasDisallowList = false;
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
        config.set(Properties.DATA_NAME, "error");

        Map<Type,Map<String,String>> dataTypeSpecificProperties = getDataTypeSpecificProperties(config);

        // if there were no datatype specific properties found
        if(dataTypeSpecificProperties.isEmpty()) {
            throw new RuntimeException(
                    "No error data types found."
            );
        }

        Collection<Type> typesAbsentFromTypeRegistry = dataTypeSpecificProperties
                .keySet()
                .stream()
                .filter(t -> TypeRegistry.getTypes().contains(t))
                .collect(Collectors.toList());

        // if any datatypes found in the configuration are not found in the TypeRegistry
        if(!typesAbsentFromTypeRegistry.isEmpty()) {
            throw new RuntimeException(
                    "Error Datatype found in configuration does not exist in TypeRegistry." +
                            " Types: " + typesAbsentFromTypeRegistry
            );
        }

        // each datatype (Type Key) will have a map (Map Value) of post-datatype-suffixes (String Key) and their associated values (String Value).
        for (Map.Entry<Type,Map<String,String>> dataTypeEntry : dataTypeSpecificProperties.entrySet()) {

            Type type = dataTypeEntry.getKey();
            Map<String,String> properties = dataTypeEntry.getValue();

            if(properties.containsKey(INDEX_FIELDS) && properties.containsKey(DISALLOWLIST_INDEX_FIELDS)) {
                throw new RuntimeException(
                        "Configuration contains Disallowlist and Allowlist for error indexed fields, it specifies both." +
                                " Type: " + type + ", " +
                                " Parameters: " + config.get(ERROR + "." + type + INDEX_FIELDS) + " | " + config.get(ERROR + "." + type  + DISALLOWLIST_INDEX_FIELDS)
                );
            }

            if(properties.containsKey(REVERSE_INDEX_FIELDS) && properties.containsKey(DISALLOWLIST_REVERSE_INDEX_FIELDS)) {
                throw new RuntimeException(
                        "Configuration contains Disallowlist and Allowlist for error reverse indexed fields, it specifies both." +
                                " Type: " + type + ", " +
                                " Parameters: " + config.get(ERROR + "." + type + REVERSE_INDEX_FIELDS) + " | " + config.get(ERROR + "." + type + DISALLOWLIST_REVERSE_INDEX_FIELDS)
                );
            }

            // --- INDEX HANDLING ---

            // check each property for those related to index fields
            // they will always have either INDEX_FIELDS (inclusive) or DISALLOWLIST_INDEX_FIELDS (exclusive), never entries for both.
            // "error.<datatype>.<possible-index-information>"
            properties.forEach((propKey, propVal) -> {

                if(propKey.equals(INDEX_FIELDS)){

                    handleIndexFields(config.getStringCollection(ERROR + "." + type + INDEX_FIELDS));

                } else if (propKey.equals(DISALLOWLIST_INDEX_FIELDS)) {

                    handleDisallowListIndexFields(config.getStringCollection(ERROR + "." + type  + DISALLOWLIST_INDEX_FIELDS));

                } else{
                    log.warn("No error index fields or error disallowlist fields specified, not generating index fields for {}", type );
                }

                // same thing, but for reverse
                if (propKey.equals(REVERSE_INDEX_FIELDS)) {

                    handleReverseIndexFields(config.getStringCollection(ERROR + "." + type  + REVERSE_INDEX_FIELDS));


                } else if (propKey.equals(DISALLOWLIST_REVERSE_INDEX_FIELDS)) {

                    handleDisallowListReverseIndexFields(config.getStringCollection(ERROR + "." + type  + DISALLOWLIST_REVERSE_INDEX_FIELDS));

                } else {
                    log.warn("No error reverse index fields or error disallowlist reverse index fields specified, not generating reverse index fields for {}", type);
                }

            });

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

    private Map<Type, Map<String, String>> getDataTypeSpecificProperties(Configuration config) {
        Map<Type,Map<String,String>> dataTypeSpecificProperties = new HashMap<>();
        Map<String,String> errorProperties = config.getPropsWithPrefix((ERROR + "."));
        for (Map.Entry<String,String> entry : errorProperties.entrySet()) {
            String property = entry.getKey();
            // Check if the property is one that allows for configuring index/reverse index fields.
            if(isIndexProperty(property)) {
                // If the third segment of the property name is 'data', then the second segment contains a datatype.
                String[] parts = property.split("\\.");
                if(parts[2].equals("data")) {
                    Type dataType = TypeRegistry.getType(parts[1]);
                    // Get the property suffix without the 'error.<datatype>' portion.
                    String propertySuffix = getPropertySuffix(property);
                    dataTypeSpecificProperties.computeIfAbsent(dataType, k -> new HashMap<>()).put(propertySuffix, entry.getValue());
                }
            }
        }
        return dataTypeSpecificProperties;
    }

    private boolean isIndexProperty(String property) {
        return property.endsWith(INDEX_FIELDS) || property.endsWith(DISALLOWLIST_INDEX_FIELDS) || property.endsWith(REVERSE_INDEX_FIELDS) || property.endsWith(DISALLOWLIST_REVERSE_INDEX_FIELDS);
    }

    private String getPropertySuffix(String property) {
        if(property.endsWith(INDEX_FIELDS)) {
            return INDEX_FIELDS;
        } else if(property.endsWith(DISALLOWLIST_INDEX_FIELDS)) {
            return DISALLOWLIST_INDEX_FIELDS;
        } else if(property.endsWith(REVERSE_INDEX_FIELDS)) {
            return REVERSE_INDEX_FIELDS;
        } else if(property.endsWith(DISALLOWLIST_REVERSE_INDEX_FIELDS)) {
            return DISALLOWLIST_REVERSE_INDEX_FIELDS;
        } else {
            throw new IllegalArgumentException("Unhandled property: " + property);
        }
    }

    private void handleIndexFields(Collection<String> errorIndexedStrings){

        // if we're using fieldConfigHelper, we don't need to do anything here.
        // SETH NOTE: Not sure if this needs to be in all of them or not. question to ask!
        if (fieldConfigHelper != null && log.isInfoEnabled()) {
            log.info("Using error field config helper for {}", getActiveDataType());
            return;
        }

        log.debug("ErrorIndexedFields specified.");

        // create an instance of the ErrorIndexFields for this datatype
        this.errorIndexedFields.putIfAbsent(getActiveDataType(), new IndexedFields());
        this.errorIndexedFields.get(getActiveDataType()).hasDisallowList = false;

        // if something wonky happened with the errorIndexedStrings, drop a warning.
        if(errorIndexedStrings == null || errorIndexedStrings.isEmpty()){
            log.warn("{} not specified.", ERROR + "." + getActiveDataType() + "." + INDEX_FIELDS);
            return;
        }

        // add the indexed fields to this datatype's ErrorIndexFields instance
        for (String entry : errorIndexedStrings) {
            this.errorIndexedFields.get(getActiveDataType()).indexedFields.add(entry.trim());
        }

        // move em to the pattern map!!
        this.moveToPatternMap(this.errorIndexedFields.get(getActiveDataType()).indexedFields, this.errorIndexedFields.get(getActiveDataType()).patterns);

    }

    private void handleDisallowListIndexFields(Collection<String> errorUnindexedStrings){

        log.debug("ErrorDisallowListIndexedFields specified.");

        // create an instance of the ErrorIndexFields for this datatype
        this.errorIndexedFields.putIfAbsent(getActiveDataType(), new IndexedFields());
        this.errorIndexedFields.get(getActiveDataType()).hasDisallowList = true;


        // if something wonky happened with the errorDisallowIndexedStrings, drop a warning.
        if(errorUnindexedStrings == null || errorUnindexedStrings.isEmpty()){
            log.warn("{} not specified.", ERROR + "." + getActiveDataType() + "." + DISALLOWLIST_INDEX_FIELDS);
            return;
        }

        // add the indexed fields to this datatype's ErrorIndexFields instance
        for (String entry : errorUnindexedStrings) {
            this.errorIndexedFields.get(getActiveDataType()).unindexedFields.add(entry.trim());
        }

        // move em to the pattern map!!
        this.moveToPatternMap(this.errorIndexedFields.get(getActiveDataType()).unindexedFields, this.errorIndexedFields.get(getActiveDataType()).patterns);

    }

    private void handleReverseIndexFields(Collection<String> errorReverseIndexedStrings){

        log.debug("ErrorReverseIndexedFields specified.");

        // create an instance of the ErrorIndexFields for this datatype
        this.errorIndexedFields
                .computeIfAbsent(getActiveDataType(), (k) -> new IndexedFields())
                .hasDisallowList = false;

        // if something wonky happened with the errorIndexedStrings, drop a warning.
        if(errorReverseIndexedStrings == null || errorReverseIndexedStrings.isEmpty()){
            log.warn("{} not specified.", ERROR + "." + getActiveDataType() + "." + REVERSE_INDEX_FIELDS);
            return;
        }

        // add the indexed fields to this datatype's ErrorReverseIndexFields instance
        for (String entry : errorReverseIndexedStrings) {
            this.errorReverseIndexedFields.get(getActiveDataType()).indexedFields.add(entry.trim());
        }

        // move em to the pattern map!!
        this.moveToPatternMap(this.errorReverseIndexedFields.get(getActiveDataType()).indexedFields, this.errorReverseIndexedFields.get(getActiveDataType()).patterns);

    }

    private void handleDisallowListReverseIndexFields(Collection<String> errorReverseUnindexedStrings){

        log.debug("ErrorDisallowListReverseIndexedFields specified.");

        // create an instance of the ErrorIndexFields for this datatype
        this.errorReverseIndexedFields.putIfAbsent(getActiveDataType(), new IndexedFields());
        this.errorReverseIndexedFields.get(getActiveDataType()).hasDisallowList = true;


        // if something wonky happened with the errorDisallowIndexedStrings, drop a warning.
        if(errorReverseUnindexedStrings == null || errorReverseUnindexedStrings.isEmpty()){
            log.warn("{} not specified.", ERROR + "." + getActiveDataType() + "." + DISALLOWLIST_REVERSE_INDEX_FIELDS);
            return;
        }

        // add the indexed fields to this datatype's ErrorIndexFields instance
        for (String entry : errorReverseUnindexedStrings) {
            this.errorReverseIndexedFields.get(getActiveDataType()).unindexedFields.add(entry.trim());
        }

        // move em to the pattern map!!
        this.moveToPatternMap(this.errorReverseIndexedFields.get(getActiveDataType()).unindexedFields, this.errorReverseIndexedFields.get(getActiveDataType()).patterns);

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

    public void setActiveDataType(Type dataType) {
        activeDataType = dataType;
    }

    public Type getActiveDataType() {
        return activeDataType;
    }
}
