package datawave.ingest.data.config.ingest;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
     * Configuration prefix for per-datatype error index properties. Properties follow the pattern: {@code error.<datatype>.data.name.<suffix>}
     */
    private static final String ERROR = "error";

    private IngestHelperInterface delegate = null;

    private final Map<Type,IndexedFields> errorIndexedFields = new HashMap<>();
    private final Map<Type,IndexedFields> errorReverseIndexedFields = new HashMap<>();

    private Type activeDataType;

    private static class IndexedFields {
        private final Set<String> indexedFields = new HashSet<>();
        private final Map<String,Pattern> patterns = new HashMap<>();
        private final Set<String> unindexedFields = new HashSet<>();
        private boolean hasDisallowList = false;

        void addAllowlistFields(Collection<String> fields) {
            hasDisallowList = false;
            if (fields == null || fields.isEmpty()) {
                return;
            }
            for (String entry : fields) {
                indexedFields.add(entry.trim());
            }
        }

        void addDisallowlistFields(Collection<String> fields) {
            hasDisallowList = true;
            if (fields == null || fields.isEmpty()) {
                return;
            }
            for (String entry : fields) {
                unindexedFields.add(entry.trim());
            }
        }

        boolean matches(String fieldName) {
            return hasDisallowList ? !unindexedFields.contains(fieldName) : indexedFields.contains(fieldName);
        }
    }

    @Override
    public void setup(Configuration config) {

        config.set(Properties.DATA_NAME, "error");
        super.setup(config);

        Map<Type,Map<String,String>> dataTypeSpecificProperties = getDataTypeSpecificProperties(config);

        if (dataTypeSpecificProperties.isEmpty()) {
            log.warn("No per-datatype error index configurations found. Using base error index fields only.");
            return;
        }

        Collection<Type> typesAbsentFromTypeRegistry = dataTypeSpecificProperties.keySet().stream().filter(t -> !TypeRegistry.getTypes().contains(t))
                        .collect(Collectors.toList());

        if (!typesAbsentFromTypeRegistry.isEmpty()) {
            throw new RuntimeException("Error Datatype found in configuration does not exist in TypeRegistry." + " Types: " + typesAbsentFromTypeRegistry);
        }

        for (Map.Entry<Type,Map<String,String>> dataTypeEntry : dataTypeSpecificProperties.entrySet()) {

            Type type = dataTypeEntry.getKey();
            Map<String,String> properties = dataTypeEntry.getValue();
            String prefix = ERROR + "." + type.typeName();

            validateNoConflictingLists(type, properties, config, prefix);

            processFieldConfig(type, properties, config, prefix, INDEX_FIELDS, DISALLOWLIST_INDEX_FIELDS, errorIndexedFields);
            processFieldConfig(type, properties, config, prefix, REVERSE_INDEX_FIELDS, DISALLOWLIST_REVERSE_INDEX_FIELDS, errorReverseIndexedFields);
        }

        // add the trimmed indexed fields to the main index field lists
        for (Type type : TypeRegistry.getTypes()) {
            Collection<String> indexedStrings = config.getStringCollection(ERROR + "." + type.typeName() + INDEX_FIELDS);
            if (null != indexedStrings && !indexedStrings.isEmpty()) {
                for (String indexedString : indexedStrings) {
                    allIndexFields.add(indexedString.trim());
                }
            }
            Collection<String> reverseIndexedStrings = config.getStringCollection(ERROR + "." + type.typeName() + REVERSE_INDEX_FIELDS);
            if (null != reverseIndexedStrings && !reverseIndexedStrings.isEmpty()) {
                for (String reverseIndexedString : reverseIndexedStrings) {
                    allReverseIndexFields.add(reverseIndexedString.trim());
                }
            }
        }
    }

    private void validateNoConflictingLists(Type type, Map<String,String> properties, Configuration config, String prefix) {
        if (properties.containsKey(INDEX_FIELDS) && properties.containsKey(DISALLOWLIST_INDEX_FIELDS)) {
            throw new RuntimeException("Configuration contains Disallowlist and Allowlist for error indexed fields, it specifies both." + " Type: "
                            + type.typeName() + ", " + " Parameters: " + config.get(prefix + INDEX_FIELDS) + " | "
                            + config.get(prefix + DISALLOWLIST_INDEX_FIELDS));
        }

        if (properties.containsKey(REVERSE_INDEX_FIELDS) && properties.containsKey(DISALLOWLIST_REVERSE_INDEX_FIELDS)) {
            throw new RuntimeException("Configuration contains Disallowlist and Allowlist for error reverse indexed fields, it specifies both." + " Type: "
                            + type.typeName() + ", " + " Parameters: " + config.get(prefix + REVERSE_INDEX_FIELDS) + " | "
                            + config.get(prefix + DISALLOWLIST_REVERSE_INDEX_FIELDS));
        }
    }

    private void processFieldConfig(Type type, Map<String,String> properties, Configuration config, String prefix, String allowlistSuffix,
                    String disallowlistSuffix, Map<Type,IndexedFields> targetMap) {

        if (properties.containsKey(allowlistSuffix)) {
            if (fieldConfigHelper != null && log.isInfoEnabled()) {
                log.info("Using error field config helper for {}", type.typeName());
                return;
            }
            IndexedFields fields = targetMap.computeIfAbsent(type, k -> new IndexedFields());
            fields.addAllowlistFields(config.getStringCollection(prefix + allowlistSuffix));
            this.moveToPatternMap(fields.indexedFields, fields.patterns);
        } else if (properties.containsKey(disallowlistSuffix)) {
            IndexedFields fields = targetMap.computeIfAbsent(type, k -> new IndexedFields());
            fields.addDisallowlistFields(config.getStringCollection(prefix + disallowlistSuffix));
            this.moveToPatternMap(fields.unindexedFields, fields.patterns);
        }
    }

    private Map<Type,Map<String,String>> getDataTypeSpecificProperties(Configuration config) {
        Map<Type,Map<String,String>> dataTypeSpecificProperties = new HashMap<>();

        // strips the "error." from the props
        Map<String,String> errorProperties = config.getPropsWithPrefix((ERROR + "."));
        for (Map.Entry<String,String> entry : errorProperties.entrySet()) {
            String property = entry.getKey();
            if (isIndexProperty(property)) {
                // Property format after stripping "error." is: <datatype>.data.name.<suffix>
                // The datatype is everything before the first ".data.name." occurrence
                String propertySuffix = getPropertySuffix(property);
                if (propertySuffix == null) {
                    continue;
                }
                // Strip the suffix to get the datatype portion
                String datatypePortion = property.substring(0, property.length() - propertySuffix.length());
                if (datatypePortion.isEmpty() || datatypePortion.endsWith(".")) {
                    continue;
                }
                // Remove trailing dot if present
                if (datatypePortion.endsWith(".")) {
                    datatypePortion = datatypePortion.substring(0, datatypePortion.length() - 1);
                }
                try {
                    Type dataType = TypeRegistry.getType(datatypePortion);
                    dataTypeSpecificProperties.computeIfAbsent(dataType, k -> new HashMap<>()).put(propertySuffix, entry.getValue());
                } catch (Exception e) {
                    log.debug("Skipping property with unrecognized datatype portion: {}", property);
                }
            }
        }
        return dataTypeSpecificProperties;
    }

    private boolean isIndexProperty(String property) {
        return property.endsWith(INDEX_FIELDS) || property.endsWith(DISALLOWLIST_INDEX_FIELDS) || property.endsWith(REVERSE_INDEX_FIELDS)
                        || property.endsWith(DISALLOWLIST_REVERSE_INDEX_FIELDS);
    }

    private String getPropertySuffix(String property) {
        if (property.endsWith(DISALLOWLIST_REVERSE_INDEX_FIELDS)) {
            return DISALLOWLIST_REVERSE_INDEX_FIELDS;
        } else if (property.endsWith(DISALLOWLIST_INDEX_FIELDS)) {
            return DISALLOWLIST_INDEX_FIELDS;
        } else if (property.endsWith(REVERSE_INDEX_FIELDS)) {
            return REVERSE_INDEX_FIELDS;
        } else if (property.endsWith(INDEX_FIELDS)) {
            return INDEX_FIELDS;
        }
        return null;
    }

    public void setDelegateHelper(IngestHelperInterface delegate) {
        this.delegate = delegate;
    }

    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        // we need to do this safely, make our best attempt to get some fields
        try {
            return delegate.getEventFields(event);
        } catch (Exception e) {
            return HashMultimap.create();
        }
    }

    @Override
    public Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> fields) {
        return super.normalizeMap(fields);
    }

    @Override
    public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
        return null;
    }

    private boolean hasErrorIndexConfig() {
        return !errorIndexedFields.isEmpty();
    }

    private boolean hasErrorReverseIndexConfig() {
        return !errorReverseIndexedFields.isEmpty();
    }

    @Override
    public boolean isIndexedField(String fieldName) {
        IndexedFields dataTypeIndexFields = errorIndexedFields.get(getActiveDataType());
        if (dataTypeIndexFields == null) {
            if (getActiveDataType() == null && hasErrorIndexConfig()) {
                return super.isIndexedField(fieldName) || anyErrorIndexMatch(errorIndexedFields, fieldName);
            }
            return super.isIndexedField(fieldName);
        }

        return dataTypeIndexFields.matches(fieldName);
    }

    public Set<String> getIndexedFields(Type dataType) {
        return errorIndexedFields.containsKey(dataType) ? errorIndexedFields.get(dataType).indexedFields : Set.of();
    }

    public Set<String> getReverseIndexedFields(Type dataType) {
        return errorReverseIndexedFields.containsKey(dataType) ? errorReverseIndexedFields.get(dataType).indexedFields : Set.of();
    }

    @Override
    public boolean isReverseIndexedField(String fieldName) {
        IndexedFields dataTypeIndexFields = errorReverseIndexedFields.get(getActiveDataType());
        if (dataTypeIndexFields == null) {
            if (getActiveDataType() == null && hasErrorReverseIndexConfig()) {
                return super.isReverseIndexedField(fieldName) || anyErrorIndexMatch(errorReverseIndexedFields, fieldName);
            }
            return super.isReverseIndexedField(fieldName);
        }

        return dataTypeIndexFields.matches(fieldName);
    }

    private boolean anyErrorIndexMatch(Map<Type,IndexedFields> configMap, String fieldName) {
        for (IndexedFields fields : configMap.values()) {
            if (fields.matches(fieldName)) {
                return true;
            }
        }
        return false;
    }

    public void setActiveDataType(Type dataType) {
        activeDataType = dataType;
    }

    public Type getActiveDataType() {
        return activeDataType;
    }
}
