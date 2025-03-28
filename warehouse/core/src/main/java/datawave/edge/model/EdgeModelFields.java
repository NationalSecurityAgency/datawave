package datawave.edge.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Interface that allows internal field names used within the edge model to be configured and injected transparently into classes that need them.<br>
 * <br>
 *
 * The intention is to enforce a unified approach to field name management and usage throughout the entire codebase, so that the actual field names in use can
 * be dictated by the deployment environment rather than the code itself.
 */
public class EdgeModelFields implements Serializable {

    /** internal fields common to all application tiers */
    private Map<String,String> baseFieldMap;
    /** internal fields used in key processing (as previously defined by enum within EdgeKeyUtil.java) */
    private Map<String,String> keyUtilFieldMap;
    /** internal fields used in query result transformation (eg, EdgeQueryTransformer.java) */
    private Map<String,String> transformFieldMap;
    /** A mapping of field names to field keys */
    private Map<String,FieldKey> reverseMap = new HashMap<>();

    public static String EQUALS = "==";
    public static String EQUALS_REGEX = "=~";
    public static String NOT_EQUALS = "!=";
    public static String NOT_EQUALS_REGEX = "!~";
    public static String OR = " || ";
    public static String AND = " && ";
    public static char STRING_QUOTE = '\'';
    public static char BACKSLASH = '\\';

    public Map<String,String> getBaseFieldMap() {
        return baseFieldMap;
    }

    public void setBaseFieldMap(Map<String,String> baseFieldMap) {
        this.baseFieldMap = baseFieldMap;
        updateReverseMap(baseFieldMap);
    }

    public Map<String,String> getKeyUtilFieldMap() {
        return keyUtilFieldMap;
    }

    public void setKeyUtilFieldMap(Map<String,String> keyUtilFieldMap) {
        this.keyUtilFieldMap = keyUtilFieldMap;
        updateReverseMap(keyUtilFieldMap);
    }

    public void setTransformFieldMap(Map<String,String> transformFieldMap) {
        this.transformFieldMap = transformFieldMap;
        updateReverseMap(transformFieldMap);
    }

    /**
     * Enum that can be used for convenience to lookup the internal edge field name values.
     */
    public enum FieldKey {
        /**
         * Key to the internal field name used to denote edge vertex 1
         */
        EDGE_SOURCE,
        /**
         * Key to the internal field name used to denote edge vertex 2
         */
        EDGE_SINK,
        /**
         * Key to the internal field name used to denote the edge type defined by the two vertices
         */
        EDGE_TYPE,
        /**
         * Key to the internal field name used to denote the source-sink relationship
         */
        EDGE_RELATIONSHIP,
        /**
         * Key to the internal field name used to denote the edge's 1st attribute
         */
        EDGE_ATTRIBUTE1,
        /**
         * Key to the internal field name used to denote the edge's 2nd attribute
         */
        EDGE_ATTRIBUTE2,
        /**
         * Key to the internal field name used to denote the edge's 3rd attribute
         */
        EDGE_ATTRIBUTE3,
        /**
         * Key to the internal field name used to denote a stats edge
         */
        STATS_EDGE,
        /**
         * Key to the internal field name used to denote the edge's date
         */
        DATE,
        /**
         * Key to the internal field name used to denote the edge enrichment type
         */
        ENRICHMENT_TYPE,
        /**
         * Key to the internal field name used to denote the edge fact type
         */
        FACT_TYPE,
        /**
         * Key to the internal field name used to denote the edge grouped fields
         */
        GROUPED_FIELDS,
        /**
         * Key to the internal field name used to convey an edge count
         */
        COUNT,
        /**
         * Key to the internal field name used to convey edge counts
         */
        COUNTS,
        /**
         * Key to the internal field name used to denote an edge load date
         */
        LOAD_DATE,
        /**
         * Key to the internal field name used to denote and edge id
         */
        UUID,
        /**
         * Key to the internal field name used to denote the edge activity date
         */
        ACTIVITY_DATE,
        /**
         * Key to the internal field name used to denote weather the edge activity date was good or bad
         */
        BAD_ACTIVITY_DATE,
        /**
         * Key to the function
         */
        FUNCTION;
    }

    /**
     * Returns the FieldKey associated with the actual (ie, configured) edge field name, ie, reverse lookup...
     */
    public FieldKey parse(String internalFieldName) {
        FieldKey key = reverseMap.get(internalFieldName);
        // if not specified in the maps, then try it as the enum name
        if (key == null && internalFieldName != null) {
            key = FieldKey.valueOf(internalFieldName);
        }
        return key;
    }

    private void updateReverseMap(Map<String,String> fields) {
        if (fields != null) {
            for (Map.Entry<String,String> entry : fields.entrySet()) {
                reverseMap.put(entry.getValue(), FieldKey.valueOf(entry.getKey()));
            }
        }
    }

    public String getSourceFieldName() {
        return baseFieldMap.get(FieldKey.EDGE_SOURCE.name());
    }

    public String getSinkFieldName() {
        return baseFieldMap.get(FieldKey.EDGE_SINK.name());
    }

    public String getTypeFieldName() {
        return baseFieldMap.get(FieldKey.EDGE_TYPE.name());
    }

    public String getRelationshipFieldName() {
        return baseFieldMap.get(FieldKey.EDGE_RELATIONSHIP.name());
    }

    public String getAttribute1FieldName() {
        return baseFieldMap.get(FieldKey.EDGE_ATTRIBUTE1.name());
    }

    public String getAttribute2FieldName() {
        return baseFieldMap.get(FieldKey.EDGE_ATTRIBUTE2.name());
    }

    public String getAttribute3FieldName() {
        return baseFieldMap.get(FieldKey.EDGE_ATTRIBUTE3.name());
    }

    public String getDateFieldName() {
        return baseFieldMap.get(FieldKey.DATE.name());
    }

    public String getStatsEdgeFieldName() {
        return baseFieldMap.get(FieldKey.STATS_EDGE.name());
    }

    public String getGroupedFieldsFieldName() {
        return keyUtilFieldMap.get(FieldKey.GROUPED_FIELDS.name());
    }

    public String getEnrichmentTypeFieldName() {
        return keyUtilFieldMap.get(FieldKey.ENRICHMENT_TYPE.name());
    }

    public String getFactTypeFieldName() {
        return keyUtilFieldMap.get(FieldKey.FACT_TYPE.name());
    }

    public String getCountFieldName() {
        return transformFieldMap.get(FieldKey.COUNT.name());
    }

    public String getCountsFieldName() {
        return transformFieldMap.get(FieldKey.COUNTS.name());
    }

    public String getLoadDateFieldName() {
        return transformFieldMap.get(FieldKey.LOAD_DATE.name());
    }

    public String getUuidFieldName() {
        return transformFieldMap.get(FieldKey.UUID.name());
    }

    public String getActivityDateFieldName() {
        return transformFieldMap.get(FieldKey.ACTIVITY_DATE.name());
    }

    public String getBadActivityDateFieldName() {
        return transformFieldMap.get(FieldKey.BAD_ACTIVITY_DATE.name());
    }

    /**
     * Returns the subset of all edge-related field names which are common to all application tiers.
     *
     * @return
     */
    public Collection<String> getBaseFieldNames() {
        return Collections.unmodifiableCollection(baseFieldMap.values());
    }

    /**
     * Returns the field names associated with key manipulation and processing, a superset of the fields given by the {@link EdgeModelFields#getBaseFieldNames}
     * method
     *
     * @return
     */
    public Collection<String> getKeyProcessingFieldNames() {
        HashSet<String> fields = new HashSet<>();
        fields.addAll(baseFieldMap.values());
        fields.addAll(keyUtilFieldMap.values());
        return Collections.unmodifiableCollection(fields);
    }

    /**
     * Returns the field names associated with query result transformation, a superset of the fields given by the {@link EdgeModelFields#getBaseFieldNames}
     * method
     *
     * @return
     */
    public Collection<String> getTransformFieldNames() {
        HashSet<String> fields = new HashSet<>();
        fields.addAll(baseFieldMap.values());
        fields.addAll(transformFieldMap.values());
        return Collections.unmodifiableCollection(fields);
    }

    /**
     * Returns the mapped fields associated with edge key manipulation and processing, where the keys are represented as FieldKey.name()
     *
     * @return
     */
    public Map<String,String> getKeyProcessingFieldMap() {
        HashMap<String,String> all = new HashMap<>();
        all.putAll(baseFieldMap);
        all.putAll(keyUtilFieldMap);
        return Collections.unmodifiableMap(all);
    }

    /**
     * Returns the mapping for fields associated with query result transformation, where the keys are represented as FieldKey.name()
     *
     * @return
     */
    public Map<String,String> getTransformFieldMap() {
        HashMap<String,String> all = new HashMap<>();
        all.putAll(baseFieldMap);
        all.putAll(transformFieldMap);
        return Collections.unmodifiableMap(all);
    }

    /**
     * Returns all mapped field names, where the keys are represented as FieldKey.name()
     *
     * @return
     */
    public Map<String,String> getAllFieldsMap() {
        HashMap<String,String> all = new HashMap<>();
        all.putAll(baseFieldMap);
        all.putAll(keyUtilFieldMap);
        all.putAll(transformFieldMap);
        return Collections.unmodifiableMap(all);
    }

    /**
     * Returns the field name mapped to the specified FieldKey
     *
     * @param key
     * @return
     */
    public String getFieldName(FieldKey key) {
        String name = getAllFieldsMap().get(key.name());
        if (name == null) {
            name = key.name();
        }
        return name;
    }
}
