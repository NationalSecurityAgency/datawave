package datawave.ingest.data.config.ingest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

/**
 * Implements methods provided by the WhindexIngest interface.
 * <p>
 * The WhindexFieldIngestHelper is responsible for parsing whindex rules from a Hadoop Configuration and generating a mapping between event map fields and
 * corresponding whindex configurations. The configuration rules determine how and when new whindex fields are created from an input event map.
 * <ul>
 * <li><strong>valueFieldsToWhindexConfigs</strong>: Associates an event map's value field with a corresponding WhindexConfig that specifies the source field,
 * destination field, values, and whether the source field is overloaded (i.e. to be removed).</li>
 * <li><strong>overloadedFields</strong>: A set of source fields that are marked as overloaded (i.e., should be deleted) based on the configuration.</li>
 * <li><strong>destinationFields</strong>: A set of fields that are defined as new whindex (destination) fields. (Note: In this implementation, destination
 * fields are added during setup but the getter for destination fields currently returns a copy of the overloadedFields. Verify if this behavior meets your
 * requirements.)</li>
 * </ul>
 * <p>
 * After setup, getWhindexFields() processes an event map to produce new whindex fields based on the defined rules.
 * </p>
 */
public class WhindexFieldIngestHelper implements WhindexIngest {

    // Logger for logging warnings and debug information.
    private static final Logger log = Logger.getLogger(WhindexFieldIngestHelper.class);

    // Constants defining property names used in the configuration.
    public static final String WHINDEX_RULES = "whindex.rules";
    public static final String VALUE_FIELD = "value_field";
    public static final String SRC_FIELD = "src_field";
    public static final String DELETE_SRC_FIELD = "delete_src_field";
    public static final String DST_FIELD = "dst_field";
    public static final String VALUES = "values";

    // The Type object representing the context of ingestion.
    private final Type type;
    // Multimap mapping value field names to their corresponding WhindexConfig instances.
    private final Multimap<String,WhindexConfig> valueFieldsToWhindexConfigs = HashMultimap.create();
    // Set of source fields that are marked as overloaded (to be deleted after processing).
    private final Set<String> overloadedFields = new HashSet<>();
    // Set of destination (whindex) fields generated from configuration rules.
    private final Set<String> destinationFields = new HashSet<>();

    /**
     * Constructs a new WhindexFieldIngestHelper for the given ingestion type.
     *
     * @param type
     *            the ingestion type that this helper will operate on.
     */
    public WhindexFieldIngestHelper(Type type) {
        this.type = type;
    }

    /**
     * Parses the whindex rules from the provided Hadoop Configuration.
     * <p>
     * The configuration is expected to have properties in the following form: <code>typeName.whindex.rules.[groupID].[property]=value</code>, where the
     * property is one of VALUE_FIELD, SRC_FIELD, DELETE_SRC_FIELD, DST_FIELD, or VALUES. Each groupID represents a separate whindex rule.
     * </p>
     *
     * @param config
     *            the Hadoop Configuration containing whindex rules.
     * @throws IllegalArgumentException
     *             if there are configuration issues.
     */
    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        // Construct a common prefix based on the type name and the whindex rules constant.
        String commonPrefix = type.typeName() + "." + WHINDEX_RULES + ".";

        // Get all properties from the configuration that start with the common prefix.
        Map<String,String> properties = config.getPropsWithPrefix(commonPrefix);
        // Map to temporarily group properties by their group ID.
        Map<String,WhindexConfig> groupingsToConfigs = new HashMap<>();

        // Process each configuration property.
        for (Map.Entry<String,String> entry : properties.entrySet()) {
            // Split the key by '.' to extract the group ID and property name.
            String[] parts = entry.getKey().split("\\.");
            String groupID = parts[0];
            String property = parts[1];

            // Retrieve or create the WhindexConfig for the given group ID.
            WhindexConfig whindexConfig = groupingsToConfigs.computeIfAbsent(groupID, (k) -> new WhindexConfig());
            // Set the appropriate property in the WhindexConfig based on the property name.
            switch (property) {
                case VALUE_FIELD:
                    whindexConfig.setValueField(entry.getValue());
                    break;
                case SRC_FIELD:
                    whindexConfig.setSourceField(entry.getValue());
                    break;
                case DELETE_SRC_FIELD:
                    whindexConfig.setOverloaded(Boolean.parseBoolean(entry.getValue()));
                    // If the configuration specifies deletion of the source field, add it to overloadedFields.
                    if (whindexConfig.isOverloaded()) {
                        overloadedFields.add(whindexConfig.getSourceField());
                    }
                    break;
                case DST_FIELD:
                    whindexConfig.setDestField(entry.getValue());
                    // Add the destination (whindex) field to the set of destinationFields.
                    destinationFields.add(entry.getValue());
                    break;
                case VALUES:
                    // Split the comma-separated list of values and set them.
                    whindexConfig.setValues(List.of(entry.getValue().split(",")));
                    break;
                default:
                    // Log a warning for any unexpected property found in the configuration.
                    String originalProperty = commonPrefix + groupID + "." + property;
                    log.warn("Unexpected whindex property given:" + originalProperty + "=" + entry.getValue());
            }
        }

        // After processing, map each WhindexConfig to its value field.
        groupingsToConfigs.values().forEach((wc) -> {
            valueFieldsToWhindexConfigs.put(wc.getValueField(), wc);
        });
    }

    /**
     * Processes the provided event map to generate new whindex fields based on the whindex rules.
     * <p>
     * The helper examines each WhindexConfig and checks:
     * <ul>
     * <li>That both the value field and source field exist in the input event map.</li>
     * <li>That at least one of the entries in the event map for the value field matches one of the configured values, either by event field value or indexed
     * field value.</li>
     * </ul>
     * If a match is found, a copy of the source field's NormalizedContentInterface objects is created and added to the whindex fields under the configured
     * destination field.
     * </p>
     *
     * @param eventMap
     *            the input event map containing field names and their corresponding normalized content.
     * @return a multimap of whindex fields generated from the event map.
     */
    @Override
    public Multimap<String,NormalizedContentInterface> getWhindexFields(Multimap<String,NormalizedContentInterface> eventMap) {
        Multimap<String,NormalizedContentInterface> whindicesInEventMap = HashMultimap.create();

        // Filter whindex rules for which both the value field and source field are present in the event map.
        List<WhindexConfig> matchingConfigs = valueFieldsToWhindexConfigs.entries().stream()
                        .filter(entry -> eventMap.containsKey(entry.getValue().getValueField()) && eventMap.containsKey(entry.getValue().getSourceField()))
                        .map(Map.Entry::getValue).collect(Collectors.toList());

        // Iterate over the matching configurations to generate the whindex fields.
        for (WhindexConfig currWhindexConfig : matchingConfigs) {
            // Retrieve all normalized content corresponding to the value field.
            Collection<NormalizedContentInterface> relatedValueEventContents = eventMap.get(currWhindexConfig.getValueField());
            // Check if any of these contents match the allowed values (either event field or indexed field).
            boolean containsAnyMatchingValue = relatedValueEventContents.stream().anyMatch(nci -> {
                List<String> currentValues = currWhindexConfig.getValues();
                return currentValues.contains(nci.getEventFieldValue()) || currentValues.contains(nci.getIndexedFieldValue());
            });

            if (containsAnyMatchingValue) {
                // Retrieve all normalized content corresponding to the source field.
                Collection<NormalizedContentInterface> relatedSourceEventContents = eventMap.get(currWhindexConfig.getSourceField());
                List<NormalizedContentInterface> copies = new ArrayList<>();
                // Create copies of the source field's normalized content.
                for (NormalizedContentInterface content : relatedSourceEventContents) {
                    NormalizedFieldAndValue copy = new NormalizedFieldAndValue(content);
                    copies.add(copy);
                }
                // Map the copies to the destination field (whindex field).
                whindicesInEventMap.putAll(currWhindexConfig.getDestField(), copies);
            }
        }
        return whindicesInEventMap;
    }

    /**
     * Determines if the provided field name is recognized as a whindex (destination) field.
     *
     * @param field
     *            the field name to check.
     * @return true if the field is a whindex field; false otherwise.
     */
    @Override
    public boolean isWhindexField(String field) {
        return destinationFields.contains(field);
    }

    /**
     * Determines if the provided field name is considered overloaded (i.e., the source field that should be deleted).
     *
     * @param field
     *            the field name to check.
     * @return true if the field is overloaded; false otherwise.
     */
    @Override
    public boolean isOverloadedWhindexField(String field) {
        return overloadedFields.contains(field);
    }

    /**
     * Returns an immutable multimap of value fields to their corresponding Whindex configurations.
     *
     * @return an immutable view of the whindex configurations mapped by value field.
     */
    @Override
    public ImmutableMultimap<String,WhindexConfig> getValueFieldsToWhindexConfigs() {
        return ImmutableMultimap.copyOf(valueFieldsToWhindexConfigs);
    }

    /**
     * Returns an immutable set of fields that are marked as overloaded.
     *
     * @return an immutable set of overloaded field names.
     */
    public ImmutableSet<String> getOverloadedFields() {
        return ImmutableSet.copyOf(overloadedFields);
    }

    /**
     * Returns an immutable set of destination (whindex) fields.
     * <p>
     * Note: This method currently returns a copy of the overloaded fields. Verify if the intended behavior is to return the actual destination fields.
     * </p>
     *
     * @return an immutable set of destination field names.
     */
    public ImmutableSet<String> getDestinationFields() {
        return ImmutableSet.copyOf(overloadedFields);
    }
}
