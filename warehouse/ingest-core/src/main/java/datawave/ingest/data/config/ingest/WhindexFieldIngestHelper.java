package datawave.ingest.data.config.ingest;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import jline.internal.Log;
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
 * Implements methods provided by the {@link WhindexIngest} interface, enabling parsing and application of Whindex rules defined by the constants:
 * <ul>
 * <li>{@link #WHINDEX_RULES}</li>
 * <li>{@link #VALUE_FIELD}</li>
 * <li>{@link #SRC_FIELD}</li>
 * <li>{@link #DELETE_SRC_FIELD}</li>
 * <li>{@link #DST_FIELD}</li>
 * <li>{@link #VALUES}</li>
 * </ul>
 * <p>
 * Call {@link #setup(Configuration)} (which reads properties prefixed by <code>typeName. {@link #WHINDEX_RULES}.</code>) before
 * {@link #processWhindexFields(Multimap)}, or no Whindex rules will be applied.
 * </p>
 */
public class WhindexFieldIngestHelper implements WhindexIngest {

    /** Logger for logging warnings and debug information. */
    private static final Logger log = Logger.getLogger(WhindexFieldIngestHelper.class);

    /** Prefix for Whindex rules in configuration (e.g. <code>myType.whindex.rules</code>). */
    public static final String WHINDEX_RULES = "whindex.rules";

    /** Property name for the value field to check. */
    public static final String VALUE_FIELD = "value_field";

    /** Property name for the source field to read from. */
    public static final String SRC_FIELD = "src_field";

    /** Property name to indicate whether to delete the source field after processing. */
    public static final String DELETE_SRC_FIELD = "delete_src_field";

    /** Property name for the destination field to write Whindex entries into. */
    public static final String DST_FIELD = "dst_field";

    /** Property name for the comma-separated list of trigger values. */
    public static final String VALUES = "values";

    /** The data type this helper applies to. */
    private final Type type;

    /**
     * Maps a value-field name to the set of WhindexConfig instances that should fire when that field's values match.
     */
    private ImmutableMultimap<String,WhindexConfig> valueFieldsToWhindexConfigs;

    /** All destination fields produced by any built configs (used in {@link #isWhindexField(String)}). */
    private ImmutableSet<String> destinationFields;

    /**
     * Create a new helper bound to the given data type.
     *
     * @param type
     *            the Type whose configuration properties to read
     */
    public WhindexFieldIngestHelper(Type type) {
        this.type = type;
    }

    /**
     * Reads all Whindex rule properties from the given Hadoop Configuration. Each group of properties (by prefix) is accumulated into a
     * {@link WhindexConfig.Builder}, then built and stored in the internal multimaps.
     *
     * @param config
     *            the Hadoop Configuration containing properties like <code>typeName.whindex.rules.X.value_field</code>
     * @throws RuntimeException
     *             if any property is malformed or missing required values
     */
    @Override
    public void setup(Configuration config) {
        try {

            HashMultimap<String,WhindexConfig> vfToWindexCfg = HashMultimap.create();
            Set<String> dstFields = new HashSet<>();

            String prefix = type.typeName() + "." + WHINDEX_RULES + ".";
            Map<String,String> properties = config.getPropsWithPrefix(prefix);
            Map<String,WhindexConfig.Builder> builders = new HashMap<>();

            for (Map.Entry<String,String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String val = entry.getValue();
                String[] parts = key.split("\\.");
                String group = parts[0];
                String prop = parts[1];

                if (val.isEmpty()) {
                    throw new IllegalArgumentException("Empty value for Whindex property: " + key);
                }

                WhindexConfig.Builder builder = builders.computeIfAbsent(group, g -> WhindexConfig.builder());

                switch (prop) {
                    case VALUE_FIELD:
                        builder.withValueField(val);
                        break;

                    case SRC_FIELD:
                        builder.withSourceField(val);
                        break;

                    case DELETE_SRC_FIELD:
                        builder.withOverloaded(Boolean.parseBoolean(val));
                        break;

                    case DST_FIELD:
                        builder.withDestField(val);
                        dstFields.add(val);
                        break;

                    case VALUES:
                        List<String> vals = List.of(val.split(","));
                        builder.withValues(vals);
                        break;

                    default:
                        throw new IllegalArgumentException("Unexpected Whindex property: " + key);
                }
            }

            // Build all configs and populate the multimap
            for (WhindexConfig cfg : builders.values().stream().map(WhindexConfig.Builder::build).collect(Collectors.toList())) {
                vfToWindexCfg.put(cfg.getValueField(), cfg);
            }

            valueFieldsToWhindexConfigs = ImmutableMultimap.<String,WhindexConfig> builder().putAll(vfToWindexCfg).build();
            destinationFields = ImmutableSet.<String> builder().addAll(dstFields).build();

            log.debug("Loaded Whindex configs: " + valueFieldsToWhindexConfigs.entries());
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup WhindexFieldIngestHelper", e);
        }
    }

    /**
     * Parses the {@param eventMap} applying each Whindex rule initalized via {@link #setup(Configuration)} that have <b>all</b> their requirements met by the
     * entries in the {@param eventMap}. Once these rules have been applied, all srcFields that were used to create a new whindexField and are marked as
     * overloaded are removed from the eventMap.
     *
     * @param eventMap
     *            the original multimap of field -> values
     * @return a new Multimap reflecting added Whindex entries and any removed sources
     */
    @Override
    public Multimap<String,NormalizedContentInterface> processWhindexFields(Multimap<String,NormalizedContentInterface> eventMap) {
        if (valueFieldsToWhindexConfigs.isEmpty()) {
            return eventMap;
        }

        Multimap<String,NormalizedContentInterface> result = HashMultimap.create(eventMap);
        Multimap<String,NormalizedContentInterface> whindices = HashMultimap.create();
        Set<String> toRemove = new HashSet<>();

        // Determine which configs apply
        List<WhindexConfig> configs = valueFieldsToWhindexConfigs.entries().stream().map(Map.Entry::getValue)
                        .filter(cfg -> result.containsKey(cfg.getValueField()) && result.containsKey(cfg.getSourceField())).collect(Collectors.toList());

        for (WhindexConfig cfg : configs) {
            // Check if any existing values match the config's trigger list
            boolean match = result.get(cfg.getValueField()).stream()
                            .anyMatch(nci -> cfg.getValues().contains(nci.getEventFieldValue()) || cfg.getValues().contains(nci.getIndexedFieldValue()));
            if (!match)
                continue;

            // Copy source entries into new dest field
            Collection<NormalizedContentInterface> sources = result.get(cfg.getSourceField());
            for (NormalizedContentInterface nci : sources) {
                whindices.put(cfg.getDestField(), new NormalizedFieldAndValue(nci));
            }

            if (cfg.isOverloaded()) {
                toRemove.add(cfg.getSourceField());
            }
        }

        // Remove any overloaded source fields and add in new whindex entries
        toRemove.forEach(result::removeAll);
        result.putAll(whindices);

        return result;
    }

    /**
     * Checks if the given field name is one of the Whindex-generated destination fields.
     *
     * @param field
     *            the field name to check
     * @return true if this helper can generate values for that field
     */
    @Override
    public boolean isWhindexField(String field) {
        if(destinationFields == null){
            log.warn("destinationFields has not been initialized. You probably need to run WhindexFieldIngestHelper's .setup() method first.");
            return false;
        }
        return destinationFields.contains(field);
    }

    /**
     * Retrieves an immutable view of the mapping from trigger fields to configs.
     *
     * @return a Guava ImmutableMultimap of valueField -> WhindexConfig
     */
    @Override
    public ImmutableMultimap<String,WhindexConfig> getValueFieldsToWhindexConfigs() {
        return valueFieldsToWhindexConfigs;
    }

    /**
     * All configured destination fields (for example by DELETE_SRC_FIELD rules).
     *
     * @return an ImmutableSet of field names
     */
    public ImmutableSet<String> getDestinationFields() {
        return ImmutableSet.copyOf(destinationFields);
    }
}
