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
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

public class WhindexFieldIngestHelper implements WhindexIngest {

    private static final Logger log = Logger.getLogger(WhindexFieldIngestHelper.class);

    public static final String WHINDEX_RULES = "whindex.rules";
    public static final String VALUE_FIELD = "value_field";
    public static final String SRC_FIELD = "src_field";
    public static final String DELETE_SRC_FIELD = "delete_src_field";
    public static final String DST_FIELD = "dst_field";
    public static final String VALUES = "values";

    private final Type type;

    private final Multimap<String,String> whindexFieldDefinitions = LinkedListMultimap.create();
    private final Multimap<String,WhindexConfig> valueFieldsToWhindexConfigs = HashMultimap.create();
    private final Set<String> overloadedFields = new HashSet<>();

    public WhindexFieldIngestHelper(Type type) {
        this.type = type;
    }

    @Override
    public void setup(Configuration config) throws IllegalArgumentException {

        String commonPrefix = type.typeName() + "." + WHINDEX_RULES + ".";

        Map<String,String> properties = config.getPropsWithPrefix(commonPrefix);
        Map<String,WhindexConfig> groupingsToConfigs = new HashMap<>();

        for (Map.Entry<String,String> entry : properties.entrySet()) {
            String[] parts = entry.getKey().split("\\.");
            String groupID = parts[0];
            String property = parts[1];

            WhindexConfig whindexConfig = groupingsToConfigs.computeIfAbsent(groupID, (k) -> new WhindexConfig());

            switch (property) {
                case VALUE_FIELD:
                    whindexConfig.setValueField(entry.getValue());
                    break;
                case SRC_FIELD:
                    whindexConfig.setSourceField(entry.getValue());
                    break;
                case DELETE_SRC_FIELD:
                    whindexConfig.setOverloaded(Boolean.parseBoolean(entry.getValue()));
                    if (whindexConfig.isOverloaded()) {
                        overloadedFields.add(whindexConfig.getSourceField());
                    }
                    break;
                case DST_FIELD:
                    whindexConfig.setDestField(entry.getValue());
                    break;
                case VALUES:
                    whindexConfig.setValues(List.of(entry.getValue().split(",")));
                    break;
                default:
                    String originalProperty = commonPrefix + groupID + "." + property;
                    log.warn("Unexpected whindex property given:" + originalProperty + "=" + entry.getValue());
            }
        }

        groupingsToConfigs.values().forEach((v) -> valueFieldsToWhindexConfigs.put(v.getValueField(), v));

    }

    @Override
    public Multimap<String,NormalizedContentInterface> getWhindexFields(Multimap<String,NormalizedContentInterface> eventMap) {

        Multimap<String,NormalizedContentInterface> whindicesInEventMap = HashMultimap.create();

        // Get all the wc that have both vf and sf in the eventMap
        List<WhindexConfig> matchingConfigs = valueFieldsToWhindexConfigs.entries().stream()
                        .filter(entry -> eventMap.containsKey(entry.getValue().getValueField()) && eventMap.containsKey(entry.getValue().getSourceField()))
                        .map(Map.Entry::getValue).collect(Collectors.toList());

        // Check that the eventMap entry has EITHER an EventField or IndexedField in common with the wc's VALUES
        for (WhindexConfig curWhindexConfig : matchingConfigs) {
            Collection<NormalizedContentInterface> relatedValueEventContents = eventMap.get(curWhindexConfig.getValueField());
            // if any of the NCI's have either EF or IF that's GOOD!
            boolean containsAnyMatchingValue = relatedValueEventContents.stream()
                            .anyMatch(nci -> curWhindexConfig.getValues().contains(nci.getEventFieldValue())
                                            || curWhindexConfig.getValues().contains(nci.getIndexedFieldValue()));

            if (containsAnyMatchingValue) {
                Collection<NormalizedContentInterface> relatedSourceEventContents = eventMap.get(curWhindexConfig.getSourceField());
                List<NormalizedContentInterface> copies = new ArrayList<>();
                for (NormalizedContentInterface content : relatedSourceEventContents) {
                    NormalizedFieldAndValue copy = new NormalizedFieldAndValue(content);
                    copies.add(copy);
                }

                // Create whindex fields
                whindicesInEventMap.putAll(curWhindexConfig.getDestField(), copies);
            }
        }

        return whindicesInEventMap;

    }

    @Override
    public Multimap<String,String> getWhindexFieldDefinitions() {
        return whindexFieldDefinitions;
    }

    @Override
    public boolean isWhindexField(String field) {
        return whindexFieldDefinitions.containsKey(field);
    }

    @Override
    public boolean isOverloadedWhindexField(String field) {
        return overloadedFields.contains(field);
    }

}
