package datawave.ingest.data.config.ingest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;

public interface WhindexIngest {

    /**
     * Used to allow external scopes to interface with the WhindexIngest's WhindexFieldNormalizer's .setup() method. Initializes the WhindexIngest from a
     * {@link Configuration}.
     *
     * @param config
     *            the {@link Configuration}.
     */
    void setup(Configuration config) throws IllegalArgumentException;

    /**
     * Given a "{@code RULE}", return a {@code Multimap<String, String>} of whindex fields ("{@code DST_FIELD}") mapped to the values specified by the
     * {@code RULE}.
     *
     * @return the mapping of whindex fields to values.
     */
    Multimap<String,String> getWhindexFieldDefinitions();

    /**
     * @param field
     *            the field to check.
     * @return {@code true} if {@code field} is a whindex field.
     */
    boolean isWhindexField(String field);

    /**
     * {@code OverloadedWhindexField}s are source fields ("{@code SRC_FIELD}") that become redundant once whindex entries are generated. They are marked to be
     * removed.
     *
     * @param field
     *            the field to check.
     * @return {@code true} if {@code field} is an {@code OverloadedWhindexField} (marked for removal)
     */
    boolean isOverloadedWhindexField(String field);

    /**
     * // todo Given a "{@code RULE}", return a {@code Multimap<String, NormalizedContentInterface>} of whindex fields ("{@code DST_FIELD}") mapped to the
     * values specified by the {@code RULE}.
     *
     * @return the mapping of whindex fields to values.
     */
    Multimap<String,NormalizedContentInterface> getWhindexFields(Multimap<String,NormalizedContentInterface> eventFields);

    /**
     * Responsible for parsing the {@code .rules} passed to the {@link WhindexIngest} and holding configuration information.
     */
    class WhindexFieldNormalizer {

        private static final Logger log = Logger.getLogger(WhindexFieldNormalizer.class);

        // The property name that holds the whindex rules string in the configuration xml
        public static final String RULE = "rules";

        // The delimiter used to separate distinct RULEs within the <type>.RULES property
        // ie: RULE;RULE;RULE;...
        public static final String RULE_DELIMITER = ";";

        // The delimiter used to separate the FIELDS side of a RULE from the VALUE,VALUE,... side of a RULE.
        public static final String FIELDS_VALUES_DELIMITER = "::";

        // The delimiter used to separate distinct FIELDs and VALUEs within a RULE.
        // ie: FIELD,FIELD,FIELD::VALUE,VALUE,VALUE
        // The left side is shorthanded (?) to FIELDS::VALUE,VALUE,VALUE,... which omits the fact that FIELDS is made up of several FIELDs.
        public static final String FIELD_VALUE_DELIMITER = ",";

        // The delimiter used to separate SRC_FIELD:DST_FIELD:DELETE_SRC within the <type>.FIELDS.<some_field> property
        public static final String SRC_DST_DEL_DELIMITER = ":";

        // Mappings of source fields to their respective whindex field.
        private final Multimap<String,String> whindexFieldDefinitions = LinkedListMultimap.create();
        private final Set<String> overloadedFields = new HashSet<>();
        private MarkingFunctions markingFunctions;

        /**
         * Parses the {@code config}'s "{@code <datatype>.rules"} property, generating whindex entries based on each rule.
         *
         * @param type
         *            the datatype we're looking for.
         * @param config
         *            the config instance that holds the rules.
         */
        public void setup(Type type, Configuration config) {
            markingFunctions = MarkingFunctionsFactory.createMarkingFunctions();

            String whindexRules = config.get(type.typeName() + "." + RULE, "");
            if (whindexRules.isEmpty()) {
                return;
            }

            // Get each individual rule
            for (String rule : whindexRules.split(RULE_DELIMITER)) {
                String[] parts = rule.split(FIELDS_VALUES_DELIMITER);
                String[] curFields = parts[0].split(FIELD_VALUE_DELIMITER);
                String[] curValues = parts[1].split(FIELD_VALUE_DELIMITER);

                // Add each field from the current rule into the whindexFields map as a k.
                // Each rule entry will have all related values as a v.
                for (String field : curFields) {

                    // fieldParts[0] is SRC_FIELD
                    // fieldParts[1] is DST_FIELD (whindex)
                    // fieldParts[2] is DELETE_SRC
                    String[] fieldParts = field.split(SRC_DST_DEL_DELIMITER);
                    String srcField = fieldParts[0];
                    String whindexField = fieldParts[1];

                    whindexFieldDefinitions.putAll(whindexField, Arrays.asList(curValues));

                    // Only add the SRC_FIELD to the overloaded list if DELETE_SRC is true
                    if (fieldParts.length > 2 && Boolean.parseBoolean(fieldParts[2])) {
                        overloadedFields.add(srcField);
                    }
                }
            }
        }

        // TODO: MIGRATE THIS TO WFN
        // TODO: WRITE TESTS ON THIS BAD BOY
        // TODO: IMPLEMENT EQUALS AND HASH CODE FOR WHINDEX CONFIG

        public static final String WHINDEX_RULES = "whindex.rules";
        public static final String VALUE_FIELD = "value_field";
        public static final String SRC_FIELD = "src_field";
        public static final String DELETE_SRC_FIELD = "delete_src_field";
        public static final String DST_FIELD = "dst_field";
        public static final String VALUES = "values";

        private class WhindexConfig {

            // The name of the FIELD that contains the VALUEs
            /**
             * The field name extracted from the {@value #VALUE_FIELD} property of a whindex configuration.
             */
            private String valueField;

            // The VALUEs associated with a given VALUE_FIELD
            private List<String> values;
            private String sourceField;
            private String destField;
            private boolean overloaded;

            public String getValueField() {
                return valueField;
            }

            public void setValueField(String valueField) {
                this.valueField = valueField;
            }

            public List<String> getValues() {
                return values;
            }

            public void setValues(List<String> values) {
                this.values = values;
            }

            public String getSourceField() {
                return sourceField;
            }

            public void setSourceField(String sourceField) {
                this.sourceField = sourceField;
            }

            public String getDestField() {
                return destField;
            }

            public void setDestField(String destField) {
                this.destField = destField;
            }

            public boolean isOverloaded() {
                return overloaded;
            }

            public void setOverloaded(boolean overloaded) {
                this.overloaded = overloaded;
            }

            @Override
            public boolean equals(Object o) {
                if (o == null || getClass() != o.getClass())
                    return false;
                WhindexConfig config = (WhindexConfig) o;
                return overloaded == config.overloaded && Objects.equals(valueField, config.valueField) && Objects.equals(values, config.values)
                                && Objects.equals(sourceField, config.sourceField) && Objects.equals(destField, config.destField);
            }

            @Override
            public int hashCode() {
                return Objects.hash(valueField, values, sourceField, destField, overloaded);
            }

        }

        // why do we need a vf->wc multimap? OH because the same vf can have different wc associated with them.
        // renaming this would be nice, this is a:
        // Map that contains all WhindexConfigs related to each [valueField] (many to 1)
        private Multimap<String,WhindexConfig> valueFieldsToWhindexConfigs = HashMultimap.create();

        public void setup2(Type type, Configuration config) {

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
                        whindexConfig.valueField = entry.getValue();
                        break;
                    case SRC_FIELD:
                        whindexConfig.sourceField = entry.getValue();
                        break;
                    case DELETE_SRC_FIELD:
                        whindexConfig.overloaded = Boolean.parseBoolean(entry.getValue());
                        break;
                    case DST_FIELD:
                        whindexConfig.destField = entry.getValue();
                        break;
                    case VALUES:
                        whindexConfig.values = List.of(entry.getValue().split(","));
                        break;
                    default:
                        String originalProperty = commonPrefix + groupID + "." + property;
                        log.warn("Unexpected whindex property given:" + originalProperty + "=" + entry.getValue());
                }
            }

            groupingsToConfigs.values().forEach((v) -> valueFieldsToWhindexConfigs.put(v.valueField, v));
        }

        /**
         * Given a "{@code RULE}", return a {@code Multimap<String, String>} of whindex fields ("{@code DST_FIELD}") mapped to the values specified by the
         * {@code RULE}.
         *
         * @return the mapping of whindex fields to values.
         */
        public Multimap<String,String> getWhindexFieldDefinitions() {
            return whindexFieldDefinitions;
        }

        /**
         * {@code OverloadedWhindexField}s are source fields ("{@code SRC_FIELD}") that become redundant once whindex entries are generated. They are marked to
         * be removed.
         *
         * @return all {@code OverloadedWhindexField}s parsed from the {@code .rules}.
         */
        public Set<String> getOverloadedFields() {
            return overloadedFields;
        }

        /**
         * @param field
         *            the field to check.
         * @return {@code true} if {@code field} is a whindex field.
         */
        public boolean isWhindexField(String field) {
            return whindexFieldDefinitions.containsKey(field);
        }

        /**
         * // todo Given a "{@code RULE}", return a {@code Multimap<String, NormalizedContentInterface>} of whindex fields ("{@code DST_FIELD}") mapped to the
         * values specified by the {@code RULE}. FieldName / Normalized FieldName and Normalized Values
         *
         * @return the mapping of whindex fields to values.
         */
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
    }
}
