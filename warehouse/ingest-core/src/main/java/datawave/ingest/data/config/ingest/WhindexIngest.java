package datawave.ingest.data.config.ingest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;
import org.apache.log4j.Logger;

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

        /* Property examples:
        <type>.whindex.rules.1.value_field=APPLE
        <type>.whindex.rules.1.values=X,Y,Z
        <type>.whindex.rules.1.src_field=BANANA
        <type>.whindex.rules.1.dst_field=HAT

        <type>.whindex.rules.2.value_field=FRISBEE
        <type>.whindex.rules.2.src_field=BASEBALL
        <type>.whindex.rules.2.delete_src_field=true
        <type>.whindex.rules.2.dst_field=KICKBALL
        <type>.whindex.rules.2.values=X,Y,Z
        <type>.whindex.rules.2.heyImNew=Whatever


        If the event field contains one of the given values for the defined valueField, and has a mapping for the source
        field, then add a field mapping that has the whindex field with the value of the source field.

        */


        /*
        Sample event:

        vf: APPLE -> vs: Y
        sf: BANANA -> sfv: Blue

        Rule 1 tells us to add the following field -> value mappings to the event fields:
        df: HAT -> from-event-sfv: Blue


         */

        /*
        Event 1:
        FRISBEE -> AAA

        BASEBALL -> Homerun

        What would you make?
        => Nothing, AAA is not part of the set of values for FRISBEE

        Event 2:
        APPLE -> Y

        BANANA -> Blue
        BANANA -> Green
        BANANA -> Orange

        What would you make?
        HAT -> Blue
        HAT -> Green
        HAT -> Orange

        Event 3:
        FRISBEE -> X

        GOLF -> Boring
        FOOTBALL -> Tackle



        What would you make?
        => Nothing, neither GOLF nor FOOTBALL are in the SRC fields for Frisbee
         */


        private class WhindexConfig {
            private String valueField;
            private List<String> values;
            private String sourceField;
            private String destField;
            private boolean overloaded;


        }

        private Multimap<String, WhindexConfig> valueFieldsToConfigs = HashMultimap.create();

        public void setup2(Type type, Configuration config) {
            // The prefix common to all rules will be: <type>.whindex.rules.'
            String commonPrefix = type.typeName() + "." + WHINDEX_RULES + ".";

            /*
            1.value_field=APPLE
            1.values=X,Y,Z
            1.src_field=BANANA
            1.dst_field=HAT
             */
/*
            class WHINDEX {
                valueF:string
                values:List<string>
                srcF:string
                df:string
            }

            Map<ID:string , WHINDEX>
            */

            Map<String, String> properties = config.getPropsWithPrefix(commonPrefix);
            Map<String, WhindexConfig> groupingsToConfigs = new HashMap<>();

            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String[] parts = entry.getKey().split("\\.");
                String groupID = parts[0];
                String property = parts[1];

                WhindexConfig whindexConfig = groupingsToConfigs.computeIfAbsent(groupID, (k) -> new WhindexConfig());

                switch (property){
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

            groupingsToConfigs.values().forEach((v) -> valueFieldsToConfigs.put(v.valueField, v));
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

        // todo: discuss what this actually needs to do
        public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            for (Map.Entry<String,String> field : fields.entries()) {
                eventFields.put(field.getKey(), new NormalizedFieldAndValue(field.getKey(), field.getValue()));
            }
            return normalizeMap(eventFields);
        }

        // todo: discuss what this actually needs to do
        public Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> eventFields) {
            Multimap<String,NormalizedContentInterface> whindexFields = HashMultimap.create();

            List<NormalizedContentInterface> tempResults = new ArrayList<>();
            for (Map.Entry<String,String> entry : whindexFieldDefinitions.entries()) {
                tempResults.clear();

                String field = entry.getKey();
                String whindexField = entry.getKey();
                addWhindexFields(tempResults, eventFields, field, whindexField);

                for (NormalizedContentInterface value : tempResults) {
                    whindexFields.put(value.getIndexedFieldName(), value);
                }
            }

            // return whindexFields;
            return eventFields;
        }

        // todo: discuss what this actually needs to do
        public void addWhindexFields(List<NormalizedContentInterface> whindexResults, Multimap<String,NormalizedContentInterface> eventFields, String fieldName,
                        String whindexField) {

            if (eventFields.containsKey(fieldName)) {
                for (NormalizedContentInterface value : eventFields.get(fieldName)) {
                    NormalizedFieldAndValue fieldAndValue = new NormalizedFieldAndValue();
                    fieldAndValue.setFieldName(whindexField);
                    fieldAndValue.setEventFieldName(value.getEventFieldName());
                    fieldAndValue.setIndexedFieldName(value.getIndexedFieldName());
                    fieldAndValue.setMarkings(value.getMarkings());
                    whindexResults.add(fieldAndValue);
                }
            }
        }
    }
}
