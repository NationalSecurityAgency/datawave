package datawave.ingest.data.config.ingest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.marking.MarkingFunctions;

/**
 * Similar to VirtualIngest virtual fields, but composite fields are not written in the event section of the shard table (only in the field index section)
 *
 */
public interface CompositeIngest {

    String COMPOSITE_FIELD_MAP = CompositeFieldNormalizer.COMPOSITE_FIELD_MAP;
    String COMPOSITE_FIELD_SEPARATOR = CompositeFieldNormalizer.COMPOSITE_FIELD_SEPARATOR;
    String DEFAULT_SEPARATOR = CompositeFieldNormalizer.DEFAULT_SEPARATOR;
    String COMPOSITE_FIELD_ALLOW_MISSING = CompositeFieldNormalizer.COMPOSITE_FIELD_ALLOW_MISSING;
    String COMPOSITE_FIELD_GROUPING_POLICY = CompositeFieldNormalizer.COMPOSITE_FIELD_GROUPING_POLICY;

    enum GroupingPolicy {
        SAME_GROUP_ONLY, GROUPED_WITH_NON_GROUPED, IGNORE_GROUPS
    }

    void setup(Configuration config) throws IllegalArgumentException;

    Multimap<String,String> getCompositeFieldDefinitions();

    Map<String,String> getCompositeFieldSeparators();

    void setCompositeFieldDefinitions(Multimap<String,String> compositeFieldDefinitions);

    boolean isCompositeField(String fieldName);

    boolean isOverloadedCompositeField(String fieldName);

    Multimap<String,NormalizedContentInterface> getCompositeFields(Multimap<String,NormalizedContentInterface> fields);

    static boolean isOverloadedCompositeField(Map<String,String[]> compositeFieldDefinitions, String compositeFieldName) {
        return isOverloadedCompositeField(Arrays.asList(compositeFieldDefinitions.get(compositeFieldName)), compositeFieldName);
    }

    static boolean isOverloadedCompositeField(Multimap<String,String> compositeFieldDefinitions, String compositeFieldName) {
        return isOverloadedCompositeField(compositeFieldDefinitions.get(compositeFieldName), compositeFieldName);
    }

    static boolean isOverloadedCompositeField(Collection<String> compFields, String compositeFieldName) {
        if (compFields != null && !compFields.isEmpty()) {
            return compFields.stream().findFirst().get().equals(compositeFieldName);
        }
        return false;
    }

    class CompositeFieldNormalizer {

        private static final long serialVersionUID = -3892470989028896718L;
        private static final Logger log = Logger.getLogger(CompositeFieldNormalizer.class);

        private static final String DEFAULT_SEPARATOR = new String(Character.toChars(Character.MAX_CODE_POINT));

        /**
         * Parameter for specifying the component fields that make up each composite field. The value of this parameter is a comma separated list of component
         * fields for each composite field.
         *
         * Example: Key: "myType.COMPOSITE_FIELD_NAME.data.composite.field.map" Value: "FIELD1,FIELD2"
         */
        public static final String COMPOSITE_FIELD_MAP = ".data.composite.field.map";

        /**
         * Parameter for specifying the separator override to use when combining component fields into a composite field. By default, the max code point
         * character will be used. The use of regex meta characters is currently NOT supported. You are also advised against using the null character "\0" as
         * this is used commonly in the index.
         *
         * Example: Key: "myType.COMPOSITE_FIELD_NAME.data.composite.separator" Value: "_"
         */
        public static final String COMPOSITE_FIELD_SEPARATOR = ".data.composite.field.separator";

        /**
         * Boolean parameter for specifying a whether missing parts of a composite field are permitted. These values are specified per composite field.
         *
         * Example: Key: "myType.COMPOSITE_FIELD_NAME.data.composite.allow.missing" Value: "SAME_GROUP_ONLY"
         */
        public static final String COMPOSITE_FIELD_ALLOW_MISSING = ".data.composite.allow.missing";

        /**
         * Parameter for specifying whether non-grouped fields can be combined with grouped fields. Grouped fields of different groups will never be combined
         * together, one may or may not want non-grouped fields (i.e. global in context) merged with grouped fields. The possible values are: SAME_GROUP_ONLY:
         * only fields of the same group will be merged together. Two non-grouped fields are considered GROUPED_WITH_NON_GROUPED: Grouped fields can be merged
         * with non-grouped fields. Two grouped fields in different groups will never be merged. IGNORE_GROUPS: Ignore grouping altogether. A group or
         * non-grouped can be merged. The default is GROUPED_WITH_NON_GROUPED.
         *
         * Example: Key: "myType.COMPOSITE_FIELD_NAME.data.composite.grouping.policy" Value: "true"
         */
        public static final String COMPOSITE_FIELD_GROUPING_POLICY = ".data.composite.grouping.policy";
        public static final GroupingPolicy DEFAULT_GROUPING_POLICY = GroupingPolicy.GROUPED_WITH_NON_GROUPED;

        /**
         *
         */
        public static final String COMPOSITE_FIELD_IGNORE_NORMALIZATION_ON_FIELD = ".data.composite.ignore.normalization.on.fields";

        protected Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        protected Map<String,String> compositeSeparator = new HashMap<>();
        protected Map<String,Pattern> compiledFieldPatterns = null;
        protected Map<String,GroupingPolicy> grouping = new HashMap<>();
        protected Map<String,Boolean> allowMissing = new HashMap<>();
        private Set<String> ignoreNormalizationForFields = new HashSet<>();

        protected MarkingFunctions markingFunctions;

        public void setup(Type type, Configuration config) {

            markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();

            Set<String> indexOnly = Sets.newHashSet(getStrings(type, config, BaseIngestHelper.INDEX_ONLY_FIELDS, new String[0]));

            for (String typeName : new String[] {"all", type.typeName()}) {

                // determine the mapping from composite field to component fields
                String fieldMapKey = (typeName + ".*" + COMPOSITE_FIELD_MAP).replaceAll("\\.", "\\.");
                for (Entry<String,String> entry : config.getValByRegex(fieldMapKey).entrySet()) {
                    String compositeField = entry.getKey().substring(typeName.length() + 1, entry.getKey().indexOf(COMPOSITE_FIELD_MAP));
                    List<String> componentFields = Arrays.stream(entry.getValue().replaceAll("\\s+", "").split(",")).filter(x -> !x.isEmpty())
                                    .collect(Collectors.toList());

                    // if any members are indexOnly fields, skip this one
                    if (!Sets.intersection(Sets.newHashSet(componentFields), indexOnly).isEmpty()) {
                        log.warn("rejecting " + compositeField + " which includes index only field in " + indexOnly);
                        continue;
                    }

                    if (!compositeField.isEmpty() && !componentFields.isEmpty()) {
                        if (compositeToFieldMap.containsKey(compositeField))
                            compositeToFieldMap.replaceValues(compositeField, componentFields);
                        else
                            compositeToFieldMap.putAll(compositeField, componentFields);
                    }

                    // determine whether a custom separator is being used
                    String separator = config.get(typeName + "." + compositeField + COMPOSITE_FIELD_SEPARATOR);
                    if (separator != null)
                        compositeSeparator.put(compositeField, separator);
                    else
                        compositeSeparator.putIfAbsent(compositeField, DEFAULT_SEPARATOR);

                    // determine whether a custom grouping policy is being used
                    String groupingPolicy = config.get(typeName + "." + compositeField + COMPOSITE_FIELD_GROUPING_POLICY);
                    if (groupingPolicy != null)
                        grouping.put(compositeField, GroupingPolicy.valueOf(groupingPolicy));
                    else
                        grouping.putIfAbsent(compositeField, DEFAULT_GROUPING_POLICY);

                    // determine whether a custom missing policy is being used
                    String missingPolicy = config.get(typeName + "." + compositeField + COMPOSITE_FIELD_ALLOW_MISSING);
                    if (missingPolicy != null)
                        allowMissing.put(compositeField, Boolean.valueOf(missingPolicy));
                    else
                        allowMissing.putIfAbsent(compositeField, Boolean.FALSE);
                }
            }

            String[] ignoreNormalization = getStrings(type, config, COMPOSITE_FIELD_IGNORE_NORMALIZATION_ON_FIELD, null);
            Set<String> emptySet = Collections.emptySet();
            ignoreNormalizationForFields = (null != ignoreNormalization) ? cleanSet(ignoreNormalization) : emptySet;

            log.debug("setup with composites " + this.compositeToFieldMap);
        }

        /**
         * A convenience routine to get a configuration value
         *
         * @param type
         *            a {@link Type}
         * @param config
         *            a hadoop {@link Configuration}
         * @param key
         *            the key
         * @param defaultVal
         *            the default value to use if no value is found in the configuration
         * @return The value, or the default value provided
         */
        protected String get(Type type, Configuration config, String key, String defaultVal) {
            for (String prefix : getConfPrefixes(type)) {
                String value = config.get(prefix + key, null);
                if (value != null) {
                    return value;
                }
            }
            return defaultVal;
        }

        /**
         * A convenience routine to get a configuration value
         *
         * @param type
         *            a {@link Type}
         * @param config
         *            a hadoop {@link Configuration}
         * @param key
         *            the key
         * @param defaultVal
         *            the default value to use if no value is found in the configuration
         * @return The value, null if not available
         */
        protected boolean getBoolean(Type type, Configuration config, String key, boolean defaultVal) {
            for (String prefix : getConfPrefixes(type)) {
                String value = config.get(prefix + key, null);
                if (value != null) {
                    return Boolean.parseBoolean(value);
                }
            }
            return defaultVal;
        }

        /**
         * A convenience routine to get a configuration value
         *
         * @param type
         *            the {@link Type}
         * @param config
         *            a hadoop {@link Configuration}
         * @param key
         *            the key
         * @param defaultVal
         *            an array of default values
         * @return The value, null if not available
         */
        protected String[] getStrings(Type type, Configuration config, String key, String[] defaultVal) {
            for (String prefix : getConfPrefixes(type)) {
                String[] value = config.getStrings(prefix + key, (String[]) null);
                if (value != null) {
                    return value;
                }
            }
            return defaultVal;
        }

        /**
         * Get the configuration key prefixes in precedence order: &lt;datatype&gt;.&lt;classname&gt;.&lt;instance&gt; &lt;datatype&gt;.&lt;classname&gt;
         * &lt;datatype&gt;.&lt;instance&gt; &lt;datatype&gt; all.&lt;classname&gt; all
         *
         * @param type
         *            the {@link Type}
         * @return an array of prefixes in order of precedent
         */
        protected String[] getConfPrefixes(Type type) {
            List<String> prefixes = new ArrayList<>();
            // type specific ones first, then the "all" ones
            prefixes.addAll(Arrays.asList(getConfPrefixes(type.typeName())));
            prefixes.addAll(Arrays.asList(getConfPrefixes("all")));
            return prefixes.toArray(new String[prefixes.size()]);
        }

        private String[] getConfPrefixes(String type) {
            StringBuilder builder = new StringBuilder();
            builder.append(type);
            String str1 = builder.toString();
            builder.append('.').append(this.getClass().getSimpleName());
            String str2 = builder.toString();
            return new String[] {str2, str1};
        }

        /**
         * A helper routine to merge markings maps when merging fields of a NormalizedContentInterface
         *
         * @param markings1
         *            a map of markings
         * @param markings2
         *            a different map of markings
         * @return the merged markings
         */
        protected Map<String,String> mergeMarkings(Map<String,String> markings1, Map<String,String> markings2) {
            if (markings2 != null) {
                if (markings1 == null) {
                    markings1 = markings2;
                } else {
                    try {
                        markings1 = markingFunctions.combine(markings1, markings2);
                    } catch (MarkingFunctions.Exception e) {
                        throw new RuntimeException("Unable to combine markings.", e);
                    }
                }
            }
            return markings1;
        }

        public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            for (Entry<String,String> field : fields.entries()) {
                eventFields.put(field.getKey(), new NormalizedFieldAndValue(field.getKey(), field.getValue()));
            }
            return normalizeMap(eventFields);
        }

        public Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> eventFields) {
            Multimap<String,NormalizedContentInterface> compositeFields = HashMultimap.create();

            if (this.compiledFieldPatterns == null)
                compilePatterns();
            if (this.compositeToFieldMap != null && !this.compositeToFieldMap.isEmpty()) {
                List<NormalizedContentInterface> tempResults = new ArrayList<>();
                for (String compositeField : this.compositeToFieldMap.keySet()) {
                    tempResults.clear();
                    addCompositeFields(tempResults, eventFields, compositeField, null, null, GroupingPolicy.IGNORE_GROUPS, allowMissing.get(compositeField),
                                    this.compositeToFieldMap.get(compositeField).toArray(new String[0]), 0, new StringBuilder(), new StringBuilder(), null,
                                    CompositeIngest.isOverloadedCompositeField(this.compositeToFieldMap, compositeField));
                    for (NormalizedContentInterface value : tempResults) {
                        compositeFields.put(value.getIndexedFieldName(), value);
                    }
                }
            }

            return compositeFields;
        }

        private void compilePatterns() {
            Map<String,Pattern> patterns = new HashMap<>();
            for (String compositeField : this.compositeToFieldMap.keySet()) {
                if (compositeField.indexOf('*') >= 0) {
                    patterns.put(compositeField, Pattern.compile(compositeField.replace("*", "(.*)")));
                    for (String member : this.compositeToFieldMap.get(compositeField)) {
                        if (member.indexOf('*') >= 0) {
                            patterns.put(member, Pattern.compile(member.replace("*", "(.*)")));
                        }
                    }
                }
            }
            compiledFieldPatterns = patterns;
        }

        // Create the composite field from the event Fields members
        public void addCompositeFields(List<NormalizedContentInterface> compositeFields, Multimap<String,NormalizedContentInterface> eventFields,
                        String compositeFieldName, String replacement, String[] grouping, GroupingPolicy groupingPolicy, boolean allowMissing, String[] fields,
                        int pos, StringBuilder originalValue, StringBuilder normalizedValue, Map<String,String> markings, boolean isOverloadedField) {
            String separator = (pos > 0) ? compositeSeparator.get(compositeFieldName) : "";
            // append any constants that have been specified
            while (pos < fields.length && isConstant(fields[pos])) {
                String constant = getConstant(fields[pos]);
                if (!isOverloadedField || (isOverloadedField && pos == 0))
                    originalValue.append(constant);
                normalizedValue.append(constant);
                // given we found a constant, drop the separator for this round
                separator = "";
                pos++;
            }

            if (pos < fields.length) {
                String memberName = fields[pos];

                if ((memberName.charAt(0) == '\'' && memberName.charAt(memberName.length() - 1) == '\'')
                                || (memberName.charAt(0) == '"' && memberName.charAt(memberName.length() - 1) == '"')) {
                    separator = memberName.substring(1, memberName.length() - 1);
                    pos++;
                    memberName = fields[pos + 1];
                }

                if (memberName.indexOf('*') >= 0 && replacement != null) {
                    memberName = memberName.replace("*", replacement);
                }
                if (compiledFieldPatterns.containsKey(memberName)) {
                    Pattern pattern = compiledFieldPatterns.get(memberName);
                    for (String key : eventFields.keySet()) {
                        Matcher matcher = pattern.matcher(key);
                        if (matcher.matches()) {
                            replacement = matcher.group(1);
                            int oLen = originalValue.length();
                            int nLen = normalizedValue.length();
                            for (NormalizedContentInterface value : eventFields.get(key)) {
                                String[] newGrouping = getGrouping(value, groupingPolicy);
                                if (oLen != 0 || nLen != 0) {
                                    if (!matchingGrouping(grouping, newGrouping, groupingPolicy)) {
                                        continue;
                                    }
                                }
                                // ensure that we have a matching nesting level if required
                                if (!isOverloadedField || (isOverloadedField && pos == 0)) {
                                    originalValue.append(separator);
                                    originalValue.append(value.getEventFieldValue());
                                }
                                normalizedValue.append(separator);
                                normalizedValue.append(value.getIndexedFieldValue());
                                if (pos + 1 < fields.length) {
                                    addCompositeFields(compositeFields, eventFields, compositeFieldName.replace("*", replacement), replacement,
                                                    (grouping == null ? newGrouping : grouping), groupingPolicy, allowMissing, fields, pos + 1, originalValue,
                                                    normalizedValue, mergeMarkings(markings, value.getMarkings()), isOverloadedField);
                                }
                                originalValue.setLength(oLen);
                                normalizedValue.setLength(nLen);
                            }
                        }
                    }
                } else if (!eventFields.containsKey(memberName) && allowMissing) {
                    addCompositeFields(compositeFields, eventFields, compositeFieldName, replacement, grouping, groupingPolicy, allowMissing, fields, pos + 1,
                                    originalValue, normalizedValue, markings, isOverloadedField);
                } else {
                    int oLen = originalValue.length();
                    int nLen = normalizedValue.length();
                    for (NormalizedContentInterface value : eventFields.get(memberName)) {
                        String[] newGrouping = getGrouping(value, groupingPolicy);
                        if (oLen != 0 || nLen != 0) {
                            if (!matchingGrouping(grouping, newGrouping, groupingPolicy)) {
                                continue;
                            }
                        }
                        if (!isOverloadedField || (isOverloadedField && pos == 0)) {
                            originalValue.append(separator);
                            originalValue.append(value.getEventFieldValue());
                        }
                        normalizedValue.append(separator);
                        if (ignoreNormalizationForFields.contains(value.getIndexedFieldName())) {
                            normalizedValue.append(value.getEventFieldValue());
                        } else {
                            normalizedValue.append(value.getIndexedFieldValue());
                        }
                        addCompositeFields(compositeFields, eventFields, compositeFieldName, replacement, (grouping == null ? newGrouping : grouping),
                                        groupingPolicy, allowMissing, fields, pos + 1, originalValue, normalizedValue,
                                        mergeMarkings(markings, value.getMarkings()), isOverloadedField);
                        originalValue.setLength(oLen);
                        normalizedValue.setLength(nLen);
                    }
                }
            } else if (originalValue.length() > 0) {
                NormalizedFieldAndValue n = new NormalizedFieldAndValue();
                n.setFieldName(compositeFieldName);
                n.setEventFieldValue(originalValue.toString());
                n.setIndexedFieldValue(normalizedValue.toString());
                n.setMarkings(markings);
                if (grouping != null) {
                    n.setGrouped(true);
                    n.setGroup(grouping[0]);
                    n.setSubGroup(grouping[1]);
                }
                compositeFields.add(n);
            }
        }

        private boolean isConstant(String name) {
            char start = name.charAt(0);
            char end = name.charAt(name.length() - 1);
            return (start == end && (start == '\'' || start == '"'));
        }

        private String getConstant(String name) {
            return name.substring(1, name.length() - 1);
        }

        private boolean matchingGrouping(String[] grouping, String[] newGrouping, GroupingPolicy groupingPolicy) {
            switch (groupingPolicy) {
                case SAME_GROUP_ONLY:
                    if (grouping == null) {
                        return (newGrouping == null);
                    } else if (newGrouping == null) {
                        return false;
                    } else {
                        return (equals(grouping[0], newGrouping[0]) && equals(grouping[1], newGrouping[1]));
                    }
                case GROUPED_WITH_NON_GROUPED:
                    if (grouping == null || newGrouping == null) {
                        return true;
                    } else {
                        return (equals(grouping[0], newGrouping[0]) && equals(grouping[1], newGrouping[1]));
                    }
                case IGNORE_GROUPS:
                    return true;
                default:
                    throw new NotImplementedException("Cannot handle the " + groupingPolicy + " grouping policy");
            }
        }

        private boolean equals(String s1, String s2) {
            return Objects.equal(s1, s2);
        }

        private String[] getGrouping(NormalizedContentInterface value, GroupingPolicy groupingPolicy) {
            if (!groupingPolicy.equals(GroupingPolicy.IGNORE_GROUPS)) {
                if (value instanceof GroupedNormalizedContentInterface) {
                    if (((GroupedNormalizedContentInterface) value).isGrouped()) {
                        return new String[] {((GroupedNormalizedContentInterface) value).getGroup(), ((GroupedNormalizedContentInterface) value).getSubGroup()};
                    }
                }
            }
            return null;
        }

        public Multimap<String,String> getCompositeToFieldMap() {
            return compositeToFieldMap;
        }

        public Map<String,String> getCompositeFieldSeparators() {
            return compositeSeparator;
        }

        public void setCompositeToFieldMap(Multimap<String,String> compositeToFieldMap) {
            this.compositeToFieldMap = compositeToFieldMap;
        }

        public static Set<String> cleanSet(String[] items) {
            Set<String> itemSet = new HashSet<>();
            for (String item : items) {
                itemSet.add(item.trim());
            }
            return itemSet;
        }
    }
}
