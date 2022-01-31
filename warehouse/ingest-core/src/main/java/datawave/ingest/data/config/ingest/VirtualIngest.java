package datawave.ingest.data.config.ingest;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import datawave.data.type.NoOpType;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.marking.MarkingFunctions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.conf.Configuration;

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

public interface VirtualIngest {
    
    String VIRTUAL_FIELD_NAMES = VirtualFieldNormalizer.VIRTUAL_FIELD_NAMES;
    String VIRTUAL_FIELD_MEMBERS = VirtualFieldNormalizer.VIRTUAL_FIELD_MEMBERS;
    String VIRTUAL_FIELD_VALUE_SEPARATOR = VirtualFieldNormalizer.VIRTUAL_FIELD_VALUE_SEPARATOR;
    String VIRTUAL_FIELD_ALLOW_MISSING = VirtualFieldNormalizer.VIRTUAL_FIELD_ALLOW_MISSING;
    String VIRTUAL_FIELD_GROUPING_POLICY = VirtualFieldNormalizer.VIRTUAL_FIELD_GROUPING_POLICY;
    
    enum GroupingPolicy {
        SAME_GROUP_ONLY, GROUPED_WITH_NON_GROUPED, IGNORE_GROUPS
    }
    
    void setup(Configuration config) throws IllegalArgumentException;
    
    Map<String,String[]> getVirtualFieldDefinitions();
    
    void setVirtualFieldDefinitions(Map<String,String[]> virtualFieldDefinitions);
    
    String getDefaultVirtualFieldSeparator();
    
    void setDefaultVirtualFieldSeparator(String separator);
    
    boolean isVirtualIndexedField(String fieldName);
    
    Map<String,String[]> getVirtualNameAndIndex(String virtualFieldName);
    
    Multimap<String,NormalizedContentInterface> getVirtualFields(Multimap<String,NormalizedContentInterface> values);
    
    class VirtualFieldNormalizer extends NoOpType {
        private static final long serialVersionUID = -185193472535582555L;
        
        /**
         * Parameter for specifying the name of a virtual field. A virtual field is a field that does not exist in the raw data, but is derived from the values
         * of other fields in the raw data. The value of this parameter is a comma separated list of virtual field names. This parameter supports multiple
         * datatypes, so a valid value would be something like myDataType.data.combine.name
         */
        public static final String VIRTUAL_FIELD_NAMES = ".data.combine.name";
        
        /**
         * Parameter for specifying the fields that make up each virtual field. The value of this parameter is a period separated list of fields for each
         * virtual field. Multiple virtual fields can be specified by separating them with a comma. For example: "FIELD1.FIELD2,FIELD1.FIELD3" One can specify
         * constant strings to include in the mix by using quotes which will override including the default separator. "FIELD1.' and '.FIELD2" Constant strings
         * will always be included in the resulting value, however the default separator will only be included if not overridden by a constant AND there are
         * field values to separate. This parameter supports multiple datatypes, so a valid value would be something like myDataType.data.combine.fields
         */
        public static final String VIRTUAL_FIELD_MEMBERS = ".data.combine.fields";
        
        /**
         * Parameter that denotes the beginning of a separator
         */
        public static final String VIRTUAL_FIELD_VALUE_START_SEPATATOR = ".data.combine.start.separator";
        
        /**
         * Parameter that denotes the ending of a separator
         */
        
        public static final String VIRTUAL_FIELD_VALUE_END_SEPATATOR = ".data.combine.end.separator";
        
        /**
         * Parameter for specifying a separator in the virtual field to be used between the values of the field members. This parameter supports multiple
         * datatypes, so a valid value would be something like myDataType.data.combine.separator
         */
        public static final String VIRTUAL_FIELD_VALUE_SEPARATOR = ".data.combine.separator";
        
        /**
         * Boolean parameter for specifying a whether missing parts of a virtual field are permitted. If only one value is specified, the it applies to all of
         * the fields. Otherwise a value can be specified per virtual field (comma separated). This parameter supports multiple datatypes, so a valid value
         * would be something like myDataType.data.combine.allow.missing.
         */
        public static final String VIRTUAL_FIELD_ALLOW_MISSING = ".data.combine.allow.missing";
        
        /**
         * Parameter for specifying whether non-grouped fields can be combined with grouped fields. Grouped fields of different groups will never be combined
         * together, one may or may not want non-grouped fields (i.e. global in context) merged with grouped fields. The possible values are: SAME_GROUP_ONLY:
         * only fields of the same group will be merged together. Two non-grouped fields are considered GROUPED_WITH_NON_GROUPED: Grouped fields can be merged
         * with non-grouped fields. Two grouped fields in different groups will never be merged. IGNORE_GROUPS: Ignore grouping altogether. An group or
         * non-grouped can be merged. If only one value is specified, the it applies to all of the fields. Otherwise a value can be specified per virtual field
         * (comma separated). The default is GROUPED_WITH_NON_GROUPED This parameter supports multiple datatypes, so a valid value would be something like
         * myDataType.data.combine.grouped.with.nongrouped.
         */
        public static final String VIRTUAL_FIELD_GROUPING_POLICY = ".data.combine.grouping.policy";
        public static final GroupingPolicy DEFAULT_GROUPING_POLICY = GroupingPolicy.GROUPED_WITH_NON_GROUPED;
        
        /**
         *
         */
        public static final String VIRTUAL_FIELD_IGNORE_NORMALIZATION_ON_FIELD = ".data.combine.ignore.normalization.on.fields";
        
        protected Map<String,String[]> virtualFieldDefinitions = new HashMap<>();
        protected Map<String,Pattern> compiledFieldPatterns = null;
        protected String defaultSeparator = null;
        protected String defaultStartSeparator = null;
        protected String defaultEndSeparator = null;
        protected Map<String,GroupingPolicy> grouping = new HashMap<>();
        protected Map<String,Boolean> allowMissing = new HashMap<>();
        private Set<String> ignoreNormalizationForFields = new HashSet<>();
        
        private MarkingFunctions markingFunctions;
        
        public void setup(Type type, String instance, Configuration config) {
            
            markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
            
            String[] fieldNames = getStrings(type, instance, config, VIRTUAL_FIELD_NAMES, null);
            String[] fieldMembers = getStrings(type, instance, config, VIRTUAL_FIELD_MEMBERS, null);
            
            String[] groupingPolicies = getStrings(type, instance, config, VIRTUAL_FIELD_GROUPING_POLICY, new String[0]);
            String[] missingPolicies = getStrings(type, instance, config, VIRTUAL_FIELD_ALLOW_MISSING, new String[0]);
            defaultSeparator = get(type, instance, config, VIRTUAL_FIELD_VALUE_SEPARATOR, " ");
            defaultStartSeparator = get(type, instance, config, VIRTUAL_FIELD_VALUE_START_SEPATATOR, null);
            defaultEndSeparator = get(type, instance, config, VIRTUAL_FIELD_VALUE_END_SEPATATOR, null);
            
            String[] ignoreNormalization = getStrings(type, instance, config, VIRTUAL_FIELD_IGNORE_NORMALIZATION_ON_FIELD, null);
            Set<String> emptySet = Collections.emptySet();
            ignoreNormalizationForFields = (null != ignoreNormalization) ? cleanSet(ignoreNormalization) : emptySet;
            
            if (null == fieldNames && null == fieldMembers)
                return;
            
            if (null == fieldNames)
                throw new IllegalArgumentException(getConfPrefixes(type, instance)[0] + VIRTUAL_FIELD_NAMES + " must be specified.");
            
            if (null == fieldMembers)
                throw new IllegalArgumentException(getConfPrefixes(type, instance)[0] + VIRTUAL_FIELD_MEMBERS + " must be specified.");
            
            if (fieldNames.length != fieldMembers.length)
                throw new IllegalArgumentException("Virtual field names and members do not match. Fix configuration and try again.");
            
            if (groupingPolicies.length != 0 && groupingPolicies.length != 1 && groupingPolicies.length != fieldNames.length)
                throw new IllegalArgumentException(
                                "Virtual field names and grouping policies do not match.  Specify 0, 1, or the same number of policies. Fix configuration and try again.");
            
            if (missingPolicies.length != 0 && missingPolicies.length != 1 && missingPolicies.length != fieldNames.length)
                throw new IllegalArgumentException(
                                "Virtual field names and missing policies do not match.  Specify 0, 1, or the same number of policies. Fix configuration and try again.");
            
            if (fieldNames.length == 0)
                return;
            
            if (StringUtils.isEmpty(defaultSeparator) && StringUtils.isEmpty(defaultStartSeparator) && StringUtils.isEmpty(defaultEndSeparator))
                throw new IllegalArgumentException(getConfPrefixes(type, instance)[0] + VIRTUAL_FIELD_VALUE_SEPARATOR + " must be specified.");
            
            if (StringUtils.isEmpty(defaultStartSeparator) && StringUtils.isEmpty(defaultEndSeparator)) {
                this.defaultStartSeparator = this.defaultSeparator;
                this.defaultEndSeparator = "";
            }
            
            for (int i = 0; i < fieldNames.length; i++) {
                String name = fieldNames[i];
                
                String[] members = datawave.util.StringUtils.split(fieldMembers[i], '.');
                virtualFieldDefinitions.put(name, members);
                
                if (groupingPolicies.length == 0) {
                    grouping.put(name, DEFAULT_GROUPING_POLICY);
                } else if (groupingPolicies.length == 1) {
                    grouping.put(name, GroupingPolicy.valueOf(groupingPolicies[0]));
                } else {
                    grouping.put(name, GroupingPolicy.valueOf(groupingPolicies[i]));
                }
                
                if (missingPolicies.length == 0) {
                    allowMissing.put(name, Boolean.FALSE);
                } else if (missingPolicies.length == 1) {
                    allowMissing.put(name, Boolean.valueOf(missingPolicies[0]));
                } else {
                    allowMissing.put(name, Boolean.valueOf(missingPolicies[i]));
                }
            }
        }
        
        /**
         * A convenience routine to get a configuration value
         *
         * @param type
         * @param instance
         * @param config
         * @param key
         * @return The value, null if not available
         */
        protected String get(Type type, String instance, Configuration config, String key, String defaultVal) {
            for (String prefix : getConfPrefixes(type, instance)) {
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
         * @param instance
         * @param config
         * @param key
         * @return The value, null if not available
         */
        protected boolean getBoolean(Type type, String instance, Configuration config, String key, boolean defaultVal) {
            for (String prefix : getConfPrefixes(type, instance)) {
                String value = config.get(prefix + key, null);
                if (value != null) {
                    return Boolean.valueOf(value);
                }
            }
            return defaultVal;
        }
        
        /**
         * A convenience routine to get a configuration value
         *
         * @param type
         * @param instance
         * @param config
         * @param key
         * @return The value, null if not available
         */
        protected String[] getStrings(Type type, String instance, Configuration config, String key, String[] defaultVal) {
            for (String prefix : getConfPrefixes(type, instance)) {
                String[] value = config.getStrings(prefix + key, (String[]) null);
                if (value != null) {
                    String concatValue = "";
                    ArrayList<String> escapedValue = new ArrayList<>();
                    for (int i = 0; i < value.length; i++) {
                        concatValue += value[i];
                        if (i != value.length - 1)
                            concatValue += ",";
                    }
                    int start = 0;
                    int end = 0;
                    while (end < concatValue.length()) {
                        if (concatValue.charAt(end) == ',' && concatValue.charAt(end - 1) != '\\') {
                            escapedValue.add(concatValue.substring(start, end).replace("\\,", ","));
                            start = end + 1;
                        }
                        end++;
                    }
                    if (start != end)
                        escapedValue.add(concatValue.substring(start, end).replace("\\,", ","));
                    return escapedValue.toArray(new String[escapedValue.size()]);
                }
            }
            return defaultVal;
        }
        
        /**
         * Get the configuration key prefixes in precedence order: &lt;datatype&gt;.&lt;classname&gt;.&lt;instance&gt; &lt;datatype&gt;.&lt;classname&gt;
         * &lt;datatype&gt;.&lt;instance&gt; &lt;datatype&gt; all.&lt;classname&gt; all
         *
         * @param type
         * @param instance
         * @return
         */
        protected String[] getConfPrefixes(Type type, String instance) {
            List<String> prefixes = new ArrayList<>();
            // type specific ones first, then the "all" ones
            prefixes.addAll(Arrays.asList(getConfPrefixes(type.typeName(), instance)));
            prefixes.addAll(Arrays.asList(getConfPrefixes("all", null)));
            return prefixes.toArray(new String[prefixes.size()]);
        }
        
        private String[] getConfPrefixes(String type, String instance) {
            StringBuilder builder = new StringBuilder();
            builder.append(type);
            if (instance != null) {
                // <datatype>
                String str1 = builder.toString();
                builder.append('.').append(instance);
                // <datatype>.<instance>
                String str2 = builder.toString();
                builder.setLength(builder.length() - instance.length());
                builder.append(this.getClass().getSimpleName());
                // <datatype>.<classname>
                String str3 = builder.toString();
                builder.append('.').append(instance);
                // <datatype>.<classname>.<instance>
                String str4 = builder.toString();
                return new String[] {str4, str3, str2, str1};
            } else {
                // all
                String str1 = builder.toString();
                builder.append('.').append(this.getClass().getSimpleName());
                // all.<classname>
                String str2 = builder.toString();
                return new String[] {str2, str1};
            }
        }
        
        /**
         * A helper routine to merge markings maps when mergings fields of a NormalizedContentInterface
         *
         * @param markings1
         * @param markings2
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
        
        /**
         * Create the normalized form of a map of fields, and add the virtual fields as configured above.
         * 
         * @param fields
         * @return The multimap of normalized fields including virtual fields.
         */
        public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            for (Entry<String,String> field : fields.entries()) {
                eventFields.put(field.getKey(), new NormalizedFieldAndValue(field.getKey(), field.getValue()));
            }
            return normalizeMap(eventFields);
        }
        
        /**
         * Add the virtual fields as configured above to the existing normalized event fields. This is generally the main entry point to the virtual field
         * generation.
         * 
         * @param eventFields
         * @return The multimap of normalized fields including virtual fields.
         */
        public Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> eventFields) {
            Multimap<String,NormalizedContentInterface> virtualFields = HashMultimap.create();
            
            // lazily create the compilied field patterns
            if (this.compiledFieldPatterns == null)
                compilePatterns();
            
            if (this.virtualFieldDefinitions != null && !this.virtualFieldDefinitions.isEmpty()) {
                List<NormalizedContentInterface> tempResults = new ArrayList<>();
                
                // create the map that will hold the fields mapped by field grouping (lazily filled for fields when needed)
                Map<String,Multimap<VirtualFieldGrouping,NormalizedContentInterface>> groupedEventFields = new HashMap<>();
                
                // for each of the virtual field definitions, add the virtual fields.
                for (Entry<String,String[]> vFields : this.virtualFieldDefinitions.entrySet()) {
                    tempResults.clear();
                    GroupingPolicy groupingPolicy = grouping.get(vFields.getKey());
                    addVirtualFields(tempResults, eventFields, groupedEventFields, vFields.getKey(), null, null, groupingPolicy,
                                    allowMissing.get(vFields.getKey()), vFields.getValue(), 0, "", "", // separator is initially empty
                                    new StringBuilder(), new StringBuilder(), null);
                    for (NormalizedContentInterface value : tempResults) {
                        virtualFields.put(value.getIndexedFieldName(), value);
                    }
                }
            }
            
            return virtualFields;
        }
        
        /**
         * Add the grouped map of fields to the specified map if not already created.
         * 
         * @param eventFields
         * @param field
         * @param groupedEventFields
         */
        private void updateGroupedEventFields(Multimap<String,NormalizedContentInterface> eventFields, String field,
                        Map<String,Multimap<VirtualFieldGrouping,NormalizedContentInterface>> groupedEventFields) {
            if (!groupedEventFields.containsKey(field)) {
                groupedEventFields.put(field, groupFields(eventFields.get(field)));
            }
        }
        
        /**
         * Return the map of group to field for a list of fields.
         * 
         * @param fields
         * @return the map of grouping to fields.
         */
        private Multimap<VirtualFieldGrouping,NormalizedContentInterface> groupFields(Collection<NormalizedContentInterface> fields) {
            Multimap<VirtualFieldGrouping,NormalizedContentInterface> groups = HashMultimap.create();
            for (NormalizedContentInterface field : fields) {
                groups.put(getGrouping(field), field);
            }
            return groups;
        }
        
        /**
         * Compile the virual field definition patterns.
         */
        private void compilePatterns() {
            Map<String,Pattern> patterns = new HashMap<>();
            for (Entry<String,String[]> vFields : this.virtualFieldDefinitions.entrySet()) {
                if (vFields.getKey().indexOf('*') >= 0) {
                    patterns.put(vFields.getKey(), Pattern.compile(vFields.getKey().replace("*", "(.*)")));
                    for (String member : vFields.getValue()) {
                        if (member.indexOf('*') >= 0) {
                            patterns.put(member, Pattern.compile(member.replace("*", "(.*)")));
                        }
                    }
                }
            }
            compiledFieldPatterns = patterns;
        }
        
        /**
         * A class representing the group and subgroup for a field.
         */
        public class VirtualFieldGrouping {
            private final String group;
            private final String subGroup;
            
            public VirtualFieldGrouping(String group, String subGroup) {
                this.group = group;
                this.subGroup = subGroup;
            }
            
            public String getSubGroup() {
                return subGroup;
            }
            
            public String getGroup() {
                return group;
            }
            
            @Override
            public int hashCode() {
                return new HashCodeBuilder().append(group).append(subGroup).toHashCode();
            }
            
            @Override
            public boolean equals(Object obj) {
                if (obj instanceof VirtualFieldGrouping) {
                    VirtualFieldGrouping other = (VirtualFieldGrouping) obj;
                    return new EqualsBuilder().append(group, other.group).append(subGroup, other.subGroup).isEquals();
                }
                return false;
            }
            
            @Override
            public String toString() {
                return group + '.' + subGroup;
            }
        }
        
        /**
         * Create the virtual field from the event Fields members This is the workhorse method for this class. This is a recursive routine that will expand the
         * fields with next part of the virtual field definition at each stage. So if we have a pattern of FIELD_1.'-'.FIELD_2, then the first round will create
         * fields containing the FIELD_1 values, the second round will append the '-' constant and then add the FIELD_2 values. So the depth of recursion will
         * be equivalent to the number of fields in the virtual field definition plus 1 to add the virtual field to the map.
         *
         * @param virtualFields
         * @param eventFields
         * @param groupings
         * @param virtualFieldName
         * @param replacement
         * @param grouping
         * @param groupingPolicy
         * @param allowMissing
         * @param fields
         * @param pos
         * @param startSeparator
         * @param endSeparator
         * @param originalValue
         * @param normalizedValue
         * @param markings
         */
        public void addVirtualFields(List<NormalizedContentInterface> virtualFields, Multimap<String,NormalizedContentInterface> eventFields,
                        Map<String,Multimap<VirtualFieldGrouping,NormalizedContentInterface>> groupings, String virtualFieldName, String replacement,
                        VirtualFieldGrouping grouping, GroupingPolicy groupingPolicy, boolean allowMissing, String[] fields, int pos, String startSeparator,
                        String endSeparator, StringBuilder originalValue, StringBuilder normalizedValue, Map<String,String> markings) {
            String separator = "";
            // append any constants that have been specified
            while (pos < fields.length && isConstant(fields[pos])) {
                String constant = getConstant(fields[pos++]);
                originalValue.append(constant);
                normalizedValue.append(constant);
                // given we found a constant, drop the separator for this round
                startSeparator = "";
                endSeparator = "";
            }
            
            // if we have not exhausted the virtual field definition segments
            if (pos < fields.length) {
                String memberName = fields[pos];
                
                // determine if we have a field pattern
                if ((memberName.charAt(0) == '\'' && memberName.charAt(memberName.length() - 1) == '\'')
                                || (memberName.charAt(0) == '"' && memberName.charAt(memberName.length() - 1) == '"')) {
                    separator = memberName.substring(1, memberName.length() - 1);
                    startSeparator = separator;
                    pos++;
                    memberName = fields[pos + 1];
                }
                
                if (memberName.indexOf('*') >= 0 && replacement != null) {
                    memberName = memberName.replace("*", replacement);
                }
                
                // if we have a field pattern, the expanding with those fields that match the pattern
                if (compiledFieldPatterns.containsKey(memberName)) {
                    Pattern pattern = compiledFieldPatterns.get(memberName);
                    for (String key : eventFields.keySet()) {
                        Matcher matcher = pattern.matcher(key);
                        if (matcher.matches()) {
                            replacement = matcher.group(1);
                            int oLen = originalValue.length();
                            int nLen = normalizedValue.length();
                            boolean firstField = (oLen == 0 && nLen == 0);
                            
                            // for each field value that matches the grouping policy for this virtual field definition
                            for (NormalizedContentInterface value : getEventFields(firstField, key, groupingPolicy, grouping, eventFields, groupings)) {
                                
                                // get the new grouping if any
                                VirtualFieldGrouping newGrouping = getGrouping(value, groupingPolicy);
                                
                                // add the value to the virtual field we are building
                                originalValue.append(startSeparator);
                                originalValue.append(value.getEventFieldValue());
                                originalValue.append(endSeparator);
                                normalizedValue.append(startSeparator);
                                normalizedValue.append(value.getIndexedFieldValue());
                                normalizedValue.append(endSeparator);
                                
                                // recurse for the next virtual field definition segment
                                addVirtualFields(virtualFields, eventFields, groupings, virtualFieldName.replace("*", replacement), replacement,
                                                (grouping == null ? newGrouping : grouping), groupingPolicy, allowMissing, fields, pos + 1,
                                                this.defaultStartSeparator, this.defaultEndSeparator, originalValue, normalizedValue,
                                                mergeMarkings(markings, value.getMarkings()));
                                
                                // reset the values to the original length
                                originalValue.setLength(oLen);
                                normalizedValue.setLength(nLen);
                            }
                        }
                    }
                    
                } else if (!eventFields.containsKey(memberName) && allowMissing) {
                    // recurse for the next virtual field definition segment in the case that we are adding missing fields
                    addVirtualFields(virtualFields, eventFields, groupings, virtualFieldName, replacement, grouping, groupingPolicy, allowMissing, fields,
                                    pos + 1, startSeparator, endSeparator, originalValue, normalizedValue, markings);
                    
                } else {
                    int oLen = originalValue.length();
                    int nLen = normalizedValue.length();
                    boolean firstField = (oLen == 0 && nLen == 0);
                    
                    // for each value for this field
                    for (NormalizedContentInterface value : getEventFields(firstField, memberName, groupingPolicy, grouping, eventFields, groupings)) {
                        VirtualFieldGrouping newGrouping = getGrouping(value, groupingPolicy);
                        
                        // append the value
                        originalValue.append(startSeparator);
                        originalValue.append(value.getEventFieldValue());
                        originalValue.append(endSeparator);
                        normalizedValue.append(startSeparator);
                        if (ignoreNormalizationForFields.contains(value.getIndexedFieldName())) {
                            normalizedValue.append(value.getEventFieldValue());
                        } else {
                            normalizedValue.append(value.getIndexedFieldValue());
                        }
                        normalizedValue.append(endSeparator);
                        
                        // recurse on the next virtual field segment
                        addVirtualFields(virtualFields, eventFields, groupings, virtualFieldName, replacement, (grouping == null ? newGrouping : grouping),
                                        groupingPolicy, allowMissing, fields, pos + 1, this.defaultStartSeparator, this.defaultEndSeparator, originalValue,
                                        normalizedValue, mergeMarkings(markings, value.getMarkings()));
                        
                        // reset the values to the original length
                        originalValue.setLength(oLen);
                        normalizedValue.setLength(nLen);
                    }
                }
            } else if (originalValue.length() > 0) {
                // this is the tail of the recursion where we will actually add the value to the map of virtual fields.
                NormalizedFieldAndValue n = new NormalizedFieldAndValue();
                n.setFieldName(virtualFieldName);
                n.setEventFieldValue(originalValue.toString());
                n.setIndexedFieldValue(normalizedValue.toString());
                n.setMarkings(markings);
                if (grouping != null) {
                    n.setGrouped(true);
                    n.setGroup(grouping.getGroup());
                    n.setSubGroup(grouping.getSubGroup());
                }
                virtualFields.add(n);
            }
        }
        
        /**
         * This will get the list of values given for the given grouping policy. The cached grouping map will be updated if needed to satisfy this call.
         * 
         * @param firstField
         * @param field
         * @param groupingPolicy
         * @param grouping
         * @param eventFields
         * @param groupings
         * @return
         */
        private Iterable<NormalizedContentInterface> getEventFields(boolean firstField, String field, GroupingPolicy groupingPolicy,
                        VirtualFieldGrouping grouping, Multimap<String,NormalizedContentInterface> eventFields,
                        Map<String,Multimap<VirtualFieldGrouping,NormalizedContentInterface>> groupings) {
            if (firstField) {
                return eventFields.get(field);
            }
            switch (groupingPolicy) {
                case GROUPED_WITH_NON_GROUPED:
                    updateGroupedEventFields(eventFields, field, groupings);
                    if (grouping == null) {
                        // if this grouping is null, then we can match with anything
                        return eventFields.get(field);
                    } else {
                        // if we have a grouping, then we can match those with the same grouping or no grouping
                        return Iterables.concat(groupings.get(field).get(null), groupings.get(field).get(grouping));
                    }
                case SAME_GROUP_ONLY:
                    updateGroupedEventFields(eventFields, field, groupings);
                    // only return those with the same grouping
                    return groupings.get(field).get(grouping);
                default:
                    // by default grouping does not matter
                    return eventFields.get(field);
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
        
        private VirtualFieldGrouping getGrouping(NormalizedContentInterface value, GroupingPolicy groupingPolicy) {
            if (!groupingPolicy.equals(GroupingPolicy.IGNORE_GROUPS)) {
                return getGrouping(value);
            }
            return null;
        }
        
        private VirtualFieldGrouping getGrouping(NormalizedContentInterface value) {
            if (value instanceof GroupedNormalizedContentInterface) {
                if (((GroupedNormalizedContentInterface) value).isGrouped()) {
                    return new VirtualFieldGrouping(((GroupedNormalizedContentInterface) value).getGroup(),
                                    ((GroupedNormalizedContentInterface) value).getSubGroup());
                }
            }
            return null;
        }
        
        public Map<String,String[]> getVirtualFieldDefinitions() {
            return virtualFieldDefinitions;
        }
        
        public void setVirtualFieldDefinitions(Map<String,String[]> virtualFieldDefinitions) {
            this.virtualFieldDefinitions = virtualFieldDefinitions;
        }
        
        public String getDefaultSeparator() {
            return defaultSeparator;
        }
        
        public void setDefaultSeparator(String sep) {
            this.defaultSeparator = sep;
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
