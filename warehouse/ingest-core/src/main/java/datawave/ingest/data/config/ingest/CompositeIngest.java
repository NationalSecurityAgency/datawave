package datawave.ingest.data.config.ingest;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.marking.MarkingFunctions;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Similar to VirtualIngest virtual fields, but composite fields are not written in the event section of the shard table (only in the field index section)
 * 
 */
public interface CompositeIngest {
    
    String COMPOSITE_FIELD_NAMES = CompositeFieldNormalizer.COMPOSITE_FIELD_NAMES;
    String COMPOSITE_FIELD_MEMBERS = CompositeFieldNormalizer.COMPOSITE_FIELD_MEMBERS;
    String COMPOSITE_FIELDS_FIXED_LENGTH = CompositeFieldNormalizer.COMPOSITE_FIELDS_FIXED_LENGTH;
    String COMPOSITE_FIELDS_TRANSITION_DATES = CompositeFieldNormalizer.COMPOSITE_FIELDS_TRANSITION_DATES;
    String COMPOSITE_FIELD_ALLOW_MISSING = CompositeFieldNormalizer.COMPOSITE_FIELD_ALLOW_MISSING;
    String COMPOSITE_FIELD_GROUPING_POLICY = CompositeFieldNormalizer.COMPOSITE_FIELD_GROUPING_POLICY;
    String COMPOSITE_DEFAULT_SEPARATOR = new String(Character.toChars(Character.MAX_CODE_POINT));
    
    enum GroupingPolicy {
        SAME_GROUP_ONLY, GROUPED_WITH_NON_GROUPED, IGNORE_GROUPS
    }
    
    void setup(Configuration config) throws IllegalArgumentException;
    
    Map<String,String[]> getCompositeFieldDefinitions();
    
    void setCompositeFieldDefinitions(Map<String,String[]> compositeFieldDefinitions);
    
    boolean isCompositeField(String fieldName);
    
    boolean isFixedLengthCompositeField(String fieldName);
    
    boolean isTransitionedCompositeField(String fieldName);
    
    Date getCompositeFieldTransitionDate(String fieldName);
    
    boolean isOverloadedCompositeField(String fieldName);
    
    Multimap<String,NormalizedContentInterface> getCompositeFields(Multimap<String,NormalizedContentInterface> fields);
    
    static boolean isOverloadedCompositeField(Map<String,String[]> compositeFieldDefinitions, String compositeFieldName) {
        return isOverloadedCompositeField(Arrays.asList(compositeFieldDefinitions.get(compositeFieldName)), compositeFieldName);
    }
    
    static boolean isOverloadedCompositeField(Multimap<String,String> compositeFieldDefinitions, String compositeFieldName) {
        return isOverloadedCompositeField(compositeFieldDefinitions.get(compositeFieldName), compositeFieldName);
    }
    
    static boolean isOverloadedCompositeField(Collection<String> compFields, String compositeFieldName) {
        if (compFields != null && compFields.size() > 0)
            return compFields.stream().findFirst().get().equals(compositeFieldName);
        return false;
    }
    
    class CompositeFieldNormalizer {
        
        private static final long serialVersionUID = -3892470989028896718L;
        private static final Logger log = Logger.getLogger(CompositeFieldNormalizer.class);
        
        public static final String formatPattern = "yyyyMMdd HHmmss.SSS";
        public static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);
        
        /**
         * Parameter for specifying the name of a composite field. A composite field is a field that does not exist in the raw data, but is derived from the
         * values of other fields in the raw data. The value of this parameter is a comma separated list of composite field names. This parameter supports
         * multiple datatypes, so a valid value would be something like mydatatype.data.composite.name
         * 
         */
        public static final String COMPOSITE_FIELD_NAMES = ".data.composite.name";
        
        /**
         * Parameter for specifying the fields that make up each composite field. The value of this parameter is a period separated list of fields for each
         * composite field. Multiple composite fields can be specified by separating them with a comma. For example: "FIELD1.FIELD2,FIELD1.FIELD3" One can
         * specify constant strings to include in the mix by using quotes which will override including the default separator. "FIELD1.' and '.FIELD2" Constant
         * strings will always be included in the resulting value, however the default separator will only be included if not overridden by a constant AND there
         * are field values to separate. This parameter supports multiple datatypes, so a valid value would be something like mydatatype.data.composite.fields
         */
        public static final String COMPOSITE_FIELD_MEMBERS = ".data.composite.fields";
        
        /**
         * Parameter for specifying which fields generate queries against ranges whose terms are of fixed length. This becomes important during the query
         * planning phase when trying to create composite ranges. Composite ranges will only be generated if all terms in the composite but the final term
         * generate queries against ranges whose terms and values are of fixed length. GeoWave query ranges are a good example of this.
         *
         * This is represented as a comma separated list of said fields.
         */
        public static final String COMPOSITE_FIELDS_FIXED_LENGTH = ".data.composite.fields.fixed.length";
        
        /**
         * Parameter for specifying which fields have been transitioned from non-composite to overloaded composite fields, and when. The 'when' is important,
         * because for queries which span the transition date, we will need to expand our composite range to include both composite and non-composite terms.
         * This also affects how and which iterators will be run against the data.
         *
         * This is represented as a comma separated list of fieldName􏿿date, with the separator being Composite.START_SEPARATOR, and the date being of the
         * format yyyyMMdd HHmmss.SSS.
         */
        public static final String COMPOSITE_FIELDS_TRANSITION_DATES = ".data.composite.fields.transition.dates";
        
        /**
         * Boolean parameter for specifying a whether missing parts of a composite field are permitted. If only one value is specified, the it applies to all of
         * the fields. Otherwise a value can be specified per composite field (comma separated). This parameter supports multiple datatypes, so a valid value
         * would be something like mydatatype.data.composite.allow.missing.
         */
        public static final String COMPOSITE_FIELD_ALLOW_MISSING = ".data.composite.allow.missing";
        
        /**
         * Parameter for specifying whether non-grouped fields can be combined with grouped fields. Grouped fields of different groups will never be combined
         * together, one may or may not want non-grouped fields (i.e. global in context) merged with grouped fields. The possible values are: SAME_GROUP_ONLY:
         * only fields of the same group will be merged together. Two non-grouped fields are considered GROUPED_WITH_NON_GROUPED: Grouped fields can be merged
         * with non-grouped fields. Two grouped fields in different groups will never be merged. IGNORE_GROUPS: Ignore grouping altogether. An group or
         * non-grouped can be merged. If only one value is specified, the it applies to all of the fields. Otherwise a value can be specified per composite
         * field (comma separated). The default is GROUPED_WITH_NON_GROUPED This parameter supports multiple datatypes, so a valid value would be something like
         * mydatatype.data.composite.grouped.with.nongrouped.
         */
        public static final String COMPOSITE_FIELD_GROUPING_POLICY = ".data.composite.grouping.policy";
        public static final GroupingPolicy DEFAULT_GROUPING_POLICY = GroupingPolicy.GROUPED_WITH_NON_GROUPED;
        
        /**
         *
         */
        public static final String COMPOSITE_FIELD_IGNORE_NORMALIZATION_ON_FIELD = ".data.composite.ignore.normalization.on.fields";
        
        protected Map<String,String[]> compositeFieldDefinitions = new HashMap<>();
        protected Map<String,Pattern> compiledFieldPatterns = null;
        protected Set<String> fixedLengthFields = new HashSet<>();
        protected Map<String,Date> fieldTransitionDateMap = null;
        protected Map<String,GroupingPolicy> grouping = new HashMap<>();
        protected Map<String,Boolean> allowMissing = new HashMap<>();
        private Set<String> ignoreNormalizationForFields = new HashSet<>();
        
        protected MarkingFunctions markingFunctions;
        
        public void setup(Type type, String instance, Configuration config) {
            
            markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
            
            String[] fieldNames = getStrings(type, instance, config, COMPOSITE_FIELD_NAMES, null);
            String[] fieldMembers = getStrings(type, instance, config, COMPOSITE_FIELD_MEMBERS, null);
            
            String[] fixedLengthFields = getStrings(type, instance, config, COMPOSITE_FIELDS_FIXED_LENGTH, null);
            if (fixedLengthFields != null)
                this.fixedLengthFields.addAll(Arrays.asList(fixedLengthFields));
            
            String[] fieldTransitionDates = getStrings(type, instance, config, COMPOSITE_FIELDS_TRANSITION_DATES, null);
            if (fieldTransitionDates != null) {
                try {
                    fieldTransitionDateMap = new HashMap<>();
                    for (String fieldDate : fieldTransitionDates) {
                        String[] kv = fieldDate.split("\\|");
                        fieldTransitionDateMap.put(kv[0], formatter.parse(kv[1]));
                    }
                } catch (ParseException e) {
                    log.trace("Unable to parse composite field transition date", e);
                }
            }
            
            String[] groupingPolicies = getStrings(type, instance, config, COMPOSITE_FIELD_GROUPING_POLICY, new String[0]);
            String[] missingPolicies = getStrings(type, instance, config, COMPOSITE_FIELD_ALLOW_MISSING, new String[0]);
            
            String[] ignoreNormalization = getStrings(type, instance, config, COMPOSITE_FIELD_IGNORE_NORMALIZATION_ON_FIELD, null);
            Set<String> emptySet = Collections.emptySet();
            ignoreNormalizationForFields = (null != ignoreNormalization) ? cleanSet(ignoreNormalization) : emptySet;
            Set<String> indexOnly = Sets.newHashSet(getStrings(type, instance, config, BaseIngestHelper.INDEX_ONLY_FIELDS, new String[0]));
            
            if (null == fieldNames && null == fieldMembers)
                return;
            
            if (null == fieldNames)
                throw new IllegalArgumentException(getConfPrefixes(type, instance)[0] + COMPOSITE_FIELD_NAMES + " must be specified.");
            
            if (null == fieldMembers)
                throw new IllegalArgumentException(getConfPrefixes(type, instance)[0] + COMPOSITE_FIELD_MEMBERS + " must be specified.");
            
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
            
            for (int i = 0; i < fieldNames.length; i++) {
                String name = fieldNames[i];
                
                String[] members = datawave.util.StringUtils.split(fieldMembers[i], '.');
                // if any members are indexOnly fields, skip this one
                if (Sets.intersection(Sets.newHashSet(members), indexOnly).size() > 0) {
                    log.warn("rejecting " + name + " which includes index only field in " + indexOnly);
                    continue;
                }
                compositeFieldDefinitions.put(name, members);
                
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
            log.debug("setup with composites " + compositeFieldDefinitions);
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
         * A helper routine to merge markings maps when merging fields of a NormalizedContentInterface
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
            if (this.compositeFieldDefinitions != null && !this.compositeFieldDefinitions.isEmpty()) {
                List<NormalizedContentInterface> tempResults = new ArrayList<>();
                for (Entry<String,String[]> vFields : this.compositeFieldDefinitions.entrySet()) {
                    tempResults.clear();
                    addCompositeFields(tempResults, eventFields, vFields.getKey(), null, null, GroupingPolicy.IGNORE_GROUPS,
                                    allowMissing.get(vFields.getKey()), vFields.getValue(), 0, new StringBuilder(), new StringBuilder(), null,
                                    CompositeIngest.isOverloadedCompositeField(Arrays.asList(vFields.getValue()), vFields.getKey()));
                    for (NormalizedContentInterface value : tempResults) {
                        compositeFields.put(value.getIndexedFieldName(), value);
                    }
                }
            }
            
            return compositeFields;
        }
        
        private void compilePatterns() {
            Map<String,Pattern> patterns = new HashMap<>();
            for (Entry<String,String[]> vFields : this.compositeFieldDefinitions.entrySet()) {
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
        
        // Create the composite field from the event Fields members
        public void addCompositeFields(List<NormalizedContentInterface> compositeFields, Multimap<String,NormalizedContentInterface> eventFields,
                        String compositeFieldName, String replacement, String[] grouping, GroupingPolicy groupingPolicy, boolean allowMissing, String[] fields,
                        int pos, StringBuilder originalValue, StringBuilder normalizedValue, Map<String,String> markings, boolean isOverloadedField) {
            String separator = (pos > 0) ? COMPOSITE_DEFAULT_SEPARATOR : "";
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
        
        public Map<String,String[]> getCompositeFieldDefinitions() {
            return compositeFieldDefinitions;
        }
        
        public void setCompositeFieldDefinitions(Map<String,String[]> compositeFieldDefinitions) {
            this.compositeFieldDefinitions = compositeFieldDefinitions;
        }
        
        public Set<String> getFixedLengthFields() {
            return fixedLengthFields;
        }
        
        public void setFixedLengthFields(Set<String> fixedLengthFields) {
            this.fixedLengthFields = fixedLengthFields;
        }
        
        public Map<String,Date> getFieldTransitionDateMap() {
            return fieldTransitionDateMap;
        }
        
        public void setFieldTransitionDateMap(Map<String,Date> fieldTransitionDateMap) {
            this.fieldTransitionDateMap = fieldTransitionDateMap;
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
