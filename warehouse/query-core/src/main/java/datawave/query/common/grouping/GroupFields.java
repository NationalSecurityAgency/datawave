package datawave.query.common.grouping;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import datawave.query.Constants;
import datawave.query.attributes.TemporalGranularity;
import datawave.query.jexl.JexlASTHelper;

/**
 * Represents a set of fields that have been specified within a {@code #groupby()} function, as well as any fields specified in the functions {@code #sum()},
 * {@code #count()}, {@code #average()}, {@code #min()}, and {@code #max()} that should be used when preforming a group-by operation on documents. This class
 * can easily be captured as a parameter string using {@link GroupFields#toString()}, and transformed back into a {@link GroupFields} instance via
 * {@link GroupFields#from(String)}.
 */
public class GroupFields implements Serializable {
    private static final long serialVersionUID = -5206581051889027175L;

    private static final String GROUP = "GROUP";
    private static final String SUM = "SUM";
    private static final String COUNT = "COUNT";
    private static final String AVERAGE = "AVERAGE";
    private static final String MIN = "MIN";
    private static final String MAX = "MAX";
    private static final String MODEL_MAP = "REVERSE_MODEL_MAP";

    private TreeMultimap<String,TemporalGranularity> groupByFieldMap = TreeMultimap.create();
    private Set<String> sumFields = new HashSet<>();
    private Set<String> countFields = new HashSet<>();
    private Set<String> averageFields = new HashSet<>();
    private Set<String> minFields = new HashSet<>();
    private Set<String> maxFields = new HashSet<>();
    private Map<String,String> reverseModelMap = new HashMap<>();

    /**
     * Returns a new {@link GroupFields} parsed the given string. The string is expected to have the format returned by {@link GroupFields#toString()}, but may
     * also be a comma-delimited string of fields to group-by to support backwards-compatibility with the legacy format. See below for certain edge cases:
     * <ul>
     * <li>Given null, null will be returned.</li>
     * <li>Given an empty or blank string, an empty {@link GroupFields} will be returned.</li>
     * <li>Given a comma-delimited list of fields, e.g {@code AGE,GENDER}, a {@link GroupFields} with the fields set as the group-by fields will be
     * returned.</li>
     * </ul>
     *
     * @param string
     *            the string to parse
     * @return the parsed {@link GroupFields}
     */
    @JsonCreator
    public static GroupFields from(String string) {
        if (string == null) {
            return null;
        }

        // Strip whitespaces.
        string = StringUtils.deleteWhitespace(string);

        GroupFields groupFields = new GroupFields();
        if (!string.isEmpty()) {
            // The string contains group fields in the latest formatting GROUP(field,...)...
            if (string.contains(Constants.LEFT_PAREN)) {
                // Individual elements are separated by a pipe.
                String[] elements = StringUtils.split(string, Constants.PIPE);

                // Each element starts NAME().
                for (String element : elements) {
                    int leftParen = element.indexOf(Constants.LEFT_PAREN);
                    int rightParen = element.length() - 1;
                    String name = element.substring(0, leftParen);
                    String elementContents = element.substring(leftParen + 1, rightParen);
                    switch (name) {
                        case GROUP:
                            groupFields.groupByFieldMap = parseGroupByFields(elementContents);
                            break;
                        case SUM:
                            groupFields.sumFields = parseSet(elementContents);
                            break;
                        case COUNT:
                            groupFields.countFields = parseSet(elementContents);
                            break;
                        case AVERAGE:
                            groupFields.averageFields = parseSet(elementContents);
                            break;
                        case MIN:
                            groupFields.minFields = parseSet(elementContents);
                            break;
                        case MAX:
                            groupFields.maxFields = parseSet(elementContents);
                            break;
                        case MODEL_MAP:
                            groupFields.reverseModelMap = parseMap(elementContents);
                            break;
                        default:
                            throw new IllegalArgumentException("Invalid element " + name);
                    }
                }
            } else {
                // Otherwise, the string may be in the legacy format of a comma-delimited string with group-fields only, or are comma-delimited sets of fields
                // followed by date granularities to group on.
                groupFields.groupByFieldMap = parseGroupByFields(string);
            }
        }
        return groupFields;
    }

    /**
     * Returns a map of fields to one or more {@link TemporalGranularity} parsed from the given string. The provided string is expected to have comma delimited
     * sets of fields that may be directly followed by bracketed, comma-delimited {@link TemporalGranularity} names. For example,
     * {@code "fieldA,fieldB[HOUR],fieldC[DAY,HOUR]"}. Any fields not specified with a {@link TemporalGranularity} name will be added with the default
     * {@link TemporalGranularity#ALL} granularity. All whitespace will be stripped before parsing. See below for certain edge cases.
     * <ul>
     * <li>Given null or a blank string, an empty map will be returned</li>
     * <li>Given {@code field1[],field2[DAY]}, or {@code field1,field2[DAY]}, or {@code field1[ALL],field2[DAY]}, a map will be returned where the key field1 is
     * added with {@link TemporalGranularity#ALL}, and the key field2 is added with {@link TemporalGranularity#TRUNCATE_TEMPORAL_TO_DAY}.</li>
     * </ul>
     *
     * @param string
     *            the string
     * @return a map of fields and associated temporal granularities
     */
    public static TreeMultimap<String,TemporalGranularity> parseGroupByFields(String string) {
        TreeMultimap<String,TemporalGranularity> map = TreeMultimap.create();

        if (string == null) {
            return map;
        }

        // Strip whitespaces.
        string = StringUtils.deleteWhitespace(string);

        int currentIndex = 0;
        int finalIndex = string.length() - 1;
        while (currentIndex < finalIndex) {
            int nextComma = string.indexOf(Constants.COMMA, currentIndex);
            int nextStartBracket = string.indexOf(Constants.BRACKET_START, currentIndex);
            // If there is no comma or start bracket to be found, we have a trailing field at the end of the string with no specified granularity,
            // e.g.
            //
            // field1[ALL],field2[HOUR],field3
            //
            // Add the field with the ALL granularity.
            if (nextComma == -1 && nextStartBracket == -1) {
                String field = string.substring(currentIndex);
                if (!field.isEmpty()) {
                    // Add the field only if it's not blank. Ignore cases with consecutive trailing commas like field1[ALL],
                    map.put(JexlASTHelper.deconstructIdentifier(field).toUpperCase(), TemporalGranularity.ALL);
                }
                break; // There are no more fields to be parsed.
            } else if (nextComma != -1 && (nextStartBracket == -1 || nextComma < nextStartBracket)) {
                // If a comma is located before the next starting bracket, we have a field without a granularity located somewhere before the end of the
                // string, e.g.
                //
                // field1,field2[HOUR,DAY]
                // field1[MINUTE],field2,field3[HOUR,DAY]
                // field1,field2
                //
                // Add the field with the ALL granularity.
                String field = string.substring(currentIndex, nextComma);
                if (!field.isEmpty()) {
                    // Add the field only if it's not blank. Ignore cases with consecutive commas like field1,,field2[DAY]
                    map.put(JexlASTHelper.deconstructIdentifier(field).toUpperCase(), TemporalGranularity.ALL);
                }
                currentIndex = nextComma + 1; // Advance to the start of the next field.
            } else {
                // The current field has granularities defined within brackets, e.g.
                //
                // field[DAY,MINUTE]
                //
                // Parse and add each granularity for the field.
                String field = string.substring(currentIndex, nextStartBracket);
                int nextEndBracket = string.indexOf(Constants.BRACKET_END, currentIndex);
                if (!field.isEmpty()) {
                    String granularityList = string.substring((nextStartBracket + 1), nextEndBracket);
                    // An empty granularity list, e.g. field[] is equivalent to field[ALL].
                    if (granularityList.isEmpty()) {
                        map.put(JexlASTHelper.deconstructIdentifier(field).toUpperCase(), TemporalGranularity.ALL);
                    } else {
                        String[] granularities = StringUtils.split(granularityList, Constants.COMMA);
                        for (String granularity : granularities) {
                            map.put(JexlASTHelper.deconstructIdentifier(field).toUpperCase(), TemporalGranularity.of(granularity));
                        }
                    }
                }
                currentIndex = nextEndBracket + 1; // Advance to the start of the next field.
            }
        }
        return map;
    }

    // Parse a set of fields from the string.
    private static Set<String> parseSet(String str) {
        return Sets.newHashSet(StringUtils.split(str, Constants.COMMA));
    }

    // Parse a map from the given string.
    private static Map<String,String> parseMap(String str) {
        Map<String,String> map = new HashMap<>();
        String[] entries = StringUtils.split(str, Constants.COLON);
        for (String entry : entries) {
            int equals = entry.indexOf(Constants.EQUALS);
            String key = entry.substring(0, equals);
            String value = entry.substring(equals + 1);
            map.put(key, value);
        }
        return map;
    }

    /**
     * Return a copy of the given {@link GroupFields}.
     *
     * @param other
     *            the other instance to copy
     * @return the copy
     */
    public static GroupFields copyOf(GroupFields other) {
        if (other == null) {
            return null;
        }

        GroupFields copy = new GroupFields();
        copy.groupByFieldMap = other.groupByFieldMap == null ? null : TreeMultimap.create(other.groupByFieldMap);
        copy.sumFields = other.sumFields == null ? null : Sets.newHashSet(other.sumFields);
        copy.countFields = other.countFields == null ? null : Sets.newHashSet(other.countFields);
        copy.averageFields = other.averageFields == null ? null : Sets.newHashSet(other.averageFields);
        copy.minFields = other.minFields == null ? null : Sets.newHashSet(other.minFields);
        copy.maxFields = other.maxFields == null ? null : Sets.newHashSet(other.maxFields);
        copy.reverseModelMap = other.reverseModelMap == null ? null : Maps.newHashMap(other.reverseModelMap);
        return copy;
    }

    /**
     * Set the fields to group on, as well as the temporal granularities to subsequently group on.
     *
     * @param map
     *            the field map
     */
    public void setGroupByFieldMap(Multimap<String,TemporalGranularity> map) {
        this.groupByFieldMap.clear();
        this.groupByFieldMap.putAll(map);
    }

    /**
     * Put a field-{@link TemporalGranularity} mapping into this {@link GroupFields}.
     *
     * @param field
     *            the field
     * @param temporalGranularity
     *            the granularity
     */
    public void put(String field, TemporalGranularity temporalGranularity) {
        this.groupByFieldMap.put(JexlASTHelper.deconstructIdentifier(field).toUpperCase(), temporalGranularity);
    }

    /**
     * Put all field-granularity pairings from the provided field map into this {@link GroupFields}.
     *
     * @param fieldMap
     *            the field map to add entries from
     */
    public GroupFields putAll(Multimap<String,TemporalGranularity> fieldMap) {
        if (fieldMap != null) {
            for (String field : fieldMap.keySet()) {
                this.groupByFieldMap.putAll(JexlASTHelper.deconstructIdentifier(field).toUpperCase(), fieldMap.get(field));
            }
        }
        return this;
    }

    /**
     * Set the fields to sum.
     *
     * @param fields
     *            the fields
     */
    public void setSumFields(Set<String> fields) {
        this.sumFields.clear();
        this.sumFields.addAll(fields);
    }

    /**
     * Set the fields to count.
     *
     * @param fields
     *            the fields
     */
    public void setCountFields(Set<String> fields) {
        this.countFields.clear();
        this.countFields.addAll(fields);
    }

    /**
     * Set the fields to average.
     *
     * @param fields
     *            the fields
     */
    public void setAverageFields(Set<String> fields) {
        this.averageFields.clear();
        this.averageFields.addAll(fields);
    }

    /**
     * Set the fields to find the min of.
     *
     * @param fields
     *            the fields
     */
    public void setMinFields(Set<String> fields) {
        this.minFields.clear();
        this.minFields.addAll(fields);
    }

    /**
     * Set the fields to find the max of.
     *
     * @param fields
     *            the fields
     */
    public void setMaxFields(Set<String> fields) {
        this.maxFields.clear();
        this.maxFields.addAll(fields);
    }

    /**
     * Return the fields to group by along with their temporal granularities to group by.
     *
     * @return the field map
     */
    public TreeMultimap<String,TemporalGranularity> getGroupByFieldMap() {
        return groupByFieldMap;
    }

    /**
     * Return the set of fields to group by.
     *
     * @return the fields
     */
    public Set<String> getGroupByFields() {
        return groupByFieldMap.keySet();
    }

    /**
     * Return whether this {@link GroupFields} has any fields to group by.
     *
     * @return true if there are fields to group by, or false otherwise
     */
    public boolean hasGroupByFields() {
        return groupByFieldMap != null && !groupByFieldMap.isEmpty();
    }

    /**
     * Replace a field mapping with another field.
     *
     * @param field
     *            the field
     * @param replacement
     *            the replacement
     */
    public void replaceGroupByField(String field, String replacement) {
        Collection<TemporalGranularity> value = groupByFieldMap.removeAll(field);
        if (!value.isEmpty()) {
            groupByFieldMap.putAll(replacement, value);
        }
    }

    /**
     * Return the fields to sum.
     *
     * @return the fields
     */
    public Set<String> getSumFields() {
        return sumFields;
    }

    /**
     * Return the fields to count.
     *
     * @return the fields
     */
    public Set<String> getCountFields() {
        return countFields;
    }

    /**
     * Return the fields to average.
     *
     * @return the fields
     */
    public Set<String> getAverageFields() {
        return averageFields;
    }

    /**
     * Return the fields to find the min of.
     *
     * @return the fields
     */
    public Set<String> getMinFields() {
        return minFields;
    }

    /**
     * Return the fields to find the max of.
     *
     * @return the fields
     */
    public Set<String> getMaxFields() {
        return maxFields;
    }

    /**
     * Return the set of all fields to group by, sum, count, average, and find the min and max of that must be included in projection.
     *
     * @return the fields required to be included in projection
     */
    public Set<String> getProjectionFields() {
        Set<String> fields = new HashSet<>();
        fields.addAll(this.groupByFieldMap.keySet());
        fields.addAll(this.sumFields);
        fields.addAll(this.countFields);
        fields.addAll(this.averageFields);
        fields.addAll(this.minFields);
        fields.addAll(this.maxFields);
        fields.addAll(this.reverseModelMap.keySet());
        fields.addAll(this.reverseModelMap.values());
        return fields;
    }

    /**
     * Deconstruct the identifiers of all fields in this {@link GroupFields}.
     */
    public void deconstructIdentifiers() {
        TreeMultimap<String,TemporalGranularity> newGroupByFieldMap = TreeMultimap.create();
        for (String field : groupByFieldMap.keySet()) {
            String deconstructedId = JexlASTHelper.deconstructIdentifier(field);
            newGroupByFieldMap.putAll(deconstructedId, groupByFieldMap.get(field));
        }
        this.groupByFieldMap = newGroupByFieldMap;

        this.sumFields = deconstructIdentifiers(this.sumFields);
        this.countFields = deconstructIdentifiers(this.countFields);
        this.averageFields = deconstructIdentifiers(this.averageFields);
        this.minFields = deconstructIdentifiers(this.minFields);
        this.maxFields = deconstructIdentifiers(this.maxFields);
    }

    // Return a copy of the given set with all identifiers deconstructed.
    private Set<String> deconstructIdentifiers(Set<String> set) {
        return set.stream().map(JexlASTHelper::deconstructIdentifier).map(String::toUpperCase).collect(Collectors.toSet());
    }

    /**
     * Modify this {@link GroupFields} to ensure that all sets of fields also include their alternative mappings, and set the model map to the given map.
     *
     * @param modelMap
     *            the map to retrieve alternative field mappings from
     * @param reverseModelMap
     *            the reverse model map
     */
    public void remapFields(Multimap<String,String> modelMap, Map<String,String> reverseModelMap) {
        final TreeMultimap<String,TemporalGranularity> expandedGroupByFields = TreeMultimap.create(groupByFieldMap);
        for (String field : groupByFieldMap.keySet()) {
            if (modelMap.containsKey(field.toUpperCase())) {
                Collection<TemporalGranularity> granularities = groupByFieldMap.get(field);
                modelMap.get(field).forEach(newField -> expandedGroupByFields.putAll(newField, granularities));
            }
        }
        this.groupByFieldMap = expandedGroupByFields;
        this.sumFields = remap(this.sumFields, modelMap);
        this.countFields = remap(this.countFields, modelMap);
        this.averageFields = remap(this.averageFields, modelMap);
        this.minFields = remap(this.minFields, modelMap);
        this.maxFields = remap(this.maxFields, modelMap);

        // Make a copy of the given reverse model map that only contains relevant mappings for efficiency.
        Set<String> allFields = new HashSet<>();
        allFields.addAll(groupByFieldMap.keySet());
        allFields.addAll(sumFields);
        allFields.addAll(countFields);
        allFields.addAll(averageFields);
        allFields.addAll(minFields);
        allFields.addAll(maxFields);

        this.reverseModelMap = new HashMap<>();
        for (String field : allFields) {
            if (reverseModelMap.containsKey(field)) {
                this.reverseModelMap.put(field, reverseModelMap.get(field));
            }
        }

        // Now we can reduce the fields to only those that map to themselves wrt the reverse model map.
        TreeMultimap<String,TemporalGranularity> reducedGroupByFields = TreeMultimap.create(groupByFieldMap);
        for (String field : groupByFieldMap.keySet()) {
            if (!field.equals(this.reverseModelMap.getOrDefault(field, field))) {
                reducedGroupByFields.removeAll(field);
            }
        }
        this.groupByFieldMap = reducedGroupByFields;
        this.sumFields = reduce(this.sumFields, this.reverseModelMap);
        this.countFields = reduce(this.countFields, this.reverseModelMap);
        this.averageFields = reduce(this.averageFields, this.reverseModelMap);
        this.minFields = reduce(this.minFields, this.reverseModelMap);
        this.maxFields = reduce(this.maxFields, this.reverseModelMap);
    }

    private Set<String> reduce(Set<String> set, Map<String,String> map) {
        return set.stream().filter(s -> s.equals(map.getOrDefault(s, s))).collect(Collectors.toSet());
    }

    // Return a copy of the given set with all alternative field mappings included.
    private Set<String> remap(Set<String> set, Multimap<String,String> map) {
        Set<String> newMappings = new HashSet<>(set);
        for (String field : set) {
            field = field.toUpperCase();
            if (map.containsKey(field)) {
                newMappings.addAll(map.get(field));
            }
        }
        return newMappings;
    }

    /**
     * Return the model map. This map will never be null, but may be empty if this {@link GroupFields} was never remapped via
     * {@link GroupFields#remapFields(Multimap, Map)}.
     *
     * @return the reverse model map
     */
    public Map<String,String> getReverseModelMap() {
        return reverseModelMap;
    }

    /**
     * Return a new {@link FieldAggregator.Factory} instance configured with the aggregation fields of this {@link GroupFields}.
     *
     * @return a configured {@link FieldAggregator.Factory} instance
     */
    public FieldAggregator.Factory getFieldAggregatorFactory() {
        return new FieldAggregator.Factory().withSumFields(sumFields).withCountFields(countFields).withAverageFields(averageFields).withMinFields(minFields)
                        .withMaxFields(maxFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupFields that = (GroupFields) o;
        return Objects.equals(groupByFieldMap, that.groupByFieldMap) && Objects.equals(sumFields, that.sumFields)
                        && Objects.equals(countFields, that.countFields) && Objects.equals(averageFields, that.averageFields)
                        && Objects.equals(minFields, that.minFields) && Objects.equals(maxFields, that.maxFields)
                        && Objects.equals(reverseModelMap, that.reverseModelMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByFieldMap, sumFields, countFields, averageFields, minFields, maxFields, reverseModelMap);
    }

    /**
     * Returns this {@link GroupFields} as a formatted string that can later be parsed back into a {@link GroupFields} using {@link GroupFields#from(String)}.
     * This is also what will be used when serializing a {@link GroupFields} to JSON/XML. The string will have the format
     * {@code GROUP(field[Granularity,...],...)|SUM(field,...)|COUNT(field,...)|AVERAGE(field,...)|MIN(field,...)|MAX(field,...)|REVERSE_MODEL_MAP(field1=model1,field1=model2:field2=model3,...)}
     *
     * @return a formatted string
     */
    @JsonValue
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        writeGroupByFieldMap(sb);
        writeFormattedSet(sb, SUM, this.sumFields);
        writeFormattedSet(sb, COUNT, this.countFields);
        writeFormattedSet(sb, AVERAGE, this.averageFields);
        writeFormattedSet(sb, MIN, this.minFields);
        writeFormattedSet(sb, MAX, this.maxFields);
        writeFormattedModelMap(sb);
        return sb.toString();
    }

    // Write the fields to group on to the given string builder.
    private void writeGroupByFieldMap(StringBuilder sb) {
        if (!groupByFieldMap.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(Constants.PIPE);
            }

            sb.append(GROUP).append(Constants.LEFT_PAREN);
            Iterator<String> fieldIterator = groupByFieldMap.keySet().iterator();
            while (fieldIterator.hasNext()) {
                String field = fieldIterator.next();
                sb.append(field);
                Iterator<TemporalGranularity> temporalGranularityIterator = groupByFieldMap.get(field).iterator();
                if (temporalGranularityIterator.hasNext()) {
                    sb.append(Constants.BRACKET_START);
                    while (temporalGranularityIterator.hasNext()) {
                        TemporalGranularity granularity = temporalGranularityIterator.next();
                        sb.append(granularity.getName());
                        if (temporalGranularityIterator.hasNext()) {
                            sb.append(Constants.COMMA);
                        }
                    }
                }
                sb.append(Constants.BRACKET_END);

                if (fieldIterator.hasNext()) {
                    sb.append(Constants.COMMA);
                }
            }
            sb.append(Constants.RIGHT_PAREN);
        }
    }

    // Write the given set if not empty to the given string builder.
    private void writeFormattedSet(StringBuilder sb, String name, Set<String> set) {
        if (!set.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(Constants.PIPE);
            }
            sb.append(name);
            sb.append(Constants.LEFT_PAREN);
            Iterator<String> iterator = set.iterator();
            while (iterator.hasNext()) {
                String next = iterator.next();
                sb.append(next);
                if (iterator.hasNext()) {
                    sb.append(Constants.COMMA);
                }
            }
            sb.append(Constants.RIGHT_PAREN);
        }
    }

    // Write the model map if not empty to the given string builder.
    private void writeFormattedModelMap(StringBuilder sb) {
        if (!reverseModelMap.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(Constants.PIPE);
            }
            sb.append(MODEL_MAP).append(Constants.LEFT_PAREN);
            Iterator<Map.Entry<String,String>> entryIterator = reverseModelMap.entrySet().iterator();
            while (entryIterator.hasNext()) {
                Map.Entry<String,String> next = entryIterator.next();
                sb.append(next.getKey()).append(Constants.EQUALS).append(next.getValue());
                if (entryIterator.hasNext()) {
                    sb.append(Constants.COLON);
                }
            }
            sb.append(Constants.RIGHT_PAREN);
        }
    }
}
