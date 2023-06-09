package datawave.query.common.grouping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupFields {
    
    private static final String GROUP = "GROUP";
    private static final String SUM = "SUM";
    private static final String COUNT = "COUNT";
    private static final String AVERAGE = "AVERAGE";
    private static final String MIN = "MIN";
    private static final String MAX = "MAX";
    private static final String MODEL_MAP = "MODEL_MAP";
    
    private Set<String> groupByFields = new HashSet<>();
    private Set<String> sumFields = new HashSet<>();
    private Set<String> countFields = new HashSet<>();
    private Set<String> averageFields = new HashSet<>();
    private Set<String> minFields = new HashSet<>();
    private Set<String> maxFields = new HashSet<>();
    private Multimap<String,String> modelMap = HashMultimap.create();
    
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
                            groupFields.groupByFields = parseSet(elementContents);
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
                            groupFields.modelMap = parseMap(elementContents);
                            break;
                        default:
                            throw new IllegalArgumentException("Invalid element " + name);
                    }
                }
            } else {
                // Otherwise, the string may be in the legacy format of a comma-delimited string with group-fields only.
                String[] groupByFields = StringUtils.split(string, Constants.PARAM_VALUE_SEP);
                groupFields.setGroupByFields(Sets.newHashSet(groupByFields));
            }
        }
        return groupFields;
    }
    
    private static Set<String> parseSet(String str) {
        return Sets.newHashSet(StringUtils.split(str, Constants.COMMA));
    }
    
    private static Multimap<String,String> parseMap(String str) {
        Multimap<String,String> map = HashMultimap.create();
        String[] entries = StringUtils.split(str, Constants.COLON);
        for (String entry : entries) {
            int startBracket = entry.indexOf(Constants.BRACKET_START);
            int endBracket = entry.length() - 1;
            String key = entry.substring(0, startBracket);
            Set<String> values = parseSet(entry.substring(startBracket + 1, endBracket));
            map.putAll(key, values);
        }
        return map;
    }
    
    public static GroupFields copyOf(GroupFields other) {
        if (other == null) {
            return null;
        }
    
        GroupFields copy = new GroupFields();
        copy.groupByFields = other.groupByFields == null ? null : Sets.newHashSet(other.groupByFields);
        copy.sumFields = other.sumFields == null ? null : Sets.newHashSet(other.sumFields);
        copy.countFields = other.countFields == null ? null : Sets.newHashSet(other.countFields);
        copy.averageFields = other.averageFields == null ? null : Sets.newHashSet(other.averageFields);
        copy.minFields = other.minFields == null ? null : Sets.newHashSet(other.minFields);
        copy.maxFields = other.maxFields == null ? null : Sets.newHashSet(other.maxFields);
        copy.modelMap = other.modelMap == null ? null : HashMultimap.create(other.modelMap);
        return copy;
    }
    
    public void setGroupByFields(Set<String> fields) {
        this.groupByFields = fields;
    }
    
    public void setSumFields(Set<String> sumFields) {
        this.sumFields = sumFields;
    }
    
    public void setCountFields(Set<String> countFields) {
        this.countFields = countFields;
    }
    
    public void setAverageFields(Set<String> averageFields) {
        this.averageFields = averageFields;
    }
    
    public void setMinFields(Set<String> minFields) {
        this.minFields = minFields;
    }
    
    public void setMaxFields(Set<String> maxFields) {
        this.maxFields = maxFields;
    }
    
    public Set<String> getGroupByFields() {
        return groupByFields;
    }
    
    public Set<String> getSumFields() {
        return sumFields;
    }
    
    public Set<String> getCountFields() {
        return countFields;
    }
    
    public Set<String> getAverageFields() {
        return averageFields;
    }
    
    public Set<String> getMinFields() {
        return minFields;
    }
    
    public Set<String> getMaxFields() {
        return maxFields;
    }
    
    public boolean hasGroupByFields() {
        return groupByFields != null && !groupByFields.isEmpty();
    }
    
    public Set<String> getProjectionFields() {
        Set<String> fields = new HashSet<>();
        fields.addAll(this.groupByFields);
        fields.addAll(this.sumFields);
        fields.addAll(this.countFields);
        fields.addAll(this.averageFields);
        fields.addAll(this.minFields);
        fields.addAll(this.maxFields);
        return fields;
    }
    
    public void deconstructIdentifiers() {
        this.groupByFields = deconstructIdentifiers(this.groupByFields);
        this.sumFields = deconstructIdentifiers(this.sumFields);
        this.countFields = deconstructIdentifiers(this.countFields);
        this.averageFields = deconstructIdentifiers(this.averageFields);
        this.minFields = deconstructIdentifiers(this.minFields);
        this.maxFields = deconstructIdentifiers(this.maxFields);
    }
    
    private Set<String> deconstructIdentifiers(Set<String> set) {
        return set.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());
    }
    
    public void remapFields(Multimap<String, String> modelMap) {
        this.groupByFields = remap(this.groupByFields, modelMap);
        this.sumFields = remap(this.sumFields, modelMap);
        this.countFields = remap(this.countFields, modelMap);
        this.averageFields = remap(this.averageFields, modelMap);
        this.minFields = remap(this.minFields, modelMap);
        this.maxFields = remap(this.maxFields, modelMap);
        this.modelMap = modelMap;
    }
    
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
    
    public Multimap<String,String> getModelMap() {
        return modelMap;
    }
    
    public Map<String, String> getReverseModelMap() {
        Map<String,String> map = new HashMap<>();
        for (String key : modelMap.keySet()) {
            map.put(key, key);
            modelMap.get(key).forEach(value -> map.put(value, key));
        }
        return map;
    }
    
    public FieldAggregator.Factory getFieldAggregatorFactory() {
        return new FieldAggregator.Factory()
                        .withSumFields(sumFields)
                        .withCountFields(countFields)
                        .withAverageFields(averageFields)
                        .withMinFields(minFields)
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
        return Objects.equals(groupByFields, that.groupByFields) && Objects.equals(sumFields, that.sumFields) && Objects.equals(countFields, that.countFields)
                        && Objects.equals(averageFields, that.averageFields) && Objects.equals(minFields, that.minFields) && Objects.equals(maxFields,
                        that.maxFields) && Objects.equals(modelMap, that.modelMap);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(groupByFields, sumFields, countFields, averageFields, minFields, maxFields, modelMap);
    }
    
    @JsonValue
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        writeFormattedSet(sb, GROUP, this.groupByFields);
        writeFormattedSet(sb, SUM, this.sumFields);
        writeFormattedSet(sb, COUNT, this.countFields);
        writeFormattedSet(sb, AVERAGE, this.averageFields);
        writeFormattedSet(sb, MIN, this.minFields);
        writeFormattedSet(sb, MAX, this.maxFields);
        writeFormattedModelMap(sb);
        return sb.toString();
    }
    
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
    
    private void writeFormattedModelMap(StringBuilder sb) {
        if (!modelMap.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(Constants.PIPE);
            }
            sb.append(MODEL_MAP).append(Constants.LEFT_PAREN);
            Iterator<Map.Entry<String,Collection<String>>> entryIterator = modelMap.asMap().entrySet().iterator();
            while (entryIterator.hasNext()) {
                Map.Entry<String,Collection<String>> next = entryIterator.next();
                sb.append(next.getKey()).append(Constants.BRACKET_START);
                Iterator<String> valueIterator = next.getValue().iterator();
                while (valueIterator.hasNext()) {
                    String value = valueIterator.next();
                    sb.append(value);
                    if (valueIterator.hasNext()) {
                        sb.append(Constants.COMMA);
                    }
                }
                sb.append(Constants.BRACKET_END);
                if (entryIterator.hasNext()) {
                    sb.append(Constants.COLON);
                }
            }
            sb.append(Constants.RIGHT_PAREN);
        }
    }
}
