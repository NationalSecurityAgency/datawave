package datawave.query.common.grouping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class GroupAggregateFields {
    
    private static final String GROUP = "GROUP";
    private static final String SUM = "SUM";
    private static final String COUNT = "COUNT";
    private static final String AVERAGE = "AVERAGE";
    private static final String MIN = "MIN";
    private static final String MAX = "MAX";
    
    private Multimap<String,String> groupFields = TreeMultimap.create();
    private Multimap<String,String> sumFields = TreeMultimap.create();
    private Multimap<String,String> countFields = TreeMultimap.create();
    private Multimap<String,String> averageFields = TreeMultimap.create();
    private Multimap<String,String> minFields = TreeMultimap.create();
    private Multimap<String,String> maxFields = TreeMultimap.create();
    
    /**
     * Returns a new {@link GroupAggregateFields} parsed from this string. The provided string is expected to have the format returned by
     * {@link GroupAggregateFields#toString()}.
     * <ul>
     * <li>Given null, null will be returned.</li>
     * <li>Given an empty or blank string, an empty {@link GroupAggregateFields} will be returned.</li>
     * </ul>
     *
     * @param string
     *            the string to parse
     * @return the parsed {@link GroupAggregateFields}
     */
    @JsonCreator
    public static GroupAggregateFields from(String string) {
        if (string == null) {
            return null;
        }
        
        // Strip whitespaces.
        string = StringUtils.deleteWhitespace(string);
        
        GroupAggregateFields groupAggregateFields = new GroupAggregateFields();
        // Individual maps are separated by a colon.
        String[] mapStrings = StringUtils.split(string, Constants.COLON);
        
        // Each map string is in the format NAME(mapKey[mapValue,...]|mapKey[mapValue,...]|...)
        for (String mapString : mapStrings) {
            int leftParen = mapString.indexOf(Constants.LEFT_PAREN);
            int rightParen = mapString.length() - 1;
            
            Multimap<String,String> map = TreeMultimap.create();
            // Each map entry is separated by a pipe.
            String[] entries = StringUtils.split(mapString.substring((leftParen + 1), rightParen), Constants.PIPE);
            for (String entryString : entries) {
                int startBracket = entryString.indexOf(Constants.BRACKET_START);
                int endBracket = entryString.indexOf(Constants.BRACKET_END);
                // Extract the entry key.
                String key = entryString.substring(0, startBracket);
                // The entry values are separated by commas.
                String[] values = StringUtils.split(entryString.substring((startBracket + 1), endBracket), Constants.COMMA);
                map.putAll(key, Arrays.asList(values));
            }
            // Determine which map to set based on the map name.
            String mapName = mapString.substring(0, leftParen);
            switch (mapName) {
                case GROUP:
                    groupAggregateFields.setGroupFields(map);
                    break;
                case SUM:
                    groupAggregateFields.setSumFields(map);
                    break;
                case COUNT:
                    groupAggregateFields.setCountFields(map);
                    break;
                case AVERAGE:
                    groupAggregateFields.setAverageFields(map);
                    break;
                case MIN:
                    groupAggregateFields.setMinFields(map);
                    break;
                case MAX:
                    groupAggregateFields.setMaxFields(map);
                    break;
            }
        }
        
        return groupAggregateFields;
    }
    
    /**
     * Return a copy of the given {@link GroupAggregateFields}.
     *
     * @param other
     *            the other instance to copy
     * @return the copy
     */
    public static GroupAggregateFields copyOf(GroupAggregateFields other) {
        if (other == null) {
            return null;
        }
        GroupAggregateFields copy = new GroupAggregateFields();
        copy.groupFields = TreeMultimap.create(other.groupFields);
        copy.sumFields = TreeMultimap.create(other.sumFields);
        copy.countFields = TreeMultimap.create(other.countFields);
        copy.averageFields = TreeMultimap.create(other.averageFields);
        copy.minFields = TreeMultimap.create(other.minFields);
        copy.maxFields = TreeMultimap.create(other.maxFields);
        
        return copy;
    }
    
    /**
     * Add fields to use when performing a group-by operation.
     * 
     * @param fields
     *            the fields
     */
    public void addGroupFields(Collection<String> fields) {
        mapToSelf(fields, this.groupFields);
    }
    
    /**
     * Add fields to use when performing a group-by operation.
     * 
     * @param fields
     *            the fields
     */
    public void addGroupFields(String... fields) {
        mapToSelf(this.groupFields, fields);
    }
    
    /**
     * Set the group fields map.
     * 
     * @param map
     *            the map
     */
    public void setGroupFields(Multimap<String,String> map) {
        this.groupFields = TreeMultimap.create(map);
    }
    
    /**
     * Add fields whose sum should be aggregated during group-by operations.
     * 
     * @param fields
     *            the fields
     */
    public void addSumFields(Collection<String> fields) {
        mapToSelf(fields, this.sumFields);
    }
    
    /**
     * Add fields whose sum should be aggregated during group-by operations.
     * 
     * @param fields
     *            the fields
     */
    public void addSumFields(String... fields) {
        mapToSelf(this.sumFields, fields);
    }
    
    /**
     * Set the sum fields map.
     * 
     * @param map
     *            the map
     */
    public void setSumFields(Multimap<String,String> map) {
        this.sumFields = TreeMultimap.create(map);
    }
    
    /**
     * Add fields whose count be aggregated during group-by operations.
     * 
     * @param fields
     *            the fields
     */
    public void addCountFields(Collection<String> fields) {
        mapToSelf(fields, this.countFields);
    }
    
    /**
     * Add fields whose count be aggregated during group-by operations.
     * 
     * @param fields
     *            the fields
     */
    public void addCountFields(String... fields) {
        mapToSelf(this.countFields, fields);
    }
    
    /**
     * Set the count fields map.
     * 
     * @param map
     *            the map
     */
    public void setCountFields(Multimap<String,String> map) {
        this.countFields = TreeMultimap.create(map);
    }
    
    /**
     * Add fields whose average should be aggregated during group-by operations.
     * 
     * @param fields
     *            the fields
     */
    public void addAverageFields(Collection<String> fields) {
        mapToSelf(fields, this.averageFields);
    }
    
    /**
     * Add fields whose average should be aggregated during group-by operations.
     * 
     * @param fields
     *            the fields
     */
    public void addAverageFields(String... fields) {
        mapToSelf(this.averageFields, fields);
    }
    
    /**
     * Set the average fields map.
     * 
     * @param map
     *            the map
     */
    public void setAverageFields(Multimap<String,String> map) {
        this.averageFields = TreeMultimap.create(map);
    }
    
    /**
     * Add fields whose max should be aggregated during group-by operations.
     * 
     * @param fields
     *            the fields
     */
    public void addMaxFields(Collection<String> fields) {
        mapToSelf(fields, this.maxFields);
    }
    
    /**
     * Add fields whose max should be aggregated during group-by operations.
     * 
     * @param fields
     *            the fields
     */
    public void addMaxFields(String... fields) {
        mapToSelf(this.maxFields, fields);
    }
    
    /**
     * Set the max fields map.
     * 
     * @param map
     *            the map
     */
    public void setMaxFields(Multimap<String,String> map) {
        this.maxFields = TreeMultimap.create(map);
    }
    
    /**
     * Add fields whose min should be aggregated during group-by operations.
     * 
     * @param fields
     *            the fields
     */
    public void addMinFields(Collection<String> fields) {
        mapToSelf(fields, this.minFields);
    }
    
    /**
     * Add fields whose min should be aggregated during group-by operations.
     * 
     * @param fields
     *            the fields
     */
    public void addMinFields(String... fields) {
        mapToSelf(this.minFields, fields);
    }
    
    /**
     * Set the min fields map.
     * 
     * @param map
     *            the map
     */
    public void setMinFields(Multimap<String,String> map) {
        this.minFields = TreeMultimap.create(map);
    }
    
    private void mapToSelf(Collection<String> fields, Multimap<String,String> map) {
        fields.forEach(field -> map.put(field, field));
    }
    
    private void mapToSelf(Multimap<String,String> map, String... fields) {
        for (String field : fields) {
            map.put(field, field);
        }
    }
    
    /**
     * Return whether there are any fields specified to use to perform a group-by operation.
     * 
     * @return true if there are any group fields or false otherwise
     */
    public boolean hasGroupFields() {
        return !this.groupFields.isEmpty();
    }
    
    public int requiredGroupSize() {
        return this.groupFields.keySet().size();
    }
    
    public Multimap<String,String> getGroupFieldsMap() {
        return this.groupFields;
    }
    
    public Set<String> getGroupFields() {
        return this.groupFields.keySet();
    }
    
    public Multimap<String,String> getSumFields() {
        return this.sumFields;
    }
    
    public Multimap<String,String> getCountFields() {
        return this.countFields;
    }
    
    public Multimap<String,String> getAverageFields() {
        return this.averageFields;
    }
    
    public Multimap<String,String> getMinFields() {
        return this.minFields;
    }
    
    public Multimap<String,String> getMaxFields() {
        return this.maxFields;
    }
    
    public FieldAggregator.Factory getFieldAggregatorFactory() {
        // @formatter:off
        return new FieldAggregator.Factory()
                        .withSumFields(this.sumFields.keySet())
                        .withCountFields(this.countFields.keySet())
                        .withAverageFields(this.averageFields.keySet())
                        .withMinFields(this.minFields.keySet())
                        .withMaxFields(this.maxFields.keySet());
        // @formatter:on
    }
    
    public Map<String,String> getReverseModelMapping() {
        Map<String,String> map = new HashMap<>();
        addReverseMappings(map, this.groupFields);
        addReverseMappings(map, this.sumFields);
        addReverseMappings(map, this.countFields);
        addReverseMappings(map, this.averageFields);
        addReverseMappings(map, this.minFields);
        addReverseMappings(map, this.maxFields);
        return map;
    }
    
    private void addReverseMappings(Map<String,String> map, Multimap<String,String> multimap) {
        for (String key : multimap.keySet()) {
            for (String value : multimap.get(key)) {
                map.put(value, key);
            }
        }
    }
    
    /**
     * Deconstruct the identifiers for each field in this {@link GroupAggregateFields}.
     */
    public void deconstructIdentifiers() {
        this.groupFields = deconstructIdentifiers(this.groupFields);
        this.sumFields = deconstructIdentifiers(this.sumFields);
        this.countFields = deconstructIdentifiers(this.countFields);
        this.averageFields = deconstructIdentifiers(this.averageFields);
        this.minFields = deconstructIdentifiers(this.minFields);
        this.maxFields = deconstructIdentifiers(this.maxFields);
    }
    
    private Multimap<String,String> deconstructIdentifiers(Multimap<String,String> map) {
        Multimap<String,String> newMap = HashMultimap.create();
        for (String field : map.keySet()) {
            String key = JexlASTHelper.deconstructIdentifier(field);
            for (String value : map.get(field)) {
                newMap.put(key, JexlASTHelper.deconstructIdentifier(value));
            }
        }
        return newMap;
    }
    
    /**
     * Remap each field in this {@link GroupAggregateFields} to include the mappings in the provided model map if the model map is not empty. The primary field
     * key for each map will be changed to reflect the primary field key in the given model map.
     * 
     * @param inverseReverseModel
     *            the model map
     */
    public void remapFields(Multimap<String,String> inverseReverseModel) {
        if (!inverseReverseModel.isEmpty()) {
            // Construct a reverse model to make it easy to identify the primary keys.
            Map<String,String> reverseModel = new HashMap<>();
            for (String field : inverseReverseModel.keySet()) {
                reverseModel.put(field, field);
                for (String value : inverseReverseModel.get(field)) {
                    reverseModel.put(value, field);
                }
            }
            this.groupFields = remapFields(groupFields, inverseReverseModel, reverseModel);
            this.sumFields = remapFields(sumFields, inverseReverseModel, reverseModel);
            this.countFields = remapFields(countFields, inverseReverseModel, reverseModel);
            this.averageFields = remapFields(averageFields, inverseReverseModel, reverseModel);
            this.minFields = remapFields(minFields, inverseReverseModel, reverseModel);
            this.maxFields = remapFields(maxFields, inverseReverseModel, reverseModel);
        }
    }
    
    private Multimap<String,String> remapFields(Multimap<String,String> map, Multimap<String,String> inverseReverseModel, Map<String,String> reverseModel) {
        Multimap<String,String> newMap = TreeMultimap.create();
        for (String field : map.keySet()) {
            String primaryKey = reverseModel.get(field);
            newMap.putAll(primaryKey, inverseReverseModel.get(primaryKey));
        }
        return newMap;
    }
    
    /**
     * Get all distinct fields in this {@link GroupAggregateFields}.
     * 
     * @return the distinct fields
     */
    public Set<String> getDistinctFields() {
        Set<String> fields = new HashSet<>();
        addKeysAndValues(fields, this.groupFields);
        addKeysAndValues(fields, this.sumFields);
        addKeysAndValues(fields, this.countFields);
        addKeysAndValues(fields, this.averageFields);
        addKeysAndValues(fields, this.minFields);
        addKeysAndValues(fields, this.maxFields);
        return fields;
    }
    
    private void addKeysAndValues(Set<String> set, Multimap<String,String> map) {
        set.addAll(map.keys());
        set.addAll(map.values());
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupAggregateFields that = (GroupAggregateFields) o;
        return Objects.equals(groupFields, that.groupFields) && Objects.equals(sumFields, that.sumFields) && Objects.equals(countFields, that.countFields)
                        && Objects.equals(averageFields, that.averageFields) && Objects.equals(minFields, that.minFields)
                        && Objects.equals(maxFields, that.maxFields);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(groupFields, sumFields, countFields, averageFields, minFields, maxFields);
    }
    
    /**
     * Returns this {@link GroupAggregateFields} as a formatted string that can later be parsed back into a {@link GroupAggregateFields} using
     * {@link GroupAggregateFields#from(String)}. This is also what will be used when serializing a {@link GroupAggregateFields} to JSON/XML. The string will
     * have the format:
     * <p>
     * {@code GROUP(key[value,...]|...):SUM(key[value,...]|...):COUNT(key[value,...]|...):AVERAGE(key[value,...]|...):MIN(key[value,...]|...):MAX(key[value,...]|...)}.
     *
     * @return a formatted string
     */
    @JsonValue
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        if (!this.groupFields.isEmpty()) {
            writeFormattedMap(sb, GROUP, this.groupFields);
        }
        
        if (!this.sumFields.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(Constants.COLON);
            }
            writeFormattedMap(sb, SUM, this.sumFields);
        }
        
        if (!this.countFields.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(Constants.COLON);
            }
            writeFormattedMap(sb, COUNT, this.countFields);
        }
        
        if (!this.averageFields.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(Constants.COLON);
            }
            writeFormattedMap(sb, AVERAGE, this.averageFields);
        }
        
        if (!this.minFields.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(Constants.COLON);
            }
            writeFormattedMap(sb, MIN, this.minFields);
        }
        
        if (!this.maxFields.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(Constants.COLON);
            }
            writeFormattedMap(sb, MAX, this.maxFields);
        }
        
        return sb.toString();
    }
    
    private void writeFormattedMap(StringBuilder sb, String mapName, Multimap<String,String> map) {
        sb.append(mapName).append(Constants.LEFT_PAREN);
        if (!map.isEmpty()) {
            Iterator<String> keyIterator = map.keySet().iterator();
            while (keyIterator.hasNext()) {
                String key = keyIterator.next();
                sb.append(key).append(Constants.BRACKET_START);
                Iterator<String> valueIterator = map.get(key).iterator();
                while (valueIterator.hasNext()) {
                    sb.append(valueIterator.next());
                    if (valueIterator.hasNext()) {
                        sb.append(Constants.COMMA);
                    }
                }
                sb.append(Constants.BRACKET_END);
                if (keyIterator.hasNext()) {
                    sb.append(Constants.PIPE);
                }
            }
        }
        sb.append(Constants.RIGHT_PAREN);
    }
}
