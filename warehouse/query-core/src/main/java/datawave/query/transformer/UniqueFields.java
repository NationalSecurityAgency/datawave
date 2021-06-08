package datawave.query.transformer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class UniqueFields implements Serializable {
    
    private Multimap<String,ValueTransformer> fieldMap;
    
    /**
     * Returns a new {@link UniqueFields} parsed from this string. The provided string is expected to have the format returned by
     * {@link UniqueFields#toFormattedString()}. Any fields not specified with a {@link ValueTransformer} name will be added with the default ORIGINAL
     * transformer. All whitespace will be stripped before parsing. See below for certain edge cases:
     * <ul>
     * <li>Given null, null will be returned.</li>
     * <li>Given an empty or blank string, an empty {@link UniqueFields} will be returned.</li>
     * <li>Given {@code field1:[],field2:[DAY]}, or {@code field1,field2:[DAY]}, or {@code field1:[ORIGINAL],field2:[DAY]}, a {@link UniqueFields} will be
     * returned where field1 is added with {@link ValueTransformer#ORIGINAL}, and field2 is added with {@link ValueTransformer#TRUNCATE_TEMPORAL_TO_DAY}.</li>
     * </ul>
     * 
     * @param string
     *            the string to parse
     * @return the parsed {@link UniqueFields}
     */
    @JsonCreator
    public static UniqueFields from(String string) {
        if (string == null) {
            return null;
        }
        // Strip whitespaces.
        string = StringUtils.deleteWhitespace(string);
        
        if (string.isEmpty()) {
            return new UniqueFields();
        }
        
        UniqueFields uniqueFields = new UniqueFields();
        
        final int finalIndex = string.length() - 1;
        int currentIndex = 0;
        String field = null;
        
        while (currentIndex < finalIndex) {
            if (field == null) {
                int nextColon = string.indexOf(Constants.COLON, currentIndex);
                int nextComma = string.indexOf(Constants.COMMA, currentIndex);
                if (nextComma == -1 && nextColon == -1) {
                    // If there is no comma or colon to be found, we have a trailing field at the end of the string with no specified value transformer, e.g.
                    //
                    // field1:[ORIGINAL],field2:[HOUR],field3
                    //
                    // Add the field with the ORIGINAL transformer.
                    field = string.substring(currentIndex);
                    uniqueFields.put(field, ValueTransformer.ORIGINAL);
                    break; // There are no more fields to be parsed.
                } else if (nextComma != -1 && (nextColon == -1 || nextComma < nextColon)) {
                    // If a comma is located before the colon, we have a field without a transformer located somewhere before the end of the string, e.g.
                    //
                    // field1,field2[HOUR,DAY]
                    // field1:[MINUTE],field2,field3[HOUR,DAY]
                    //
                    // Add the field with the ORIGINAL transformer.
                    field = string.substring(currentIndex, nextComma);
                    uniqueFields.put(field, ValueTransformer.ORIGINAL);
                    currentIndex = nextComma + 1; // Advanced to the start of the next field.
                    field = null; // Reset the field so that we attempt to find the next field in the following loop iteration.
                } else {
                    // The current field has the standard format, e.g.field:[HOUR,DAY].
                    field = string.substring(currentIndex, nextColon);
                    int nextEndBracket = string.indexOf(Constants.BRACKET_END, currentIndex);
                    currentIndex = nextColon + 2; // Advanced past the anticipated starting bracket
                    if (currentIndex == nextEndBracket) {
                        // If the current index is directly before the next end bracket, we have the weird case of a string with an empty list of value
                        // transformers, e.g. field: []
                        // Add it with the ORIGINAL transformer.
                        uniqueFields.put(field, ValueTransformer.ORIGINAL);
                        field = null;
                        if (nextComma != -1) {
                            currentIndex = nextComma + 1; // Advance to the start of the next field.
                        } else {
                            break; // There are no more fields to parse.
                        }
                    }
                }
            } else {
                int nextEndBracket = string.indexOf(Constants.BRACKET_END, currentIndex);
                int nextComma = string.indexOf(Constants.COMMA, currentIndex);
                if (nextComma == -1) {
                    // If there is no comma to be found, this is the last value transformer in the list for the current field, and there are no more field
                    // pairs, e.g. ORIGINAL in
                    //
                    // field:[HOUR,ORIGINAL]
                    String valueTransformer = string.substring(currentIndex, nextEndBracket);
                    uniqueFields.put(field, ValueTransformer.of(valueTransformer));
                    break; // There are no more fields to be parsed.
                } else {
                    if (nextComma < nextEndBracket) {
                        // If a comma was found before the next end bracket, there is at least one additional value transformer in the list after the current
                        // one, e.g.
                        //
                        // HOUR in field:[HOUR,ORIGINAL],field2[DAY]
                        String valueTransformer = string.substring(currentIndex, nextComma);
                        uniqueFields.put(field, ValueTransformer.of(valueTransformer));
                        currentIndex = nextComma + 1; // Advance to the start of the next transformer.
                    } else {
                        // If an end bracket was found before the next comma, we are on the last transformer in the list, e.g.
                        //
                        // ORIGINAL in field:[HOUR,ORIGINAL],field2[DAY]
                        String valueTransformer = string.substring(currentIndex, nextEndBracket);
                        uniqueFields.put(field, ValueTransformer.of(valueTransformer));
                        currentIndex = nextComma + 1; // Advance past the bracket and comma to the start of the next field.
                        field = null; // This was the last transformer for the current field. Reset the field.
                    }
                }
            }
        }
        
        return uniqueFields;
    }
    
    /**
     * Return a copy of the given {@link UniqueFields}.
     * 
     * @param other
     *            the other instance to copy
     * @return the copy
     */
    public static UniqueFields copyOf(UniqueFields other) {
        if (other == null) {
            return null;
        }
        UniqueFields uniqueFields = new UniqueFields();
        uniqueFields.fieldMap = TreeMultimap.create(other.fieldMap);
        return uniqueFields;
    }
    
    public UniqueFields() {
        fieldMap = TreeMultimap.create();
    }
    
    /**
     * Create a new {@link UniqueFields} with the provided map as the underlying field map.
     * 
     * @param fieldMap
     *            the field map to use
     */
    public UniqueFields(SortedSetMultimap<String,ValueTransformer> fieldMap) {
        this.fieldMap = fieldMap;
    }
    
    /**
     * Put a field-{@link ValueTransformer} key pair into this {@link UniqueFields}.
     * 
     * @param field
     *            the field
     * @param valueTransformer
     *            the value transformer
     */
    public void put(String field, ValueTransformer valueTransformer) {
        fieldMap.put(field, valueTransformer);
    }
    
    /**
     * Put all field-transformer pairings from the provided field map into this {@link UniqueFields}.
     * 
     * @param fieldMap
     *            the field map to add entries from
     */
    public void putAll(Multimap<String,ValueTransformer> fieldMap) {
        if (fieldMap != null) {
            this.fieldMap.putAll(fieldMap);
        }
    }
    
    /**
     * Return a copy of the fields contained within this {@link UniqueFields}. Modifications to this set will not modify the fields in this {@link UniqueFields}
     * .
     * 
     * @return the
     */
    public Set<String> getFields() {
        return Sets.newHashSet(fieldMap.keySet());
    }
    
    /**
     * Return the underlying field-transformer map from this {@link UniqueFields}.
     * 
     * @return the field map
     */
    public Multimap<String,ValueTransformer> getFieldMap() {
        return fieldMap;
    }
    
    /**
     * Replace any identifier fields with their deconstructed version.
     */
    public void deconstructIdentifierFields() {
        Multimap<String,ValueTransformer> newFieldMap = TreeMultimap.create();
        for (String field : fieldMap.keySet()) {
            String newField = JexlASTHelper.deconstructIdentifier(field);
            if (newField.equals(field)) {
                newFieldMap.putAll(field, fieldMap.get(field));
            } else {
                newFieldMap.putAll(newField, fieldMap.get(field));
            }
        }
        this.fieldMap = newFieldMap;
    }
    
    /**
     * Remap all fields to include any matches from the provided model. The original field entries will be retained.
     * 
     * @param model
     *            the model to find mappings from
     */
    public void remapFields(Multimap<String,String> model) {
        Multimap<String,ValueTransformer> newFieldMap = TreeMultimap.create(fieldMap);
        for (String field : fieldMap.keySet()) {
            Collection<ValueTransformer> transformers = fieldMap.get(field);
            field = field.toUpperCase();
            if (model.containsKey(field)) {
                model.get(field).forEach((newField) -> newFieldMap.putAll(newField, transformers));
            }
        }
        this.fieldMap = newFieldMap;
    }
    
    /**
     * Returns whether or not this {@link UniqueFields} contains any fields.
     * 
     * @return true if this {@link UniqueFields} contains no fields, or false otherwise
     */
    public boolean isEmpty() {
        return fieldMap.isEmpty();
    }
    
    public Set<String> transformValues(String field, Collection<String> values) {
        Collection<ValueTransformer> transformers = fieldMap.get(field);
        // If there is no transformer, or only the ORIGINAL transformer was specified, return the original values.
        if (transformers.isEmpty() || (transformers.size() == 1 && transformers.contains(ValueTransformer.ORIGINAL))) {
            return Sets.newHashSet(values);
        } else {
            Set<String> transformedValues = new HashSet<>();
            for (ValueTransformer transformer : transformers) {
                values.stream().map(transformer::transform).forEach(transformedValues::add);
            }
            return transformedValues;
        }
    }
    
    /**
     * Returns this {@link UniqueFields} as a formatted string that can later be parsed back into a {@link UniqueFields} using {@link UniqueFields#from(String)}
     * . This is also what will be used when serializing a {@link UniqueFields} to JSON/XML. The string will have the format
     * {@code field:[ValueTransformer, ...],...}, e.g. {@code field1:[DAY,HOUR],field2:[ORIGINAL]}
     *
     * @return a formatted string
     */
    @JsonValue
    public String toFormattedString() {
        StringBuilder sb = new StringBuilder();
        Iterator<String> fieldIterator = fieldMap.keySet().iterator();
        while (fieldIterator.hasNext()) {
            // Write the field.
            String field = fieldIterator.next();
            sb.append(field).append(Constants.COLON).append(Constants.BRACKET_START);
            // Write each transformer for the field.
            Iterator<ValueTransformer> valueIterator = fieldMap.get(field).iterator();
            while (valueIterator.hasNext()) {
                sb.append(valueIterator.next().getName());
                if (valueIterator.hasNext()) {
                    sb.append(Constants.COMMA);
                }
            }
            sb.append(Constants.BRACKET_END);
            if (fieldIterator.hasNext()) {
                sb.append(Constants.COMMA);
            }
        }
        return sb.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UniqueFields that = (UniqueFields) o;
        return Objects.equals(fieldMap, that.fieldMap);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(fieldMap);
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("fields", fieldMap).toString();
    }
}
