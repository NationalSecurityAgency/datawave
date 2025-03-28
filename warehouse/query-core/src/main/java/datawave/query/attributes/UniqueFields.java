package datawave.query.attributes;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;

/**
 * Represents a set of fields that have been specified within a {@code #unique()} function, as well any specified granularity levels for each individual field
 * that should be used when determining uniqueness for the values of each field within a {@link Document}. An instance of {@link UniqueFields} can easily be
 * captured as a parameter string using {@link UniqueFields#toString()}, and transformed back into a {@link UniqueFields} instance via
 * {@link UniqueFields#from(String)}.
 */
public class UniqueFields implements Serializable, Cloneable {

    private final TreeMultimap<String,UniqueGranularity> fieldMap = TreeMultimap.create();
    private boolean mostRecent = false;
    private static String MOST_RECENT_UNIQUE = "_MOST_RECENT_";

    /**
     * Returns a new {@link UniqueFields} parsed from this string. The provided string is expected to have the format returned by
     * {@link UniqueFields#toString()}. Any fields not specified with a {@link UniqueGranularity} name will be added with the default ALL granularity. All
     * whitespace will be stripped before parsing. See below for certain edge cases:
     * <ul>
     * <li>Given null, null will be returned.</li>
     * <li>Given an empty or blank string, an empty {@link UniqueFields} will be returned.</li>
     * <li>Given {@code field1[],field2[DAY]}, or {@code field1,field2[DAY]}, or {@code field1[ALL],field2[DAY]}, a {@link UniqueFields} will be returned where
     * field1 is added with {@link UniqueGranularity#ALL}, and field2 is added with {@link UniqueGranularity#TRUNCATE_TEMPORAL_TO_DAY}.</li>
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
                    if (field.equals(MOST_RECENT_UNIQUE)) {
                        uniqueFields.setMostRecent(true);
                    } else {
                        // Add the field only if its not blank. Ignore cases with consecutive trailing commas like field1[ALL],,
                        uniqueFields.put(field, UniqueGranularity.ALL);
                    }
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
                    if (field.equals(MOST_RECENT_UNIQUE)) {
                        uniqueFields.setMostRecent(true);
                    } else {
                        // Add the field only if its not blank. Ignore cases with consecutive commas like field1,,field2[DAY]
                        uniqueFields.put(field, UniqueGranularity.ALL);
                    }
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
                    if (field.equals(MOST_RECENT_UNIQUE)) {
                        uniqueFields.setMostRecent(true);
                    } else {
                        String granularityList = string.substring((nextStartBracket + 1), nextEndBracket);
                        // An empty granularity list, e.g. field[] is equivalent to field[ALL].
                        if (granularityList.isEmpty()) {
                            uniqueFields.put(field, UniqueGranularity.ALL);
                        } else {
                            String[] granularities = StringUtils.split(granularityList, Constants.COMMA);
                            for (String granularity : granularities) {
                                uniqueFields.put(field, parseGranularity(granularity));
                            }
                        }
                    }
                }
                currentIndex = nextEndBracket + 1; // Advance to the start of the next field.
            }
        }

        return uniqueFields;
    }

    // Return the parsed granularity instance, or throw an exception if one could not be parsed.
    private static UniqueGranularity parseGranularity(String granularity) {
        try {
            return UniqueGranularity.of(granularity.toUpperCase());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid unique granularity given: " + granularity);
        }
    }

    /**
     * Return a clone of this class
     *
     * @return the copy
     */
    @Override
    public UniqueFields clone() {
        UniqueFields newFields = new UniqueFields();
        newFields.fieldMap.putAll(this.fieldMap);
        newFields.mostRecent = this.mostRecent;
        return newFields;
    }

    public UniqueFields() {}

    /**
     * Create a new {@link UniqueFields} with the provided map as the underlying field map.
     *
     * @param fieldMap
     *            the field map to use
     */
    public UniqueFields(SortedSetMultimap<String,UniqueGranularity> fieldMap) {
        putAll(fieldMap);
    }

    /**
     * Clear out the field map
     */
    public UniqueFields clear() {
        this.fieldMap.clear();
        return this;
    }

    /**
     * Set the field map
     *
     * @param fields
     */
    public UniqueFields set(Multimap<String,UniqueGranularity> fields) {
        return clear().putAll(fields);
    }

    /**
     * Put a field-{@link UniqueGranularity} key pair into this {@link UniqueFields}.
     *
     * @param field
     *            the field
     * @param uniqueGranularity
     *            the granularity
     */
    public UniqueFields put(String field, UniqueGranularity uniqueGranularity) {
        fieldMap.put(JexlASTHelper.deconstructIdentifier(field).toUpperCase(), uniqueGranularity);
        return this;
    }

    /**
     * Put all field-granularity pairings from the provided field map into this {@link UniqueFields}.
     *
     * @param fieldMap
     *            the field map to add entries from
     */
    public UniqueFields putAll(Multimap<String,UniqueGranularity> fieldMap) {
        if (fieldMap != null) {
            for (String field : fieldMap.keySet()) {
                this.fieldMap.putAll(JexlASTHelper.deconstructIdentifier(field).toUpperCase(), fieldMap.get(field));
            }
        }
        return this;
    }

    /**
     * Replace a field mapping with another field
     *
     * @param field
     * @param replacement
     */
    public void replace(String field, String replacement) {
        Collection<UniqueGranularity> value = fieldMap.removeAll(field);
        if (value != null && !value.isEmpty()) {
            fieldMap.putAll(replacement, value);
        }
    }

    /**
     * Return the fields within this {@link UniqueFields}. Modifications to this set will modify the fields in this {@link UniqueFields}.
     *
     * @return a copy of the fields
     */
    public NavigableSet<String> getFields() {
        return fieldMap.keySet();
    }

    /**
     * Return the underlying field-granularity map from this {@link UniqueFields}.
     *
     * @return the field map
     */
    public TreeMultimap<String,UniqueGranularity> getFieldMap() {
        return fieldMap;
    }

    /**
     * Remap all fields to include any matches from the provided model. The original field entries will be retained.
     *
     * @param model
     *            the model to find mappings from
     */
    public void remapFields(Multimap<String,String> model) {
        Multimap<String,UniqueGranularity> newFieldMap = TreeMultimap.create(fieldMap);
        for (String field : fieldMap.keySet()) {
            Collection<UniqueGranularity> granularities = fieldMap.get(field);
            if (model.containsKey(field)) {
                model.get(field).forEach((newField) -> newFieldMap.putAll(newField, granularities));
            }
        }
        set(newFieldMap);
    }

    /**
     * Returns whether or not this {@link UniqueFields} contains any fields.
     *
     * @return true if this {@link UniqueFields} contains no fields, or false otherwise
     */
    public boolean isEmpty() {
        return fieldMap.isEmpty();
    }

    /**
     * Returns a set that contains the result of each value transformed by each granularity defined for specified field. If no granularity is found, a copy of
     * the original value set will be returned.
     *
     * @param field
     *            the field
     * @param values
     *            the values to transform
     * @return a set containing the result of each transformation
     */
    public Set<String> transformValues(String field, Collection<String> values) {
        Collection<UniqueGranularity> granularities = fieldMap.get(field);
        // If there is no granularity, or only the ALL granularity was specified, return the original values.
        if (granularities.isEmpty() || (granularities.size() == 1 && granularities.contains(UniqueGranularity.ALL))) {
            return Sets.newHashSet(values);
        } else {
            Set<String> transformedValues = new HashSet<>();
            for (UniqueGranularity granularity : granularities) {
                values.stream().map(granularity::transform).forEach(transformedValues::add);
            }
            return transformedValues;
        }
    }

    public String transformValue(String field, String value) {
        Collection<UniqueGranularity> granularities = fieldMap.get(field);
        // If there is no granularity, or only the ALL granularity was specified, return the original values.
        if (granularities.isEmpty() || (granularities.size() == 1 && granularities.contains(UniqueGranularity.ALL))) {
            return value;
        } else {
            StringBuilder combinedValue = new StringBuilder();
            String separator = "";
            for (UniqueGranularity granularity : granularities) {
                combinedValue.append(separator).append(granularity.transform(value));
            }
            return combinedValue.toString();
        }
    }

    /**
     * Returns this {@link UniqueFields} as a formatted string that can later be parsed back into a {@link UniqueFields} using {@link UniqueFields#from(String)}
     * . This is also what will be used when serializing a {@link UniqueFields} to JSON/XML. The string will have the format
     * {@code field:[UniqueGranularity, ...],...}, e.g. {@code field1[DAY,HOUR],field2[ALL]}
     *
     * @return a formatted string
     */
    @JsonValue
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (mostRecent) {
            sb.append(MOST_RECENT_UNIQUE);
            sb.append(Constants.COMMA);
        }
        Iterator<String> fieldIterator = fieldMap.keySet().iterator();
        while (fieldIterator.hasNext()) {
            // Write the field.
            String field = fieldIterator.next();
            sb.append(field).append(Constants.BRACKET_START);
            // Write each granularity for the field.
            Iterator<UniqueGranularity> valueIterator = fieldMap.get(field).iterator();
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

    public boolean isMostRecent() {
        return mostRecent;
    }

    public UniqueFields setMostRecent(boolean mostRecent) {
        this.mostRecent = mostRecent;
        return this;
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
        return Objects.equals(fieldMap, that.fieldMap) && mostRecent == that.mostRecent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldMap, mostRecent);
    }

}
