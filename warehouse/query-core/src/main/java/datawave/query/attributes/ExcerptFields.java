package datawave.query.attributes;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Multimap;
import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Represents a set of fields that have been specified within an #EXCERPT_FIELDS function, as well as their corresponding target offsets that should be used to
 * retrieve excerpts for hits that were found for a content:phrase or content:within functions. An instance of {@link ExcerptFields} can easily be captured as a
 * parameter string using {@link ExcerptFields#toString()}, and transformed back into a {@link ExcerptFields} instance via {@link ExcerptFields#from(String)}.
 */
public class ExcerptFields implements Serializable {

    private static final long serialVersionUID = 5380671489827552579L;

    private SortedMap<String,Integer> fieldMap;

    /**
     * Returns a new {@link ExcerptFields} parsed from the string. The provided string is expected to have the format returned by
     * {@link ExcerptFields#toString()}.
     * <ul>
     * <li>Given null, null will be returned.</li>
     * <li>Given an empty or blank string, an empty {@link ExcerptFields} will be returned.</li>
     * <li>Given {@code BODY/10,CONTENT/5}, an {@link ExcerptFields} will be returned with the field BODY with an offset of 10, and the field CONTENT with an
     * offset of 5.
     * </ul>
     *
     * @param string
     *            the string to parse
     * @return the parsed {@link ExcerptFields}
     */
    @JsonCreator
    public static ExcerptFields from(String string) {
        if (string == null) {
            return null;
        }
        // Strip whitespaces.
        string = StringUtils.deleteWhitespace(string);

        if (string.isEmpty()) {
            return new ExcerptFields();
        }

        ExcerptFields excerptFields = new ExcerptFields();

        String[] fieldParts = string.split(Constants.COMMA);
        for (String fieldPart : fieldParts) {
            String[] parts = StringUtils.split(fieldPart, Constants.FORWARD_SLASH);
            excerptFields.put(parts[0], Integer.valueOf(parts[1]));
        }
        return excerptFields;
    }

    /**
     * Returns a copy of the given {@link ExcerptFields}
     *
     * @param other
     *            the instance to copy
     * @return the copy
     */
    public static ExcerptFields copyOf(ExcerptFields other) {
        if (other == null) {
            return null;
        }
        ExcerptFields excerptFields = new ExcerptFields();
        excerptFields.fieldMap = new TreeMap<>(other.fieldMap);
        return excerptFields;
    }

    public ExcerptFields() {
        fieldMap = new TreeMap<>();
    }

    /**
     * Return the set of fields for which excerpts should be retrieved.
     *
     * @return the fields
     */
    public Set<String> getFields() {
        return fieldMap.keySet();
    }

    /**
     * Do we have this field in the list of excerpt fields.
     *
     * @param field
     *            the field
     * @return true if we have configuration for this field, false otherwise
     */
    public boolean containsField(String field) {
        return fieldMap.containsKey(field);
    }

    /**
     * Return the offset to use when retrieving excerpts for the specified field.
     *
     * @param field
     *            the field
     * @return the offset
     */
    public Integer getOffset(String field) {
        return fieldMap.get(field);
    }

    /**
     * Put the offset to use when retrieving excerpts for the specified field.
     *
     * @param field
     *            the field
     * @param offset
     *            the offset
     */
    public void put(String field, Integer offset) {
        fieldMap.put(field, offset);
    }

    /**
     * Return whether this {@link ExcerptFields} is empty.
     *
     * @return true if empty, or false otherwise
     */
    public boolean isEmpty() {
        return fieldMap.isEmpty();
    }

    /**
     * Replaces any field within this {@link ExcerptFields} with their deconstructed version.
     */
    public void deconstructFields() {
        SortedMap<String,Integer> deconstructedMap = new TreeMap<>();
        for (Map.Entry<String,Integer> entry : fieldMap.entrySet()) {
            String deconstructedField = JexlASTHelper.deconstructIdentifier(entry.getKey());
            deconstructedMap.put(deconstructedField, entry.getValue());
        }
        this.fieldMap = deconstructedMap;
    }

    /**
     * Expands this {@link ExcerptFields} to include any matching fields from the provided model. Original fields will be retained, but all fields will be
     * uppercase as a result of calling this method.
     *
     * @param model
     *            the model to find mappings from
     */
    public void expandFields(Multimap<String,String> model) {
        SortedMap<String,Integer> expandedMap = new TreeMap<>();
        for (String field : fieldMap.keySet()) {
            int offset = fieldMap.get(field);
            field = field.toUpperCase();
            // Add the expanded fields.
            if (model.containsKey(field)) {
                for (String expandedField : model.get(field)) {
                    expandedMap.put(expandedField, offset);
                }
            }
            // Ensure original fields are retained.
            expandedMap.put(field, offset);
        }
        this.fieldMap = expandedMap;
    }

    /**
     * Returns this {@link ExcerptFields} as a formatted string that can later be parsed back into a {@link ExcerptFields} using
     * {@link ExcerptFields#from(String)}. This is also what will be used when serializing a {@link ExcerptFields} to JSON/XML. The string will have the format
     * {@code field/offset,field/offset,...}, e.g. {@code BODY/10,CONTENT/5}.
     *
     * @return a formatted string
     */
    @JsonValue
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String,Integer>> iterator = fieldMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String,Integer> entry = iterator.next();
            sb.append(entry.getKey()).append(Constants.FORWARD_SLASH).append(entry.getValue());
            if (iterator.hasNext()) {
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
        ExcerptFields that = (ExcerptFields) o;
        return Objects.equals(fieldMap, that.fieldMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldMap);
    }
}
