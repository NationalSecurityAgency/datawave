package datawave.test.framework;

import static datawave.test.framework.util.MetadataColumn.T;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.data.type.Type;
import datawave.test.framework.generators.id.EventIdGenerator;
import datawave.test.framework.generators.value.ValueGenerator;
import datawave.test.framework.util.MetadataColumn;

/**
 * Contains all information required to generate a field and its associated values
 * <p>
 * The {@link ValueGenerator} determines what kind of values are generated
 * <p>
 * The {@link EventIdGenerator} determines the distribution of values.
 * <p>
 * Note: if the value generator is a singleton the values will be repeated across events
 * <p>
 * If the event id count is greater than the number of values then repeats are possible.
 */
public class FieldMetadata {

    private static final Logger log = LoggerFactory.getLogger(FieldMetadata.class);

    private final String fieldName;
    private List<MetadataColumn> metadataColumns;
    private List<Type<?>> normalizers;
    private List<String> datatypes;

    private int offset;
    private List<Integer> eventIds = null;
    private Map<Integer,Integer> eventIdIndex = null;
    private List<String> values = null;

    private boolean isFieldNumeric = false;
    private ValueGenerator<?> valueGenerator;
    private EventIdGenerator eventIdGenerator;

    public FieldMetadata(String fieldName) {
        Preconditions.checkNotNull(fieldName, "FieldName cannot be null");
        this.fieldName = fieldName;
    }

    public void setNormalizers(List<Type<?>> normalizers) {
        this.normalizers = normalizers;
        Preconditions.checkNotNull(metadataColumns, "metadataColumns cannot be null");
        if (!metadataColumns.contains(T)) {
            metadataColumns.add(T);
        }
    }

    public void setMetadataColumns(List<MetadataColumn> metadataColumns) {
        this.metadataColumns = new ArrayList<>(metadataColumns);
    }

    public void setDatatypes(List<String> datatypes) {
        this.datatypes = datatypes;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public void setValueGenerator(ValueGenerator<?> valueGenerator) {
        this.valueGenerator = valueGenerator;
    }

    public String getFieldName() {
        return fieldName;
    }

    /**
     * JEXL prepends a dollar sign to field names that are numeric
     *
     * @return the normalized field name
     */
    public String getNormalizedFieldName() {
        if (isFieldNumeric) {
            return "$" + fieldName;
        }
        return fieldName;
    }

    public List<Type<?>> getNormalizers() {
        return normalizers;
    }

    public List<MetadataColumn> getMetadataColumns() {
        return metadataColumns;
    }

    public List<String> getDatatypes() {
        return datatypes;
    }

    public List<String> getValues() {
        Preconditions.checkNotNull(values, "FieldMetadata values were not created");
        return values;
    }

    public ValueGenerator<?> getValueGenerator() {
        return valueGenerator;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public boolean isFieldNumeric() {
        return isFieldNumeric;
    }

    public void setFieldNumeric(boolean isFieldNumeric) {
        this.isFieldNumeric = isFieldNumeric;
    }

    /**
     * The presence of the {@link MetadataColumn#TF} column is itself the signal that content functions (e.g. {@code content:phrase}) apply to this field: its
     * value is a tokenizable phrase rather than an atomic term.
     *
     * @return true if this field carries a term frequency column
     */
    public boolean isContentField() {
        return metadataColumns.contains(MetadataColumn.TF);
    }

    public EventIdGenerator getEventIdGenerator() {
        return eventIdGenerator;
    }

    /**
     * Set the {@link EventIdGenerator} used by this instance of FieldMetadata.
     *
     * @param eventIdGenerator
     *            the generator
     */
    public void setEventIdGenerator(EventIdGenerator eventIdGenerator) {
        this.eventIdGenerator = eventIdGenerator;
    }

    /**
     * Populate the values given the event id count and the values per field
     *
     * @param eventCount
     *            the number of events
     * @param valuesPerField
     *            the number of values to generate
     */
    public void populateValues(int eventCount, int valuesPerField) {
        Preconditions.checkNotNull(eventIdGenerator, "EventIdGenerator cannot be null");
        eventIdGenerator.setOffset(offset);

        setEventIds(eventIdGenerator.generateWithinBound(eventCount));

        values = new ArrayList<>();
        // valuesPerField is a fixed property of the field's configuration and must not vary with the event distribution, otherwise the number of queries
        // generated from these values (one per distinct value) would change whenever the event count changes. A value with no backing events is a valid
        // "matches nothing" test case; writers must skip persisting such a value rather than this method dropping it from the list.
        for (int i = 0; i < valuesPerField; i++) {
            values.add(String.valueOf(valueGenerator.next()));
        }
    }

    public List<Integer> getEventIds() {
        return eventIds;
    }

    public void setEventIds(List<Integer> eventIds) {
        this.eventIds = eventIds;
        this.eventIdIndex = new HashMap<>();
        for (int i = 0; i < eventIds.size(); i++) {
            eventIdIndex.put(eventIds.get(i), i);
        }
    }

    /**
     * Find the value at the provided event index and return it. If the field does not map to the index then a null value is returned.
     *
     * @param eventId
     *            the event id
     * @return the value at the event index, or null if the field does not appear in the event
     */
    public String getValueForEventId(int eventId) {
        Preconditions.checkNotNull(eventIds, "EventIds cannot be null");
        Preconditions.checkNotNull(values, "FieldMetadata values were not created");

        Integer eventIndex = eventIdIndex.get(eventId);
        if (eventIndex == null) {
            return null;
        }

        int modifiedIndex = eventIndex % values.size();
        String value = values.get(modifiedIndex);
        log.trace("field {} index {} modified index {} value {}", fieldName, eventId, modifiedIndex, value);
        return value;
    }

    /**
     * Get all event ids for the given value.
     *
     * @param value
     *            the value
     * @return the list of event ids where the value appears
     */
    public List<Integer> getEventIdsForValue(String value) {
        Preconditions.checkNotNull(value, "Cannot find event ids for a null value");
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < eventIds.size(); i++) {
            int modifiedIndex = i % values.size();
            if (values.get(modifiedIndex).equals(value)) {
                results.add(eventIds.get(i));
            }
        }
        return results;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        FieldMetadata metadata = (FieldMetadata) o;
        //  @formatter:off
        return new EqualsBuilder()
                .append(offset, metadata.offset)
                .append(fieldName, metadata.fieldName)
                .append(isFieldNumeric, metadata.isFieldNumeric)
                .append(metadataColumns, metadata.metadataColumns)
                .append(normalizers, metadata.normalizers)
                .append(datatypes, metadata.datatypes)
                .append(eventIds, metadata.eventIds)
                .append(values, metadata.values)
                .isEquals();
        //  @formatter:on
    }

    @Override
    public int hashCode() {
        //  @formatter:off
        return new HashCodeBuilder(17, 37)
                .append(fieldName)
                .append(isFieldNumeric)
                .append(metadataColumns)
                .append(normalizers)
                .append(datatypes)
                .append(offset)
                .append(eventIds)
                .append(values)
                .toHashCode();
        //  @formatter:on
    }
}
