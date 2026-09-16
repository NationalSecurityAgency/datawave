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

    // built on first use and discarded whenever the values or event ids change, so repeated query generation does not rescan the event ids per value
    private Map<String,List<Integer>> eventIdsByValue = null;

    private boolean isFieldNumeric = false;
    private ValueGenerator<?> valueGenerator;
    private EventIdGenerator eventIdGenerator;

    public FieldMetadata(String fieldName) {
        Preconditions.checkNotNull(fieldName, "FieldName cannot be null");
        this.fieldName = fieldName;
    }

    /**
     * Set the normalizers applied to this field's values.
     * <p>
     * A normalized field always carries a type ({@link MetadataColumn#T}) column, which is derived rather than set by the caller. This setter and
     * {@link #setMetadataColumns(List)} may therefore be called in either order.
     *
     * @param normalizers
     *            the normalizers
     */
    public void setNormalizers(List<Type<?>> normalizers) {
        Preconditions.checkNotNull(normalizers, "normalizers cannot be null");
        this.normalizers = normalizers;
        addTypeColumnIfNormalized();
    }

    /**
     * Set the metadata columns this field is written to. See {@link #setNormalizers(List)} for how the type column is derived.
     *
     * @param metadataColumns
     *            the metadata columns
     */
    public void setMetadataColumns(List<MetadataColumn> metadataColumns) {
        Preconditions.checkNotNull(metadataColumns, "metadataColumns cannot be null");
        this.metadataColumns = new ArrayList<>(metadataColumns);
        addTypeColumnIfNormalized();
    }

    /**
     * Add the type column once both halves of the pair are known, so neither setter depends on being called first.
     */
    private void addTypeColumnIfNormalized() {
        if (normalizers == null || normalizers.isEmpty() || metadataColumns == null) {
            return;
        }
        if (!metadataColumns.contains(T)) {
            metadataColumns.add(T);
        }
    }

    public void setDatatypes(List<String> datatypes) {
        Preconditions.checkNotNull(datatypes, "datatypes cannot be null");
        this.datatypes = List.copyOf(datatypes);
    }

    public void setValues(List<String> values) {
        Preconditions.checkNotNull(values, "values cannot be null");
        // an empty list would make the value-to-event mapping divide by zero
        Preconditions.checkArgument(!values.isEmpty(), "values cannot be empty");
        this.values = values;
        this.eventIdsByValue = null;
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
        Preconditions.checkNotNull(valueGenerator, "ValueGenerator cannot be null");
        Preconditions.checkArgument(eventCount > 0, "eventCount must be greater than 0");
        // a field with no values has no value-to-event mapping to divide by
        Preconditions.checkArgument(valuesPerField > 0, "valuesPerField must be greater than 0");
        eventIdGenerator.setOffset(offset);

        setEventIds(eventIdGenerator.generateWithinBound(eventCount));

        values = new ArrayList<>();
        eventIdsByValue = null;
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
        Preconditions.checkNotNull(eventIds, "eventIds cannot be null");
        this.eventIds = eventIds;
        this.eventIdIndex = new HashMap<>();
        for (int i = 0; i < eventIds.size(); i++) {
            eventIdIndex.put(eventIds.get(i), i);
        }
        this.eventIdsByValue = null;
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
     * Get all event ids for the given value, in ascending event id order.
     * <p>
     * Query generation calls this once per value per field, so the mapping is indexed on first use rather than rescanning every event id on each call. The
     * returned list is immutable and shared between callers.
     *
     * @param value
     *            the value
     * @return the list of event ids where the value appears, empty if the value has no backing events
     */
    public List<Integer> getEventIdsForValue(String value) {
        Preconditions.checkNotNull(value, "Cannot find event ids for a null value");
        if (eventIdsByValue == null) {
            eventIdsByValue = indexEventIdsByValue();
        }
        return eventIdsByValue.getOrDefault(value, List.of());
    }

    /**
     * Build the value to event id mapping, walking the event ids once.
     *
     * @return an immutable map of value to the event ids carrying it
     */
    private Map<String,List<Integer>> indexEventIdsByValue() {
        Preconditions.checkNotNull(eventIds, "EventIds cannot be null");
        Preconditions.checkNotNull(values, "FieldMetadata values were not created");

        Map<String,List<Integer>> byValue = new HashMap<>();
        for (int i = 0; i < eventIds.size(); i++) {
            String value = values.get(i % values.size());
            byValue.computeIfAbsent(value, k -> new ArrayList<>()).add(eventIds.get(i));
        }

        Map<String,List<Integer>> immutable = new HashMap<>();
        for (Map.Entry<String,List<Integer>> entry : byValue.entrySet()) {
            immutable.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return immutable;
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
