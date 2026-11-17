package datawave.test.framework;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Predicate;

import datawave.test.framework.util.MetadataColumn;

/**
 * A wrapper around a list of {@link FieldMetadata} that allows the test framework to generate queries and calculate the expected event ids.
 * <p>
 * Supports access to logical groups of {@link FieldMetadata} such as "index-only fields" or "event-only" fields.
 */
public class QueryFieldMetadata {

    private final List<FieldMetadata> fields;

    /**
     * Wrap a snapshot of the fields. The list is copied, so a view stays fixed even if the source list is added to afterwards.
     *
     * @param fields
     *            the fields to expose
     * @return the query view over the fields
     */
    public static QueryFieldMetadata of(List<FieldMetadata> fields) {
        return new QueryFieldMetadata(List.copyOf(fields));
    }

    private QueryFieldMetadata(List<FieldMetadata> fields) {
        this.fields = fields;
    }

    /**
     * Get any {@link FieldMetadata} that has a {@link MetadataColumn#I} entry.
     * <p>
     * Excludes the synthetic ID field (see {@link #getId()}): it is indexed like a content field, but its value count is defined to always equal the event
     * count, so mixing it into this group would make query counts derived from it vary with the event count.
     *
     * @return all indexed field metadata
     */
    public List<FieldMetadata> getIndexed() {
        return selectNonIdFields(field -> has(field, I));
    }

    /**
     * Get any {@link FieldMetadata} that has a {@link MetadataColumn#I} entry without a {@link MetadataColumn#E} entry.
     *
     * @return all index only field metadata
     */
    public List<FieldMetadata> getIndexOnly() {
        return selectNonIdFields(field -> has(field, I) && !has(field, E));
    }

    /**
     * Get any {@link FieldMetadata} that has a {@link MetadataColumn#E} entry without a {@link MetadataColumn#I} entry.
     *
     * @return all event only field metadata
     */
    public List<FieldMetadata> getEventOnly() {
        return selectNonIdFields(field -> has(field, E) && !has(field, I));
    }

    /**
     * Get any {@link FieldMetadata} that is a content field (see {@link FieldMetadata#isContentField()}) with a {@link MetadataColumn#I} entry, without a
     * {@link MetadataColumn#E} entry.
     * <p>
     * These fields can self-anchor a {@code content:phrase(...)} query via the function's own index-expansion into {@code FIELD == 'word'} nodes, the same way
     * {@link #getIndexOnly()} fields self-anchor an equality query.
     *
     * @return all tokenized, index-only field metadata
     */
    public List<FieldMetadata> getTokenizedIndexOnly() {
        return selectNonIdFields(field -> field.isContentField() && has(field, I) && !has(field, E));
    }

    /**
     * Get any {@link FieldMetadata} that is a content field (see {@link FieldMetadata#isContentField()}) with a {@link MetadataColumn#E} entry, without a
     * {@link MetadataColumn#I} entry.
     * <p>
     * These fields have no index anchor of their own, so a {@code content:phrase(...)} query against them must be paired with an external indexed anchor term
     * elsewhere in the query, the same way {@link #getEventOnly()} fields must be paired with filter functions.
     *
     * @return all tokenized, event-only field metadata
     */
    public List<FieldMetadata> getTokenizedEventOnly() {
        return selectNonIdFields(field -> field.isContentField() && has(field, E) && !has(field, I));
    }

    /**
     * Get the synthetic, per-event unique identifier field (see {@link IngestMetadata#ID_FIELD_NAME}), if present.
     *
     * @return the ID field metadata, or an empty list if it is not present in this instance's fields
     */
    public List<FieldMetadata> getId() {
        return select(this::isIdField);
    }

    /**
     * Select the fields matching the predicate, excluding the synthetic ID field.
     * <p>
     * Every group except {@link #getId()} excludes it, so the exclusion lives here rather than being restated in each accessor where it could be forgotten.
     *
     * @param predicate
     *            the predicate a field must satisfy
     * @return the matching field metadata
     */
    private List<FieldMetadata> selectNonIdFields(Predicate<FieldMetadata> predicate) {
        return select(field -> !isIdField(field) && predicate.test(field));
    }

    /**
     * Select the fields matching the predicate.
     *
     * @param predicate
     *            the predicate a field must satisfy
     * @return the matching field metadata
     */
    private List<FieldMetadata> select(Predicate<FieldMetadata> predicate) {
        List<FieldMetadata> results = new ArrayList<>();
        for (FieldMetadata field : fields) {
            if (predicate.test(field)) {
                results.add(field);
            }
        }
        return results;
    }

    private boolean has(FieldMetadata field, MetadataColumn column) {
        return field.getMetadataColumns().contains(column);
    }

    private boolean isIdField(FieldMetadata field) {
        return field.getFieldName().equals(IngestMetadata.ID_FIELD_NAME);
    }

    /**
     * Count how many of a field's generated values equal the given value.
     * <p>
     * This is a count of values, not of events. For the number of events carrying a value use {@link FieldMetadata#getEventIdsForValue(String)}.
     *
     * @param fieldValue
     *            the field value pair
     * @return the number of the field's values equal to the given value
     */
    public int getValueCountForEntry(Entry<String,String> fieldValue) {
        int count = 0;
        for (FieldMetadata field : fields) {
            if (field.getFieldName().equals(fieldValue.getKey())) {
                List<String> values = field.getValues();
                for (String value : values) {
                    if (value.equals(fieldValue.getValue())) {
                        count++;
                    }
                }
            }
        }
        return count;
    }
}
