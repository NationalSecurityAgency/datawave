package datawave.test.framework.generators.query.term;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import datawave.data.type.Type;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.generators.query.QueryMetadata;

/**
 * Handles building a portion of {@link QueryMetadata} for a bounded range term.
 * <p>
 * <code>((_Bounded_ = true) &amp;&amp; (FIELD &gt;= 'low' &amp;&amp; FIELD &lt;= 'high'))</code>
 * <p>
 * Not negated and not a filter function: this term's ids are self-contained and require no external context to interpret.
 * <p>
 * Bounds are drawn from pairs of a field's own generated values, so every range is guaranteed to have at least one indexed value inside it. When a query
 * planner fully expands a bounded range (see {@link #givenValue(String)}), a range whose bounds normalize to the same value collapses to a plain equality, the
 * same way DataWave's own query planner collapses a single-value bounded range; wider ranges expand into an OR of equalities.
 * <p>
 * Not supported against content fields (see {@link FieldMetadata#isContentField()}): a content field's index carries an entry per tokenized word in addition to
 * the field's own whole-value entries (needed for {@code content:phrase(...)}), so a range scan between two of the field's whole values can also sweep in
 * unrelated per-word entries that this framework's value model has no way to predict. {@link #valuesFor(FieldMetadata)} returns no ranges for such fields.
 */
public class BoundedRangeTerm extends AbstractQueryTerm {

    private static final String INDEX_DELIMITER = ",";

    /**
     * Every distinct pair of the field's values, encoded as a "i{@value #INDEX_DELIMITER}j" pair of indices into {@link FieldMetadata#getValues()} per
     * {@link QueryTerm#givenValue(String)}'s single-value contract. Indices (rather than the values themselves) are used so the encoding is unambiguous
     * regardless of what characters a generated value contains (e.g. multi-word content/phrase values, which themselves contain spaces).
     * <p>
     * Returns no ranges for content fields; see the class-level Javadoc.
     */
    @Override
    public List<String> valuesFor(FieldMetadata fieldMetadata) {
        Preconditions.checkState(fieldMetadata.getNormalizers().size() == 1, "range fields must have exactly one normalizer");

        if (fieldMetadata.isContentField()) {
            return List.of();
        }

        int size = fieldMetadata.getValues().size();
        List<String> encoded = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++) {
                encoded.add(i + INDEX_DELIMITER + j);
            }
        }
        return encoded;
    }

    @Override
    public void givenValue(String value) {
        Preconditions.checkNotNull(value, "Value cannot be null");
        Preconditions.checkNotNull(metadata, "must call givenFieldMetadata() first");

        String[] indices = value.split(INDEX_DELIMITER, 2);
        String first = metadata.getValues().get(Integer.parseInt(indices[0]));
        String second = metadata.getValues().get(Integer.parseInt(indices[1]));

        Type<?> normalizer = metadata.getNormalizers().get(0);
        boolean firstIsLow = normalizer.normalize(first).compareTo(normalizer.normalize(second)) <= 0;
        String low = firstIsLow ? first : second;
        String high = firstIsLow ? second : first;

        List<String> normalizedLow = createNormalizedValues(low);
        List<String> normalizedHigh = createNormalizedValues(high);
        if (normalizedLow.size() != 1 || normalizedHigh.size() != 1) {
            throw new RuntimeException("Expected a single normalized value for each bound but got: " + normalizedLow + ", " + normalizedHigh);
        }

        String field = metadata.getNormalizedFieldName();
        this.queryTerm = "((_Bounded_ = true) && (" + field + " >= '" + low + "' && " + field + " <= '" + high + "'))";
        this.planTerm = normalizedLow.get(0).equals(normalizedHigh.get(0)) ? field + " == '" + normalizedLow.get(0) + "'"
                        : "(" + field + " == '" + normalizedLow.get(0) + "' || " + field + " == '" + normalizedHigh.get(0) + "')";
        this.eventIds = metadata.getEventIdsForRange(low, high);
    }
}
