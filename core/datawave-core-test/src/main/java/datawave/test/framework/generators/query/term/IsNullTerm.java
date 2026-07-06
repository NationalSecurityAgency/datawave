package datawave.test.framework.generators.query.term;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;

import datawave.test.framework.FieldMetadata;
import datawave.test.framework.generators.query.QueryMetadata;

/**
 * Builds a portion of {@link QueryMetadata} for an isNull term.
 * <p>
 * <code>filter:isNull(FIELD)</code>
 * <p>
 * Negated: requires external context. This term only tracks the ids where the field is present, i.e. the same ids as {@link IsNotNullTerm}. It is negated so
 * that combining factories (e.g. {@link datawave.test.framework.generators.query.IntersectionFactory}) know to treat those ids as ones to exclude rather than
 * match, e.g. {@code FIELD_A == 'x' && filter:isNull(FIELD_B)} matches events present in FIELD_A's ids but absent from FIELD_B's ids.
 * <p>
 * This is also a filter function and requires external context of a different kind - it cannot anchor a query on its own and must always be paired with a
 * non-filter, non-negated term elsewhere in the query.
 */
public class IsNullTerm extends AbstractQueryTerm {

    public IsNullTerm() {
        this.isNegated = true;
    }

    /**
     * This term does not depend on a specific value, so it should only be evaluated once per field.
     */
    @Override
    public List<String> valuesFor(FieldMetadata fieldMetadata) {
        return Collections.singletonList(null);
    }

    /**
     * The value is not used to generate the query term and plan term.
     */
    @Override
    public void givenValue(String value) {
        Preconditions.checkNotNull(metadata, "must call givenFieldMetadata() first");
        this.queryTerm = "filter:isNull(" + metadata.getNormalizedFieldName() + ")";
        this.planTerm = metadata.getNormalizedFieldName() + " == null";
        this.eventIds = metadata.getEventIds();
    }
}
