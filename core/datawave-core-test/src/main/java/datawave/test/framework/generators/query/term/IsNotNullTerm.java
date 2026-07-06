package datawave.test.framework.generators.query.term;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;

import datawave.test.framework.FieldMetadata;
import datawave.test.framework.generators.query.QueryMetadata;

/**
 * Builds a portion of {@link QueryMetadata} for an isNotNull term.
 * <p>
 * <code>filter:isNotNull(FIELD)</code>
 * <p>
 * Not negated: the ids tracked here (the field's own presence set) are already the term's final result set, no external interpretation needed. However, this is
 * a filter function and requires external context of a different kind - it cannot anchor a query on its own and must always be paired with a non-filter,
 * non-negated term elsewhere in the query (e.g. via {@code IntersectionFactory}).
 */
public class IsNotNullTerm extends AbstractQueryTerm {

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
        this.queryTerm = "filter:isNotNull(" + metadata.getNormalizedFieldName() + ")";
        this.planTerm = "!(" + metadata.getNormalizedFieldName() + " == null)";
        this.eventIds = metadata.getEventIds();
    }
}
