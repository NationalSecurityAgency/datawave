package datawave.test.framework.generators.query.term;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import datawave.data.type.Type;
import datawave.test.framework.FieldMetadata;

/**
 * Base class for {@link QueryTerm}, handles most logic.
 * <p>
 * Extending classes populate most variables upon a call to {@link #givenValue(String)}.
 */
public abstract class AbstractQueryTerm implements QueryTerm {

    protected FieldMetadata metadata;

    protected String queryTerm;
    protected String planTerm;
    protected List<Integer> eventIds;

    protected boolean isNegated = false;

    @Override
    public final void givenFieldMetadata(FieldMetadata metadata) {
        this.metadata = metadata;

    }

    /**
     * Shared implementation for equality-based terms ({@code EQ}/{@code NE}): validates the value, normalizes it, builds the query/plan text for
     * {@code FIELD == 'value'} (optionally negated), and resolves the matching event ids.
     *
     * @param value
     *            the value to build the equality term against
     * @param negate
     *            true to wrap the term as {@code !(FIELD == 'value')}
     */
    protected void applyEqualityTerm(String value, boolean negate) {
        Preconditions.checkNotNull(value, "Value cannot be null");
        Preconditions.checkNotNull(metadata, "must call givenFieldMetadata() first");
        List<String> normalizedValues = createNormalizedValues(value);
        if (normalizedValues.size() != 1) {
            throw new RuntimeException("Expected a single normalized value but got: " + normalizedValues);
        }
        String normalizedValue = normalizedValues.get(0);

        String eq = metadata.getNormalizedFieldName() + " == '" + value + "'";
        String planEq = metadata.getNormalizedFieldName() + " == '" + normalizedValue + "'";
        this.queryTerm = negate ? "!(" + eq + ")" : eq;
        this.planTerm = negate ? "!(" + planEq + ")" : planEq;
        this.eventIds = metadata.getEventIdsForValue(value);
    }

    @Override
    public final List<String> createNormalizedValues(String value) {
        List<String> normalizedValues = new ArrayList<>();
        for (Type<?> type : metadata.getNormalizers()) {
            String normalizedValue = type.normalize(value);
            normalizedValues.add(normalizedValue);
        }
        return normalizedValues;
    }

    @Override
    public final String getQueryTerm() {
        return queryTerm;
    }

    @Override
    public final String getPlanTerm() {
        return planTerm;
    }

    @Override
    public final List<Integer> getEventIds() {
        return eventIds;
    }

    @Override
    public final boolean isNegated() {
        return isNegated;
    }

    @Override
    public final void setIsNegated(boolean isNegated) {
        this.isNegated = isNegated;
    }
}
