package datawave.test.framework.generators.query.term;

/**
 * Builds {@code filter:isNull(FIELD)} using the field's presence ids. Combining logic excludes those ids. As a filter, it must be paired with a non-filter,
 * non-negated term.
 */
public class IsNullTerm extends ValueIndependentQueryTerm {

    public IsNullTerm() {
        this.isNegated = true;
    }

    @Override
    protected void apply() {
        this.queryTerm = "filter:isNull(" + metadata.getNormalizedFieldName() + ")";
        this.planTerm = metadata.getNormalizedFieldName() + " == null";
        this.eventIds = metadata.getEventIds();
    }
}
