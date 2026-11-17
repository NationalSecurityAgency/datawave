package datawave.test.framework.generators.query.term;

/**
 * Builds {@code filter:isNotNull(FIELD)} using the field's presence ids. As a filter, it must be paired with a non-filter, non-negated term.
 */
public class IsNotNullTerm extends ValueIndependentQueryTerm {

    @Override
    protected void apply() {
        this.queryTerm = "filter:isNotNull(" + metadata.getNormalizedFieldName() + ")";
        this.planTerm = "!(" + metadata.getNormalizedFieldName() + " == null)";
        this.eventIds = metadata.getEventIds();
    }
}
