package datawave.test.framework.generators.query.term;

/**
 * Builds {@code FIELD == 'value'}. Its event ids identify events matching that value.
 */
public class EqTerm extends ValueDependentQueryTerm {

    @Override
    protected void apply(String value) {
        applyEqualityTerm(value, false);
    }
}
