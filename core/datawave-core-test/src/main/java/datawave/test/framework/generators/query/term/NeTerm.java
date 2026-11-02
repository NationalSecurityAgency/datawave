package datawave.test.framework.generators.query.term;

/**
 * Builds {@code !(FIELD == 'value')}. Its event ids still identify events matching the value; combining logic applies the negation.
 */
public class NeTerm extends ValueDependentQueryTerm {

    public NeTerm() {
        this.isNegated = true;
    }

    @Override
    protected void apply(String value) {
        applyEqualityTerm(value, true);
    }
}
