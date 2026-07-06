package datawave.test.framework.generators.query.term;

import datawave.test.framework.generators.query.QueryMetadata;

/**
 * Handles building a portion of {@link QueryMetadata} for a NE term.
 * <p>
 * <code>!(FIELD == 'value')</code>
 * <p>
 * Negated: requires external context. The ids tracked here are the ids matching the given value, not the term's final result set - the combining
 * {@code QueryMetadataFactory} is responsible for interpreting them (e.g. as a set difference against the paired term's ids).
 */
public class NeTerm extends AbstractQueryTerm {

    public NeTerm() {
        this.isNegated = true;
    }

    @Override
    public void givenValue(String value) {
        applyEqualityTerm(value, true);
    }
}
