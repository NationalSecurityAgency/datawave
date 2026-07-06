package datawave.test.framework.generators.query.term;

import datawave.test.framework.generators.query.QueryMetadata;

/**
 * Handles building a portion of {@link QueryMetadata} for an EQ term.
 * <p>
 * <code>FIELD == 'value'</code>
 * <p>
 * Not negated and not a filter function: this term's ids are self-contained and require no external context to interpret.
 */
public class EqTerm extends AbstractQueryTerm {

    @Override
    public void givenValue(String value) {
        applyEqualityTerm(value, false);
    }
}
