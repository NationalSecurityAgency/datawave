package datawave.test.framework.generators.query;

import java.util.Set;

import datawave.test.framework.generators.query.term.QueryTerm;

/**
 * A generic {@link QueryMetadataFactory} that unions two {@link QueryTerm} instances: {@code left || right}
 * <p>
 * Neither term may be negated, since each branch of a union must independently resolve via the index.
 */
public class UnionFactory extends BinaryTermFactory {

    public UnionFactory(QueryTerm leftTerm, QueryTerm rightTerm) {
        super(leftTerm, rightTerm);
    }

    @Override
    protected String operator() {
        return "||";
    }

    @Override
    protected Set<Integer> combine(Set<Integer> leftIds, boolean leftNegated, Set<Integer> rightIds, boolean rightNegated) {
        leftIds.addAll(rightIds);
        return leftIds;
    }

    @Override
    protected void validateTerms(QueryTerm leftTerm, QueryTerm rightTerm) {
        if (leftTerm.isNegated() || rightTerm.isNegated()) {
            throw new IllegalStateException("A union cannot contain negated terms");
        }
    }
}
