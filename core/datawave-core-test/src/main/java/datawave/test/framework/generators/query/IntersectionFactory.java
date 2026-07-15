package datawave.test.framework.generators.query;

import java.util.HashSet;
import java.util.Set;

import datawave.test.framework.generators.query.term.QueryTerm;

/**
 * A generic {@link QueryMetadataFactory} that intersects two {@link QueryTerm} instances: {@code left && right}
 * <p>
 * At most one of the two terms may be negated, since an intersection needs at least one positive term to anchor the query. A negated term's ids are subtracted
 * from the other side's ids rather than intersected, e.g. {@code FIELD_A == 'x' && filter:isNull(FIELD_B)} matches events where FIELD_A's ids do not include
 * FIELD_B's ids.
 */
public class IntersectionFactory extends BinaryTermFactory {

    public IntersectionFactory(QueryTerm leftTerm, QueryTerm rightTerm) {
        super(leftTerm, rightTerm);
    }

    @Override
    protected String operator() {
        return "&&";
    }

    @Override
    protected Set<Integer> combine(Set<Integer> leftIds, boolean leftNegated, Set<Integer> rightIds, boolean rightNegated) {
        if (leftNegated) {
            Set<Integer> result = new HashSet<>(rightIds);
            result.removeAll(leftIds);
            return result;
        }
        if (rightNegated) {
            Set<Integer> result = new HashSet<>(leftIds);
            result.removeAll(rightIds);
            return result;
        }
        Set<Integer> result = new HashSet<>(leftIds);
        result.retainAll(rightIds);
        return result;
    }

    @Override
    protected void validateTerms(QueryTerm leftTerm, QueryTerm rightTerm) {
        if (leftTerm.isNegated() && rightTerm.isNegated()) {
            throw new IllegalStateException("An intersection cannot contain two negated terms");
        }
    }
}
