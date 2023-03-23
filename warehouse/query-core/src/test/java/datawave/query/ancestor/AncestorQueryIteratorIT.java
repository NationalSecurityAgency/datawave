package datawave.query.ancestor;

import datawave.query.iterator.QueryIteratorIT;

/**
 * AncestorQueryIterator integration tests. Ancestor Query should find any hits event query finds plus its own unique cases
 */
public class AncestorQueryIteratorIT extends QueryIteratorIT {

    protected Class getIteratorClass() {
        return AncestorQueryIterator.class;
    }

    /**
     * ancestor query will always use HitListArithmetic which will add the HIT_TERM field to all results regardless of the option, overload all test to expect
     * and include this
     *
     * @return true
     */
    @Override
    protected boolean isExpectHitTerm() {
        return true;
    }
}
