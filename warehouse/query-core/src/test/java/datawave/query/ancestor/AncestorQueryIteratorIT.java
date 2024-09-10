package datawave.query.ancestor;

import java.io.IOException;

import org.junit.Before;

import datawave.query.iterator.QueryIteratorIT;

/**
 * AncestorQueryIterator integration tests. Ancestor Query should find any hits event query finds plus its own unique cases
 */
public class AncestorQueryIteratorIT extends QueryIteratorIT {
    @Before
    public void setup() throws IOException {
        super.setup();
        iterator = new AncestorQueryIterator();
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
