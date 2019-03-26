package datawave.query.ancestor;

import datawave.query.iterator.QueryIteratorIT;
import org.junit.Before;

import java.io.IOException;

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
     * @return
     */
    @Override
    protected boolean isExpectHitTerm() {
        return true;
    }
}
