package datawave.query.iterator;

import datawave.query.function.RangeProvider;
import datawave.query.predicate.ParentRangeProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ParentQueryIteratorTest {
    
    @Test
    public void testGetRangeProvider() {
        ParentQueryIterator iterator = new ParentQueryIterator();
        RangeProvider provider = iterator.getRangeProvider();
        assertEquals(ParentRangeProvider.class.getSimpleName(), provider.getClass().getSimpleName());
    }
    
}
