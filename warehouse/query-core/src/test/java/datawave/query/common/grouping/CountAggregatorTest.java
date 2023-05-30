package datawave.query.common.grouping;

import datawave.query.attributes.Content;
import org.apache.accumulo.core.data.Key;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link CountAggregator}.
 */
public class CountAggregatorTest {
    
    private final Content value = new Content("i am content", new Key(), true);
    private CountAggregator aggregator;
    
    @Before
    public void setUp() throws Exception {
        aggregator = new CountAggregator("FIELD");
    }
    
    /**
     * Verify that the initial count is 0.
     */
    @Test
    public void testInitialCount() {
        assertCount(0L);
    }
    
    /**
     * Verify that when a number of values are aggregated, that the count is increased by the number of values aggregated.
     */
    @Test
    public void testIncrementingCount() {
        aggregator.aggregate(value);
        assertCount(1L);
        
        aggregator.aggregate(value);
        assertCount(2L);
        
        aggregator.aggregate(value);
        aggregator.aggregate(value);
        assertCount(4L);
    }
    
    private void assertCount(Long count) {
        assertEquals(count, aggregator.getAggregation());
    }
    
}
