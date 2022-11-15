package datawave.query.common.grouping;

import datawave.query.attributes.Content;
import datawave.query.attributes.Numeric;
import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link AverageAggregator}.
 */
public class AverageAggregatorTest {
    
    private AverageAggregator aggregator;
    
    @Before
    public void setUp() throws Exception {
        aggregator = new AverageAggregator("FIELD");
    }
    
    /**
     * Verify the initial average is 0.
     */
    @Test
    public void testInitialAverage() {
        assertAverage(0d);
    }
    
    /**
     * Verify that if given a non-numeric value, that an exception is thrown.
     */
    @Test
    public void testNonNumericValue() {
        Content content = new Content("i am content", new Key(), true);
        
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> aggregator.aggregate(content));
        assertEquals("Unable to calculate an average for an attribute of type datawave.query.attributes.Content", exception.getMessage());
    }
    
    /**
     * Verify that given additional numeric values, that the averages are correctly calculated.
     */
    @Test
    public void testAggregation() {
        aggregator.aggregate(createNumeric("4"));
        assertAverage(4d);
        
        aggregator.aggregate(createNumeric("1"));
        aggregator.aggregate(createNumeric("1")); // Sum 6, count 3
        assertAverage(2d);
        
        aggregator.aggregate(createNumeric("4.5"));
        assertAverage(2.625d);
    }
    
    private Numeric createNumeric(String number) {
        return new Numeric(number, new Key(), true);
    }
    
    private void assertAverage(Double average) {
        assertEquals(average, aggregator.getAggregation());
    }
}
