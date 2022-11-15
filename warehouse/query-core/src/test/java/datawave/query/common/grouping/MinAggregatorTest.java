package datawave.query.common.grouping;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Content;
import datawave.query.attributes.DateContent;
import datawave.query.attributes.DiacriticContent;
import datawave.query.attributes.Numeric;
import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MinAggregatorTest {
    
    private MinAggregator aggregator;
    
    @Before
    public void setUp() throws Exception {
        aggregator = new MinAggregator("FIELD");
    }
    
    /**
     * Verify that the initial min is null.
     */
    @Test
    public void testInitialMin() {
        assertMin(null);
    }
    
    /**
     * Verify that if given a value that is of a different type than the current min, that an exception is thrown.
     */
    @Test
    public void testConflictingTypes() {
        Content first = createContent("aaa");
        aggregator.aggregate(first);
        assertMin(first);
        
        DiacriticContent diacriticContent = new DiacriticContent("different content type", new Key(), true);
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> aggregator.aggregate(diacriticContent));
        assertEquals("Unable to compare value of type class datawave.query.attributes.Content to min of type class datawave.query.attributes.DiacriticContent",
                        exception.getMessage());
    }
    
    /**
     * Verify that finding the min of string values works.
     */
    @Test
    public void testStringAggregation() {
        Content content = createContent("d");
        aggregator.aggregate(content);
        assertMin(content);
        
        // Verify the min updated.
        content = createContent("a");
        aggregator.aggregate(content);
        assertMin(content);
        
        // Verify the min did not change.
        aggregator.aggregate(createContent("b"));
        assertMin(content);
    }
    
    /**
     * Verify that finding the min of number values works.
     */
    @Test
    public void testNumericAggregation() {
        Numeric numeric = createNumeric("10");
        aggregator.aggregate(numeric);
        assertMin(numeric);
        
        // Verify the max updated.
        numeric = createNumeric("1.5");
        aggregator.aggregate(numeric);
        assertMin(numeric);
        
        // Verify the max did not change.
        aggregator.aggregate(createNumeric("6"));
        assertMin(numeric);
    }
    
    /**
     * Verify that finding the min of date values work.
     */
    @Test
    public void testDateAggregation() {
        DateContent dateContent = createDateContent("20251201120000");
        aggregator.aggregate(dateContent);
        assertMin(dateContent);
        
        // Verify the max updated.
        dateContent = createDateContent("20221201120000");
        aggregator.aggregate(dateContent);
        assertMin(dateContent);
        
        // Verify the max did not change.
        aggregator.aggregate(createDateContent("20231201120000"));
        assertMin(dateContent);
    }
    
    private Content createContent(String content) {
        return new Content(content, new Key(), true);
    }
    
    private Numeric createNumeric(String number) {
        return new Numeric(number, new Key(), true);
    }
    
    private DateContent createDateContent(String date) {
        return new DateContent(date, new Key(), true);
    }
    
    private void assertMin(Attribute<?> expected) {
        assertEquals(expected, aggregator.getAggregation());
    }
}
