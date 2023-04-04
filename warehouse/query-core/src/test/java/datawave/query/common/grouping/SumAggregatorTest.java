package datawave.query.common.grouping;

import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.attributes.Content;
import datawave.query.attributes.Numeric;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.K;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link SumAggregator}.
 */
public class SumAggregatorTest {
    
    private SumAggregator aggregator;
    
    @Before
    public void setUp() throws Exception {
        aggregator = new SumAggregator("FIELD");
    }
    
    /**
     * Verify the initial sum is 0.
     */
    @Test
    public void testInitialSum() {
        assertSum(null);
    }
    
    /**
     * Verify that if given a non-numeric value, that an exception is thrown.
     */
    @Test
    public void testNonNumericValue() {
        Content content = new Content("i am content", new Key(), true);
        
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> aggregator.aggregate(content));
        assertEquals("Unable to calculate a sum with non-numerical value 'i am content'", exception.getMessage());
    }
    
    /**
     * Verify that given additional numeric values, that the sum is correctly calculated.
     */
    @Test
    public void testAggregation() {
        aggregator.aggregate(createNumeric("4"));
        assertSum(new BigDecimal("4"));
        
        aggregator.aggregate(createNumeric("1"));
        aggregator.aggregate(createNumeric("1"));
        assertSum(new BigDecimal("6"));
        
        aggregator.aggregate(createNumeric("4.5"));
        assertSum(new BigDecimal("10.5"));
    }
    
    private TypeAttribute<BigDecimal> createNumeric(String number) {
        Type<BigDecimal> type = new NumberType(number);
        return new TypeAttribute<>(type, new Key(), true);
    }
    
    private void assertSum(BigDecimal sum) {
        assertEquals(sum, aggregator.getAggregation());
    }
}
