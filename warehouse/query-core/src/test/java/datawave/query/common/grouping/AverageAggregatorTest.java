package datawave.query.common.grouping;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.attributes.Content;
import datawave.query.attributes.TypeAttribute;

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
        assertAverage(null);
    }

    /**
     * Verify that if given a non-numeric value, that an exception is thrown.
     */
    @Test
    public void testNonNumericValue() {
        Content content = new Content("i am content", new Key(), true);

        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> aggregator.aggregate(content));
        assertEquals("Character i is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark.", exception.getMessage());
    }

    /**
     * Verify that given additional numeric values, that the averages are correctly calculated.
     */
    @Test
    public void testAggregation() {
        aggregator.aggregate(createNumeric("4"));
        assertAverage(new BigDecimal("4"));

        aggregator.aggregate(createNumeric("1"));
        aggregator.aggregate(createNumeric("1")); // Sum 6, count 3
        assertAverage(new BigDecimal("2"));

        aggregator.aggregate(createNumeric("4.5"));
        assertAverage(new BigDecimal("2.625"));
    }

    private TypeAttribute<BigDecimal> createNumeric(String number) {
        Type<BigDecimal> type = new NumberType(number);
        return new TypeAttribute<>(type, new Key(), true);
    }

    private void assertAverage(BigDecimal average) {
        assertEquals(average, aggregator.getAggregation());
    }
}
