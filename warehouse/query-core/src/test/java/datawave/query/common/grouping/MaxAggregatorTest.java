package datawave.query.common.grouping;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Content;
import datawave.query.attributes.DateContent;
import datawave.query.attributes.DiacriticContent;
import datawave.query.attributes.Numeric;

/**
 * Tests for {@link MaxAggregator}.
 */
public class MaxAggregatorTest {

    private MaxAggregator aggregator;

    @Before
    public void setUp() throws Exception {
        aggregator = new MaxAggregator("FIELD");
    }

    /**
     * Verify that the initial max is null.
     */
    @Test
    public void testInitialMax() {
        assertMax(null);
    }

    /**
     * Verify that if given a value that is of a different type than the current max, that an exception is thrown.
     */
    @Test
    public void testConflictingTypes() {
        Content first = createContent("aaa");
        aggregator.aggregate(first);
        assertMax(first);

        DiacriticContent diacriticContent = new DiacriticContent("different content type", new Key(), true);
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> aggregator.aggregate(diacriticContent));
        assertEquals("Failed to compare current max 'aaa' to new value 'different content type'", exception.getMessage());
    }

    /**
     * Verify that finding the max of string values works.
     */
    @Test
    public void testStringAggregation() {
        Content content = createContent("a");
        aggregator.aggregate(content);
        assertMax(content);

        // Verify the max updated.
        content = createContent("d");
        aggregator.aggregate(content);
        assertMax(content);

        // Verify the max did not change.
        aggregator.aggregate(createContent("b"));
        assertMax(content);
    }

    /**
     * Verify that finding the max of number values works.
     */
    @Test
    public void testNumericAggregation() {
        Numeric numeric = createNumeric("1.5");
        aggregator.aggregate(numeric);
        assertMax(numeric);

        // Verify the max updated.
        numeric = createNumeric("10");
        aggregator.aggregate(numeric);
        assertMax(numeric);

        // Verify the max did not change.
        aggregator.aggregate(createNumeric("6"));
        assertMax(numeric);
    }

    /**
     * Verify that finding the max of date values work.
     */
    @Test
    public void testDateAggregation() {
        DateContent dateContent = createDateContent("20221201120000");
        aggregator.aggregate(dateContent);
        assertMax(dateContent);

        // Verify the max updated.
        dateContent = createDateContent("20251201120000");
        aggregator.aggregate(dateContent);
        assertMax(dateContent);

        // Verify the max did not change.
        aggregator.aggregate(createDateContent("20231201120000"));
        assertMax(dateContent);
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

    private void assertMax(Attribute<?> expected) {

        assertEquals(expected, aggregator.getAggregation());
    }
}
