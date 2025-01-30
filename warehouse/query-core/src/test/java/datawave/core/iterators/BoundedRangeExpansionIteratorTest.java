package datawave.core.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;

public class BoundedRangeExpansionIteratorTest {

    private static final Value emptyValue = new Value();

    private String startDate;
    private String endDate;

    private String lower;
    private String upper;

    private Set<String> datatypes = new HashSet<>();
    private Set<String> expected = new HashSet<>();

    @BeforeEach
    public void beforeEach() {
        startDate = null;
        endDate = null;
        lower = null;
        upper = null;
        datatypes.clear();
        expected.clear();
    }

    @Test
    public void testSingleDay_SingleValue_EmptyDatatypeFilter() {
        withBoundedRange("value-1", "value-1");
        withDateRange("20240501", "20240501");
        withExpected(Set.of("value-1"));
        drive();
    }

    @Test
    public void testSingleDay_SingleValue_CorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-1");
        withDateRange("20240501", "20240501");
        withDatatypes(Set.of("datatype-a"));
        withExpected(Set.of("value-1"));
        drive();
    }

    @Test
    public void testSingleDay_SingleValue_IncorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-1");
        withDateRange("20240501", "20240501");
        withDatatypes(Set.of("datatype-z"));
        drive();
    }

    @Test
    public void testSingleDay_MultiValue_EmptyDatatypeFilter() {
        withBoundedRange("value-1", "value-2");
        withDateRange("20240501", "20240501");
        withExpected(Set.of("value-1", "value-2"));
        drive();
    }

    @Test
    public void testSingleDay_MultiValue_CorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-2");
        withDateRange("20240501", "20240501");
        withDatatypes(Set.of("datatype-a"));
        withExpected(Set.of("value-1", "value-2"));
        drive();
    }

    @Test
    public void testSingleDay_MultiValue_IncorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-2");
        withDateRange("20240501", "20240501");
        withDatatypes(Set.of("datatype-z"));
        drive();
    }

    @Test
    public void testSingleDay_AllValues_EmptyDatatypeFilter() {
        withBoundedRange("value-1", "value-3");
        withDateRange("20240501", "20240501");
        withExpected(Set.of("value-1", "value-2", "value-3"));
        drive();
    }

    @Test
    public void testSingleDay_AllValues_CorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-3");
        withDateRange("20240501", "20240501");
        withDatatypes(Set.of("datatype-a"));
        withExpected(Set.of("value-1", "value-2"));
        // value-3 does not contain datatype-a
        drive();
    }

    @Test
    public void testSingleDay_AllValues_IncorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-3");
        withDateRange("20240501", "20240501");
        withDatatypes(Set.of("datatype-z"));
        drive();
    }

    //

    @Test
    public void testAllDays_SingleValue_EmptyDatatypeFilter() {
        withBoundedRange("value-1", "value-1");
        withDateRange("20240501", "20240505");
        withExpected(Set.of("value-1"));
        drive();
    }

    @Test
    public void testMultiDay_SingleValue_CorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-1");
        withDateRange("20240501", "20240505");
        withDatatypes(Set.of("datatype-a"));
        withExpected(Set.of("value-1"));
        drive();
    }

    @Test
    public void testMultiDay_SingleValue_IncorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-1");
        withDateRange("20240501", "20240505");
        withDatatypes(Set.of("datatype-z"));
        drive();
    }

    @Test
    public void testMultiDay_MultiValue_EmptyDatatypeFilter() {
        withBoundedRange("value-1", "value-2");
        withDateRange("20240501", "20240505");
        withExpected(Set.of("value-1", "value-2"));
        drive();
    }

    @Test
    public void testMultiDay_MultiValue_CorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-2");
        withDateRange("20240501", "20240505");
        withDatatypes(Set.of("datatype-a"));
        withExpected(Set.of("value-1", "value-2"));
        drive();
    }

    @Test
    public void testMultiDay_MultiValue_IncorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-2");
        withDateRange("20240501", "20240505");
        withDatatypes(Set.of("datatype-z"));
        drive();
    }

    @Test
    public void testMultiDay_AllValues_EmptyDatatypeFilter() {
        withBoundedRange("value-1", "value-3");
        withDateRange("20240501", "20240505");
        withExpected(Set.of("value-1", "value-2", "value-3"));
        drive();
    }

    @Test
    public void testMultiDay_AllValues_CorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-3");
        withDateRange("20240501", "20240505");
        withDatatypes(Set.of("datatype-a"));
        withExpected(Set.of("value-1", "value-2"));
        // value-3 does not contain datatype-a
        drive();
    }

    @Test
    public void testMultiDay_AllValues_IncorrectDatatypeFilter() {
        withBoundedRange("value-1", "value-3");
        withDateRange("20240501", "20240505");
        withDatatypes(Set.of("datatype-z"));
        drive();
    }

    private void drive() {
        assertNotNull(lower, "lower bound must be specified");
        assertNotNull(upper, "upper bound must be specified");
        assertNotNull(startDate, "start date must be specified");
        assertNotNull(endDate, "end date must be specified");

        Map<String,String> options = new HashMap<>();
        options.put(BoundedRangeExpansionIterator.START_DATE, startDate);
        options.put(BoundedRangeExpansionIterator.END_DATE, endDate);
        if (!datatypes.isEmpty()) {
            options.put(BoundedRangeExpansionIterator.DATATYPES_OPT, Joiner.on(',').join(datatypes));
        }

        SortedMapIterator data = createData();
        BoundedRangeExpansionIterator iter = new BoundedRangeExpansionIterator();

        Range range = new Range(lower, true, upper, true);

        try {
            iter.init(data, options, null);
            iter.seek(range, Collections.emptySet(), true);

            Set<String> results = new HashSet<>();
            while (iter.hasTop()) {
                Key k = iter.getTopKey();
                boolean first = results.add(k.getRow().toString());
                assertTrue(first, "Iterator returned the same row twice");
                iter.next();
            }

            assertEquals(expected, results);

        } catch (Exception e) {
            fail("Failed to execute test", e);
        }
    }

    @Test
    public void testTeardownRebuild() {
        withDateRange("20240501", "20240505");

        Map<String,String> options = new HashMap<>();
        options.put(BoundedRangeExpansionIterator.START_DATE, startDate);
        options.put(BoundedRangeExpansionIterator.END_DATE, endDate);
        if (!datatypes.isEmpty()) {
            options.put(BoundedRangeExpansionIterator.DATATYPES_OPT, Joiner.on(',').join(datatypes));
        }

        SortedMapIterator data = createData();
        BoundedRangeExpansionIterator iter = new BoundedRangeExpansionIterator();

        Range range = new Range("value-2", false, "value-3", true);

        try {
            iter.init(data, options, null);
            iter.seek(range, Collections.emptySet(), true);

            assertTrue(iter.hasTop());
            Key k = iter.getTopKey();

            assertEquals("value-3", k.getRow().toString());
        } catch (Exception e) {
            fail("Failed to execute test", e);
        }
    }

    private void withBoundedRange(String lower, String upper) {
        assertNotNull(lower);
        assertNotNull(upper);
        this.lower = lower;
        this.upper = upper;
    }

    private void withDateRange(String startDate, String endDate) {
        assertNotNull(startDate);
        assertNotNull(endDate);
        this.startDate = startDate;
        this.endDate = endDate;
    }

    private void withDatatypes(Set<String> datatypes) {
        assertFalse(datatypes.isEmpty());
        this.datatypes = datatypes;
    }

    private void withExpected(Set<String> expectedRows) {
        assertFalse(expectedRows.isEmpty());
        this.expected = expectedRows;
    }

    /**
     * Simulate fetching the column family by only having one field
     *
     * @return the data
     */
    private SortedMapIterator createData() {
        SortedMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("value-1", "FIELD_A", "20240501_0\u0000datatype-a"), emptyValue);
        data.put(new Key("value-1", "FIELD_A", "20240501_1\u0000datatype-a"), emptyValue);
        data.put(new Key("value-1", "FIELD_A", "20240501_2\u0000datatype-a"), emptyValue);
        data.put(new Key("value-1", "FIELD_A", "20240501_3\u0000datatype-a"), emptyValue);
        data.put(new Key("value-1", "FIELD_A", "20240502_0\u0000datatype-a"), emptyValue);
        data.put(new Key("value-1", "FIELD_A", "20240503_0\u0000datatype-a"), emptyValue);
        data.put(new Key("value-1", "FIELD_A", "20240504_0\u0000datatype-a"), emptyValue);
        data.put(new Key("value-1", "FIELD_A", "20240505_0\u0000datatype-a"), emptyValue);

        data.put(new Key("value-2", "FIELD_A", "20240501_0\u0000datatype-a"), emptyValue);
        data.put(new Key("value-2", "FIELD_A", "20240501_0\u0000datatype-b"), emptyValue);
        data.put(new Key("value-2", "FIELD_A", "20240502_0\u0000datatype-a"), emptyValue);
        data.put(new Key("value-2", "FIELD_A", "20240502_0\u0000datatype-b"), emptyValue);
        data.put(new Key("value-2", "FIELD_A", "20240503_0\u0000datatype-a"), emptyValue);
        data.put(new Key("value-2", "FIELD_A", "20240503_0\u0000datatype-b"), emptyValue);
        data.put(new Key("value-2", "FIELD_A", "20240504_0\u0000datatype-a"), emptyValue);
        data.put(new Key("value-2", "FIELD_A", "20240504_0\u0000datatype-b"), emptyValue);
        data.put(new Key("value-2", "FIELD_A", "20240505_0\u0000datatype-a"), emptyValue);
        data.put(new Key("value-2", "FIELD_A", "20240505_0\u0000datatype-b"), emptyValue);

        data.put(new Key("value-3", "FIELD_A", "20240501_0\u0000datatype-b"), emptyValue);
        data.put(new Key("value-3", "FIELD_A", "20240501_1\u0000datatype-b"), emptyValue);
        data.put(new Key("value-3", "FIELD_A", "20240502_0\u0000datatype-b"), emptyValue);
        data.put(new Key("value-3", "FIELD_A", "20240502_1\u0000datatype-b"), emptyValue);
        data.put(new Key("value-3", "FIELD_A", "20240503_0\u0000datatype-b"), emptyValue);
        data.put(new Key("value-3", "FIELD_A", "20240503_1\u0000datatype-b"), emptyValue);
        data.put(new Key("value-3", "FIELD_A", "20240504_0\u0000datatype-b"), emptyValue);
        data.put(new Key("value-3", "FIELD_A", "20240504_1\u0000datatype-b"), emptyValue);
        data.put(new Key("value-3", "FIELD_A", "20240505_0\u0000datatype-b"), emptyValue);
        data.put(new Key("value-3", "FIELD_A", "20240505_1\u0000datatype-b"), emptyValue);
        return new SortedMapIterator(data);
    }
}
