package datawave.core.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

/**
 * Unit tests for the {@link FieldedRegexExpansionIterator}
 * <p>
 * Each test will exercise the forward and reverse index so care should be taken when constructing regexes
 */
public class FieldedRegexExpansionIteratorTest {

    private final SortedMap<Key,Value> forwardData = new TreeMap<>();
    private final SortedMap<Key,Value> reverseData = new TreeMap<>();
    private final Map<String,String> options = new HashMap<>();
    private final Set<String> expected = new HashSet<>();
    private final Set<String> results = new HashSet<>();

    private String pattern;
    private final StringBuilder sb = new StringBuilder();

    private static final Value EMPTY_VALUE = new Value();

    @BeforeEach
    public void setup() {
        forwardData.clear();
        reverseData.clear();
        options.clear();
        expected.clear();
        results.clear();
    }

    public void withData(String value, String field, String shard, String datatype) {
        // don't need to worry about visibility for this test
        Key key = new Key(value, field, shard + "\0" + datatype);
        forwardData.put(key, EMPTY_VALUE);

        String reversedValue = new StringBuilder(value).reverse().toString();
        Key reversedKey = new Key(reversedValue, field, shard + "\0" + datatype);
        reverseData.put(reversedKey, EMPTY_VALUE);
    }

    public void withDates(String startDate, String endDate) {
        options.put(FieldedRegexExpansionIterator.START_DATE, startDate);
        options.put(FieldedRegexExpansionIterator.END_DATE, endDate);
    }

    public void withDatatypeFilter(Set<String> datatypes) {
        options.put(FieldedRegexExpansionIterator.DATATYPES, Joiner.on(',').join(datatypes));
    }

    public void withFieldPattern(String field, String pattern) {
        options.put(FieldedRegexExpansionIterator.FIELD, field);
        this.pattern = pattern;
    }

    public void withExpected(String... values) {
        expected.addAll(List.of(values));
    }

    @Test
    public void testOptionsMissingField() {
        withFieldPattern(null, "val");
        assertThrows(IllegalArgumentException.class, this::drive);
    }

    @Test
    public void testOptionsMissingPattern() {
        withFieldPattern("FIELD", null);
        assertThrows(IllegalArgumentException.class, this::drive);
    }

    @Test
    public void testOptionsMissingDate() {
        withFieldPattern("FIELD", "val");
        assertThrows(IllegalArgumentException.class, this::drive);
    }

    @Test
    public void testSingleValueExpansion() throws Exception {
        withData("aaa", "FIELD_A", "20250804_0", "datatype-a");
        withFieldPattern("FIELD_A", "aa");
        withDates("20250804", "20250804");
        withExpected("aaa");
        drive();
    }

    @Test
    public void testSingleValueExpansionWithDatatypeFilter() throws Exception {
        withData("aaa", "FIELD_A", "20250804_0", "datatype-a");
        withFieldPattern("FIELD_A", "aa");
        withDates("20250804", "20250804");
        withDatatypeFilter(Set.of("datatype-a"));
        withExpected("aaa");
        drive();
    }

    @Test
    public void testSingleValueExpansionExcludedByDatatypeFilter() throws Exception {
        withData("aaa", "FIELD_A", "20250804_0", "datatype-a");
        withFieldPattern("FIELD_A", "aa");
        withDates("20250804", "20250804");
        withDatatypeFilter(Set.of("datatype-b"));
        drive();
    }

    @Test
    public void testMultiValueExpansion() throws Exception {
        withData("aa", "FIELD_A", "20250804_0", "datatype-a");
        withData("ab", "FIELD_A", "20250804_0", "datatype-a");
        withData("ac", "FIELD_A", "20250804_0", "datatype-a");
        withFieldPattern("FIELD_A", "a");
        withDates("20250804", "20250804");
        withExpected("aa", "ab", "ac");
        drive();
    }

    @Test
    public void testSkipField() throws Exception {
        withData("aa", "FIELD_B", "20250804_0", "datatype-a");
        withData("ab", "FIELD_A", "20250804_0", "datatype-a");
        withData("ac", "FIELD_B", "20250804_0", "datatype-a");
        withFieldPattern("FIELD_B", "a");
        withDates("20250804", "20250804");
        withExpected("aa", "ac");
        drive();
    }

    @Test
    public void testSkipDate() throws Exception {
        withData("aa", "FIELD_B", "20250804_0", "datatype-a");
        withData("ab", "FIELD_B", "20250803_0", "datatype-a");
        withData("ab", "FIELD_B", "20250805_0", "datatype-a");
        withData("ac", "FIELD_B", "20250804_0", "datatype-a");
        withFieldPattern("FIELD_B", "a");
        withDates("20250804", "20250804");
        withExpected("aa", "ac");
        drive();
    }

    @Test
    public void testVerifyDatatypeFilter() throws Exception {
        withData("aa", "FIELD_A", "20250804_0", "datatype-a");
        withData("ab", "FIELD_A", "20250804_0", "datatype-b");
        withData("ac", "FIELD_A", "20250804_0", "datatype-c");
        withFieldPattern("FIELD_A", "a");
        withDates("20250804", "20250804");

        withDatatypeFilter(Set.of("datatype-a"));
        withExpected("aa");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-b"));
        withExpected("ab");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-c"));
        withExpected("ac");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-a", "datatype-b"));
        withExpected("aa", "ab");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-a", "datatype-c"));
        withExpected("aa", "ac");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-b", "datatype-c"));
        withExpected("ab", "ac");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-a", "datatype-b", "datatype-c"));
        withExpected("aa", "ab", "ac");
        drive();
    }

    @Test
    public void testVerifyDateBounds() throws Exception {
        withData("aa", "FIELD_A", "20250803_0", "datatype-a");
        withData("ab", "FIELD_A", "20250804_0", "datatype-a");
        withData("ac", "FIELD_A", "20250805_0", "datatype-a");
        withFieldPattern("FIELD_A", "a");
        withDates("20250804", "20250804");
        withExpected("ab");
        drive();
    }

    private void drive() throws Exception {
        exerciseForwardIndex();
        exerciseReverseIndex();
    }

    private void exerciseForwardIndex() throws Exception {
        String trailingRegex = pattern + ".*";
        options.put(FieldedRegexExpansionIterator.PATTERN, trailingRegex);
        driveIterator(forwardData, false);
    }

    private void exerciseReverseIndex() throws Exception {
        String leadingRegex = ".*" + pattern;
        options.put(FieldedRegexExpansionIterator.PATTERN, leadingRegex);
        driveIterator(reverseData, true);
    }

    private void driveIterator(SortedMap<Key,Value> data, boolean reverse) throws Exception {
        FieldedRegexExpansionIterator iterator = new FieldedRegexExpansionIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        iterator.seek(new Range(), Collections.emptySet(), false);

        results.clear();
        while (iterator.hasTop()) {
            Key tk = iterator.getTopKey();
            String value = tk.getRow().toString();
            results.add(handleValue(value, reverse));
            iterator.next();
        }

        assertEquals(expected, results);
    }

    private String handleValue(String value, boolean reverse) {
        if (reverse) {
            sb.setLength(0);
            sb.append(value);
            sb.reverse();
            return sb.toString();
        }
        return value;
    }

}
