package datawave.core.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

import datawave.query.data.parsers.ShardIndexKey;

/**
 * Unit tests for the {@link UnfieldedRegexExpansionIterator}
 * <p>
 * The UnfieldedRegexExpansionIterator should handle any test case that the FieldedRegexExpansionIterator can. It won't be as efficient, but the results should
 * be the same.
 */
public class UnfieldedRegexExpansionIteratorTest {

    private final SortedMap<Key,Value> data = new TreeMap<>();
    private final Map<String,String> options = new HashMap<>();
    private final List<String> expected = new ArrayList<>();
    private final List<String> results = new ArrayList<>();

    private final ShardIndexKey parser = new ShardIndexKey();

    private static final Value EMPTY_VALUE = new Value();

    @BeforeEach
    public void setup() {
        data.clear();
        options.clear();
        expected.clear();
        results.clear();
    }

    public void withData(String value, String field, String shard, String datatype) {
        // don't need to worry about visibility for this test
        Key key = new Key(value, field, shard + "\0" + datatype);
        data.put(key, EMPTY_VALUE);
    }

    public void withDates(String startDate, String endDate) {
        options.put(UnfieldedRegexExpansionIterator.START_DATE, startDate);
        options.put(UnfieldedRegexExpansionIterator.END_DATE, endDate);
    }

    public void withDatatypeFilter(Set<String> datatypes) {
        options.put(UnfieldedRegexExpansionIterator.DATATYPES, Joiner.on(',').join(datatypes));
    }

    public void withPattern(String pattern) {
        options.put(UnfieldedRegexExpansionIterator.PATTERN, pattern);
    }

    public void withExpected(String... values) {
        expected.addAll(List.of(values));
    }

    @Test
    public void testOptionsMissingField() {
        withPattern("val.*");
        assertThrows(IllegalArgumentException.class, this::drive);
    }

    @Test
    public void testOptionsMissingPattern() {
        withPattern(null);
        assertThrows(IllegalArgumentException.class, this::drive);
    }

    @Test
    public void testOptionsMissingDate() {
        withPattern("val.*");
        assertThrows(IllegalArgumentException.class, this::drive);
    }

    @Test
    public void testSingleValueExpansion() throws Exception {
        withData("aaa", "FIELD_A", "20250804", "datatype-a");
        withPattern("aa.*");
        withDates("20250804", "20250804");
        withExpected("aaa FIELD_A");
        drive();
    }

    @Test
    public void testSingleValueExpansionWithDatatypeFilter() throws Exception {
        withData("aaa", "FIELD_A", "20250804_0", "datatype-a");
        withPattern("aa.*");
        withDates("20250804", "20250804");
        withDatatypeFilter(Set.of("datatype-a"));
        withExpected("aaa FIELD_A");
        drive();
    }

    @Test
    public void testSingleValueExpansionExcludedByDatatypeFilter() throws Exception {
        withData("aaa", "FIELD_A", "20250804_0", "datatype-a");
        withPattern("aa.*");
        withDates("20250804", "20250804");
        withDatatypeFilter(Set.of("datatype-b"));
        drive();
    }

    @Test
    public void testMultiValueExpansion() throws Exception {
        withData("aa", "FIELD_A", "20250804_0", "datatype-a");
        withData("ab", "FIELD_A", "20250804_0", "datatype-a");
        withData("ac", "FIELD_A", "20250804_0", "datatype-a");
        withPattern("a.*");
        withDates("20250804", "20250804");
        withExpected("aa FIELD_A", "ab FIELD_A", "ac FIELD_A");
        drive();
    }

    @Test
    public void testSkipField() throws Exception {
        withData("aa", "FIELD_B", "20250804_0", "datatype-a");
        withData("ab", "FIELD_A", "20250804_0", "datatype-a");
        withData("ac", "FIELD_B", "20250804_0", "datatype-a");
        withPattern("a.*");
        withDates("20250804", "20250804");
        withExpected("aa FIELD_B", "ab FIELD_A", "ac FIELD_B");
        drive();
    }

    @Test
    public void testSkipDate() throws Exception {
        withData("aa", "FIELD_B", "20250804_0", "datatype-a");
        withData("ab", "FIELD_B", "20250803_0", "datatype-a");
        withData("ab", "FIELD_B", "20250804_0", "datatype-a");
        withData("ac", "FIELD_B", "20250804_0", "datatype-a");
        withPattern("a.*");
        withDates("20250804", "20250804");
        withExpected("aa FIELD_B", "ac FIELD_B");
        drive();
    }

    @Test
    public void testVerifyDatatypeFilter() throws Exception {
        withData("aa", "FIELD_A", "20250804_0", "datatype-a");
        withData("ab", "FIELD_A", "20250804_0", "datatype-b");
        withData("ac", "FIELD_A", "20250804_0", "datatype-c");
        withPattern("a.*");
        withDates("20250804", "20250804");

        withDatatypeFilter(Set.of("datatype-a"));
        withExpected("aa FIELD_A");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-b"));
        withExpected("ab FIELD_A");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-c"));
        withExpected("ac FIELD_A");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-a", "datatype-b"));
        withExpected("aa FIELD_A", "ab FIELD_A");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-a", "datatype-c"));
        withExpected("aa FIELD_A", "ac FIELD_A");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-b", "datatype-c"));
        withExpected("ab FIELD_A", "ac FIELD_A");
        drive();

        expected.clear();
        results.clear();
        withDatatypeFilter(Set.of("datatype-a", "datatype-b", "datatype-c"));
        withExpected("aa FIELD_A", "ab FIELD_A", "ac FIELD_A");
        drive();
    }

    @Test
    public void testVerifyDateBounds() throws Exception {
        withData("aa", "FIELD_A", "20250803_0", "datatype-a");
        withData("ab", "FIELD_A", "20250804_0", "datatype-a");
        withData("ac", "FIELD_A", "20250805_0", "datatype-a");
        withPattern("a.*");
        withDates("20250804", "20250804");
        withExpected("ab FIELD_A");
        drive();
    }

    @Test
    public void testUnfieldedExpansionReturnsUniqueValues() throws Exception {
        withData("aa", "FIELD_A", "20250803_0", "datatype-a");
        withData("aa", "FIELD_A", "20250804_0", "datatype-a");
        withData("aa", "FIELD_A", "20250805_0", "datatype-a");
        withPattern("a.*");
        withDates("20250803", "20250805");
        withExpected("aa FIELD_A");
        drive();
    }

    @Test
    public void testValueExpansionThreshold() throws Exception {
        withData("aa", "FIELD_A", "20250803_0", "datatype-a");
        withData("ab", "FIELD_A", "20250803_0", "datatype-a");
        withData("ac", "FIELD_A", "20250803_0", "datatype-a");
        withPattern("a.*");
        withDates("20250803", "20250804");
        withExpected("aa FIELD_A");
        withExpected("ab FIELD_A");
        withExpected("ac FIELD_A");
        drive();
    }

    @Test
    public void testFieldExpansionThreshold() throws Exception {
        withData("aa", "FIELD_A", "20250803_0", "datatype-a");
        withData("aa", "FIELD_B", "20250803_0", "datatype-a");
        withData("aa", "FIELD_C", "20250803_0", "datatype-a");
        withPattern("a.*");
        withDates("20250803", "20250804");
        withExpected("aa FIELD_A");
        withExpected("aa FIELD_B");
        withExpected("aa FIELD_C");
        drive();
    }

    private void drive() throws Exception {
        UnfieldedRegexExpansionIterator iterator = new UnfieldedRegexExpansionIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        iterator.seek(new Range(), Collections.emptySet(), false);

        while (iterator.hasTop()) {
            Key tk = iterator.getTopKey();
            parser.parse(tk);
            String result = parser.getValue() + " " + parser.getField();
            results.add(result);
            iterator.next();
        }

        assertEquals(expected, results);
    }

}
