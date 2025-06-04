package datawave.next;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Preconditions;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Test cases for the {@link RangeDocIdIterator}
 */
public class RangeDocIdIteratorTest extends FieldIndexDataTestUtil {

    @BeforeEach
    public void setup() {
        clearState();
    }

    @Test
    public void testSimpleScan() {
        writeIndex("FIELD_A", "1", "datatype-a", 1);
        writeIndex("FIELD_A", "2", "datatype-a", 2);
        writeIndex("FIELD_A", "3", "datatype-a", 3);
        writeIndex("FIELD_A", "4", "datatype-a", 4);
        writeIndex("FIELD_A", "5", "datatype-a", 5);
        withQuery("((_Bounded_ = true) && (FIELD_A >= '2' && FIELD_A <= '4'))");
        drive();
        assertResultSize(3);
        assertEquals(3, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
    }

    @Test
    public void testLowerExclusive() {
        writeIndex("FIELD_A", "1", "datatype-a", 1);
        writeIndex("FIELD_A", "2", "datatype-a", 2);
        writeIndex("FIELD_A", "3", "datatype-a", 3);
        writeIndex("FIELD_A", "4", "datatype-a", 4);
        writeIndex("FIELD_A", "5", "datatype-a", 5);
        withQuery("((_Bounded_ = true) && (FIELD_A > '2' && FIELD_A <= '4'))");
        drive();
        assertResultSize(2);
        assertEquals(2, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
    }

    @Test
    public void testUpperExclusive() {
        writeIndex("FIELD_A", "1", "datatype-a", 1);
        writeIndex("FIELD_A", "2", "datatype-a", 2);
        writeIndex("FIELD_A", "3", "datatype-a", 3);
        writeIndex("FIELD_A", "4", "datatype-a", 4);
        writeIndex("FIELD_A", "5", "datatype-a", 5);
        withQuery("((_Bounded_ = true) && (FIELD_A >= '2' && FIELD_A < '4'))");
        drive();
        assertResultSize(2);
        assertEquals(2, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
    }

    @Test
    public void testLowerAndUpperExclusive() {
        writeIndex("FIELD_A", "1", "datatype-a", 1);
        writeIndex("FIELD_A", "2", "datatype-a", 2);
        writeIndex("FIELD_A", "3", "datatype-a", 3);
        writeIndex("FIELD_A", "4", "datatype-a", 4);
        writeIndex("FIELD_A", "5", "datatype-a", 5);
        withQuery("((_Bounded_ = true) && (FIELD_A > '2' && FIELD_A < '4'))");
        drive();
        assertResultSize(1);
        assertEquals(1, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
    }

    @Test
    public void testSingleValueRange() {
        writeIndex("FIELD_A", "1", "datatype-a", 1);
        writeIndex("FIELD_A", "2", "datatype-a", 2);
        writeIndex("FIELD_A", "3", "datatype-a", 3);
        writeIndex("FIELD_A", "4", "datatype-a", 4);
        writeIndex("FIELD_A", "5", "datatype-a", 5);
        withQuery("((_Bounded_ = true) && (FIELD_A >= '3' && FIELD_A <= '3'))");
        drive();
        assertResultSize(1);
        assertEquals(1, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
    }

    @Test
    public void testSingleValueUpperAndLowerExclusive() {
        writeIndex("FIELD_A", "1", "datatype-a", 1);
        writeIndex("FIELD_A", "2", "datatype-a", 2);
        writeIndex("FIELD_A", "3", "datatype-a", 3);
        writeIndex("FIELD_A", "4", "datatype-a", 4);
        writeIndex("FIELD_A", "5", "datatype-a", 5);
        withQuery("((_Bounded_ = true) && (FIELD_A > '3' && FIELD_A < '3'))");
        // accumulo throws an exception because the start key must be less than the end key
        assertThrows(IllegalArgumentException.class, this::drive);
    }

    @Test
    public void testDatatypeFilter() {
        writeIndex("FIELD_A", "1", "datatype-a", 1);
        writeIndex("FIELD_A", "2", "datatype-b", 2);
        writeIndex("FIELD_A", "3", "datatype-c", 3);
        writeIndex("FIELD_A", "4", "datatype-d", 4);
        writeIndex("FIELD_A", "5", "datatype-e", 5);
        withQuery("((_Bounded_ = true) && (FIELD_A >= '1' && FIELD_A <= '5'))");
        withDataTypes("datatype-a", "datatype-c", "datatype-d", "datatype-e");
        drive();
        assertResultSize(4);
        assertEquals(4, stats.getNextCount());
        assertEquals(2, stats.getSeekCount());
        assertEquals(1, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
    }

    @Test
    public void testBoundedScanCreatesSingletonFilter() {
        writeIndex("FIELD_A", "abc", "datatype-a", 1);
        writeIndex("FIELD_A", "abc", "datatype-b", 2);
        writeIndex("FIELD_A", "abc", "datatype-c", 3);
        withQuery("((_Bounded_ = true) && (FIELD_A >= 'ab' && FIELD_A <= 'ac'))");
        withMinMax("datatype-b\0uid-1001", "datatype-b\0uid-1003");
        drive();
        assertResultSize(1);
        assertEquals(1, stats.getNextCount());
        assertEquals(3, stats.getSeekCount());
        assertEquals(2, stats.getDatatypeFilterMiss());
    }

    @Test
    public void testBoundedScanCreatesRangeFilter() {
        writeIndex("FIELD_A", "abc", "datatype-a", 1);
        writeIndex("FIELD_A", "abc", "datatype-b", 2);
        writeIndex("FIELD_A", "abc", "datatype-c", 3);
        writeIndex("FIELD_A", "abc", "datatype-d", 4);
        writeIndex("FIELD_A", "abc", "datatype-e", 5);
        withQuery("((_Bounded_ = true) && (FIELD_A >= 'ab' && FIELD_A <= 'ac'))");
        withMinMax("datatype-b\0uid-1001", "datatype-c\0uid-1003");
        drive();
        assertResultSize(2);
        assertEquals(2, stats.getNextCount());
        assertEquals(3, stats.getSeekCount());
        assertEquals(2, stats.getDatatypeFilterMiss());
    }

    @Test
    public void testBoundedScanReducesExistingFilterToSingleton() {
        writeIndex("FIELD_A", "abc", "datatype-a", 1);
        writeIndex("FIELD_A", "abc", "datatype-b", 2);
        writeIndex("FIELD_A", "abc", "datatype-c", 3);
        withQuery("((_Bounded_ = true) && (FIELD_A >= 'ab' && FIELD_A <= 'ac'))");
        withDataTypes("datatype-a", "datatype-b", "datatype-c");
        withMinMax("datatype-a\0uid-1001", "datatype-a\0uid-1003");
        drive();
        assertResultSize(1);
        assertEquals(1, stats.getNextCount());
        assertEquals(2, stats.getSeekCount());
        assertEquals(1, stats.getDatatypeFilterMiss());
    }

    @Test
    public void testBoundedScanReducesExistingFilter() {
        writeIndex("FIELD_A", "abc", "datatype-a", 1);
        writeIndex("FIELD_A", "abc", "datatype-b", 2);
        writeIndex("FIELD_A", "abc", "datatype-c", 3);
        withQuery("((_Bounded_ = true) && (FIELD_A >= 'ab' && FIELD_A <= 'ac'))");
        withDataTypes("datatype-a", "datatype-b", "datatype-c");
        withMinMax("datatype-c\0uid-1001", "datatype-z\0uid-1003");
        drive();
        assertResultSize(1);
        assertEquals(1, stats.getNextCount());
        assertEquals(2, stats.getSeekCount());
        assertEquals(1, stats.getDatatypeFilterMiss());
    }

    public void withQuery(String query) {
        this.query = query;
    }

    @Override
    protected RangeDocIdIterator createIterator() {
        Preconditions.checkNotNull(query);
        ASTJexlScript script = parse(query);
        JexlNode child = script.jjtGetChild(0);

        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(child);
        assertEquals(BOUNDED_RANGE, instance.getType());

        SortedKeyValueIterator<Key,Value> source = createSource();
        return new RangeDocIdIterator(source, row, child);
    }
}
