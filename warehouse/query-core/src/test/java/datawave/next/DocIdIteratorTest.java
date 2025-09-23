package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases for the {@link DocIdIterator}
 */
public class DocIdIteratorTest extends FieldIndexDataTestUtil {

    @BeforeEach
    public void setup() {
        clearState();
    }

    @Test
    public void testSimpleScan() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a'");
        drive();
        assertResultSize(10);
        assertEquals(10, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
    }

    @Test
    public void testScanWithNoBackingData() {
        writeData("FIELD_A", "value-b", 10);
        withQuery("FIELD_A == 'value-a'");
        drive();
        assertResultSize(0);
        assertEquals(0, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
    }

    @Test
    public void testScanWithDataTypeFilter() {
        // a:2, b:3, c:4
        writeIndex("FIELD_A", "value-a", "datatype-a", 1);
        writeIndex("FIELD_A", "value-a", "datatype-a", 2);
        writeIndex("FIELD_A", "value-a", "datatype-b", 3);
        writeIndex("FIELD_A", "value-a", "datatype-b", 4);
        writeIndex("FIELD_A", "value-a", "datatype-b", 5);
        writeIndex("FIELD_A", "value-a", "datatype-c", 6);
        writeIndex("FIELD_A", "value-a", "datatype-c", 7);
        writeIndex("FIELD_A", "value-a", "datatype-c", 8);
        writeIndex("FIELD_A", "value-a", "datatype-c", 9);
        withQuery("FIELD_A == 'value-a'");

        // no filter, should hit every key
        drive();
        assertResultSize(9);
        assertEquals(9, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());

        // filter on single datatype
        withDataTypes("datatype-a");
        drive();
        assertResultSize(2);
        assertEquals(2, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());

        withDataTypes("datatype-b");
        drive();
        assertResultSize(3);
        assertEquals(3, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());

        withDataTypes("datatype-c");
        drive();
        assertResultSize(4);
        assertEquals(4, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());

        // filter with two datatypes
        withDataTypes("datatype-a", "datatype-b");
        drive();
        assertResultSize(5);
        assertEquals(5, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());

        withDataTypes("datatype-a", "datatype-c");
        drive();
        assertResultSize(6);
        assertEquals(6, stats.getNextCount());
        assertEquals(2, stats.getSeekCount());
        assertEquals(1, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());

        withDataTypes("datatype-b", "datatype-c");
        drive();
        assertResultSize(7);
        assertEquals(7, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());

        // filter with all datatypes
        withDataTypes("datatype-a", "datatype-b", "datatype-c");
        drive();
        assertResultSize(9);
        assertEquals(9, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
    }

    @Test
    public void testScanWithTimeFilter() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a'");
        // all keys written at timestamp 10, this will filter out all keys in the range
        withTimeFilter(LongRange.of(5, 7));
        drive();
        assertResultSize(0);
        assertEquals(10, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(10, stats.getTimeFilterMiss());
    }

    @Test
    public void testScanWithNumericField() {
        writeData("FIELD_12", "14", 10);
        withQuery("FIELD_12 == '14'");
        drive();
        assertResultSize(10);
        assertEquals(10, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
    }

    @Test
    public void testBoundedScan() {
        writeRange("FIELD_A", "value-a", 10, 30);
        withQuery("FIELD_A == 'value-a'");
        drive();

        // assert full scan works
        assertResultSize(21);
        assertEquals(21, stats.getNextCount());

        // assert bounded scan returns less data
        withMinMax("datatype-a\0uid-1015", "datatype-a\0uid-1025");
        drive();
        assertResultSize(11);
        assertEquals(11, stats.getNextCount());

        // again, with a much smaller bound
        withMinMax("datatype-a\0uid-1015", "datatype-a\0uid-1017");
        drive();
        assertResultSize(3);
        assertEquals(3, stats.getNextCount());
    }

    @Test
    public void testBoundedScanAddsSingletonDatatypeFilter() {
        writeIndex("FIELD_A", "value-a", "datatype-a", 1);
        writeIndex("FIELD_A", "value-a", "datatype-b", 2);
        writeIndex("FIELD_A", "value-a", "datatype-c", 3);
        writeIndex("FIELD_A", "value-a", "datatype-d", 4);
        writeIndex("FIELD_A", "value-a", "datatype-e", 5);
        withQuery("FIELD_A == 'value-a'");

        // should trigger case 0 reduction
        withMinMax("datatype-c\0uid-1000", "datatype-c\0uid-1111");
        drive();
        assertResultSize(1);
        assertEquals(1, stats.getNextCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
    }

    @Test
    public void testBoundedScanAddsMinMaxDatatypeFilter() {
        writeIndex("FIELD_A", "value-a", "datatype-a", 1);
        writeIndex("FIELD_A", "value-a", "datatype-b", 2);
        writeIndex("FIELD_A", "value-a", "datatype-c", 3);
        writeIndex("FIELD_A", "value-a", "datatype-d", 4);
        writeIndex("FIELD_A", "value-a", "datatype-e", 5);
        withQuery("FIELD_A == 'value-a'");

        // should trigger case 1 reduction
        withMinMax("datatype-b\0uid-1000", "datatype-d\0uid-1111");
        drive();
        assertResultSize(3);
        assertEquals(3, stats.getNextCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
    }

    @Test
    public void testBoundedScanOverridesExistingDatatypeFilterWithSingleton() {
        writeIndex("FIELD_A", "value-a", "datatype-a", 1);
        writeIndex("FIELD_A", "value-a", "datatype-b", 2);
        writeIndex("FIELD_A", "value-a", "datatype-c", 3);
        writeIndex("FIELD_A", "value-a", "datatype-d", 4);
        writeIndex("FIELD_A", "value-a", "datatype-e", 5);
        withQuery("FIELD_A == 'value-a'");

        // should trigger case 2 reduction
        withDataTypes("datatype-a", "datatype-b", "datatype-c");
        withMinMax("datatype-c\0uid-1000", "datatype-c\0uid-1111");
        drive();
        assertResultSize(1);
        assertEquals(1, stats.getNextCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
    }

    @Test
    public void testBoundedScanReducesExistingDatatypeFilter() {
        writeIndex("FIELD_A", "value-a", "datatype-a", 1);
        writeIndex("FIELD_A", "value-a", "datatype-b", 2);
        writeIndex("FIELD_A", "value-a", "datatype-c", 3);
        writeIndex("FIELD_A", "value-a", "datatype-d", 4);
        writeIndex("FIELD_A", "value-a", "datatype-e", 5);
        withQuery("FIELD_A == 'value-a'");

        // should trigger case 3 reduction
        withDataTypes("datatype-a", "datatype-b", "datatype-c");
        withMinMax("datatype-b\0uid-1000", "datatype-c\0uid-1111");
        drive();
        assertResultSize(2);
        assertEquals(2, stats.getNextCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
    }

    @Override
    protected DocIdIterator createIterator() {
        ASTJexlScript script = parse(query);
        JexlNode child = script.jjtGetChild(0);
        assertInstanceOf(ASTEQNode.class, child);

        SortedKeyValueIterator<Key,Value> source = createSource();
        return new DocIdIterator(source, row, child);
    }
}
