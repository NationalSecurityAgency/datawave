package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlNodeFactory;

public class RegexDocIdIteratorTest extends FieldIndexDataTestUtil {

    private String field;
    private String value;

    @BeforeEach
    public void setup() {
        field = null;
        value = null;
        clearState();
    }

    @Test
    public void testSimpleScan() {
        writeData("FIELD_A", "abc", 10);
        withFieldValue("FIELD_A", "ab.*");
        drive();
        assertResultSize(10);
        assertEquals(10, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
        assertEquals(0, stats.getRegexMiss());
    }

    @Test
    public void testScanMatchesMultipleValues() {
        writeData("FIELD_B", "abc", 5);
        writeData("FIELD_B", "abd", 7);
        writeData("FIELD_B", "abe", 11);
        writeData("FIELD_B", "abf", 13);
        withFieldValue("FIELD_B", "ab.*");
        drive();
        assertResultSize(13); // doc ids overlap, full set is 13
        assertEquals(36, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
        assertEquals(0, stats.getRegexMiss());
    }

    @Test
    public void testScanMatchesMultipleDatatypes() {
        // prove that matching multiple datatypes increases total result size
        // due to doc id structure of datatype + null + uid
        writeData("FIELD_A", "abc", "datatype-a", 2);
        writeData("FIELD_A", "abd", "datatype-b", 3);
        writeData("FIELD_A", "abe", "datatype-c", 5);
        withFieldValue("FIELD_A", "ab.*");
        drive();
        assertResultSize(10);
        assertEquals(10, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
        assertEquals(0, stats.getRegexMiss());
    }

    @Test
    public void testScanMatchesMultipleValuesSkipsDatatype() {
        writeData("FIELD_A", "abc", "datatype-a", 5);
        writeData("FIELD_A", "abd", "datatype-b", 7);
        writeData("FIELD_A", "abe", "datatype-c", 23);
        writeData("FIELD_A", "abf", "datatype-d", 13);
        withFieldValue("FIELD_A", "ab.*");
        withDataTypes("datatype-a", "datatype-b", "datatype-d");
        drive();
        // skip datatype-c
        assertResultSize(25);
        assertEquals(25, stats.getNextCount());
        assertEquals(2, stats.getSeekCount());
        assertEquals(1, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
        assertEquals(0, stats.getRegexMiss());
    }

    public void withFieldValue(String field, String value) {
        this.field = field;
        this.value = value;
    }

    @Override
    protected RegexDocIdIterator createIterator() {
        SortedKeyValueIterator<Key,Value> source = createSource();
        ASTERNode node = (ASTERNode) JexlNodeFactory.buildERNode(field, value);
        return new RegexDocIdIterator(source, row, node);
    }
}
