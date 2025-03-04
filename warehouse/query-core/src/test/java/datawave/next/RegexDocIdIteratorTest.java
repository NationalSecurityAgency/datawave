package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases for the {@link RegexDocIdIterator}
 */
public class RegexDocIdIteratorTest extends FieldIndexDataTestUtil {

    @BeforeEach
    public void setup() {
        clearState();
    }

    @Test
    public void testSimpleScan() {
        writeData("FIELD_A", "abc", 10);
        withQuery("FIELD_A =~ 'ab.*'");
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
        withQuery("FIELD_B =~ 'ab.*'");
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
        withQuery("FIELD_A =~ 'ab.*'");
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
        withQuery("FIELD_A =~ 'ab.*'");
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

    @Test
    public void testScanWithZeroResults() {
        writeData("FIELD_A", "abc", "datatype-a", 17);
        withQuery("FIELD_A =~ 'ab+d'");
        drive();
        assertResultSize(0);
        assertEquals(17, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
        assertEquals(17, stats.getRegexMiss());
    }

    @Override
    protected RegexDocIdIterator createIterator() {
        ASTJexlScript script = parse(query);
        JexlNode child = script.jjtGetChild(0);
        assertInstanceOf(ASTERNode.class, child);

        SortedKeyValueIterator<Key,Value> source = createSource();
        return new RegexDocIdIterator(source, row, child);
    }
}
