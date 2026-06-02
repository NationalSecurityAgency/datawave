package datawave.next;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Preconditions;

import datawave.query.jexl.nodes.QueryPropertyMarker;

public class ListDocIdIteratorTest extends FieldIndexDataTestUtil {

    @BeforeEach
    public void setUp() throws Exception {
        clearState();
    }

    @Test
    public void testSingleValue() {
        writeIndex("FIELD_A", "value-a", "datatype-a", 1);
        writeIndex("FIELD_A", "value-b", "datatype-a", 2);
        writeIndex("FIELD_A", "value-c", "datatype-a", 3);
        withQuery("((_List_ = true) && (((id = 'uuid') && (field = 'FIELD_A') && (params = '{\"values\":[\"value-a\"]}'))))");
        drive();
        assertResultSize(1);

        withQuery("((_List_ = true) && (((id = 'uuid') && (field = 'FIELD_A') && (params = '{\"values\":[\"value-b\"]}'))))");
        drive();
        assertResultSize(1);

        withQuery("((_List_ = true) && (((id = 'uuid') && (field = 'FIELD_A') && (params = '{\"values\":[\"value-c\"]}'))))");
        drive();
        assertResultSize(1);
    }

    @Test
    public void testMultiValue() {
        writeIndex("FIELD_A", "value-a", "datatype-a", 1);
        writeIndex("FIELD_A", "value-b", "datatype-a", 2);
        writeIndex("FIELD_A", "value-c", "datatype-a", 3);
        withQuery("((_List_ = true) && (((id = 'uuid') && (field = 'FIELD_A') && (params = '{\"values\":[\"value-a\",\"value-b\"]}'))))");
        drive();
        assertResultSize(2);

        withQuery("((_List_ = true) && (((id = 'uuid') && (field = 'FIELD_A') && (params = '{\"values\":[\"value-a\",\"value-c\"]}'))))");
        drive();
        assertResultSize(2);

        withQuery("((_List_ = true) && (((id = 'uuid') && (field = 'FIELD_A') && (params = '{\"values\":[\"value-b\",\"value-c\"]}'))))");
        drive();
        assertResultSize(2);
    }

    @Test
    public void testAllValues() {
        writeIndex("FIELD_A", "value-a", "datatype-a", 1);
        writeIndex("FIELD_A", "value-b", "datatype-a", 2);
        writeIndex("FIELD_A", "value-c", "datatype-a", 3);
        withQuery("((_List_ = true) && (((id = 'uuid') && (field = 'FIELD_A') && (params = '{\"values\":[\"value-a\",\"value-b\",\"value-c\"]}'))))");
        drive();
        assertResultSize(3);
    }

    @Test
    public void testDatatypeFilter() {
        writeIndex("FIELD_A", "value-a", "datatype-a", 1);
        writeIndex("FIELD_A", "value-b", "datatype-b", 2);
        writeIndex("FIELD_A", "value-c", "datatype-c", 3);
        withQuery("((_List_ = true) && (((id = 'uuid') && (field = 'FIELD_A') && (params = '{\"values\":[\"value-a\",\"value-b\",\"value-c\"]}'))))");
        withDataTypes("datatype-a");
        drive();
        assertResultSize(1);

        withDataTypes("datatype-b");
        drive();
        assertResultSize(1);

        withDataTypes("datatype-c");
        drive();
        assertResultSize(1);

        withDataTypes("datatype-a", "datatype-b");
        drive();
        assertResultSize(2);

        withDataTypes("datatype-a", "datatype-c");
        drive();
        assertResultSize(2);

        withDataTypes("datatype-a", "datatype-b", "datatype-c");
        drive();
        assertResultSize(3);
    }

    @Test
    public void testNoBackingValues() {
        writeData("FIELD_A", "value-a", "datatype-a", 10);
        withQuery("((_List_ = true) && (((id = 'uuid') && (field = 'FIELD_A') && (params = '{\"values\":[\"value-b\"]}'))))");
        withDataTypes("datatype-a");
        drive();
        assertResultSize(0);
    }

    public void withQuery(String query) {
        this.query = query;
    }

    @Override
    protected BaseDocIdIterator createIterator() {
        Preconditions.checkNotNull(query);
        ASTJexlScript script = parse(query);
        JexlNode child = script.jjtGetChild(0);

        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(child);
        assertEquals(EXCEEDED_OR, instance.getType());

        SortedKeyValueIterator<Key,Value> source = createSource();
        return new ListDocIdIterator(source, row, child);
    }
}
