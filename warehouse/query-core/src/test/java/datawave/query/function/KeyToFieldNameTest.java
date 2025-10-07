package datawave.query.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.AbstractMap;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

public class KeyToFieldNameTest {

    private final Key parentKey = new Key("row", "datatype\0uid0", "FIELD_A\0value");
    private final Key childKey = new Key("row", "datatype\0uid2", "FIELD_B.1\0value");
    private final Key grandChildKey = new Key("row", "datatype\0uid7", "FIELD_C.1.2\0value");
    private final Key malformedKey = new Key("row", "dt\0uid", "oh this isn't good");

    private final Value value = new Value();

    private final KeyToFieldName function = new KeyToFieldName();
    private final KeyToFieldName functionWithGrouping = new KeyToFieldName(true);

    @Test
    public void testGetFieldName() {
        assertEquals("FIELD_A", function.getFieldName(parentKey));
        assertEquals("FIELD_B", function.getFieldName(childKey));
        assertEquals("FIELD_C", function.getFieldName(grandChildKey));

        assertEquals("FIELD_A", functionWithGrouping.getFieldName(parentKey));
        assertEquals("FIELD_B.1", functionWithGrouping.getFieldName(childKey));
        assertEquals("FIELD_C.1.2", functionWithGrouping.getFieldName(grandChildKey));
    }

    @Test
    public void testApply() {
        assertEquals("FIELD_A", function.apply(getEntry(parentKey)).getValue());
        assertEquals("FIELD_B", function.apply(getEntry(childKey)).getValue());
        assertEquals("FIELD_C", function.apply(getEntry(grandChildKey)).getValue());

        assertEquals("FIELD_A", functionWithGrouping.apply(getEntry(parentKey)).getValue());
        assertEquals("FIELD_B.1", functionWithGrouping.apply(getEntry(childKey)).getValue());
        assertEquals("FIELD_C.1.2", functionWithGrouping.apply(getEntry(grandChildKey)).getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFunctionGetFieldNameWithMalformedKey() {
        function.getFieldName(malformedKey);
        fail("function should have thrown an exception");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFunctionWithGroupingGetFieldNameWithMalformedKey() {
        functionWithGrouping.getFieldName(malformedKey);
        fail("function should have thrown an exception");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFunctionApplyWithMalformedKey() {
        function.apply(getEntry(malformedKey));
        fail("function should have thrown an exception");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFunctionApplyWithGroupingWithMalformedKey() {
        functionWithGrouping.apply(getEntry(malformedKey));
        fail("function should have thrown an exception");
    }

    private Entry<Key,Value> getEntry(Key k) {
        return new AbstractMap.SimpleEntry<>(k, value);
    }
}
