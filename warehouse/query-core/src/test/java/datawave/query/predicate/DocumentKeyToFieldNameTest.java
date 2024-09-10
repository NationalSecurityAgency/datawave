package datawave.query.predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.BaseType;
import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.KeyToFieldName;
import datawave.query.function.KeyToFieldNameTest;
import datawave.query.util.TypeMetadata;

/**
 * Similar to {@link KeyToFieldNameTest}, tests a smaller integration between {@link KeyToFieldName} and {@link ValueToAttribute}
 */
public class DocumentKeyToFieldNameTest {

    private final Value value = new Value();
    private final ColumnVisibility cv1 = new ColumnVisibility("PUBLIC");
    private final ColumnVisibility cv2 = new ColumnVisibility("PRIVATE");

    private final KeyToFieldName fieldNameFunction = new KeyToFieldName();
    private ValueToAttribute fieldValueFunction = new ValueToAttribute(new TypeMetadata(), null);

    private final List<Entry<Key,Value>> data = new ArrayList<>();
    private final List<Entry<Key,Value>> expected = new ArrayList<>();

    @Before
    public void before() {
        data.clear();
        expected.clear();
    }

    @Test
    public void testBasicTransform() {
        data.add(createEntry("F1", "v1", cv1));
        data.add(createEntry("F1", "v2", cv2));
        data.add(createEntry("F2", "v22", cv1));
        data.add(createEntry("F3", "v33", cv2));

        expected.add(createEntry("F1", "v1", cv1));
        expected.add(createEntry("F1", "v2", cv2));
        expected.add(createEntry("F2", "v22", cv1));
        expected.add(createEntry("F3", "v33", cv2));

        executeTest();
    }

    // asserts that this transform configuration will not throw an exception on empty inputs
    @Test
    public void testEmptyTransform() {
        executeTest();
    }

    @Test
    public void testUnknownNormalizer() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("FX", "dt", TestDatatype.class.getTypeName());
        fieldValueFunction = new ValueToAttribute(metadata, null);

        data.add(createEntry("F1", "v1", cv1));
        data.add(createEntry("F1", "v2", cv2));
        data.add(createEntry("F2", "v22", cv1));
        data.add(createEntry("F3", "v33", cv2));
        data.add(createEntry("FX", "xyz", cv2));

        expected.add(createEntry("F1", "v1", cv1));
        expected.add(createEntry("F1", "v2", cv2));
        expected.add(createEntry("F2", "v22", cv1));
        expected.add(createEntry("F3", "v33", cv2));
        expected.add(createEntry("FX", "xyz", cv2));

        executeTest();
    }

    private void executeTest() {
        Iterator<Entry<Key,Value>> expectedIter = expected.iterator();
        Iterator<Entry<Key,Entry<String,Attribute<? extends Comparable<?>>>>> iter = transformAndFilter(data);
        while (iter.hasNext()) {
            Entry<Key,Entry<String,Attribute<? extends Comparable<?>>>> next = iter.next();

            assertTrue(expectedIter.hasNext());
            Entry<Key,Value> entry = expectedIter.next();
            assertEquals(entry.getKey(), next.getKey());

            Type<?> type = ((TypeAttribute<?>) next.getValue().getValue()).getType();
            assertEquals(valueFromKey(entry.getKey()), type.getNormalizedValue());
        }
        assertFalse(expectedIter.hasNext());
    }

    private Entry<Key,Value> createEntry(String f, String v, ColumnVisibility cv) {
        Key key = new Key("row", "datatype\0uid", f + '\u0000' + v, cv, -1);
        return Maps.immutableEntry(key, value);
    }

    private String valueFromKey(Key k) {
        String cq = k.getColumnQualifier().toString();
        int index = cq.indexOf('\u0000');
        return cq.substring(index + 1);
    }

    private Iterator<Entry<Key,Entry<String,Attribute<? extends Comparable<?>>>>> transformAndFilter(List<Entry<Key,Value>> list) {
        return Iterators.transform(Iterators.transform(list.iterator(), fieldNameFunction), fieldValueFunction);
    }

    private static class TestDatatype extends BaseType<String> {

        public TestDatatype(String delegateString, Normalizer<String> normalizer) {
            super(delegateString, normalizer);
        }
    }
}
