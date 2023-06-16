package datawave.webservice.query.iterator;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.collections4.functors.NOPTransformer;
import org.junit.Before;
import org.junit.Test;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;

public class KeyAggregatingTransformIteratorTest {

    @Before
    public void setUp() throws Exception {}

    @Test
    public void testSingleValue() throws Exception {
        KeyValue item1 = new KeyValue(new Key("row1"), "".getBytes());
        List<KeyValue> list = Arrays.asList(item1);

        KeyAggregatingTransformIterator it = new KeyAggregatingTransformIterator(list.iterator(), NOPTransformer.nopTransformer());
        assertTrue(it.hasNext());
        @SuppressWarnings("unchecked")
        List<Entry<Key,Value>> entries = (List<Entry<Key,Value>>) it.next();
        assertNotNull(entries);
        assertEquals(1, entries.size());
        assertEquals(item1, entries.get(0));
        assertFalse(it.hasNext());
    }

    @Test
    public void testTwoValuesEqual() throws Exception {
        KeyValue item1 = new KeyValue(new Key("row1", "cf1"), "value1".getBytes());
        KeyValue item2 = new KeyValue(new Key("row1", "cf2"), "value2".getBytes());
        List<KeyValue> list = Arrays.asList(item1, item2);

        KeyAggregatingTransformIterator it = new KeyAggregatingTransformIterator(list.iterator(), NOPTransformer.nopTransformer());
        assertTrue(it.hasNext());
        @SuppressWarnings("unchecked")
        List<Entry<Key,Value>> entries = (List<Entry<Key,Value>>) it.next();
        assertNotNull(entries);
        assertEquals(2, entries.size());
        assertEquals(item1, entries.get(0));
        assertEquals(item2, entries.get(1));
        assertFalse(it.hasNext());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMultipleValues() throws Exception {
        KeyValue item1 = new KeyValue(new Key("row1", "cf1"), "value1".getBytes());
        KeyValue item2 = new KeyValue(new Key("row1", "cf2"), "value2".getBytes());
        KeyValue item3 = new KeyValue(new Key("row2", "cf1"), "value1".getBytes());
        KeyValue item4 = new KeyValue(new Key("row3", "cf1"), "value1".getBytes());
        KeyValue item5 = new KeyValue(new Key("row4", "cf1"), "value1".getBytes());
        KeyValue item6 = new KeyValue(new Key("row4", "cf2"), "value2".getBytes());
        KeyValue item7 = new KeyValue(new Key("row4", "cf3"), "value3".getBytes());
        KeyValue item8 = new KeyValue(new Key("row5", "cf1"), "value1".getBytes());
        KeyValue item9 = new KeyValue(new Key("row5", "cf2"), "value2".getBytes());
        List<KeyValue> list = Arrays.asList(item1, item2, item3, item4, item5, item6, item7, item8, item9);

        KeyAggregatingTransformIterator it = new KeyAggregatingTransformIterator(list.iterator(), NOPTransformer.nopTransformer());
        assertTrue(it.hasNext());
        List<Entry<Key,Value>> entries = (List<Entry<Key,Value>>) it.next();
        assertNotNull(entries);
        assertEquals(2, entries.size());
        assertEquals(item1, entries.get(0));
        assertEquals(item2, entries.get(1));

        assertTrue(it.hasNext());
        entries = (List<Entry<Key,Value>>) it.next();
        assertEquals(1, entries.size());
        assertEquals(item3, entries.get(0));

        assertTrue(it.hasNext());
        entries = (List<Entry<Key,Value>>) it.next();
        assertEquals(1, entries.size());
        assertEquals(item4, entries.get(0));

        assertTrue(it.hasNext());
        entries = (List<Entry<Key,Value>>) it.next();
        assertEquals(3, entries.size());
        assertEquals(item5, entries.get(0));
        assertEquals(item6, entries.get(1));
        assertEquals(item7, entries.get(2));

        assertTrue(it.hasNext());
        entries = (List<Entry<Key,Value>>) it.next();
        assertEquals(2, entries.size());
        assertEquals(item8, entries.get(0));
        assertEquals(item9, entries.get(1));

        assertFalse(it.hasNext());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMultipleValuesPartial() throws Exception {
        KeyValue item1 = new KeyValue(new Key("row1", "cf1", "cq1"), "value1".getBytes());
        KeyValue item2 = new KeyValue(new Key("row1", "cf1", "cq2"), "value2".getBytes());
        KeyValue item3 = new KeyValue(new Key("row1", "cf2", "cq1"), "value1".getBytes());
        KeyValue item4 = new KeyValue(new Key("row2", "cf2", "cq1"), "value1".getBytes());
        List<KeyValue> list = Arrays.asList(item1, item2, item3, item4);

        KeyAggregatingTransformIterator it = new KeyAggregatingTransformIterator(PartialKey.ROW_COLFAM, list.iterator(), NOPTransformer.nopTransformer());
        assertTrue(it.hasNext());
        List<Entry<Key,Value>> entries = (List<Entry<Key,Value>>) it.next();
        assertNotNull(entries);
        assertEquals(2, entries.size());
        assertEquals(item1, entries.get(0));
        assertEquals(item2, entries.get(1));

        assertTrue(it.hasNext());
        entries = (List<Entry<Key,Value>>) it.next();
        assertEquals(1, entries.size());
        assertEquals(item3, entries.get(0));

        assertTrue(it.hasNext());
        entries = (List<Entry<Key,Value>>) it.next();
        assertEquals(1, entries.size());
        assertEquals(item4, entries.get(0));

        assertFalse(it.hasNext());
    }
}
