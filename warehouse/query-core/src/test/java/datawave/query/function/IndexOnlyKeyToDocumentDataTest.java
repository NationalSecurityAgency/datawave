package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.AbstractMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

import datawave.query.attributes.Document;
import datawave.query.iterator.aggregation.DocumentData;

/**
 * Test for the {@link IndexOnlyKeyToDocumentData} which operates on TF columns
 */
public class IndexOnlyKeyToDocumentDataTest {

    // the document key for the IndexOnlyKeyToDocumentData is different
    private final Key documentKey = new Key("20230114_17", "tf", "datatype\0uid");
    private final Key childKey = new Key("20230114_17", "tf", "datatype\0uid.1");
    private final Key grandChildKey = new Key("20230114_17", "tf", "datatype\0uid.2.7");

    private final Value value = new Value();

    @Test
    public void testEventData_singleValue() {
        String field = "FIELD_A";
        drive(field, documentKey, getSource(), 1);
    }

    @Test
    public void testEventData_multiValue() {
        String field = "FIELD_B";
        drive(field, documentKey, getSource(), 2);
    }

    @Test
    public void testEventData_noData() {
        String field = "FIELD_K";
        drive(field, documentKey, getSource(), 0);
    }

    @Test
    public void testTLDData_parent_singleValue() {
        String field = "FIELD_A";
        drive(field, documentKey, getTLDSource(), 1);
    }

    @Test
    public void testTLDData_child_singleValue() {
        String field = "FIELD_A";
        drive(field, childKey, getTLDSource(), 1);
    }

    @Test
    public void testTLDData_grandchild_singleValue() {
        String field = "FIELD_X";
        drive(field, grandChildKey, getTLDSource(), 1);
    }

    @Test
    public void testTLDData_parent_multiValue() {
        String field = "FIELD_B";
        drive(field, documentKey, getTLDSource(), 2);
    }

    @Test
    public void testTLDData_child_multiValue() {
        String field = "FIELD_B";
        drive(field, childKey, getTLDSource(), 2);
    }

    /**
     * Drive the aggregation and assert expected size
     *
     * @param field
     *            the field
     * @param documentKey
     *            the document key
     * @param source
     *            the source
     * @param expectedSize
     *            the expected size post-aggregation
     */
    private void drive(String field, Key documentKey, SortedKeyValueIterator<Key,Value> source, int expectedSize) {
        IndexOnlyKeyToDocumentData data = new IndexOnlyKeyToDocumentData(getRange(), field, source);
        Map.Entry<DocumentData,Document> result = data.apply(getEntry(documentKey));

        int dataSize = result.getKey().getData().size();
        assertEquals(expectedSize, dataSize);
    }

    private Map.Entry<Key,Document> getEntry(Key documentKey) {
        return new AbstractMap.SimpleEntry<>(documentKey, new Document());
    }

    private Range getRange() {
        Key start = new Key("20230114_17");
        return new Range(start, true, start.followingKey(PartialKey.ROW), false);
    }

    private SortedKeyValueIterator<Key,Value> getSource() {
        return new SortedMapIterator(getSourceData());
    }

    private SortedKeyValueIterator<Key,Value> getTLDSource() {
        return new SortedMapIterator(getTLDSourceData());
    }

    private SortedMap<Key,Value> getSourceData() {
        SortedMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("20230114_17", "tf", "datatype\0uid\0value_1\0FIELD_A"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid\0value_2\0FIELD_B"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid\0value_3\0FIELD_B"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid\0value_4\0FIELD_C"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid\0value_5\0FIELD_D"), value);
        return data;
    }

    private SortedMap<Key,Value> getTLDSourceData() {
        SortedMap<Key,Value> data = getSourceData();
        // parent
        data.put(new Key("20230114_17", "tf", "datatype\0uid\0value_1\0FIELD_A"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid\0value_2\0FIELD_B"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid\0value_3\0FIELD_B"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid\0value_4\0FIELD_C"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid\0value_5\0FIELD_D"), value);
        // child
        data.put(new Key("20230114_17", "tf", "datatype\0uid.1\0value_11\0FIELD_A"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid.1\0value_22\0FIELD_B"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid.1\0value_33\0FIELD_B"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid.1\0value_44\0FIELD_C"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid.1\0value_55\0FIELD_D"), value);
        // grandchild
        data.put(new Key("20230114_17", "tf", "datatype\0uid.2.7\0value_111\0FIELD_X"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid.2.7\0value_222\0FIELD_Y"), value);
        data.put(new Key("20230114_17", "tf", "datatype\0uid.2.7\0value_333\0FIELD_Z"), value);
        return data;
    }
}
