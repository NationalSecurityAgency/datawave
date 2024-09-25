package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

import datawave.query.attributes.Document;
import datawave.query.data.parsers.EventKey;
import datawave.query.iterator.aggregation.DocumentData;
import datawave.query.predicate.EventDataQueryFieldFilter;
import datawave.query.predicate.EventDataQueryFilter;

public class KeyToDocumentDataTest {

    private final Key documentKey = new Key("20230114_17", "datatype\0uid");
    private final Value value = new Value();

    private final Equality equality = new PrefixEquality(PartialKey.ROW_COLFAM);
    private final Equality tldEquality = new TLDEquality();

    private final RangeProvider rangeProvider = new DocumentRangeProvider();
    private final RangeProvider tldRangeProvider = new TLDRangeProvider();

    private final EventKey parser = new EventKey();
    private List<Map.Entry<Key,Value>> result = new ArrayList<>();

    @Test
    public void testEventData_defaultEquality_noFilter() {
        KeyToDocumentData data = new KeyToDocumentData(getSource(), equality, null, false, false).withRangeProvider(rangeProvider);
        drive(data, getEntry(), 9);
        assertEventFields();
    }

    @Test
    public void testEventData_defaultEquality_withFilter() {
        EventDataQueryFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_A", "FIELD_B"));
        KeyToDocumentData data = new KeyToDocumentData(getSource(), equality, filter, false, false).withRangeProvider(rangeProvider);
        drive(data, getEntry(), 2);
        assertFields(Set.of("FIELD_A", "FIELD_B"));

        // exclusive filter should result with nothing
        filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_Z"));
        data = new KeyToDocumentData(getSource(), equality, filter, false, false).withRangeProvider(rangeProvider);
        drive(data, getEntry(), 0);
        assertFields(Set.of());
    }

    @Test
    public void testEventData_TLDEquality_noFilter() {
        KeyToDocumentData data = new KeyToDocumentData(getSource(), tldEquality, null, false, false).withRangeProvider(rangeProvider);
        drive(data, getEntry(), 9);
        assertEventFields();
    }

    @Test
    public void testEventData_TLDEquality_withFilter() {
        EventDataQueryFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_A", "FIELD_B"));
        KeyToDocumentData data = new KeyToDocumentData(getSource(), tldEquality, filter, false, false).withRangeProvider(rangeProvider);
        drive(data, getEntry(), 2);
        assertFields(Set.of("FIELD_A", "FIELD_B"));
    }

    @Test
    public void testTLDData_defaultEquality_noFilter() {
        KeyToDocumentData data = new KeyToDocumentData(getTLDSource(), equality, null, false, false).withRangeProvider(tldRangeProvider);
        drive(data, getEntry(), 4);
        // TLDEquality is required to pick up the x,y,z fields
        assertFields(Set.of("FIELD_A", "FIELD_B", "FIELD_C"));
    }

    @Test
    public void testTLDData_defaultEquality_withFilter() {
        EventDataQueryFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_A", "FIELD_B"));
        KeyToDocumentData data = new KeyToDocumentData(getTLDSource(), equality, filter, false, false).withRangeProvider(tldRangeProvider);
        drive(data, getEntry(), 2);
        assertFields(Set.of("FIELD_A", "FIELD_B"));
    }

    @Test
    public void testTLDData_TLDEquality_noFilter() {
        KeyToDocumentData data = new KeyToDocumentData(getTLDSource(), tldEquality, null, false, false).withRangeProvider(tldRangeProvider);
        drive(data, getEntry(), 11);
        assertTLDFields();
    }

    @Test
    public void testTLDData_TLDEquality_withFilter() {
        EventDataQueryFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_A", "FIELD_B"));
        KeyToDocumentData data = new KeyToDocumentData(getTLDSource(), tldEquality, filter, false, false).withRangeProvider(tldRangeProvider);
        drive(data, getEntry(), 4);
        assertFields(Set.of("FIELD_A", "FIELD_B"));
    }

    /**
     * Drive the aggregation and assert expected size
     *
     * @param data
     *            the configured {@link KeyToDocumentData}
     * @param entry
     *            the document entry
     * @param expectedSize
     *            the expected size post-aggregation
     */
    private void drive(KeyToDocumentData data, Map.Entry<Key,Document> entry, int expectedSize) {
        result.clear(); // clear any existing data

        Map.Entry<DocumentData,Document> aggregated = data.apply(entry);

        result = aggregated.getKey().getData();
        assertEquals(expectedSize, result.size());
    }

    private void assertEventFields() {
        assertFields(Set.of("FIELD_A", "FIELD_B", "FIELD_C", "GROUP_FIELD.0", "GROUP_FIELD.1", "GROUP_FIELD.2", "GROUP_FIELD.3", "GROUP_FIELD.4"));
    }

    private void assertTLDFields() {
        assertFields(Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_X", "FIELD_Y", "FIELD_Z"));
    }

    private void assertFields(Set<String> fields) {
        Set<String> resultFields = new HashSet<>();
        for (Map.Entry<Key,Value> e : result) {
            parser.parse(e.getKey());
            resultFields.add(parser.getField());
        }

        assertEquals(fields, resultFields);
    }

    private Map.Entry<Key,Document> getEntry() {
        return getEntry(documentKey);
    }

    private Map.Entry<Key,Document> getEntry(Key documentKey) {
        return new AbstractMap.SimpleEntry<>(documentKey, new Document());
    }

    private SortedKeyValueIterator<Key,Value> getSource() {
        return new SortedMapIterator(getSourceData());
    }

    private SortedKeyValueIterator<Key,Value> getTLDSource() {
        return new SortedMapIterator(getTLDSourceData());
    }

    private SortedMap<Key,Value> getSourceData() {
        SortedMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_A\0value_1"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_B\0value_2"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_C\0value_3"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_C\0value_4"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "GROUP_FIELD.0\0value_0"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "GROUP_FIELD.1\0value_1"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "GROUP_FIELD.2\0value_2"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "GROUP_FIELD.3\0value_3"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "GROUP_FIELD.4\0value_4"), value);
        return data;
    }

    private SortedMap<Key,Value> getTLDSourceData() {
        SortedMap<Key,Value> data = new TreeMap<>();
        // parent
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_A\0value_1"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_B\0value_2"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_C\0value_3"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_C\0value_4"), value);
        // child
        data.put(new Key("20230114_17", "datatype\0uid.1", "FIELD_A\0value_1"), value);
        data.put(new Key("20230114_17", "datatype\0uid.1", "FIELD_B\0value_2"), value);
        data.put(new Key("20230114_17", "datatype\0uid.1", "FIELD_D\0value_3"), value);
        data.put(new Key("20230114_17", "datatype\0uid.1", "FIELD_D\0value_4"), value);
        // grandchild
        data.put(new Key("20230114_17", "datatype\0uid.2.7", "FIELD_X\0value_x"), value);
        data.put(new Key("20230114_17", "datatype\0uid.2.7", "FIELD_Y\0value_y"), value);
        data.put(new Key("20230114_17", "datatype\0uid.2.7", "FIELD_Z\0value_z"), value);
        return data;
    }
}
