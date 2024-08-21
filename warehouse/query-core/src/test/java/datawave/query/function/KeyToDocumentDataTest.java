package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.AbstractMap;
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
import datawave.query.iterator.aggregation.DocumentData;
import datawave.query.predicate.EventDataQueryFieldFilter;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.KeyProjection;
import datawave.query.predicate.Projection;

public class KeyToDocumentDataTest {

    private final Key documentKey = new Key("20230114_17", "datatype\0uid");
    private final Value value = new Value();

    private final Equality equality = new PrefixEquality(PartialKey.ROW_COLFAM);
    private final Equality tldEquality = new TLDEquality();

    private final RangeProvider rangeProvider = new DocumentRangeProvider();
    private final RangeProvider tldRangeProvider = new TLDRangeProvider();

    @Test
    public void testEventData_defaultEquality_noFilter() {
        KeyToDocumentData data = new KeyToDocumentData(getSource(), equality, null, false, false).withRangeProvider(rangeProvider);
        drive(data, getEntry(), 9);
    }

    @Test
    public void testEventData_defaultEquality_withFilter() {
        KeyProjection projection = new KeyProjection(Set.of("FIELD_A", "FIELD_B"), Projection.ProjectionType.INCLUDES);
        EventDataQueryFilter filter = new EventDataQueryFieldFilter(projection);
        KeyToDocumentData data = new KeyToDocumentData(getSource(), equality, filter, false, false).withRangeProvider(rangeProvider);
        drive(data, getEntry(), 2);
    }

    @Test
    public void testEventData_TLDEquality_noFilter() {
        KeyToDocumentData data = new KeyToDocumentData(getSource(), tldEquality, null, false, false).withRangeProvider(rangeProvider);
        drive(data, getEntry(), 9);
    }

    @Test
    public void testEventData_TLDEquality_withFilter() {
        KeyProjection projection = new KeyProjection(Set.of("FIELD_A", "FIELD_B"), Projection.ProjectionType.INCLUDES);
        EventDataQueryFilter filter = new EventDataQueryFieldFilter(projection);
        KeyToDocumentData data = new KeyToDocumentData(getSource(), tldEquality, filter, false, false).withRangeProvider(rangeProvider);
        drive(data, getEntry(), 2);
    }

    @Test
    public void testTLDData_defaultEquality_noFilter() {
        KeyToDocumentData data = new KeyToDocumentData(getTLDSource(), equality, null, false, false).withRangeProvider(tldRangeProvider);
        drive(data, getEntry(), 9);
    }

    @Test
    public void testTLDData_defaultEquality_withFilter() {
        KeyProjection projection = new KeyProjection(Set.of("FIELD_A", "FIELD_B"), Projection.ProjectionType.INCLUDES);
        EventDataQueryFilter filter = new EventDataQueryFieldFilter(projection);
        KeyToDocumentData data = new KeyToDocumentData(getTLDSource(), equality, filter, false, false).withRangeProvider(tldRangeProvider);
        drive(data, getEntry(), 2);
    }

    @Test
    public void testTLDData_TLDEquality_noFilter() {
        KeyToDocumentData data = new KeyToDocumentData(getTLDSource(), tldEquality, null, false, false).withRangeProvider(tldRangeProvider);
        drive(data, getEntry(), 16);
    }

    @Test
    public void testTLDData_TLDEquality_withFilter() {
        KeyProjection projection = new KeyProjection(Set.of("FIELD_A", "FIELD_B"), Projection.ProjectionType.INCLUDES);
        EventDataQueryFilter filter = new EventDataQueryFieldFilter(projection);
        KeyToDocumentData data = new KeyToDocumentData(getTLDSource(), tldEquality, filter, false, false).withRangeProvider(tldRangeProvider);
        drive(data, getEntry(), 4);
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
        Map.Entry<DocumentData,Document> result = data.apply(entry);

        int dataSize = result.getKey().getData().size();
        assertEquals(expectedSize, dataSize);
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
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_D\0value_4"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "GROUP_FIELD.0\0value_0"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "GROUP_FIELD.1\0value_1"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "GROUP_FIELD.2\0value_2"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "GROUP_FIELD.3\0value_3"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "GROUP_FIELD.4\0value_4"), value);
        return data;
    }

    private SortedMap<Key,Value> getTLDSourceData() {
        SortedMap<Key,Value> data = getSourceData();
        // parent
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_A\0value_1"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_B\0value_2"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_C\0value_3"), value);
        data.put(new Key("20230114_17", "datatype\0uid", "FIELD_D\0value_4"), value);
        // child
        data.put(new Key("20230114_17", "datatype\0uid.1", "FIELD_A\0value_1"), value);
        data.put(new Key("20230114_17", "datatype\0uid.1", "FIELD_B\0value_2"), value);
        data.put(new Key("20230114_17", "datatype\0uid.1", "FIELD_C\0value_3"), value);
        data.put(new Key("20230114_17", "datatype\0uid.1", "FIELD_D\0value_4"), value);
        // grandchild
        data.put(new Key("20230114_17", "datatype\0uid.2.7", "FIELD_X\0value_x"), value);
        data.put(new Key("20230114_17", "datatype\0uid.2.7", "FIELD_Y\0value_y"), value);
        data.put(new Key("20230114_17", "datatype\0uid.2.7", "FIELD_Z\0value_z"), value);
        return data;
    }
}
