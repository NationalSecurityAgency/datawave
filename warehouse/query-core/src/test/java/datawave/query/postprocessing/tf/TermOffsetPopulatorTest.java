package datawave.query.postprocessing.tf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.protobuf.TermWeight;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.jexl.functions.TermFrequencyList;

class TermOffsetPopulatorTest {

    private final Key docKey1 = new Key("20240101_0", "datatype\0uid1");
    private final Key docKey2 = new Key("20240101_0", "datatype\0uid2");

    @Test
    void testHit() {
        TermOffsetPopulator populator = createTermOffsetPopulator();

        Map<String,Object> map = populator.getContextMap(docKey1, Collections.singleton(docKey1), null);
        Document document = populator.document();

        assertMapFieldValue(map, "FOO", "red");
        assertMapFieldValue(map, "FOO", "sedan");

        assertEquals(2, document.size());
        assertDocumentFieldValue(document, "FOO", "red");
        assertDocumentFieldValue(document, "FOO", "sedan");
    }

    @Test
    void testMiss() {
        TermOffsetPopulator populator = createTermOffsetPopulator();

        Map<String,Object> map = populator.getContextMap(docKey2, Collections.emptySet(), null);
        Document document = populator.document();
        assertEquals(0, document.size());
    }

    @Test
    void testHitMiss() {
        TermOffsetPopulator populator = createTermOffsetPopulator();

        Map<String,Object> map = populator.getContextMap(docKey1, Collections.singleton(docKey1), null);
        Document document = populator.document();
        assertEquals(2, document.size());

        map = populator.getContextMap(docKey2, Collections.emptySet(), null);
        document = populator.document();
        assertEquals(0, document.size());
    }

    private TermOffsetPopulator createTermOffsetPopulator() {
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.putAll("FOO", Arrays.asList("red", "sedan"));
        TermFrequencyConfig config = createTermFrequencyConfig();
        return new TermOffsetPopulator(fieldValues, config);
    }

    private TermFrequencyConfig createTermFrequencyConfig() {
        TermFrequencyConfig config = new TermFrequencyConfig();
        config.setContentExpansionFields(Collections.singleton("FOO"));
        config.setEvaluationFilter(null);
        config.setSource(createSource());
        return config;
    }

    private SortedKeyValueIterator<Key,Value> createSource() {
        TreeMap<Key,Value> data = createData();
        return new SortedMapIterator(data);
    }

    private TreeMap<Key,Value> createData() {
        TreeMap<Key,Value> data = new TreeMap<>();
        // document 1 for phrase "a bright red sedan"
        data.put(new Key("20240101_0", "tf", "datatype\0uid1\0a\0FOO"), createTermWeight(0));
        data.put(new Key("20240101_0", "tf", "datatype\0uid1\0bright\0FOO"), createTermWeight(1));
        data.put(new Key("20240101_0", "tf", "datatype\0uid1\0red\0FOO"), createTermWeight(2));
        data.put(new Key("20240101_0", "tf", "datatype\0uid1\0sedan\0FOO"), createTermWeight(3));

        // document 2 for phrase "a lime green hatchback"
        data.put(new Key("20240101_0", "tf", "datatype\0uid2\0a\0FOO"), createTermWeight(0));
        data.put(new Key("20240101_0", "tf", "datatype\0uid2\0lime\0FOO"), createTermWeight(1));
        data.put(new Key("20240101_0", "tf", "datatype\0uid2\0green\0FOO"), createTermWeight(2));
        data.put(new Key("20240101_0", "tf", "datatype\0uid2\0hatchback\0FOO"), createTermWeight(3));
        return data;
    }

    private Value createTermWeight(int index) {
        //  @formatter:off
        TermWeight.Info info = TermWeight.Info.newBuilder()
                        .addAllTermOffset(Collections.singleton(index))
                        .addScore(0)
                        .addPrevSkips(0)
                        .setZeroOffsetMatch(true)
                        .build();
        //  @formatter:on
        return new Value(info.toByteArray());
    }

    private void assertMapFieldValue(Map<String,Object> map, String field, String value) {
        assertTrue(map.containsKey("termOffsetMap"));
        Object o = map.get("termOffsetMap");
        TermOffsetMap termOffsetMap = (TermOffsetMap) o;
        TermFrequencyList list = termOffsetMap.getTermFrequencyList(value);
        assertTrue(list.fields().contains(field));
    }

    private void assertDocumentFieldValue(Document document, String field, String value) {
        assertTrue(document.containsKey(field));
        Attribute<?> attr = document.get(field);
        if (attr instanceof Attributes) {
            Attributes attributes = (Attributes) attr;
            boolean found = false;
            for (Attribute<?> attribute : attributes.getAttributes()) {
                if (attribute.getData().equals(value)) {
                    found = true;
                }
            }
            assertTrue(found);
        } else {
            assertEquals(value, attr.getData());
        }
    }

}
