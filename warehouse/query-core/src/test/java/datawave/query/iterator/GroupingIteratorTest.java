package datawave.query.iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.type.NumberType;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.common.grouping.GroupFields;

public class GroupingIteratorTest {

    private static final Logger log = LoggerFactory.getLogger(GroupingIteratorTest.class);
    private int uidCount = 0;

    @BeforeAll
    public static void setup() {
        MarkingFunctions.Factory.createMarkingFunctions();
    }

    @Test
    public void testSingleDocument() {
        Iterator<Map.Entry<Key,Document>> iter = getDocumentIterator(1);
        GroupFields groupFields = new GroupFields();
        groupFields.setCountFields(Set.of("FIELD_A", "FIELD_B"));
        GroupingIterator groupingIterator = new GroupingIterator(iter, MarkingFunctions.Factory.createMarkingFunctions(), groupFields, 1, null);

        assertTrue(groupingIterator.hasNext());
        Document d = groupingIterator.next().getValue();
        assertEquals(3, d.getAttributes().size());
        assertEquals(1, ((NumberType) d.get("COUNT.0").getData()).getDelegate().intValue());
        assertEquals(1, ((NumberType) d.get("FIELD_A_COUNT.0").getData()).getDelegate().intValue());
        assertEquals(1, ((NumberType) d.get("FIELD_B_COUNT.0").getData()).getDelegate().intValue());
    }

    @Test
    public void testManyStaticDocuments() {
        Iterator<Map.Entry<Key,Document>> iter = getDocumentIterator(10_000);
        GroupFields groupFields = new GroupFields();
        groupFields.setCountFields(Set.of("FIELD_A", "FIELD_B"));
        GroupingIterator groupingIterator = new GroupingIterator(iter, MarkingFunctions.Factory.createMarkingFunctions(), groupFields, 100_000, null);

        assertTrue(groupingIterator.hasNext());
        Document d = groupingIterator.next().getValue();
        assertEquals(3, d.getAttributes().size());
        assertEquals(10_000, ((NumberType) d.get("COUNT.0").getData()).getDelegate().intValue());
        assertEquals(10_000, ((NumberType) d.get("FIELD_A_COUNT.0").getData()).getDelegate().intValue());
        assertEquals(10_000, ((NumberType) d.get("FIELD_B_COUNT.0").getData()).getDelegate().intValue());
    }

    private Iterator<Map.Entry<Key,Document>> getDocumentIterator(int count) {
        List<Map.Entry<Key,Document>> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(getDocumentEntry());
        }
        return list.iterator();
    }

    private Map.Entry<Key,Document> getDocumentEntry() {
        Key key = new Key("row", "dt\0uid-" + uidCount++);
        Document document = getDocument(key);
        return new AbstractMap.SimpleEntry<>(key, document);
    }

    private Document getDocument(Key docKey) {
        Document d = new Document();
        d.put("FIELD_A", new PreNormalizedAttribute("value_a", docKey, false));
        d.put("FIELD_B", new PreNormalizedAttribute("value_b", docKey, false));
        d.put("FIELD_C", new PreNormalizedAttribute("value_c", docKey, false));
        return d;
    }
}
