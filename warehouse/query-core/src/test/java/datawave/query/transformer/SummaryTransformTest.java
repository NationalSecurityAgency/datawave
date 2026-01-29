package datawave.query.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

import datawave.query.Constants;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;

class SummaryTransformTest {

    @Test
    void testGetEventIdsForEvent() {
        Document d = new Document();
        d.put(Document.DOCKEY_FIELD_NAME, new DocumentKey("shard", "datatype", "uid", true));

        List<DocumentKey> expected = List.of(new DocumentKey("shard", "datatype", "uid", true));
        assertEquals(expected, SummaryTransform.getEventIds(d));
    }

    @Test
    void testGetEventIdsForTLD() {
        Document d = new Document();
        d.put(Document.DOCKEY_FIELD_NAME, new DocumentKey("shard", "datatype", "uid", true));
        d.put(Document.DOCKEY_FIELD_NAME, new DocumentKey("shard", "datatype", "uid.1", true));
        d.put(Document.DOCKEY_FIELD_NAME, new DocumentKey("shard", "datatype", "uid.2", true));
        d.put(Document.DOCKEY_FIELD_NAME, new DocumentKey("shard", "datatype", "uid.18", true));

        List<DocumentKey> expected = List.of(new DocumentKey("shard", "datatype", "uid", true));
        assertEquals(expected, SummaryTransform.getEventIds(d));
    }

    @Test
    void testGetEventIdsForNoRecordId() {
        Document d = new Document(new Key("shard", "datatype" + Constants.NULL + "uid"), true);

        List<DocumentKey> expected = List.of(new DocumentKey("shard", "datatype", "uid", true));
        assertEquals(expected, SummaryTransform.getEventIds(d));
    }
}
