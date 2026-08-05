package datawave.query.predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Test;

public class EventDataQueryEntryLimitFilterTest {

    private final Key documentKey = new Key("row", "datatype\u0000uid");

    @Test
    public void testNoLimitByDefault() {
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter();
        filter.startNewDocument(documentKey);

        for (int i = 0; i < 100; i++) {
            assertTrue(filter.apply(createEntry("FIELD_" + i)));
        }
    }

    @Test
    public void testLimitEnforced() {
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter().withMaxEntries(3);
        filter.startNewDocument(documentKey);

        assertTrue(filter.apply(createEntry("FIELD_A")));
        assertTrue(filter.apply(createEntry("FIELD_B")));
        assertTrue(filter.apply(createEntry("FIELD_C")));
        assertFalse(filter.apply(createEntry("FIELD_D")));
        assertFalse(filter.apply(createEntry("FIELD_E")));
    }

    @Test
    public void testKeepTracksLimitWithoutConsumingBudget() {
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter().withMaxEntries(2);
        filter.startNewDocument(documentKey);

        // keep is called before apply for each entry and must not consume budget
        for (int i = 0; i < 10; i++) {
            assertTrue(filter.keep(createEntry("FIELD_A").getKey()));
        }
        assertTrue(filter.apply(createEntry("FIELD_A")));
        assertTrue(filter.keep(createEntry("FIELD_B").getKey()));
        assertTrue(filter.apply(createEntry("FIELD_B")));

        // budget exhausted: keep now rejects too
        assertFalse(filter.keep(createEntry("FIELD_C").getKey()));
        assertFalse(filter.apply(createEntry("FIELD_C")));
    }

    @Test
    public void testPeekDoesNotConsumeBudget() {
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter().withMaxEntries(2);
        filter.startNewDocument(documentKey);

        for (int i = 0; i < 10; i++) {
            assertTrue(filter.peek(createEntry("FIELD_A")));
        }
        assertTrue(filter.apply(createEntry("FIELD_A")));
        assertTrue(filter.apply(createEntry("FIELD_B")));
        assertFalse(filter.peek(createEntry("FIELD_C")));
    }

    @Test
    public void testMarkerEmittedExactlyOnce() {
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter().withMaxEntries(1);
        filter.startNewDocument(documentKey);

        assertTrue(filter.apply(createEntry("FIELD_A")));

        Map.Entry<Key,String> rejected = createEntry("FIELD_B");
        assertFalse(filter.apply(rejected));

        Key marker = filter.transform(rejected.getKey());
        assertNotNull(marker);
        assertEquals(rejected.getKey().getRow(), marker.getRow());
        assertEquals(rejected.getKey().getColumnFamily(), marker.getColumnFamily());
        // the marker must inherit the rejected key's visibility and timestamp or downstream visibility/time filtering drops it
        assertEquals(rejected.getKey().getColumnVisibility(), marker.getColumnVisibility());
        assertEquals(rejected.getKey().getTimestamp(), marker.getTimestamp());
        assertTrue(marker.getColumnQualifier().toString().startsWith(EventDataQueryEntryLimitFilter.INCOMPLETE_DOCUMENT_FIELD + '\u0000'));

        // only one marker per document
        assertNull(filter.transform(createEntry("FIELD_C").getKey()));
    }

    @Test
    public void testMarkerSurvivesFilterAfterLimit() {
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter().withMaxEntries(1);
        filter.startNewDocument(documentKey);

        assertTrue(filter.apply(createEntry("FIELD_A")));
        Map.Entry<Key,String> rejected = createEntry("FIELD_B");
        assertFalse(filter.apply(rejected));
        Key marker = filter.transform(rejected.getKey());

        // a fresh clone (as used by the downstream aggregation) must accept the marker even at/past its own budget
        EventDataQueryEntryLimitFilter downstream = (EventDataQueryEntryLimitFilter) filter.clone();
        downstream.startNewDocument(documentKey);
        assertTrue(downstream.apply(createEntry("FIELD_A")));
        assertTrue(downstream.keep(marker));
        assertTrue(downstream.apply(new AbstractMap.SimpleEntry<>(marker, "")));
    }

    @Test
    public void testSeekRangeRollsOverDocumentAfterLimit() {
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter().withMaxEntries(2);
        filter.startNewDocument(documentKey);

        // under budget: no seek is suggested
        Map.Entry<Key,String> first = createEntry("FIELD_A");
        assertTrue(filter.apply(first));
        assertNull(filter.getSeekRange(first.getKey(), null, false));

        assertTrue(filter.apply(createEntry("FIELD_B")));
        Map.Entry<Key,String> rejected = createEntry("FIELD_C");
        assertFalse(filter.apply(rejected));

        // a range extending past this document: seek past the entire document column family
        Key endKey = new Key("row2");
        Range seek = filter.getSeekRange(rejected.getKey(), endKey, false);
        assertNotNull(seek);
        assertTrue(seek.getStartKey().compareTo(rejected.getKey()) > 0);
        assertEquals("datatype\u0000uid\u0000", seek.getStartKey().getColumnFamily().toString());

        // a single-document range whose end IS the document rollover: an empty range cannot be represented
        Key documentEndKey = new Key("row", "datatype\u0000uid\u0000");
        assertNull(filter.getSeekRange(rejected.getKey(), documentEndKey, false));
    }

    @Test
    public void testStartNewDocumentResetsState() {
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter().withMaxEntries(1);
        filter.startNewDocument(documentKey);

        assertTrue(filter.apply(createEntry("FIELD_A")));
        assertFalse(filter.apply(createEntry("FIELD_B")));
        assertNotNull(filter.transform(createEntry("FIELD_B").getKey()));

        filter.startNewDocument(new Key("row", "datatype\u0000uid2"));
        assertTrue(filter.apply(createEntry("FIELD_A")));
        assertFalse(filter.apply(createEntry("FIELD_B")));
        // the marker is available again for the new document
        assertNotNull(filter.transform(createEntry("FIELD_B").getKey()));
    }

    @Test
    public void testCloneDoesNotCopyState() {
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter().withMaxEntries(2);
        filter.startNewDocument(documentKey);
        assertTrue(filter.apply(createEntry("FIELD_A")));
        assertTrue(filter.apply(createEntry("FIELD_B")));
        assertFalse(filter.apply(createEntry("FIELD_C")));

        EventDataQueryEntryLimitFilter copy = (EventDataQueryEntryLimitFilter) filter.clone();
        copy.startNewDocument(documentKey);
        assertEquals(2, copy.getMaxEntries());
        assertTrue(copy.apply(createEntry("FIELD_A")));
        assertTrue(copy.apply(createEntry("FIELD_B")));
        assertFalse(copy.apply(createEntry("FIELD_C")));
    }

    @Test
    public void testDelegateDecisionsRespected() {
        EventDataQueryFieldFilter delegate = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_A"));
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter(delegate).withMaxEntries(5);
        filter.startNewDocument(documentKey);

        // delegate rejections do not consume budget
        for (int i = 0; i < 10; i++) {
            assertFalse(filter.apply(createEntry("FIELD_B")));
        }
        for (int i = 0; i < 5; i++) {
            assertTrue(filter.apply(createEntry("FIELD_A")));
        }
        // budget exhausted: even delegate-accepted fields are rejected
        assertFalse(filter.apply(createEntry("FIELD_A")));
    }

    @Test
    public void testNullEntryRejected() {
        EventDataQueryEntryLimitFilter filter = new EventDataQueryEntryLimitFilter().withMaxEntries(2);
        filter.startNewDocument(documentKey);
        assertFalse(filter.apply(null));
        assertFalse(filter.peek(null));
    }

    private Map.Entry<Key,String> createEntry(String field) {
        Key key = new Key("row", "datatype\u0000uid", field + "\u0000value");
        return new AbstractMap.SimpleEntry<>(key, "");
    }
}
