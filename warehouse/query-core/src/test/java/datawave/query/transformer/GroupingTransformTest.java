package datawave.query.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Stubber;

import com.google.common.collect.TreeMultimap;

import datawave.data.type.LcNoDiacriticsType;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.attributes.TemporalGranularity;
import datawave.query.attributes.TypeAttribute;
import datawave.query.common.grouping.GroupFields;

public class GroupingTransformTest {

    /** Per-page timeout used by the intermediate-result test, paired with {@link #PAGE_START} to drive the transform's clock. */
    private static final long PAGE_TIMEOUT_MS = 1000L;
    private static final Instant PAGE_START = Instant.parse("2026-01-01T00:00:00Z");

    @BeforeAll
    public static void setup() {
        MarkingFunctions.Factory.createMarkingFunctions();
    }

    /**
     * Once the per-page timeout is exceeded, apply() may emit an intermediate result only if the page window saw activity. Emissions are paced (one per timeout
     * window) rather than being returned on every subsequent call.
     */
    @Test
    public void testIntermediateResultsRequireActivityAndArePaced() {
        GroupFields groupFields = new GroupFields();
        TreeMultimap<String,TemporalGranularity> groupBy = TreeMultimap.create();
        groupBy.put("FIELD_A", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(groupBy);

        // Start with an active page window and observe a document before the timeout.
        GroupingTransform transform = new GroupingTransform(groupFields, MarkingFunctions.Factory.createMarkingFunctions(), PAGE_TIMEOUT_MS);
        setClockTo(transform, PAGE_START);
        transform.setQueryExecutionForPageStartTime(transform.clock.millis());

        int intermediate = 0;
        int accumulated = 0;
        assertNull(transform.apply(documentEntry("FIELD_A", "value-0", 0)));
        accumulated++;
        setClockTo(transform, PAGE_START.plusMillis(PAGE_TIMEOUT_MS + 1));
        for (int i = 1; i < 5; i++) {
            Map.Entry<Key,Document> result = transform.apply(documentEntry("FIELD_A", "value-" + i, i));
            if (result == null) {
                accumulated++;
            } else if (result.getValue().isIntermediateResult()) {
                intermediate++;
            }
        }

        // Only the first apply after the timeout emits an intermediate result; it resets the timer, so the remaining
        // calls (at the same clock time) accumulate instead of flooding.
        assertEquals(1, intermediate);
        assertEquals(4, accumulated);
    }

    @Test
    public void testNoIntermediateResultWithoutDocumentWithinPageTimeout() {
        GroupFields groupFields = new GroupFields();
        TreeMultimap<String,TemporalGranularity> groupBy = TreeMultimap.create();
        groupBy.put("FIELD_A", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(groupBy);

        GroupingTransform transform = new GroupingTransform(groupFields, MarkingFunctions.Factory.createMarkingFunctions(), PAGE_TIMEOUT_MS);
        setClockTo(transform, PAGE_START);
        transform.setQueryExecutionForPageStartTime(transform.clock.millis());
        setClockTo(transform, PAGE_START.plusMillis(PAGE_TIMEOUT_MS + 1));

        // An idle page window must not emit an intermediate result, even when apply() is called with no document.
        assertNull(transform.apply(null));

        // Late documents do not qualify an otherwise idle page window for an intermediate result.
        assertNull(transform.apply(documentEntry("FIELD_A", "late-1", 1)));
        assertNull(transform.apply(documentEntry("FIELD_A", "late-2", 2)));
        assertNull(transform.apply(documentEntry("FIELD_A", "late-1", 3)));

        // A new RunningQuery.next() call starts a fresh window. Its activity can qualify a later keep-alive.
        transform.setQueryExecutionForPageStartTime(transform.clock.millis());
        assertNull(transform.apply(documentEntry("FIELD_A", "on-time", 4)));
        setClockTo(transform, PAGE_START.plusMillis((2 * PAGE_TIMEOUT_MS) + 2));
        Map.Entry<Key,Document> result = transform.apply(null);
        assertNotNull(result);
        assertTrue(result.getValue().isIntermediateResult());

        Map<String,String> counts = new HashMap<>();
        Map.Entry<Key,Document> flushed;
        while ((flushed = transform.flush()) != null) {
            String field = flushed.getValue().get("FIELD_A").getData().toString();
            assertNull(counts.put(field, flushed.getValue().get("COUNT").getData().toString()), "Duplicate group: " + field);
        }
        assertEquals(Map.of("late-1", "2", "late-2", "1", "on-time", "1"), counts);
    }

    @Test
    public void testDocumentAtPageTimeoutBoundaryCountsAsActivity() {
        GroupFields groupFields = new GroupFields();
        TreeMultimap<String,TemporalGranularity> groupBy = TreeMultimap.create();
        groupBy.put("FIELD_A", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(groupBy);

        GroupingTransform transform = new GroupingTransform(groupFields, MarkingFunctions.Factory.createMarkingFunctions(), PAGE_TIMEOUT_MS);
        setClockTo(transform, PAGE_START);
        transform.setQueryExecutionForPageStartTime(transform.clock.millis());
        setClockTo(transform, PAGE_START.plusMillis(PAGE_TIMEOUT_MS));

        // Equality is still within the allowed window and records activity, but must not emit a marker yet.
        assertNull(transform.apply(documentEntry("FIELD_A", "boundary", 1)));

        setClockTo(transform, PAGE_START.plusMillis(PAGE_TIMEOUT_MS + 1));
        Map.Entry<Key,Document> result = transform.apply(null);
        assertNotNull(result);
        assertTrue(result.getValue().isIntermediateResult());
    }

    @Test
    public void testPageResetClearsPreviousPageActivity() {
        GroupingTransform transform = givenPageTimedTransform();
        assertNull(transform.apply(documentEntry("FIELD_A", "on-time", 0)));

        // Reset while the previous page still has activity, without emitting a marker first.
        setClockTo(transform, PAGE_START.plusMillis(1));
        transform.setQueryExecutionForPageStartTime(transform.clock.millis());
        setClockTo(transform, PAGE_START.plusMillis(PAGE_TIMEOUT_MS + 2));
        assertNull(transform.apply(null));
        assertNull(transform.apply(documentEntry("FIELD_A", "late", 1)));
        assertNull(transform.apply(null));
    }

    @Test
    public void testInFlightDocumentDoesNotConsumeNewPageActivity() throws Exception {
        assertPageResetDuringApply(false);
    }

    @Test
    public void testInFlightDocumentDoesNotRecordNewPageActivity() throws Exception {
        assertPageResetDuringApply(true);
    }

    private static void assertPageResetDuringApply(boolean beforeActivity) throws Exception {
        GroupingTransform transform = givenPageTimedTransform();
        CountDownLatch processing = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        Map.Entry<Key,Document> entry = documentEntry("FIELD_A", "old-page", 0);
        Document document = spy(entry.getValue());
        Stubber pause = doAnswer(invocation -> {
            processing.countDown();
            assertTrue(resume.await(5, TimeUnit.SECONDS), "Document processing was not released");
            return invocation.callRealMethod();
        });
        if (beforeActivity) {
            pause.when(document).isIntermediateResult();
        } else {
            pause.when(document).getDictionary();
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Map.Entry<Key,Document>> inFlight = executor.submit(() -> transform.apply(new AbstractMap.SimpleEntry<>(entry.getKey(), document)));
            assertTrue(processing.await(5, TimeUnit.SECONDS), "Document processing did not start");
            setClockTo(transform, PAGE_START.plusMillis(PAGE_TIMEOUT_MS + 1));
            Future<?> reset = executor.submit(() -> transform.setQueryExecutionForPageStartTime(transform.clock.millis()));
            reset.get(5, TimeUnit.SECONDS);

            if (beforeActivity) {
                // Old-page input must not qualify an idle new page when it resumes within that window.
                resume.countDown();
                assertNull(inFlight.get(5, TimeUnit.SECONDS));
                setClockTo(transform, PAGE_START.plusMillis((2 * PAGE_TIMEOUT_MS) + 2));
                assertNull(transform.apply(null));
                assertNull(transform.apply(documentEntry("FIELD_A", "late", 1)));
            } else {
                // New-page activity must remain available when old-page processing finishes.
                assertNull(transform.apply(documentEntry("FIELD_A", "new-page", 1)));
                setClockTo(transform, PAGE_START.plusMillis((2 * PAGE_TIMEOUT_MS) + 2));
                resume.countDown();
                assertNull(inFlight.get(5, TimeUnit.SECONDS));
                Map.Entry<Key,Document> result = transform.apply(null);
                assertNotNull(result);
                assertTrue(result.getValue().isIntermediateResult());
            }
        } finally {
            resume.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Test workers did not terminate");
        }
    }

    private static GroupingTransform givenPageTimedTransform() {
        GroupFields groupFields = new GroupFields();
        TreeMultimap<String,TemporalGranularity> groupBy = TreeMultimap.create();
        groupBy.put("FIELD_A", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(groupBy);
        GroupingTransform transform = new GroupingTransform(groupFields, MarkingFunctions.Factory.createMarkingFunctions(), PAGE_TIMEOUT_MS);
        setClockTo(transform, PAGE_START);
        transform.setQueryExecutionForPageStartTime(transform.clock.millis());
        return transform;
    }

    private static void setClockTo(DocumentTransform.DefaultDocumentTransform transform, Instant instant) {
        transform.clock = Clock.fixed(instant, ZoneOffset.UTC);
    }

    private static Map.Entry<Key,Document> documentEntry(String field, String value, int uid) {
        Key key = new Key("row", "dt\0uid-" + uid);
        Document document = new Document(key, true);
        document.put(field, new TypeAttribute<>(new LcNoDiacriticsType(value), key, true), true);
        return new AbstractMap.SimpleEntry<>(key, document);
    }
}
