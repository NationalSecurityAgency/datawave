package datawave.query.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
     * Once the per-page timeout is exceeded, apply() must reset the page timer when it emits an intermediate result, so intermediate results are paced (one per
     * timeout window) instead of being emitted on every subsequent call.
     */
    @Test
    public void testIntermediateResultsArePaced_afterPageTimerReset() {
        GroupFields groupFields = new GroupFields();
        TreeMultimap<String,TemporalGranularity> groupBy = TreeMultimap.create();
        groupBy.put("FIELD_A", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(groupBy);

        // Drive the page timer from a fixed clock, started past the timeout so the very first apply() is eligible to emit an intermediate result.
        GroupingTransform transform = new GroupingTransform(groupFields, MarkingFunctions.Factory.createMarkingFunctions(), PAGE_TIMEOUT_MS);
        setClockTo(transform, PAGE_START);
        transform.setQueryExecutionForPageStartTime(transform.clock.millis());
        setClockTo(transform, PAGE_START.plusMillis(PAGE_TIMEOUT_MS + 1));

        int intermediate = 0;
        int accumulated = 0;
        for (int i = 0; i < 5; i++) {
            Map.Entry<Key,Document> result = transform.apply(documentEntry("FIELD_A", "value-" + i, i));
            if (result == null) {
                accumulated++;
            } else if (result.getValue().isIntermediateResult()) {
                intermediate++;
            }
        }

        // Only the first apply() emits an intermediate result; emitting it resets the page timer, so the remaining
        // calls (the clock does not advance again) accumulate instead of flooding.
        assertEquals(1, intermediate);
        assertEquals(4, accumulated);
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
