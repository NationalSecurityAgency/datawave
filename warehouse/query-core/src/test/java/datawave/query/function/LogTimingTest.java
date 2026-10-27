package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import datawave.query.attributes.Document;
import datawave.query.attributes.TimingMetadata;
import datawave.query.iterator.profile.QuerySpan;

class LogTimingTest {

    @Test
    void testAddTimingMetadataRecordsSpanAndResetsIt() {
        Document document = new Document();
        QuerySpan querySpan = new QuerySpan(null);
        querySpan.seek();
        querySpan.next();
        querySpan.next();
        querySpan.yield();
        querySpan.addStageTimer(QuerySpan.Stage.Aggregation, 100L);
        // below the five percent threshold, so it should not be reported
        querySpan.addStageTimer(QuerySpan.Stage.DocumentEvaluation, 1L);

        LogTiming.addTimingMetadata(document, querySpan);

        TimingMetadata timingMetadata = assertInstanceOf(TimingMetadata.class, document.get(LogTiming.TIMING_METADATA));
        assertEquals(1L, timingMetadata.getSeekCount());
        assertEquals(2L, timingMetadata.getNextCount());
        assertEquals(1L, timingMetadata.getSourceCount());
        assertEquals(1L, timingMetadata.getYieldCount());

        Map<String,Long> stageTimers = timingMetadata.getStageTimers();
        assertEquals(1, stageTimers.size());
        assertEquals(100L, stageTimers.get(QuerySpan.Stage.Aggregation.name()).longValue());

        assertEquals(0L, querySpan.getSeekCount());
        assertEquals(0L, querySpan.getNextCount());
        assertEquals(0L, querySpan.getSourceCount());
        assertFalse(querySpan.getYield());
        assertTrue(querySpan.getStageTimers().isEmpty());
    }

    @Test
    void testAddTimingMetadataIgnoresNullArguments() {
        Document document = new Document();

        LogTiming.addTimingMetadata(document, null);
        LogTiming.addTimingMetadata(null, new QuerySpan(null));

        assertFalse(document.getDictionary().containsKey(LogTiming.TIMING_METADATA));
    }
}
