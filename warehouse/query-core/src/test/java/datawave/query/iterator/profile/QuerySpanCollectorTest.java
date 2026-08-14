package datawave.query.iterator.profile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class QuerySpanCollectorTest {

    @Test
    void testGetStageTimersReturnsSnapshot() {
        QuerySpanCollector collector = new QuerySpanCollector();
        collector.addQuerySpan(spanWith(QuerySpan.Stage.Aggregation, 100L));

        Map<String,Long> snapshot = collector.getStageTimers();
        assertEquals(1, snapshot.size());
        assertEquals(100L, snapshot.get(QuerySpan.Stage.Aggregation.name()).longValue());

        collector.addQuerySpan(spanWith(QuerySpan.Stage.DocumentEvaluation, 50L));

        // the previously returned map is a copy and must not observe the new entry
        assertEquals(1, snapshot.size());
        assertEquals(100L, snapshot.get(QuerySpan.Stage.Aggregation.name()).longValue());

        assertEquals(2, collector.getStageTimers().size());
    }

    @Test
    void testGetStageTimersIsUnmodifiable() {
        QuerySpanCollector collector = new QuerySpanCollector();
        collector.addQuerySpan(spanWith(QuerySpan.Stage.Aggregation, 100L));

        Map<String,Long> stageTimers = collector.getStageTimers();
        assertThrows(UnsupportedOperationException.class, () -> stageTimers.put(QuerySpan.Stage.PostProcessing.name(), 1L));
    }

    @Test
    void testGetStageTimersIsSafeToIterateWhileCollecting() throws Exception {
        QuerySpanCollector collector = new QuerySpanCollector();
        QuerySpan.Stage[] stages = QuerySpan.Stage.values();
        int iterations = 5000;

        ExecutorService executorService = Executors.newFixedThreadPool(6);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int writer = 0; writer < 4; writer++) {
                futures.add(executorService.submit(() -> {
                    for (int i = 0; i < iterations; i++) {
                        collector.addQuerySpan(spanWith(stages[i % stages.length], 1L));
                    }
                }));
            }
            // repeatedly drain the collector so that the stage timer map is structurally modified while it is read
            futures.add(executorService.submit(() -> {
                for (int i = 0; i < iterations; i++) {
                    collector.getCombinedQuerySpan(null, true);
                }
            }));
            // any ConcurrentModificationException here surfaces via Future.get below
            futures.add(executorService.submit(() -> {
                for (int i = 0; i < iterations; i++) {
                    for (Map.Entry<String,Long> entry : collector.getStageTimers().entrySet()) {
                        assertTrue(entry.getValue() >= 0);
                    }
                }
            }));

            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    private QuerySpan spanWith(QuerySpan.Stage stage, long elapsed) {
        QuerySpan querySpan = new QuerySpan(null);
        querySpan.addStageTimer(stage, elapsed);
        return querySpan;
    }
}
