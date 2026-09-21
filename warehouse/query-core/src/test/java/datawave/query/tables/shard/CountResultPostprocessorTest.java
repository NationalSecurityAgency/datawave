package datawave.query.tables.shard;

import static datawave.query.transformer.ShardQueryCountTableTransformer.COUNT_CELL;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.marking.MarkingFunctions;
import datawave.query.tables.CountingShardQueryLogic;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;

class CountResultPostprocessorTest {

    private static final String VISIBILITY = "PUBLIC";

    /**
     * {@link CountAggregatingIterator} emits an event carrying no fields at all while the count is still being aggregated.
     */
    @Test
    void testIntermediateResultIsToleratedDuringAggregation() {
        List<Object> results = new ArrayList<>();
        results.add(intermediateEvent());
        results.add(countEvent(5L));

        assertDoesNotThrow(() -> postprocessor().apply(results));
    }

    @Test
    void testCountsAreAggregated() {
        List<Object> results = new ArrayList<>();
        results.add(countEvent(5L));
        results.add(countEvent(7L));

        postprocessor().apply(results);

        assertEquals(1, results.size());
        assertEquals(12L, countOf(results.get(0)));
    }

    /**
     * A zero wait returns an intermediate result on every call to next(), before any count can be produced.
     */
    @Test
    void testPageWaitTimeDefaultsToNonZero() {
        assertNotEquals(0L, new CountingShardQueryLogic().getPageWaitTimeMillis());
    }

    private CountResultPostprocessor postprocessor() {
        return new CountResultPostprocessor(new MarkingFunctions.Default());
    }

    private long countOf(Object result) {
        return ((Number) ((DefaultEvent) result).getFields().get(0).getValueOfTypedValue()).longValue();
    }

    private DefaultEvent intermediateEvent() {
        DefaultEvent event = new DefaultEvent();
        event.setIntermediateResult(true);
        return event;
    }

    private DefaultEvent countEvent(long count) {
        DefaultField field = new DefaultField();
        field.setName(COUNT_CELL);
        field.setColumnVisibility(VISIBILITY);
        field.setTimestamp(0L);
        field.setValue(count);

        DefaultEvent event = new DefaultEvent();
        event.setFields(Collections.singletonList(field));
        return event;
    }
}
