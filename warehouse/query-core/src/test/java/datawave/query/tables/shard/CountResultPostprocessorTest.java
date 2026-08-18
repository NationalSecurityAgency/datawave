package datawave.query.tables.shard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;

class CountResultPostprocessorTest {

    private final CountResultPostprocessor postprocessor = new CountResultPostprocessor(null);

    @Test
    void retainsIntermediateResultsUntilCountArrives() {
        DefaultEvent firstIntermediate = intermediateResult();
        DefaultEvent secondIntermediate = intermediateResult();
        List<Object> results = new ArrayList<>(List.of(firstIntermediate, secondIntermediate));

        assertTrue(firstIntermediate.isIntermediateResult());
        assertTrue(secondIntermediate.isIntermediateResult());
        postprocessor.apply(results);

        assertEquals(List.of(firstIntermediate, secondIntermediate), results);
    }

    @Test
    void removesIntermediateResultsWhenCountArrives() {
        DefaultEvent countResult = countResult(12L);
        List<Object> results = new ArrayList<>(List.of(intermediateResult(), countResult));

        postprocessor.apply(results);

        assertEquals(1, results.size());
        assertSame(countResult, results.get(0));
    }

    private DefaultEvent intermediateResult() {
        DefaultEvent event = new DefaultEvent();
        event.setIntermediateResult(true);
        return event;
    }

    private DefaultEvent countResult(long count) {
        DefaultField countField = new DefaultField();
        countField.setName("count");
        countField.setValue(count);

        DefaultEvent event = new DefaultEvent();
        event.setFields(List.of(countField));
        return event;
    }
}
