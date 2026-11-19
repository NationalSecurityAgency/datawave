package datawave.ingest.input.reader.event;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EventErrorSummaryJexlTest {

    @ParameterizedTest
    @ValueSource(strings = {"try", "catch", "finally", "throw", "instanceof"})
    void preservesPreviouslyValidFieldNames(String field) {
        EventErrorSummary summary = new EventErrorSummary();
        summary.set(field, "value");
        assertTrue(matches(summary, field + " == 'value'"));
    }

    @Test
    void preservesBooleanCoercionOfLogicalExpressions() {
        EventErrorSummary summary = new EventErrorSummary();
        summary.set("FIELD", "value");

        assertTrue(matches(summary, "FIELD || false"));
        assertTrue(matches(summary, "true && FIELD"));
        assertFalse(matches(summary, "false && FIELD"));
        assertFalse(matches(summary, "'' || false"));
    }

    private boolean matches(EventErrorSummary summary, String query) {
        return summary.matches(null, null, null, Collections.emptySet(), null, null, query, 0);
    }
}
