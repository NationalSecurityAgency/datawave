package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

public class SearchExpressionsTest {
    private static StandalonePatternExpression pattern(String source) {
        return new StandalonePatternExpression(source, 2);
    }

    @Test
    public void orderedComponentsAreStructuralAndOrderSensitive() {
        ProximityExpression first = new ProximityExpression(true, Arrays.asList(pattern("a"), pattern("b")), 1);
        ProximityExpression reversed = new ProximityExpression(true, Arrays.asList(pattern("b"), pattern("a")), 1);
        assertNotEquals(first, reversed);
    }

    @Test
    public void unorderedComponentsIgnoreOrderButRetainMultiplicity() {
        ProximityExpression first = new ProximityExpression(false, Arrays.asList(pattern("a"), pattern("b"), pattern("a")), 2);
        ProximityExpression equivalent = new ProximityExpression(false, Arrays.asList(pattern("a"), pattern("a"), pattern("b")), 2);
        ProximityExpression missingRepeat = new ProximityExpression(false, Arrays.asList(pattern("a"), pattern("b")), 2);
        assertEquals(first, equivalent);
        assertNotEquals(first, missingRepeat);
        assertEquals(3, first.getComponents().size());
    }

    @Test
    public void aggregateDeduplicatesAndExposesNoMutableState() {
        StandalonePatternExpression a = pattern("a");
        List<SearchExpression> source = new ArrayList<>(Arrays.asList(a, a));
        SearchExpressions expressions = new SearchExpressions(source);
        source.clear();
        assertEquals(1, expressions.size());
        assertThrows(UnsupportedOperationException.class, () -> expressions.getExpressions().add(a));
    }
}
