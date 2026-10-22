package datawave.query.jexl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlScript;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.collections.FunctionalSet;

class OrEvaluationTest {

    @ParameterizedTest
    @ValueSource(strings = {"A == 'value' || B == 'value' || C == 'value'", "(A == 'value' || B == 'value') || C == 'value'",
            "A == 'value' || (B == 'value' || C == 'value')", "(A == 'value') || ((B == 'value')) || C == 'value'"})
    void preservesOrderAndShortCircuiting(String query) {
        JexlScript script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);
        RecordingContext context = new RecordingContext();
        context.set("A", "value");
        context.set("B", "value");
        context.set("C", "value");
        assertTrue(ArithmeticJexlEngines.isMatched(script.execute(context)));
        assertEquals(List.of("A"), context.reads);

        context.reads.clear();
        context.set("A", "other");
        context.set("B", "other");
        assertTrue(ArithmeticJexlEngines.isMatched(script.execute(context)));
        assertEquals(List.of("A", "B", "C"), context.reads);

        context.reads.clear();
        context.set("C", "other");
        assertFalse(ArithmeticJexlEngines.isMatched(script.execute(context)));
        assertEquals(List.of("A", "B", "C"), context.reads);
    }

    @ParameterizedTest
    @ValueSource(strings = {"A || B || C", "(A || B) || C", "A || (B || C)", "(A) || ((B)) || C"})
    void aggregatesAllIdentifierValues(String query) {
        JexlScript script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);
        RecordingContext context = new RecordingContext();
        context.set("A", "a");
        context.set("B", "b");
        context.set("C", "c");

        FunctionalSet<?> values = assertInstanceOf(FunctionalSet.class, script.execute(context));
        assertEquals(Set.of("a", "b", "c"), values.stream().map(value -> value.getValue()).collect(Collectors.toSet()));
        assertEquals(List.of("A", "B", "C"), context.reads);
    }

    @ParameterizedTest
    @ValueSource(strings = {"A == 'value' || (B || C)", "A == 'value' || ((B || C))", "(A == 'value') || (B || (C || D))", "A == 'value' || ((B || C) || D)"})
    void shortCircuitsNestedIdentifiersAfterAComparison(String query) {
        JexlScript script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);
        RecordingContext context = new RecordingContext();
        context.set("A", "other");
        context.set("B", "value");
        context.set("C", "value");
        context.set("D", "value");

        assertTrue(ArithmeticJexlEngines.isMatched(script.execute(context)));
        assertEquals(List.of("A", "B"), context.reads);

        context.reads.clear();
        context.set("B", false);
        assertTrue(ArithmeticJexlEngines.isMatched(script.execute(context)));
        assertEquals(List.of("A", "B", "C"), context.reads);
    }

    @Test
    void doesNotReevaluateThePrefixWhenANestedOrIsEncountered() {
        JexlScript script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript("A || B == 'value' || (C || D)");
        RecordingContext context = new RecordingContext();
        context.set("A", false);
        context.set("B", "other");
        context.set("C", "value");
        context.set("D", "value");

        assertTrue(ArithmeticJexlEngines.isMatched(script.execute(context)));
        assertEquals(List.of("A", "B", "C"), context.reads);
    }

    @Test
    void preservesBooleanCoercionsInNestedOrs() {
        CountingArithmetic arithmetic = new CountingArithmetic();
        DatawaveJexlEngine engine = new DatawaveJexlEngine(new JexlBuilder().arithmetic(arithmetic).cache(0));
        RecordingContext context = new RecordingContext();
        context.set("A", "other");
        context.set("B", "other");
        context.set("C", "other");

        engine.createScript("A == 'value' || B == 'value' || C == 'value'").execute(context);
        int expected = arithmetic.coercions;
        assertTrue(expected > 0);
        arithmetic.coercions = 0;
        engine.createScript("A == 'value' || (B == 'value' || C == 'value')").execute(context);
        assertEquals(expected, arithmetic.coercions);
    }

    private static class CountingArithmetic extends DefaultArithmetic {
        private int coercions;

        @Override
        public boolean toBoolean(Object value) {
            coercions++;
            return super.toBoolean(value);
        }
    }

    private static class RecordingContext extends DatawaveJexlContext {
        private final List<String> reads = new ArrayList<>();

        @Override
        public Object get(String name) {
            reads.add(name);
            return super.get(name);
        }
    }
}
