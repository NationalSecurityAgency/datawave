package datawave.query.jexl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.jexl3.JexlScript;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;

class AndEvaluationTest {

    @ParameterizedTest
    @ValueSource(strings = {"((_Bounded_ = true) && (FIELD > '2' && FIELD < '8'))",
            "((_Delayed_ = true) && ((_Bounded_ = true) && (FIELD > '2' && FIELD < '8')))",
            "ENABLED == true && ((_Bounded_ = true) && (FIELD > '2' && FIELD < '8'))"})
    void markedRangesRequireOneValueToSatisfyBothBounds(String query) {
        FunctionalSet<ValueTuple> values = valuesOutsideRange();
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set("FIELD", values);
        context.set("ENABLED", true);
        JexlScript script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);

        assertFalse(ArithmeticJexlEngines.isMatched(script.execute(context)));
        values.add(new ValueTuple("FIELD", "5", "5", null));
        assertTrue(ArithmeticJexlEngines.isMatched(script.execute(context)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"FIELD > '2' && FIELD < '8'", "((_Delayed_ = true) && (FIELD > '2' && FIELD < '8'))"})
    void unmarkedComparisonsCanMatchDifferentValues(String query) {
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set("FIELD", valuesOutsideRange());
        JexlScript script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);

        assertTrue(ArithmeticJexlEngines.isMatched(script.execute(context)));
    }

    @Test
    void ordinaryAndsRetainShortCircuiting() {
        DatawaveJexlContext context = new DatawaveJexlContext() {
            @Override
            public Object get(String name) {
                if (name.equals("LATER")) {
                    throw new AssertionError("A later operand was evaluated after an AND failed");
                }
                return super.get(name);
            }
        };
        context.set("FIRST", "other");
        JexlScript script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript("FIRST == 'value' && LATER == 'value'");

        assertFalse(ArithmeticJexlEngines.isMatched(script.execute(context)));
    }

    private FunctionalSet<ValueTuple> valuesOutsideRange() {
        FunctionalSet<ValueTuple> values = new FunctionalSet<>();
        values.add(new ValueTuple("FIELD", "1", "1", null));
        values.add(new ValueTuple("FIELD", "9", "9", null));
        return values;
    }
}
