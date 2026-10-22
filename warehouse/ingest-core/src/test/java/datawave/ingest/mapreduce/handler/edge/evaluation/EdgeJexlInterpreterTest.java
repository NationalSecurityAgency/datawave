package datawave.ingest.mapreduce.handler.edge.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.attribute.EventFieldValueTuple;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;

class EdgeJexlInterpreterTest {

    private final EdgePreconditionArithmetic arithmetic = new EdgePreconditionArithmetic();
    private final EdgePreconditionCacheHelper helper = new EdgePreconditionCacheHelper(arithmetic);
    private final MapContext context = new MapContext();

    @ParameterizedTest
    @ValueSource(strings = {"try", "catch", "finally", "throw", "instanceof"})
    void preservesPreviouslyValidFieldNames(String field) {
        setField(field, "0", "match");
        assertEquals(Boolean.TRUE, evaluate(field + " == 'match'"));
        assertEquals(Set.of("0"), arithmetic.getMatchingGroups().get("PET"));

        EdgeDefinition edge = new EdgeDefinition();
        edge.setJexlPrecondition(field + " == 'match'");
        assertEquals(Set.of(field), new EdgePreconditionJexlContext(List.of(edge)).getFilterFieldKeys());
    }

    @Test
    void evaluatesEveryOrOperandToCollectMatchingGroups() {
        setField("A", "0", "match");
        setField("B", "1", "match");
        setField("C", "2", "match");
        setField("D", "3", "match");

        assertEquals(Boolean.TRUE, evaluate("A == 'match' || B == 'match' || C == 'match' || D == 'match'"));
        assertEquals(Set.of("0", "1", "2", "3"), arithmetic.getMatchingGroups().get("PET"));
    }

    @Test
    void matchesWhenOnlyTheLastOrOperandMatches() {
        setField("A", "0", "other");
        setField("B", "1", "other");
        setField("C", "2", "match");

        assertEquals(Boolean.TRUE, evaluate("A == 'match' || B == 'match' || C == 'match'"));
        assertEquals(Set.of("2"), arithmetic.getMatchingGroups().get("PET"));
    }

    @Test
    void rejectsWhenNoOrOperandMatches() {
        setField("A", "0", "other");
        setField("B", "1", "other");
        setField("C", "2", "other");

        assertEquals(Boolean.FALSE, evaluate("A == 'match' || B == 'match' || C == 'match'"));
        assertTrue(arithmetic.getMatchingGroups().isEmpty());
    }

    @Test
    void preservesGroupingAndAndShortCircuiting() {
        setField("A", "0", "match");
        setField("B", "1", "match");
        setField("C", "2", "match");
        setField("D", "3", "other");

        assertEquals(Boolean.FALSE, evaluate("(A == 'match' || B == 'match' || C == 'match') && D == 'match'"));
        assertEquals(Set.of("0", "1", "2"), arithmetic.getMatchingGroups().get("PET"));

        arithmetic.clearMatchingGroups();
        assertEquals(Boolean.FALSE, evaluate("D == 'match' && (A == 'match' || B == 'match' || C == 'match')"));
        assertTrue(arithmetic.getMatchingGroups().isEmpty());
    }

    @Test
    void preservesBooleanResultsForLogicalExpressions() {
        assertEquals(Boolean.TRUE, evaluate("'first' && 'second' && 'third'"));
        assertEquals(Boolean.FALSE, evaluate("'first' && '' && 'third'"));
        assertEquals(Boolean.TRUE, evaluate("'' || 'second' || 'third'"));
        assertFalse((Boolean) evaluate("'' || '' || ''"));
    }

    @Test
    void reusesScriptsWithoutRetainingPreviousDocumentMatches() {
        JexlScript script = helper.createScriptFromString("A == 'match' || B == 'match'");
        setField("A", "0", "match");
        setField("B", "1", "other");
        assertEquals(Boolean.TRUE, script.execute(context));
        assertEquals(Set.of("0"), arithmetic.getMatchingGroups().get("PET"));

        arithmetic.clearMatchingGroups();
        setField("A", "2", "other");
        setField("B", "3", "match");
        assertEquals(Boolean.TRUE, script.execute(context));
        assertEquals(Set.of("3"), arithmetic.getMatchingGroups().get("PET"));

        arithmetic.clearMatchingGroups();
        setField("B", "4", "other");
        assertEquals(Boolean.FALSE, script.execute(context));
        assertTrue(arithmetic.getMatchingGroups().isEmpty());
    }

    private Object evaluate(String query) {
        return helper.createScriptFromString(query).execute(context);
    }

    private void setField(String name, String group, String value) {
        EventFieldValueTuple tuple = new EventFieldValueTuple();
        tuple.setFieldName(name + ".PET." + group);
        tuple.setValue(value);
        context.set(name, List.of(tuple));
    }
}
