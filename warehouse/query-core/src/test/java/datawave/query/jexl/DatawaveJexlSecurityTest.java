package datawave.query.jexl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.attributes.Content;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;
import datawave.query.jexl.functions.GeoFunctions;
import datawave.query.jexl.functions.GeoWaveFunctions;
import datawave.query.jexl.functions.NormalizationFunctions;
import datawave.query.jexl.functions.QueryFunctions;

class DatawaveJexlSecurityTest {
    private static Stream<DatawaveJexlEngine> engines() {
        return Stream.of(ArithmeticJexlEngines.getEngine(new DefaultArithmetic()), ArithmeticJexlEngines.getEngine(new HitListArithmetic()),
                        new DatawaveJexlEngine(), new DatawaveJexlEngine(new JexlBuilder().namespaces(functions()).arithmetic(new DefaultArithmetic())
                                        .strict(false).silent(false).permissions(JexlPermissions.UNRESTRICTED)));
    }

    private static Map<String,Object> functions() {
        Map<String,Object> functions = new HashMap<>(ArithmeticJexlEngines.functions());
        functions.put("f", QueryFunctions.class);
        functions.put("geo", GeoFunctions.class);
        functions.put("geowave", GeoWaveFunctions.class);
        functions.put("normalize", NormalizationFunctions.class);
        return functions;
    }

    private static DatawaveJexlEngine engine() {
        return new DatawaveJexlEngine(new JexlBuilder().namespaces(functions()).arithmetic(new DefaultArithmetic()).strict(false).silent(false));
    }

    private static MapContext context() {
        FunctionalSet<ValueTuple> values = new FunctionalSet<>();
        values.add(new ValueTuple("FIELD.0", "abc", "abc", new Content("abc", new Key("row"), true)));
        values.add(new ValueTuple("FIELD.1", "xyz", "xyz", new Content("xyz", new Key("row"), true)));
        MapContext context = new MapContext();
        context.set("FIELD", values);
        context.set("OTHER", FunctionalSet.singleton(new ValueTuple("OTHER.0", "abc", "abc", null)));
        return context;
    }

    @ParameterizedTest
    @MethodSource("engines")
    void everyConstructorRestrictsIntrospection(DatawaveJexlEngine engine) {
        assertNull(engine.getUberspect().getMethod("abc", "getClass"));
        assertNull(engine.getUberspect().getMethod(System.class, "getProperty", "java.version"));
        assertNull(engine.getUberspect().getMethod(Runtime.class, "getRuntime"));
        assertNull(engine.getUberspect().getMethod(String.class, "forName", "java.lang.String"));
        assertNull(engine.getUberspect().getConstructor(String.class, "abc"));
        assertNull(engine.getUberspect().getPropertyGet(String.class, "classLoader"));
        assertNull(engine.getUberspect().getPropertyGet(EvaluationPhaseFilterFunctions.class, "EVAL_PHASE_FUNCTION_NAMESPACE"));
        assertNotNull(engine.getUberspect().getMethod(EvaluationPhaseFilterFunctions.class, "includeRegex", "abc", "a.*"));
        assertNull(engine.getUberspect().getMethod(EvaluationPhaseFilterFunctions.class, "getTime", "20200101"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"FIELD.getClass()", "FIELD.'getClass'()", "FIELD.getClass().forName('java.lang.String')",
            "filter:includeRegex(FIELD.getClass(), '.*')", "!filter:getTime('20200101')", "filter:getTime('20200101')",
            "FIELD.min().getSource()", "FIELD.min().source", "FIELD.min()['source']", "FIELD.class", "FIELD['class']", "FIELD.clear()", "FIELD.add('abc')", "FIELD.iterator().remove()",
            "missing:function(FIELD)", "getClass()", "FIELD.`get${'Class'}`()", "FIELD['getClass']()"})
    void forbiddenCallsFailBeforeExecution(String query) {
        DatawaveJexlEngine engine = engine();
        assertThrows(JexlException.class, () -> engine.createScript(query).execute(context()));
        assertThrows(JexlException.class, () -> engine.createExpression(query).evaluate(context()));
        assertThrows(JexlException.class, () -> engine.createComplexScript(query).execute(context()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"new('java.lang.String', 'abc')", "#pragma jexl.namespace.system java.lang.System\nsystem:getProperty('java.version')",
            "#pragma jexl.import java.lang\nnew('String', 'abc')"})
    void complexScriptsCannotReenableConstructorsOrNamespacePragmas(String query) {
        assertThrows(JexlException.class, () -> engine().createComplexScript(query));
    }

    @Test
    void unapprovedNamespacesAndCustomUberspectsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new DatawaveJexlEngine(new JexlBuilder().namespaces(Map.of("system", System.class))));
        assertThrows(IllegalArgumentException.class,
                        () -> new DatawaveJexlEngine(new JexlBuilder().uberspect(new JexlBuilder().permissions(JexlPermissions.UNRESTRICTED).create().getUberspect())));
    }

    @Test
    void collectionMethodsAndChainedTupleReadsWork() {
        DatawaveJexlEngine engine = engine();
        MapContext context = context();
        assertEquals(2, engine.createScript("FIELD.size()").execute(context));
        assertFalse((Boolean) engine.createScript("FIELD.isEmpty()").execute(context));
        assertTrue((Boolean) engine.createScript("FIELD.contains('FIELD.0:abc')").execute(context));
        assertEquals(1, engine.createScript("filter:includeRegex(FIELD, 'a.*').size()").execute(context));
        assertEquals("FIELD.0", engine.createScript("FIELD.getValueForGroup('0').getFieldName()").execute(context));
        assertEquals("abc", engine.createScript("FIELD.min().getNormalizedValue()").execute(context));
        assertEquals(0, engine.createScript("ABSENT.size()").execute(context));
        assertTrue((Boolean) engine.createScript("ABSENT.isEmpty()").execute(context));
        assertEquals(0, engine.createScript("filter:includeRegex(ABSENT, '.*').size()").execute(context));
        context.set("LIST", List.of("a", "b"));
        assertEquals("b", engine.createScript("LIST.get(1)").execute(context));
        assertTrue((Boolean) engine.createScript("FIELD.getValuesForGroups('0').containsAll(FIELD.getValuesForGroups('0'))").execute(context));
    }

    @ParameterizedTest
    @ValueSource(strings = {"filter:isNotNull(FIELD)", "filter:occurrence(FIELD, '==', 2)", "filter:compare(FIELD, '==', 'ANY', FIELD)",
            "f:matchRegex(FIELD, 'a.*')", "f:includeText(FIELD, 'abc')", "f:between(FIELD, 'a', 'z')", "f:length(FIELD, 1, 4)",
            "grouping:matchesInGroup(FIELD, 'abc', OTHER, 'abc')"})
    void queryFunctionsRetainTheirMatches(String query) {
        assertTrue(ArithmeticJexlEngines.isMatched(engine().createScript(query).execute(context())));
    }

    @Test
    void normalizationAndGeoFunctionsRemainAvailable() {
        DatawaveJexlEngine engine = engine();
        MapContext context = context();
        assertEquals("001.002.003.004", engine.createScript("normalize:ipv4('1.2.3.4')").execute(context));
        assertTrue((Boolean) engine.createScript("geo:within_bounding_box('1_1', '0_0', '2_2')").execute(context));
        assertTrue((Boolean) engine.createScript("geowave:intersects('POINT (1 1)', 'POINT (1 1)')").execute(context));
    }

    @Test
    void returnedObjectsCannotExposeSourcesOrMutateValues() {
        DatawaveJexlEngine engine = engine();
        FunctionalSet<ValueTuple> values = (FunctionalSet<ValueTuple>) context().get("FIELD");
        ValueTuple tuple = values.iterator().next();
        assertNull(engine.getUberspect().getMethod(tuple, "getSource"));
        assertNull(engine.getUberspect().getPropertyGet(tuple, "source"));
        assertNull(engine.getUberspect().getMethod(values, "clear"));
        assertNull(engine.getUberspect().getMethod(values, "add", tuple));
        assertNull(engine.getUberspect().getMethod(values.iterator(), "remove"));
        assertEquals(2, values.size());
    }
}
