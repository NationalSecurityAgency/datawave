package datawave.query.jexl;

import com.google.common.collect.Maps;
import datawave.data.type.DateType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.postprocessing.tf.TermOffsetMap;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.DatawaveJexlScript;
import org.apache.commons.jexl2.ExpressionImpl;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.easymock.EasyMock;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DatawaveInterpreterTest {
    
    @Test
    public void mergeAndNodeFunctionalSetsTest() {
        String query = "((GEO == '0321􏿿+bE4.4' || GEO == '0334􏿿+bE4.4' || GEO == '0320􏿿+bE4.4' || GEO == '0335􏿿+bE4.4') && ((_Delayed_ = true) && ((GEO >= '030a' && GEO <= '0335') && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))))";
        
        DatawaveJexlContext context = new DatawaveJexlContext();
        
        Script script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);
        
        context.set("GEO", "0321􏿿+bE4.4");
        context.set("WKT_BYTE_LENGTH", "+bE4.4");
        
        Object o = script.execute(context);
        assertTrue(matchResult(o));
    }
    
    @Test
    public void largeOrListTest() {
        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < 1000000; i++)
            uuids.add("'" + UUID.randomUUID() + "'");
        
        String query = String.join(" || ", uuids);
        
        DatawaveJexlContext context = new DatawaveJexlContext();
        
        Script script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);
        
        Object o = script.execute(context);
        assertTrue(matchResult(o));
    }
    
    @Test
    public void invocationFails_alwaysThrowsException() {
        JexlEngine engine = mock(JexlEngine.class);
        JexlContext context = mock(JexlContext.class);
        DatawaveInterpreter interpreter = new DatawaveInterpreter(engine, context, false, false);
        JexlException exception = new JexlException(new ASTStringLiteral(1), "Function failure");
        
        // Make mocks available.
        EasyMock.replay(engine, context);
        
        // Capture the expected exception.
        Exception thrown = null;
        try {
            interpreter.invocationFailed(exception);
        } catch (Exception e) {
            thrown = e;
        }
        
        // Verify that an exception is thrown even when strict == false.
        assertEquals(thrown, exception);
    }
    
    @Test
    public void testSimpleEqualities() {
        //  @formatter:off
        Object[][] array = {
                {"FOO == 'bar'", true},
                {"FOO == 'baz'", true},
                {"ZEE == 'bar'", false},
                {"!(FOO == 'bar')", false},
                {"!(FOO == 'baz')", false},
                {"!(ZEE == 'bar')", true}};   //  non-existent field
        //  @formatter:on
        
        test(array);
    }
    
    @Test
    public void testUnions() {
        //  @formatter:off
        Object[][] array = {
                {"FOO == 'bar' || FOO == 'baz'", true},
                {"FOO == 'abc' || FOO == 'xyz'", false},
                {"FOO == 'abc' || !(FOO == 'xyz')", true},
                {"FOO == 'bar' || FOO == 'baz' || FOO == 'barzee' || FOO == 'zeebar'", true},
                {"FOO == 'barzee' || FOO == 'zeebar' || FOO == 'bar' || FOO == 'baz'", true}};
        //  @formatter:on
        
        test(array);
    }
    
    @Test
    public void testIntersections() {
        //  @formatter:off
        Object[][] array = {
                {"FOO == 'bar' && FOO == 'baz'", true},
                {"!(FOO == 'bar') && FOO == 'baz'", false},
                {"FOO == 'bar' && FOO == 'baz' && FOO == 'barzee' && FOO == 'zeebar'", false},
                {"FOO == 'barzee' && FOO == 'zeebar' && FOO == 'bar' && FOO == 'baz'", false}};
        //  @formatter:on
        
        test(array);
    }
    
    @Test
    public void testMixOfAndOrExpressions() {
        //  @formatter:off
        Object[][] array = {
                {"((FOO == 'bar' && FOO == 'baz') || (FOO == 'barzee' && FOO == 'zeebar'))", true},
                {"((FOO == 'bar' && FOO == 'zeebar') || (FOO == 'barzee' && FOO == 'baz'))", false},
                {"((FOO == 'bar' || FOO == 'baz') && (FOO == 'barzee' || FOO == 'zeebar'))", false},
                {"((FOO == 'bar' || FOO == 'zeebar') && (FOO == 'barzee' || FOO == 'baz'))", true}};
        //  @formatter:on
        
        test(array);
    }
    
    @Test
    public void testBoundedRanges() {
        //  @formatter:off
        Object[][] array = {
                {"((_Bounded_ = true) && (SPEED >= '+cE1.2' && SPEED <= '+cE1.5'))", true}, // (120, 150)
                {"((_Bounded_ = true) && (SPEED >= '+bE9' && SPEED <= '+cE1'))", false},    // (90, 130)
                {"((_Bounded_ = true) && (SPEED >= '+cE1.2' && SPEED <= '+cE1.5')) && FOO == 'bar'", true}, // range matches, term matches (120, 150)
                {"((_Bounded_ = true) && (SPEED >= '+cE1.2' && SPEED <= '+cE1.5')) && !(FOO == 'bar')", false},    // range matches, term misses (120, 15)
                {"((_Bounded_ = true) && (SPEED >= '+bE9' && SPEED <= '+cE1')) && FOO == 'bar'", false}, // range misses, term hits (90, 130)
                {"((_Bounded_ = true) && (SPEED >= '+bE9' && SPEED <= '+cE1')) && !(FOO == 'bar')", false},// range misses, term misses (90, 130)
                {"((_Bounded_ = true) && (SPEED >= '+cE1.2' && SPEED <= '+cE1.5')) || FOO == 'bar'", true},// range matches, term matches (120, 150)
                {"((_Bounded_ = true) && (SPEED >= '+cE1.2' && SPEED <= '+cE1.5')) || !(FOO == 'bar')", true},
                {"((_Bounded_ = true) && (SPEED >= '+bE9' && SPEED <= '+cE1')) || FOO == 'bar'", true},
                {"((_Bounded_ = true) && (SPEED >= '+bE9' && SPEED <= '+cE1')) || !(FOO == 'bar')", false}};
        //  @formatter:on
        
        test(array);
    }
    
    @Test
    public void testContentFunctionPhrase() {
        //  @formatter:off
        Object[][] array = {
                {"content:phrase(TEXT, termOffsetMap, 'red', 'dog') && (TEXT == 'red' && TEXT == 'dog')", true},
                {"content:phrase(TEXT, termOffsetMap, 'big', 'dog') && (TEXT == 'big' && TEXT == 'dog')", false},
                //  alternate form phrases
                {"content:phrase(termOffsetMap, 'red', 'dog') && (TEXT == 'red' && TEXT == 'dog')", true},
                {"content:phrase(termOffsetMap, 'big', 'dog') && (TEXT == 'big' && TEXT == 'dog')", false},
                //  full phrase
                {"content:phrase(termOffsetMap, 'big', 'red', 'dog') && (TEXT == 'big' && TEXT == 'red' && TEXT == 'dog')", true}};
        //  @formatter:on
        
        test(array);
    }
    
    @Test
    public void testDateEquality() {
        String query = "BIRTH_DATE == '1910-12-28T00:00:05.000Z'";
        test(query, true);
    }
    
    @Test
    public void testMultipleTimeFunctions() {
        // long value is 80+ years
        String query = "FOO == 'bar' && filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 2522880000000L";
        test(query, true);
        
        query = "BIRTH_DATE.min() < '1920-12-28T00:00:05.000Z'";
        test(query, true);
        
        query = "DEATH_DATE.max() - BIRTH_DATE.min() > 1000 * 60 * 60 * 24"; // one day
        test(query, true);
        
        query = "DEATH_DATE.max() - BIRTH_DATE.min() > 1000 * 60 * 60 * 24 * 5 + 1000 * 60 * 60 * 24 * 7"; // 5 plus 7 days for the calculator-deprived
        test(query, true);
        
        query = "DEATH_DATE.min() < '20160301120000'";
        test(query, true);
    }
    
    /**
     * taken from {@link datawave.query.CompositeFunctionsTest}
     */
    @Test
    public void testFilterFunctionIsNull() {
        //  @formatter:off
        Object[][] array = {
                {"filter:isNull(FOO)", false},
                {"filter:isNull(NULL1)", true},
                {"filter:isNull(NULL1||NULL2)", true},
                {"filter:isNull(NULL1) || filter:isNull(NULL2)", true},
                {"filter:isNull(BOTH_NULL)", true},
                {"filter:isNull(FOO||NULL1)", false},   //  incorrect assertion
                {"filter:isNull(NULL1||FOO)", false},   //  incorrect assertion
                {"filter:isNull(FOO) || filter:isNull(NULL1)", true},
                {"filter:isNull(ONE_NULL)", true},
                {"FOO == 'bar' && filter:isNull(FOO)", false},
                {"FOO == 'bar' && filter:isNull(NULL1)", true},
                {"FOO == 'bar' && filter:isNull(NULL1||NULL2)", true},
                {"FOO == 'bar' && filter:isNull(BOTH_NULL)", true},
                {"FOO == 'bar' && filter:isNull(FOO||NULL1)", false},
                {"FOO == 'bar' && filter:isNull(NULL1||FOO)", false},
                {"FOO == 'bar' && filter:isNull(ONE_NULL)", true},
                //  absent field evaluates to true
                {"FOO == 'bar' && filter:isNull(ABSENT)", true}};
        //  @formatter:on
        
        test(array);
    }
    
    /**
     * taken from {@link datawave.query.CompositeFunctionsTest}
     */
    @Test
    public void testFilterFunctionIsNotNull() {
        //  @formatter:off
        Object[][] array = {
                {"filter:isNotNull(FOO)", true},
                {"filter:isNotNull(NULL1)", false},
                {"filter:isNotNull(NULL1||NULL2)", false},
                {"filter:isNotNull(NULL1) || filter:isNotNull(NULL2)", false},
                {"filter:isNotNull(BOTH_NULL)", false},
                {"filter:isNotNull(FOO||NULL1)", true},
                {"filter:isNotNull(NULL1||FOO)", true},
                {"filter:isNotNull(FOO) || filter:isNotNull(NULL1)", true},
                {"filter:isNotNull(ONE_NULL)", false},
                {"FOO == 'bar' && filter:isNotNull(FOO)", true},
                {"FOO == 'bar' && filter:isNotNull(NULL1)", false},
                {"FOO == 'bar' && filter:isNotNull(NULL1||NULL2)", false},
                {"FOO == 'bar' && filter:isNotNull(BOTH_NULL)", false},
                {"FOO == 'bar' && filter:isNotNull(FOO||NULL1)", true},
                {"FOO == 'bar' && filter:isNotNull(ONE_NULL)", false},
                //  absent field, different kinds of isNotNull
                {"FOO == 'bar' && filter:isNotNull(ABSENT)", false},
                {"FOO == 'bar' && !(filter:isNull(ABSENT))", false}};
        //  @formatter:on
        
        test(array);
    }
    
    /**
     * taken from {@link datawave.query.CompositeFunctionsTest}
     */
    @Test
    public void testFilterFunctionIncludeRegexSize() {
        //  @formatter:off
        Object[][] array = {
                {"filter:includeRegex(FOO, 'bar').size() == 0", false},
                {"filter:includeRegex(FOO, 'bar').size() == 1", true},
                {"filter:includeRegex(FOO, 'bar').size() >= 0", true},
                {"filter:includeRegex(FOO, 'bar').size() >= 1", true},
                {"filter:includeRegex(FOO, 'bar').size() > 0", true},
                {"filter:includeRegex(FOO, 'bar').size() > 1", false},
                {"filter:includeRegex(FOO, 'bar').size() <= 0", false},
                {"filter:includeRegex(FOO, 'bar').size() <= 1", true},
                {"filter:includeRegex(FOO, 'bar').size() < 0", false},
                {"filter:includeRegex(FOO, 'bar').size() < 1", false},
                {"filter:includeRegex(FOO, 'bar').size() < 2", true}};
        //  @formatter:on
        
        test(array);
    }
    
    @Test
    public void testFilterFunctionMatchesAtLeastCountOf() {
        //  @formatter:off
        Object[][] array = {
                {"filter:matchesAtLeastCountOf(2,FOO,'BAR','BAZ')", true},
                {"filter:matchesAtLeastCountOf(2,FOO,'bar','baz')", true},
                {"filter:matchesAtLeastCountOf(1,FOO,'BAR','BAZ')", true},
                {"filter:matchesAtLeastCountOf(5,FOO,'BAR','BAZ')", false},
                {"filter:matchesAtLeastCountOf(2,bar,'FOO','SPEED')", false}};
        //  @formatter:on
        
        test(array);
    }
    
    // (f1.size() + f2.size()) > x is logically equivalent to f:matchesAtLeastCountOf(x,FIELD,'value')
    @Test
    public void testFilterFunctionInsteadOfMatchesAtLeastCountOf() {
        //  @formatter:off
        Object[][] array = {
                {"filter:includeRegex(FOO, 'bar').size() == 1", true},
                {"filter:includeRegex(FOO, 'baz').size() == 1", true},
                {"filter:includeRegex(FOO, 'ba.*').size() == 1", true},
                {"filter:includeRegex(SPEED, '1.*').size() == 1", true},
                {"(filter:includeRegex(FOO, 'bar').size() + filter:includeRegex(FOO, 'baz').size()) == 2", true},
                {"(filter:includeRegex(FOO, 'bar').size() + filter:includeRegex(FOO, 'ba.*').size()) == 2", true},
                {"(filter:includeRegex(SPEED, '1.*').size() + filter:includeRegex(FOO, 'ba.*').size()) == 2", true},
                {"(filter:includeRegex(SPEED, '1.*').size() + filter:includeRegex(FOO, 'ba.*').size() + filter:includeRegex(FOO, 'baz').size()) == 3", true},
                {"(filter:includeRegex(FOO, 'bar').size() - filter:includeRegex(FOO, 'baz').size()) == 0", true},
                {"(filter:includeRegex(FOO, 'bar').size() - filter:includeRegex(FOO, 'ba.*').size()) == 0", true},
                {"(filter:includeRegex(SPEED, '1.*').size() - filter:includeRegex(FOO, 'ba.*').size()) == 0", true}};
        //  @formatter:on
        
        test(array);
    }
    
    @Test
    public void testJexlMinMaxFunctions() {
        //  @formatter:off
        Object[][] array = {
                {"SPEED.max() == 147", true},
                {"SPEED.max() > 147", false},
                {"SPEED.max() >= 147", true},
                {"SPEED.max() < 147", false},
                {"SPEED.max() <= 147", true},
                {"SPEED.min() == 123", true},
                {"SPEED.min() > 123", false},
                {"SPEED.min() >= 123", true},
                {"SPEED.min() < 123", false},
                {"SPEED.min() <= 123", true}};
        //  @formatter:on
        
        test(array);
    }
    
    @Test
    public void testArithmetic() {
        //  @formatter:off
        Object[][] array = {
                {"FOO == 'bar' && 1 + 1 + 1 == 3", true},
                {"FOO == 'bar' && 1 * 2 * 3 == 6", true},
                {"FOO == 'bar' && 12 / 2 / 3 == 2", true},
                {"FOO == 'bar' && 1 + 1 + 1 == 4", false},
                {"FOO == 'bar' && 1 * 2 * 3 == 7", false},
                {"FOO == 'bar' && 12 / 2 / 3 == 3", false},
                {"FOO == 'bar' && filter:getAllMatches(FOO,'hubert').isEmpty() == true", true},
                {"FOO == 'bar' && filter:getAllMatches(FOO,'hubert').size() == 0", true}};
        //  @formatter:on
        
        test(array);
    }
    
    @Ignore
    @Test
    public void testFilterFunctionMultiFieldedIsNull_future() {
        // Once #1604 is complete these tests will evaluate correctly
        
        //  @formatter:off
        Object[][] array = {
                {"FOO == 'bar' && filter:isNull(FOO || FOO)", false},
                {"FOO == 'bar' && filter:isNull(ABSENT || FOO)", true},
                {"FOO == 'bar' && filter:isNull(FOO || ABSENT)", true},
                {"FOO == 'bar' && filter:isNull(ABSENT || ABSENT)", true}};
        //  @formatter:on
        
        test(array);
    }
    
    @Ignore
    @Test
    public void testFilterFunctionsMultiFieldedIsNotNull() {
        // Once #1604 is complete these tests will evaluate correctly
        
        //  @formatter:off
        Object[][] array = {
                // multi field, all present
                {"FOO == 'bar' && filter:isNotNull(FOO || FOO)", true},
                {"FOO == 'bar' && !(filter:isNull(FOO || FOO))", true},
                // multi field, (present || absent)
                {"FOO == 'bar' && filter:isNotNull(FOO || ABSENT)", true},
                {"FOO == 'bar' && !(filter:isNull(FOO || ABSENT))", true},
                {"FOO == 'bar' && ( !(filter:isNull(FOO)) || !(filter:isNull(ABSENT)) )", true},
                // this is wrong. isNotNull expands into an AND so both values must be null.
                {"FOO == 'bar' && filter:isNotNull(ABSENT || FOO)", false},
                // this is wrong. isNotNull expands into an AND so both values must be null.
                {"FOO == 'bar' && !(filter:isNull(ABSENT || FOO))", false},
                {"FOO == 'bar' && filter:isNotNull(ABSENT || ABSENT)", false},
                {"FOO == 'bar' && !(filter:isNull(ABSENT || ABSENT))", false}};
        //  @formatter:on
        
        test(array);
    }
    
    @Test
    public void testGroupingFunctions() {
        //  @formatter:off
        Object[][] array = {
                //  two groups for field MALE...this is a bug
                {"grouping:getGroupsForMatchesInGroup(GENDER, 'MALE').size() == 2", true},
                //  only one group matches AGE == 21
                {"grouping:getGroupsForMatchesInGroup(GENDER, 'MALE', AGE, '21').size() == 1", true},
                //  for groups that match GENDER == 'male', there is a value for AGE less than 19
                {"AGE.getValuesForGroups(grouping:getGroupsForMatchesInGroup(GENDER, 'MALE')) < 19", true},
                //  for group that matches GENDER = 'male' and AGE == '16', there is an AGE less than 19
                {"AGE.getValuesForGroups(grouping:getGroupsForMatchesInGroup(GENDER, 'MALE', AGE, '16')) < 19", true}
        };
        //  @formatter:on
        
        for (Object[] o : array) {
            test(buildContextWithGrouping(), (String) o[0], (Boolean) o[1]);
        }
    }
    
    /**
     * Wrapper that accepts an array of [String, Boolean] pairs that are the query and expected evaluation state, respectively
     *
     * @param inputs
     *            the inputs
     */
    protected void test(Object[][] inputs) {
        for (Object[] o : inputs) {
            test((String) o[0], (Boolean) o[1]);
        }
    }
    
    /**
     * Evaluate a query against a default context
     *
     * @param query
     * @param expectedResult
     */
    protected void test(String query, boolean expectedResult) {
        test(buildDefaultContext(), query, expectedResult, false);
    }
    
    protected void test(JexlContext context, String query, boolean expectedResult) {
        test(context, query, expectedResult, false);
    }
    
    /**
     * Evaluates a query against a jexl context. Tests with both a binary tree and a flattened tree.
     *
     * @param query
     *            a jexl query
     * @param context
     *            a context representing an event
     * @param expectedResult
     *            the expected evaluation of the query given the context
     */
    protected void test(JexlContext context, String query, boolean expectedResult, boolean expectedCallbackState) {
        
        // create binary tree and execute the query
        Script script = getJexlEngine().createScript(query);
        Object executed = script.execute(context);
        boolean isMatched = matchResult(executed);
        assertEquals("Unexpected result for query (binary tree): " + query, expectedResult, isMatched);
        
        // create flattened tree and execute the query
        DatawaveJexlScript dwScript = DatawaveJexlScript.create((ExpressionImpl) script);
        executed = dwScript.execute(context);
        isMatched = matchResult(executed);
        assertEquals("Unexpected result for query (flattened tree): " + query, expectedResult, isMatched);
        assertEquals("Expected callback state of [" + expectedCallbackState + "] but was [" + dwScript.getCallback() + "]", expectedCallbackState,
                        dwScript.getCallback());
    }
    
    protected JexlEngine getJexlEngine() {
        return ArithmeticJexlEngines.getEngine(new DefaultArithmetic());
    }
    
    protected boolean matchResult(Object o) {
        return ArithmeticJexlEngines.isMatched(o);
    }
    
    protected Key docKey = new Key("dt\0uid");
    
    /**
     * Build a jexl context with default values for several different type attributes
     * 
     * <pre>
     *     FOO = {bar, baz}
     *     SPEED = {123, 147}
     *     BIRTH_DATE = {1910, 1930, 1950}
     *     DEATH_DATE = {2000}
     *     TEXT = {big, red, dog}
     *     termOffsetMap - phrase offsets
     * </pre>
     *
     * @return a context
     */
    protected JexlContext buildDefaultContext() {
        //  @formatter:off
        DatawaveJexlContext context = new DatawaveJexlContext();

        //  standard field value pair
        context.set("FOO", new FunctionalSet(Arrays.asList(
                new ValueTuple("FOO", "bar", "bar", new TypeAttribute<>(new LcNoDiacriticsType("bar"), docKey, true)),
                new ValueTuple("FOO", "baz", "baz", new TypeAttribute<>(new LcNoDiacriticsType("baz"), docKey, true)))));

        //  numerics for bounded range queries
        context.set("SPEED", new FunctionalSet(Arrays.asList(
                new ValueTuple("SPEED", "123", "+cE1.23", new TypeAttribute<>(new NumberType("123"), docKey, true)),
                new ValueTuple("SPEED", "147", "+cE1.47", new TypeAttribute<>(new NumberType("147"), docKey, true)))));

        //  dates
        DateType firstDate = new DateType("1910-12-28T00:00:05.000Z");
        DateType secondDate = new DateType("1930-12-28T00:00:05.000Z");
        DateType thirdDate = new DateType("1950-12-28T00:00:05.000Z");

        ValueTuple birthDate01 = new ValueTuple("BIRTH_DATE", firstDate, "1910-12-28T00:00:05.000Z", new TypeAttribute<>(firstDate, docKey, true));
        ValueTuple birthDate02 = new ValueTuple("BIRTH_DATE", secondDate, "1930-12-28T00:00:05.000Z", new TypeAttribute<>(secondDate, docKey, true));
        ValueTuple birthDate03 = new ValueTuple("BIRTH_DATE", thirdDate, "1950-12-28T00:00:05.000Z", new TypeAttribute<>(thirdDate, docKey, true));
        context.set("BIRTH_DATE", new FunctionalSet(Arrays.asList(birthDate01, birthDate02, birthDate03)));

        DateType fourthDate = new DateType("2000-12-28T00:00:05.000Z");
        ValueTuple deathDate01 = new ValueTuple("DEATH_DATE", fourthDate, "2000-12-28T00:00:05.000Z", new TypeAttribute<>(fourthDate, docKey, true));
        context.set("DEATH_DATE", new FunctionalSet(Arrays.asList(deathDate01)));

        //  term offsets for phrases
        TermOffsetMap map = new TermOffsetMap();
        map.putTermFrequencyList("big", buildTfList("TEXT", 1));
        map.putTermFrequencyList("red", buildTfList("TEXT", 2));
        map.putTermFrequencyList("dog", buildTfList("TEXT", 3));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, map);
        context.set("TEXT", new FunctionalSet(Arrays.asList(
                new ValueTuple("TEXT", "big", "big",  new TypeAttribute<>(new LcNoDiacriticsType("big"), docKey, true)),
                new ValueTuple("TEXT", "red", "red",  new TypeAttribute<>(new LcNoDiacriticsType("red"), docKey, true)),
                new ValueTuple("TEXT", "dog", "dog",  new TypeAttribute<>(new LcNoDiacriticsType("dog"), docKey, true)))));
        //  @formatter:on
        return context;
    }
    
    /**
     * Builds a default context and adds some fields with grouping
     *
     * @return a context with grouping
     */
    protected JexlContext buildContextWithGrouping() {
        JexlContext context = buildDefaultContext();
        
        // {16, male}, {18, female}, {21, male}
        
        context.set("AGE",
                        new FunctionalSet(Arrays.asList(new ValueTuple("AGE.0", "16", "+bE1.6", new TypeAttribute<>(new NumberType("16"), docKey, true)),
                                        new ValueTuple("AGE.1", "18", "+bE1.8", new TypeAttribute<>(new NumberType("18"), docKey, true)), new ValueTuple(
                                                        "AGE.2", "21", "+bE2.1", new TypeAttribute<>(new NumberType("18"), docKey, true)))));
        
        context.set("GENDER",
                        new FunctionalSet(Arrays.asList(new ValueTuple("GENDER.0", "MALE", "male", new TypeAttribute<>(new LcNoDiacriticsType("MALE"), docKey,
                                        true)), new ValueTuple("GENDER.1", "FEMALE", "female", new TypeAttribute<>(new LcNoDiacriticsType("FEMALE"), docKey,
                                        true)), new ValueTuple("GENDER.2", "MALE", "male", new TypeAttribute<>(new LcNoDiacriticsType("MALE"), docKey, true)))));
        
        return context;
    }
    
    protected TermFrequencyList buildTfList(String field, int... offsets) {
        TermFrequencyList.Zone zone = buildZone(field);
        List<TermWeightPosition> position = buildTermWeightPositions(offsets);
        return new TermFrequencyList(Maps.immutableEntry(zone, position));
    }
    
    protected TermFrequencyList.Zone buildZone(String field) {
        return new TermFrequencyList.Zone(field, true, "shard\0datatype\0uid");
    }
    
    protected List<TermWeightPosition> buildTermWeightPositions(int... offsets) {
        List<TermWeightPosition> list = new ArrayList<>();
        for (int offset : offsets) {
            list.add(new TermWeightPosition.Builder().setOffset(offset).setZeroOffsetMatch(true).build());
        }
        return list;
    }
}
