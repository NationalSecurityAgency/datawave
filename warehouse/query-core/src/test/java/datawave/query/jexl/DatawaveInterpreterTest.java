package datawave.query.jexl;

import com.google.common.collect.Maps;
import datawave.data.type.LcNoDiacriticsType;
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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            uuids.add("'" + UUID.randomUUID().toString() + "'");
        
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
    public void testSimpleQuery() {
        String query = "FOO == 'bar'";
        test(query, true);
        
        query = "FOO == 'baz'";
        test(query, true);
        
        query = "FOO != 'bar'";
        test(query, false);
        
        query = "FOO != 'baz'";
        test(query, false);
    }
    
    @Test
    public void testNonExistentField() {
        String query = "ZEE != 'bar'";
        test(query, true);
    }
    
    @Test
    public void testOrWithTwoTerms() {
        String query = "FOO == 'bar' || FOO == 'baz'";
        test(query, true);
        
        query = "FOO == 'abc' || FOO == 'xyz'";
        test(query, false);
    }
    
    @Test
    public void testOrWithFourTerms() {
        String query = "FOO == 'bar' || FOO == 'baz' || FOO == 'barzee' || FOO == 'zeebar'";
        test(query, true);
        
        query = "FOO == 'barzee' || FOO == 'zeebar' || FOO == 'bar' || FOO == 'baz'";
        test(query, true);
    }
    
    @Test
    public void testAndWithTwoTerms() {
        String query = "FOO == 'bar' && FOO == 'baz'";
        test(query, true);
        
        query = "FOO != 'bar' && FOO == 'baz'";
        test(query, false);
    }
    
    @Test
    public void testAndWithFourTerms() {
        String query = "FOO == 'bar' && FOO == 'baz' && FOO == 'barzee' && FOO == 'zeebar'";
        test(query, false);
        
        query = "FOO == 'barzee' && FOO == 'zeebar' && FOO == 'bar' && FOO == 'baz'";
        test(query, false);
    }
    
    @Test
    public void testMixOfAndOrExpressions() {
        String query = "((FOO == 'bar' && FOO == 'baz') || (FOO == 'barzee' && FOO == 'zeebar'))";
        test(query, true);
        
        query = "((FOO == 'bar' && FOO == 'zeebar') || (FOO == 'barzee' && FOO == 'baz'))";
        test(query, false);
        
        query = "((FOO == 'bar' || FOO == 'baz') && (FOO == 'barzee' || FOO == 'zeebar'))";
        test(query, false);
        
        query = "((FOO == 'bar' || FOO == 'zeebar') && (FOO == 'barzee' || FOO == 'baz'))";
        test(query, true);
    }
    
    @Test
    public void testBoundedRange() {
        String query = "((_Bounded_ = true) && (SPEED >= '120' && SPEED <= '150'))";
        test(query, buildBoundedRangeContext(), true);
        
        query = "((_Bounded_ = true) && (SPEED >= '90' && SPEED <= '130'))";
        test(query, buildBoundedRangeContext(), false);
    }
    
    @Test
    public void testBoundedRangeAndTerm() {
        // range matches, term matches
        String query = "((_Bounded_ = true) && (SPEED >= '120' && SPEED <= '150')) && FOO == 'bar'";
        test(query, buildBoundedRangeContext(), true);
        
        // range matches, term misses
        query = "((_Bounded_ = true) && (SPEED >= '120' && SPEED <= '150')) && FOO != 'bar'";
        test(query, buildBoundedRangeContext(), false);
        
        // range misses, term hits
        query = "((_Bounded_ = true) && (SPEED >= '90' && SPEED <= '130')) && FOO == 'bar'";
        test(query, buildBoundedRangeContext(), false);
        
        // range misses, term misses
        query = "((_Bounded_ = true) && (SPEED >= '90' && SPEED <= '130')) && FOO != 'bar'";
        test(query, buildBoundedRangeContext(), false);
    }
    
    @Test
    public void testBoundedRangeOrTerm() {
        // range matches, term matches
        String query = "((_Bounded_ = true) && (SPEED >= '120' && SPEED <= '150')) || FOO == 'bar'";
        test(query, buildBoundedRangeContext(), true);
        
        // range matches, term misses
        query = "((_Bounded_ = true) && (SPEED >= '120' && SPEED <= '150')) || FOO != 'bar'";
        test(query, buildBoundedRangeContext(), true);
        
        // range misses, term hits
        query = "((_Bounded_ = true) && (SPEED >= '90' && SPEED <= '130')) || FOO == 'bar'";
        test(query, buildBoundedRangeContext(), true);
        
        // range misses, term misses
        query = "((_Bounded_ = true) && (SPEED >= '90' && SPEED <= '130')) || FOO != 'bar'";
        test(query, buildBoundedRangeContext(), false);
    }
    
    @Test
    public void testPhraseBasicForm() {
        // this phrase hits
        String query = "content:phrase(TEXT, termOffsetMap, 'red', 'dog') && (TEXT == 'red' && TEXT == 'dog')";
        test(query, buildTermOffsetContext(), true);
        
        // this phrase doesn't
        query = "content:phrase(TEXT, termOffsetMap, 'big', 'dog') && (TEXT == 'big' && TEXT == 'dog')";
        test(query, buildTermOffsetContext(), false);
    }
    
    @Test
    public void testPhraseAlternateForm() {
        // this phrase hits
        String query = "content:phrase(termOffsetMap, 'red', 'dog') && (TEXT == 'red' && TEXT == 'dog')";
        test(query, buildTermOffsetContext(), true);
        
        // this phrase doesn't
        query = "content:phrase(termOffsetMap, 'big', 'dog') && (TEXT == 'big' && TEXT == 'dog')";
        test(query, buildTermOffsetContext(), false);
    }
    
    @Test
    public void testMultipleTimeFunctions() {
        // long value is 80+ years
        String query = "FOO == 'bar' && filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 2522880000000L";
        test(query, buildDateContext(), true);
    }
    
    /**
     * Evaluate a query against a default context
     *
     * @param query
     * @param expectedResult
     */
    protected void test(String query, boolean expectedResult) {
        test(query, buildDefaultContext(), expectedResult);
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
    protected void test(String query, JexlContext context, boolean expectedResult) {
        
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
    }
    
    protected JexlEngine getJexlEngine() {
        return ArithmeticJexlEngines.getEngine(new DefaultArithmetic());
    }
    
    protected boolean matchResult(Object o) {
        return ArithmeticJexlEngines.isMatched(o);
    }
    
    /**
     * Build a jexl context with default values
     *
     * @return a context
     */
    protected JexlContext buildDefaultContext() {
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set("FOO", new FunctionalSet(Arrays.asList("bar", "baz")));
        return context;
    }
    
    /**
     * Build a jexl context with values that will match bounded ranges
     *
     * @return a context
     */
    protected JexlContext buildBoundedRangeContext() {
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set("SPEED", new FunctionalSet(Arrays.asList("123", "147")));
        context.set("FOO", new FunctionalSet(Arrays.asList("bar", "baz")));
        return context;
    }
    
    /**
     * Build a JexlContext with date values to test time functions
     *
     * @return a context
     */
    protected JexlContext buildDateContext() {
        JexlContext context = buildDefaultContext();
        context.set("BIRTH_DATE", new FunctionalSet(Arrays.asList("1910-12-28T00:00:05.000Z", "1930-12-28T00:00:05.000Z", "1950-12-28T00:00:05.000Z")));
        context.set("DEATH_DATE", new FunctionalSet(Arrays.asList("2000-12-28T00:00:05.000Z")));
        return context;
    }
    
    protected JexlContext buildTermOffsetContext() {
        JexlContext context = buildDefaultContext();
        
        TermOffsetMap map = new TermOffsetMap();
        map.putTermFrequencyList("big", buildTfList("TEXT", 1));
        map.putTermFrequencyList("red", buildTfList("TEXT", 2));
        map.putTermFrequencyList("dog", buildTfList("TEXT", 3));
        
        //@formatter:off
        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, map);
        context.set("TEXT", new FunctionalSet(Arrays.asList(
                new ValueTuple("TEXT", "big", "big",  new TypeAttribute<>(new LcNoDiacriticsType("big"), new Key("dt\0uid"), true)),
                new ValueTuple("TEXT", "red", "red",  new TypeAttribute<>(new LcNoDiacriticsType("red"), new Key("dt\0uid"), true)),
                new ValueTuple("TEXT", "dog", "dog",  new TypeAttribute<>(new LcNoDiacriticsType("dog"), new Key("dt\0uid"), true)))));
        //@formatter:on
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
