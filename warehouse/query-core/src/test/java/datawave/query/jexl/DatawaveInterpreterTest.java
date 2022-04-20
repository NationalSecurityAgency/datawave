package datawave.query.jexl;

import datawave.query.collections.FunctionalSet;
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
        
        assertTrue(DatawaveInterpreter.isMatched(script.execute(context)));
    }
    
    @Test
    public void largeOrListTest() {
        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < 1000000; i++)
            uuids.add("'" + UUID.randomUUID().toString() + "'");
        
        String query = String.join(" || ", uuids);
        
        DatawaveJexlContext context = new DatawaveJexlContext();
        
        Script script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);
        
        assertTrue(DatawaveInterpreter.isMatched(script.execute(context)));
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
    
    // test filter:isNull
    @Test
    public void testFilterFunctionIsNull() {
        // single field, present
        String query = "FOO == 'bar' && filter:isNull(FOO)";
        test(query, buildDefaultContext(), false);
        
        // single field, absent
        query = "FOO == 'bar' && filter:isNull(ABSENT)";
        test(query, buildDefaultContext(), true);
        
        // multi field, all present
        query = "FOO == 'bar' && filter:isNull(FOO || FOO)";
        test(query, buildDefaultContext(), false);
        
        // multi field, (present || absent)
        query = "FOO == 'bar' && filter:isNull(FOO || ABSENT)";
        // test(query, buildDefaultContext(), true); // this is wrong
        
        // multi field, (absent || present)
        query = "FOO == 'bar' && filter:isNull(ABSENT || FOO)";
        // test(query, buildDefaultContext(), true); // this is wrong
        
        // multi field, all absent
        query = "FOO == 'bar' && filter:isNull(ABSENT || ABSENT)";
        test(query, buildDefaultContext(), true);
    }
    
    // test filter:isNotNull and not(filter:isNull)
    @Test
    public void testFilterFunctionIsNotNull() {
        // single field, present
        String query = "FOO == 'bar' && filter:isNotNull(FOO)";
        test(query, buildDefaultContext(), true);
        
        query = "FOO == 'bar' && !(filter:isNull(FOO))";
        test(query, buildDefaultContext(), true);
        
        // single field, absent
        query = "FOO == 'bar' && filter:isNotNull(ABSENT)";
        test(query, buildDefaultContext(), false);
        
        query = "FOO == 'bar' && !(filter:isNull(ABSENT))";
        test(query, buildDefaultContext(), false);
        
        // multi field, all present
        query = "FOO == 'bar' && filter:isNotNull(FOO || FOO)";
        test(query, buildDefaultContext(), true);
        
        query = "FOO == 'bar' && !(filter:isNull(FOO || FOO))";
        test(query, buildDefaultContext(), true);
        
        // multi field, (present || absent)
        query = "FOO == 'bar' && filter:isNotNull(FOO || ABSENT)";
        test(query, buildDefaultContext(), true);
        
        query = "FOO == 'bar' && !(filter:isNull(FOO || ABSENT))";
        test(query, buildDefaultContext(), true);
        
        query = "FOO == 'bar' && ( !(filter:isNull(FOO)) || !(filter:isNull(ABSENT)) )";
        test(query, buildDefaultContext(), true);
        
        // multi field, (absent || present)
        query = "FOO == 'bar' && filter:isNotNull(ABSENT || FOO)";
        test(query, buildDefaultContext(), true); // this is wrong
        
        query = "FOO == 'bar' && !(filter:isNull(ABSENT || FOO))";
        test(query, buildDefaultContext(), true); // this is wrong
        
        // multi field, all absent
        query = "FOO == 'bar' && filter:isNotNull(ABSENT || ABSENT)";
        test(query, buildDefaultContext(), false);
        
        query = "FOO == 'bar' && !(filter:isNull(ABSENT || ABSENT))";
        test(query, buildDefaultContext(), false);
    }
    
    /**
     * Evaluate a query against a default context
     * 
     * @param query
     * @param expectedResult
     */
    private void test(String query, boolean expectedResult) {
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
    private void test(String query, JexlContext context, boolean expectedResult) {
        
        // create binary tree and execute the query
        Script script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);
        Object executed = script.execute(context);
        boolean isMatched = ArithmeticJexlEngines.isMatched(executed);
        assertEquals("Unexpected result for query (binary tree): " + query, expectedResult, isMatched);
        
        // create flattened tree and execute the query
        DatawaveJexlScript dwScript = DatawaveJexlScript.create((ExpressionImpl) script);
        executed = dwScript.execute(context);
        isMatched = ArithmeticJexlEngines.isMatched(executed);
        assertEquals("Unexpected result for query (flattened tree): " + query, expectedResult, isMatched);
    }
    
    /**
     * Build a jexl context with default values
     * 
     * @return a context
     */
    private JexlContext buildDefaultContext() {
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set("FOO", new FunctionalSet(Arrays.asList("bar", "baz")));
        return context;
    }
    
    /**
     * Build a jexl context with values that will match bounded ranges
     * 
     * @return a context
     */
    private JexlContext buildBoundedRangeContext() {
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set("SPEED", new FunctionalSet(Arrays.asList("123", "147")));
        context.set("FOO", new FunctionalSet(Arrays.asList("bar", "baz")));
        return context;
    }
    
}
