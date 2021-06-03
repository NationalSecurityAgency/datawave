package datawave.query.jexl.visitors;

import datawave.query.QueryParameters;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryOptionsFromQueryVisitorTest {
    
    private final Map<String,String> optionsMap = new HashMap<>();
    
    @After
    public void tearDown() throws Exception {
        optionsMap.clear();
    }
    
    /**
     * Verify that a unique function with no attributes results in an empty unique parameter.
     */
    @Test
    public void testEmptyUniqueFunction() throws ParseException {
        findQueryOptions("f:unique()");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
    }
    
    /**
     * Verify that a unique function with fields not contained within a sub function are added with the default ORIGINAL transformer.
     */
    @Test
    public void testUniqueFunctionWithNoSubFunctions() throws ParseException {
        findQueryOptions("f:unique(field1,field2,field3)");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1:[ORIGINAL],field2:[ORIGINAL],field3:[ORIGINAL]");
    }
    
    /**
     * Verify that a unique function with fields contained within an f:original sub function are added with the ORIGINAL transformer.
     */
    @Test
    public void testUniqueFunctionWithOriginalSubFunction() throws ParseException {
        findQueryOptions("f:unique(f:original(field1,field2,field3))");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1:[ORIGINAL],field2:[ORIGINAL],field3:[ORIGINAL]");
    }
    
    /**
     * Verify that a unique function with fields contained within an f:day sub function are added with the DAY transformer.
     */
    @Test
    public void testUniqueFunctionWithDaySubFunction() throws ParseException {
        findQueryOptions("f:unique(f:day(field1,field2,field3))");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1:[DAY],field2:[DAY],field3:[DAY]");
    }
    
    /**
     * Verify that a unique function with fields contained within an f:hour sub function are added with the HOUR transformer.
     */
    @Test
    public void testUniqueFunctionWithHourSubFunction() throws ParseException {
        findQueryOptions("f:unique(f:hour(field1,field2,field3))");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1:[HOUR],field2:[HOUR],field3:[HOUR]");
    }
    
    /**
     * Verify that a unique function with fields contained within an f:minute sub function are added with the MINUTE transformer.
     */
    @Test
    public void testUniqueFunctionWithMinuteSubFunction() throws ParseException {
        findQueryOptions("f:unique(f:minute(field1,field2,field3))");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1:[MINUTE],field2:[MINUTE],field3:[MINUTE]");
    }
    
    /**
     * Verify that multiple unique functions separated by conjunctions are merged into the same unique parameter value.
     */
    @Test
    public void testMultipleUniqueFunctions() throws ParseException {
        findQueryOptions("f:unique(field1,field2) AND f:unique(f:day(field2,field3)) AND f:unique(f:original(field4))");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1:[ORIGINAL],field2:[ORIGINAL,DAY],field3:[DAY],field4:[ORIGINAL]");
    }
    
    /**
     * Verify that a unique function with mixed sub-functions, and fields at the start with no sub functions is parsed correctly.
     */
    @Test
    public void testMultipleUniqueSubfunctionsWithDefaultOriginalAtStart() throws ParseException {
        findQueryOptions("f:unique(field4,field5,f:day(field1,field2),f:hour(field2,field3),f:minute(field3,field4))");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1:[DAY],field2:[DAY,HOUR],field3:[HOUR,MINUTE],field4:[ORIGINAL,MINUTE],field5:[ORIGINAL]");
    }
    
    /**
     * Verify that a unique function with mixed sub-functions, and fields in the middle with no sub functions is parsed correctly.
     */
    @Test
    public void testMultipleUniqueSubfunctionsWithDefaultOriginalInMiddle() throws ParseException {
        findQueryOptions("f:unique(f:day(field1,field2),field4,f:hour(field2,field3),field5,f:minute(field3,field4))");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1:[DAY],field2:[DAY,HOUR],field3:[HOUR,MINUTE],field4:[ORIGINAL,MINUTE],field5:[ORIGINAL]");
    }
    
    /**
     * Verify that a unique function with mixed sub-functions, and fields at the end with no sub functions is parsed correctly.
     */
    @Test
    public void testMultipleUniqueSubfunctionsWithDefaultOriginalAtEnd() throws ParseException {
        findQueryOptions("f:unique(f:day(field1,field2),f:hour(field2,field3),f:minute(field3,field4),f:original(field6),field4,field5)");
        assertOption(QueryParameters.UNIQUE_FIELDS,
                        "field1:[DAY],field2:[DAY,HOUR],field3:[HOUR,MINUTE],field4:[ORIGINAL,MINUTE],field5:[ORIGINAL],field6:[ORIGINAL]");
    }
    
    /**
     * Verify that if any of the f:unique sub functions are declared at the top level, no parameters are added to the options map.
     */
    @Test
    public void testSubFunctionsAtTopLevelAreNotAdded() throws ParseException {
        findQueryOptions("f:original(field1) AND f:day(field2) AND f:hour(field3) AND f:minute(field4)");
        assertTrue(optionsMap.isEmpty());
    }
    
    private void findQueryOptions(String query) throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        QueryOptionsFromQueryVisitor.collect(node, optionsMap);
    }
    
    private void assertOption(String option, String value) {
        assertEquals(value, optionsMap.get(option));
    }
}
