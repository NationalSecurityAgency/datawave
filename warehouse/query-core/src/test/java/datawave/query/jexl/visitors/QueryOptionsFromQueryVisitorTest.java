package datawave.query.jexl.visitors;

import datawave.query.QueryParameters;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryOptionsFromQueryVisitorTest {
    
    private static final Logger log = Logger.getLogger(QueryOptionsFromQueryVisitorTest.class);
    
    private final Map<String,String> optionsMap = new HashMap<>();
    
    @Test
    public void testOptionsFunction() throws ParseException {
        // Verify that an empty options function adds no parameters.
        assertResult("f:options()", "");
        assertTrue(optionsMap.isEmpty());
        
        // Verify any specified options are added as separate parameters.
        assertResult("f:options('include.grouping.context','true','hit.list','true','limit.fields','FOO_1_BAR=3,FOO_1=2')", "");
        assertOption("include.grouping.context", "true");
        assertOption("hit.list", "true");
        assertOption("limit.fields", "FOO_1_BAR=3,FOO_1=2");
    }
    
    @Test
    public void testGroupByFunction() throws ParseException {
        // Verify that an empty groupby functions results in an empty parameter value.
        assertResult("f:groupby()", "");
        assertOption(QueryParameters.GROUP_FIELDS, "");
        
        assertResult("f:groupby('field1','field2','field3')", "");
        assertOption(QueryParameters.GROUP_FIELDS, "field1,field2,field3");
    }
    
    @Test
    public void testUniqueFunction() throws ParseException {
        // Verify an empty function results in an empty parameter value.
        assertResult("f:unique_by_day()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
        
        // Verify that fields of no specified granularity are added with the default ALL granularity.
        assertResult("f:unique('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL],field2[ALL],field3[ALL]");
        
        // Verify that fields with DAY granularity are added as such.
        assertResult("f:unique('field1[DAY]','field2[DAY]','field3[DAY]')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[DAY],field2[DAY],field3[DAY]");
        
        // Verify that fields with HOUR granularity are added as such.
        assertResult("f:unique('field1[HOUR]','field2[HOUR]','field3[HOUR]')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[HOUR],field2[HOUR],field3[HOUR]");
        
        // Verify that fields with MINUTE granularity are added as such.
        assertResult("f:unique('field1[MINUTE]','field2[MINUTE]','field3[MINUTE]')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[MINUTE],field2[MINUTE],field3[MINUTE]");
        
        // Verify that fields from multiple unique functions are merged together.
        assertResult("f:unique('field1','field2') AND f:unique('field2[DAY]','field3[DAY]') AND f:unique('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL],field2[ALL,DAY],field3[DAY],field4[ALL]");
        
        // Verify more complex fields with multiple granularity levels are merged together.
        assertResult("f:unique('field1[DAY]','field2[DAY,HOUR]','field3[HOUR,MINUTE]','field4[ALL,MINUTE]','field5')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[DAY],field2[DAY,HOUR],field3[HOUR,MINUTE],field4[ALL,MINUTE],field5[ALL]");
        
        // Lucene will parse comma-delimited granularity levels into separate strings. Ensure it still parses correctly.
        assertResult("f:unique('field1[DAY]','field2[DAY','HOUR]','field3[HOUR','MINUTE]','field4[ALL','MINUTE]','field5')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[DAY],field2[DAY,HOUR],field3[HOUR,MINUTE],field4[ALL,MINUTE],field5[ALL]");
    }
    
    @Test
    public void testUniqueByDay() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_day()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
        
        // Verify fields are added with the DAY granularity.
        assertResult("f:unique_by_day('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[DAY],field2[DAY],field3[DAY]");
        
        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[HOUR]') AND f:unique_by_day('field1','field2','field3') AND f:unique_by_day('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL,DAY],field2[DAY,HOUR],field3[DAY],field4[DAY]");
    }
    
    @Test
    public void testUniqueByHour() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_hour()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
        
        // Verify fields are added with the HOUR granularity.
        assertResult("f:unique_by_hour('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[HOUR],field2[HOUR],field3[HOUR]");
        
        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[DAY]') AND f:unique_by_hour('field1','field2','field3') AND f:unique_by_hour('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL,HOUR],field2[DAY,HOUR],field3[HOUR],field4[HOUR]");
    }
    
    @Test
    public void testUniqueByMinute() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_minute()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
        
        // Verify fields are added with the MINUTE granularity.
        assertResult("f:unique_by_minute('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[MINUTE],field2[MINUTE],field3[MINUTE]");
        
        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[DAY]') AND f:unique_by_minute('field1','field2','field3') AND f:unique_by_minute('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL,MINUTE],field2[DAY,MINUTE],field3[MINUTE],field4[MINUTE]");
    }
    
    @Test
    public void testNonFunctionNodesWithJunctions() throws ParseException {
        // Verify that only the function node is removed.
        assertResult("f:unique_by_minute('field1') AND FOO == 'bar'", "FOO == 'bar'");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[MINUTE]");
        
        // Verify that only the function node is removed.
        assertResult("f:unique_by_minute('field1') AND (FOO == 'bar' AND BAT == 'foo')", "(FOO == 'bar' AND BAT == 'foo')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[MINUTE]");
        
        // Verify that only the function node is removed.
        assertResult("f:unique_by_minute('field1') OR FOO == 'bar'", "FOO == 'bar'");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[MINUTE]");
        
        // Verify that AND nodes are cleaned up.
        assertResult("(FOO == 'bar' OR (BAR == 'foo' AND f:groupby('field1','field2')))", "(FOO == 'bar' OR (BAR == 'foo'))");
        assertOption(QueryParameters.GROUP_FIELDS, "field1,field2");
        
        // Verify that OR nodes are cleaned up.
        assertResult("(FOO == 'bar' AND (BAR == 'foo' OR f:groupby('field1','field2')))", "(FOO == 'bar' AND (BAR == 'foo'))");
        assertOption(QueryParameters.GROUP_FIELDS, "field1,field2");
    }
    
    private void assertOption(String option, String value) {
        assertEquals(value, optionsMap.get(option));
    }
    
    private void assertResult(String original, String expected) throws ParseException {
        optionsMap.clear();
        
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript result = QueryOptionsFromQueryVisitor.collect(originalScript, optionsMap);
        
        assertScriptEquality(result, expected);
        assertLineage(result);
        
        assertScriptEquality(originalScript, original);
        assertLineage(originalScript);
        
        assertTrue(TreeEqualityVisitor.isEqual(expectedScript, result));
    }
    
    private void assertScriptEquality(ASTJexlScript actual, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, actual);
        if (!comparison.isEqual()) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expected));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actual));
        }
        assertTrue(comparison.getReason(), comparison.isEqual());
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
