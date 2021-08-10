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
    
    @Test
    public void testOptionsFunction() throws ParseException {
        // Verify that an empty options function adds no parameters.
        findQueryOptions("f:options()");
        assertTrue(optionsMap.isEmpty());
        
        // Verify any specified options are added as separate parameters.
        findQueryOptions("f:options('include.grouping.context','true','hit.list','true','limit.fields','FOO_1_BAR=3,FOO_1=2')");
        assertOption("include.grouping.context", "true");
        assertOption("hit.list", "true");
        assertOption("limit.fields", "FOO_1_BAR=3,FOO_1=2");
    }
    
    @Test
    public void testGroupByFunction() throws ParseException {
        // Verify that an empty groupby functions results in an empty parameter value.
        findQueryOptions("f:groupby()");
        assertOption(QueryParameters.GROUP_FIELDS, "");
        
        findQueryOptions("f:groupby('field1','field2','field3')");
        assertOption(QueryParameters.GROUP_FIELDS, "field1,field2,field3");
    }
    
    @Test
    public void testUniqueFunction() throws ParseException {
        // Verify an empty function results in an empty parameter value.
        findQueryOptions("f:unique_by_day()");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
        
        // Verify that fields of no specified granularity are added with the default ALL granularity.
        findQueryOptions("f:unique('field1','field2','field3')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL],field2[ALL],field3[ALL]");
        
        // Verify that fields with DAY granularity are added as such.
        findQueryOptions("f:unique('field1[DAY]','field2[DAY]','field3[DAY]')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[DAY],field2[DAY],field3[DAY]");
        
        // Verify that fields with HOUR granularity are added as such.
        findQueryOptions("f:unique('field1[HOUR]','field2[HOUR]','field3[HOUR]')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[HOUR],field2[HOUR],field3[HOUR]");
        
        // Verify that fields with MINUTE granularity are added as such.
        findQueryOptions("f:unique('field1[MINUTE]','field2[MINUTE]','field3[MINUTE]')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[MINUTE],field2[MINUTE],field3[MINUTE]");
        
        // Verify that fields from multiple unique functions are merged together.
        findQueryOptions("f:unique('field1','field2') AND f:unique('field2[DAY]','field3[DAY]') AND f:unique('field4')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL],field2[ALL,DAY],field3[DAY],field4[ALL]");
        
        // Verify more complex fields with multiple granularity levels are merged together.
        findQueryOptions("f:unique('field1[DAY]','field2[DAY,HOUR]','field3[HOUR,MINUTE]','field4[ALL,MINUTE]','field5')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[DAY],field2[DAY,HOUR],field3[HOUR,MINUTE],field4[ALL,MINUTE],field5[ALL]");
        
        // Lucene will parse comma-delimited granularity levels into separate strings. Ensure it still parses correctly.
        findQueryOptions("f:unique('field1[DAY]','field2[DAY','HOUR]','field3[HOUR','MINUTE]','field4[ALL','MINUTE]','field5')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[DAY],field2[DAY,HOUR],field3[HOUR,MINUTE],field4[ALL,MINUTE],field5[ALL]");
    }
    
    @Test
    public void testUniqueByDay() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        findQueryOptions("f:unique_by_day()");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
        
        // Verify fields are added with the DAY granularity.
        findQueryOptions("f:unique_by_day('field1','field2','field3')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[DAY],field2[DAY],field3[DAY]");
        
        // Verify fields from multiple functions are merged.
        findQueryOptions("f:unique('field1','field2[HOUR]') AND f:unique_by_day('field1','field2','field3') AND f:unique_by_day('field4')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL,DAY],field2[DAY,HOUR],field3[DAY],field4[DAY]");
    }
    
    @Test
    public void testUniqueByHour() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        findQueryOptions("f:unique_by_hour()");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
        
        // Verify fields are added with the HOUR granularity.
        findQueryOptions("f:unique_by_hour('field1','field2','field3')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[HOUR],field2[HOUR],field3[HOUR]");
        
        // Verify fields from multiple functions are merged.
        findQueryOptions("f:unique('field1','field2[DAY]') AND f:unique_by_hour('field1','field2','field3') AND f:unique_by_hour('field4')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL,HOUR],field2[DAY,HOUR],field3[HOUR],field4[HOUR]");
    }
    
    @Test
    public void testUniqueByMinute() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        findQueryOptions("f:unique_by_minute()");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
        
        // Verify fields are added with the MINUTE granularity.
        findQueryOptions("f:unique_by_minute('field1','field2','field3')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[MINUTE],field2[MINUTE],field3[MINUTE]");
        
        // Verify fields from multiple functions are merged.
        findQueryOptions("f:unique('field1','field2[DAY]') AND f:unique_by_minute('field1','field2','field3') AND f:unique_by_minute('field4')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL,MINUTE],field2[DAY,MINUTE],field3[MINUTE],field4[MINUTE]");
    }
    
    private void findQueryOptions(String query) throws ParseException {
        optionsMap.clear();
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        QueryOptionsFromQueryVisitor.collect(node, optionsMap);
    }
    
    private void assertOption(String option, String value) {
        assertEquals(value, optionsMap.get(option));
    }
}
