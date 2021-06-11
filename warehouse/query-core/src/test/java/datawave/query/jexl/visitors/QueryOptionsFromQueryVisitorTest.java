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
     * Verify that a unique function with fields specified without any transformers are added with default ALL transformer.
     */
    @Test
    public void testUniqueFunctionWithoutTransformers() throws ParseException {
        findQueryOptions("f:unique(field1,field2,field3)");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL],field2[ALL],field3[ALL]");
    }
    
    /**
     * Verify that a unique function with fields specified with ALL are added with the ALL transformer.
     */
    @Test
    public void testUniqueFunctionWithOriginal() throws ParseException {
        findQueryOptions("f:unique('field1[ALL]','field2[ALL]','field3[ALL]')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL],field2[ALL],field3[ALL]");
    }
    
    /**
     * Verify that a unique function with fields specified with DAY are added with the DAY transformer.
     */
    @Test
    public void testUniqueFunctionWithDay() throws ParseException {
        findQueryOptions("f:unique('field1[DAY]','field2[DAY]','field3[DAY]')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[DAY],field2[DAY],field3[DAY]");
    }
    
    /**
     * Verify that a unique function with fields specified with HOUR are added with the HOUR transformer.
     */
    @Test
    public void testUniqueFunctionWithHour() throws ParseException {
        findQueryOptions("f:unique('field1[HOUR]','field2[HOUR]','field3[HOUR]')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[HOUR],field2[HOUR],field3[HOUR]");
    }
    
    /**
     * Verify that a unique function with fields specified with[ MINUTE are added with the MINUTE transformer.
     */
    @Test
    public void testUniqueFunctionWithMinute() throws ParseException {
        findQueryOptions("f:unique('field1[MINUTE]','field2[MINUTE]','field3[MINUTE]')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[MINUTE],field2[MINUTE],field3[MINUTE]");
    }
    
    /**
     * Verify that multiple unique functions separated by conjunctions are merged into the same unique parameter value.
     */
    @Test
    public void testMultipleUniqueFunctions() throws ParseException {
        findQueryOptions("f:unique(field1,field2) AND f:unique('field2[DAY]','field3[DAY]') AND f:unique(field4)");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[ALL],field2[ALL,DAY],field3[DAY],field4[ALL]");
    }
    
    /**
     * Verify that a unique function with mixed transformers is parsed correctly.
     */
    @Test
    public void testMultipleUniqueFunctionWithMixedTransformers() throws ParseException {
        findQueryOptions("f:unique('field1[DAY]','field2[DAY,HOUR]','field3[HOUR,MINUTE]','field4[ALL,MINUTE]','field5')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "field1[DAY],field2[DAY,HOUR],field3[HOUR,MINUTE],field4[ALL,MINUTE],field5[ALL]");
    }
    
    private void findQueryOptions(String query) throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        QueryOptionsFromQueryVisitor.collect(node, optionsMap);
    }
    
    private void assertOption(String option, String value) {
        assertEquals(value, optionsMap.get(option));
    }
}
