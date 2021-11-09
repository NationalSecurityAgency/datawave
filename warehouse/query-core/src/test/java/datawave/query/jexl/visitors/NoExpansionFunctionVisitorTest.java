package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NoExpansionFunctionVisitorTest {
    
    @Test
    public void testSimpleParseFunctionInConjunction() {
        String query = "FOO == 'bar' && filter:noExpansion(FOO)";
        String expected = "FOO == 'bar'";
        Set<String> expectedFields = Sets.newHashSet("FOO");
        test(query, expected, expectedFields);
    }
    
    @Test
    public void testSimpleParseFunctionInDisjunction() {
        String query = "FOO == 'bar' || filter:noExpansion(FOO)";
        String expected = "FOO == 'bar'";
        Set<String> expectedFields = Sets.newHashSet("FOO");
        test(query, expected, expectedFields);
    }
    
    @Test
    public void testParseMultipleFields() {
        String query = "FOO == 'bar' && filter:noExpansion(FOO,FOO2,FOO3)";
        String expected = "FOO == 'bar'";
        Set<String> expectedFields = Sets.newHashSet("FOO", "FOO2", "FOO3");
        test(query, expected, expectedFields);
    }
    
    private void test(String query, String expected, Set<String> expectedFields) {
        try {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            
            // extract fields without prune
            NoExpansionFunctionVisitor.VisitResult result = NoExpansionFunctionVisitor.findNoExpansionFields(script);
            
            assertEquals(expectedFields, result.noExpansionFields);
            assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(result.script));
            assertTrue(JexlASTHelper.validateLineage(result.script, false));
            
        } catch (ParseException e) {
            fail("Error running unit test with query: " + query);
        }
    }
}
