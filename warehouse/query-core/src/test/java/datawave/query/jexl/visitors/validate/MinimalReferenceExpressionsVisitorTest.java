package datawave.query.jexl.visitors.validate;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MinimalReferenceExpressionsVisitorTest {
    
    @Test
    public void testValidEqNode() {
        String query = "FOO == 'bar'";
        validate(query, true);
    }
    
    @Test
    public void testValidWrappedEqNode() {
        String query = "(FOO == 'bar')";
        validate(query, true);
    }
    
    @Test
    public void testInvalidEqNode() {
        String query = "((FOO == 'bar'))";
        validate(query, false);
    }
    
    @Test
    public void testValidUnion() {
        String query = "(FOO == 'bar' || FOO == 'baz')";
        validate(query, true);
    }
    
    @Test
    public void testValidUnionWithMarkerNode() {
        String query = "(FOO == 'bar' || ((_Value_ = true) && (FOO =~ 'ba.*')))";
        validate(query, true);
    }
    
    @Test
    public void testInvalidUnion() {
        String query = "(((FOO == 'bar')) || ((_Value_ = true) && (FOO =~ 'ba.*')))";
        validate(query, false);
    }
    
    @Test
    public void testValidIntersection() {
        String query = "(FOO == 'bar' && FOO == 'baz')";
        validate(query, true);
    }
    
    @Test
    public void testValidIntersectionWithMarkerNode() {
        String query = "(FOO == 'bar' && ((_Value_ = true) && (FOO =~ 'ba.*')))";
        validate(query, true);
    }
    
    @Test
    public void testInvalidIntersection() {
        String query = "(((FOO == 'bar')) && ((_Value_ = true) && (FOO =~ 'ba.*')))";
        validate(query, false);
    }
    
    @Test
    public void testValidNestedUnion() {
        String query = "(FOO == 'bar' && (FOO2 == 'baz' || FOO3 == 'baz'))";
        validate(query, true);
    }
    
    @Test
    public void testValidNestedIntersection() {
        String query = "(FOO == 'bar' || (FOO2 == 'baz' && FOO3 == 'baz'))";
        validate(query, true);
    }
    
    @Test
    public void testInvalidNestedUnion() {
        String query = "(FOO == 'bar' && ((FOO2 == 'baz') || FOO3 == 'baz'))";
        validate(query, false);
    }
    
    @Test
    public void testInvalidNestedIntersection() {
        String query = "(FOO == 'bar' || (FOO2 == 'baz' && (FOO3 == 'baz')))";
        validate(query, false);
    }
    
    @Test
    public void testInvalidNestedIntersectionBehindANegation() {
        String query = "(FOO == 'bar' || !(FOO2 == 'baz' && (FOO3 == 'baz')))";
        validate(query, false);
    }
    
    private void validate(String query, boolean expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            assertEquals(expected, MinimalReferenceExpressionsVisitor.validate(script));
            
            // validate the QueryPropertyMarker visitor didn't re-parent any nodes
            assertTrue(JexlASTHelper.validateLineage(script, false));
        } catch (ParseException e) {
            fail("error testing query: " + query + ", " + e.getMessage());
        }
    }
}
