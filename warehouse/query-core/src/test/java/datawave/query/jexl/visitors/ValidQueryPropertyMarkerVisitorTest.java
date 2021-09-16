package datawave.query.jexl.visitors;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ValidQueryPropertyMarkerVisitorTest {
    
    // Verify that a query with no markers is not invalid.
    @Test
    public void testNoMarkers() throws ParseException {
        String original = "FOO == 'a' && BAR == 'b'";
        assertResult(original);
    }
    
    // Verify that a query with a marker that has a singular unwrapped source is not invalid.
    @Test
    public void testMarkerWithUnwrappedSingularSource() throws ParseException {
        String original = "(( _List_ = true) && FOO == 'a')";
        assertResult(original);
    }
    
    // Verify that a query with a marker that has a singular wrapped source is not invalid.
    @Test
    public void testMarkerWithWrappedSingularSource() throws ParseException {
        String original = "(( _List_ = true) && (FOO == 'a'))";
        assertResult(original);
    }
    
    // Verify that a query with a marker that has multiple source nodes is invalid.
    @Test
    public void testMultipleSources() {
        String original = "(( _List_ = true) && FOO == 'a' && FOO == 'b' && FOO == 'c')";
        
        DatawaveFatalQueryException exception = assertThrows(DatawaveFatalQueryException.class, () -> assertResult(original));
        assertEquals("Query contains a query property marker with multiple source nodes: (_List_ = true) && FOO == 'a' && FOO == 'b' && FOO == 'c'",
                        exception.getMessage());
    }
    
    // Verify that a query with a nested marker that has multiple source nodes is invalid.
    @Test
    public void testMultipleSourcesInNestedMarker() {
        String original = "BAR == 'a' && (NAME == 'b' || (( _List_ = true) && FOO == 'a' && FOO == 'b' && FOO == 'c'))";
        
        DatawaveFatalQueryException exception = assertThrows(DatawaveFatalQueryException.class, () -> assertResult(original));
        assertEquals("Query contains a query property marker with multiple source nodes: (_List_ = true) && FOO == 'a' && FOO == 'b' && FOO == 'c'",
                        exception.getMessage());
    }
    
    // Verify that a query with multiple markers with multiple source nodes is invalid.
    @Test
    public void testMultipleMarkersWithMultipleSources() {
        String original = "((_Bounded_ = true) && FOO > 5 && FOO < 10) && ((_List_ = true) && FOO == 'a' && FOO == 'b') && ((_Term_ = true) && BAR =~ 'a*' && BAR =~ 'b*')";
        
        DatawaveFatalQueryException exception = assertThrows(DatawaveFatalQueryException.class, () -> assertResult(original));
        assertEquals("Query contains a query property marker with multiple source nodes: (_Bounded_ = true) && FOO > 5 && FOO < 10", exception.getMessage());
    }
    
    private void assertResult(String original) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        ValidQueryPropertyMarkerVisitor.validate(originalScript);
    }
}
