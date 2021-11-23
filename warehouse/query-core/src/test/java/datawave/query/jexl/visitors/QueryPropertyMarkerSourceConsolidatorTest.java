package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

public class QueryPropertyMarkerSourceConsolidatorTest {
    
    // Verify that a query with no markers is not modified.
    @Test
    public void testNoMarkers() throws ParseException {
        String original = "FOO == 'a' && BAR == 'b'";
        String expected = "FOO == 'a' && BAR == 'b'";
        assertResult(original, expected);
    }
    
    // Verify that a query with a marker that has a singular unwrapped source is not modified.
    @Test
    public void testMarkerWithUnwrappedSingularSource() throws ParseException {
        String original = "((_List_ = true) && FOO == 'a')";
        String expected = "((_List_ = true) && FOO == 'a')";
        assertResult(original, expected);
    }
    
    // Verify that a query with a marker that has a singular wrapped source is not modified.
    @Test
    public void testMarkerWithWrappedSingularSource() throws ParseException {
        String original = "((_List_ = true) && (FOO == 'a'))";
        String expected = "((_List_ = true) && (FOO == 'a'))";
        assertResult(original, expected);
    }
    
    // Verify that a query with a marker that has multiple source nodes is corrected.
    @Test
    public void testMultipleSources() throws ParseException {
        String original = "((_List_ = true) && FOO == 'a' && FOO == 'b' && FOO == 'c')";
        String expected = "((_List_ = true) && (FOO == 'a' && FOO == 'b' && FOO == 'c'))";
        assertResult(original, expected);
    }
    
    // Verify that a query with a nested marker that has multiple source nodes is corrected.
    @Test
    public void testMultipleSourcesInNestedMarker() throws ParseException {
        String original = "BAR == 'a' && (NAME == 'b' || ((_List_ = true) && FOO == 'a' && FOO == 'b' && FOO == 'c'))";
        String expected = "BAR == 'a' && (NAME == 'b' || ((_List_ = true) && (FOO == 'a' && FOO == 'b' && FOO == 'c')))";
        assertResult(original, expected);
    }
    
    // Verify that a query with multiple markers with multiple source nodes is corrected.
    @Test
    public void testMultipleMarkersWithMultipleSources() throws ParseException {
        String original = "((_Bounded_ = true) && FOO > 5 && FOO < 10) && ((_List_ = true) && FOO == 'a' && FOO == 'b') && ((_Term_ = true) && BAR =~ 'a*' && BAR =~ 'b*')";
        String expected = "((_Bounded_ = true) && (FOO > 5 && FOO < 10)) && ((_List_ = true) && (FOO == 'a' && FOO == 'b')) && ((_Term_ = true) && (BAR =~ 'a*' && BAR =~ 'b*'))";
        assertResult(original, expected);
    }
    
    @Test
    public void testNestedSources() throws ParseException {
        String original = "((_List_ = true) && FOO == 'a' && (BAR == 'b' && BAT == 'c'))";
        String expected = "((_List_ = true) && (FOO == 'a' && (BAR == 'b' && BAT == 'c')))";
        assertResult(original, expected);
    }
    
    private void assertResult(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript actual = QueryPropertyMarkerSourceConsolidator.consolidate(originalScript);
        
        JexlNodeAssert.assertThat(actual).isEqualTo(expected).hasValidLineage();
        JexlNodeAssert.assertThat(originalScript).hasExactQueryString(original).hasValidLineage();
    }
}
