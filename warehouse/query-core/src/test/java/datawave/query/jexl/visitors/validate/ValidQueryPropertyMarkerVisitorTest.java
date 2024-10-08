package datawave.query.jexl.visitors.validate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;

public class ValidQueryPropertyMarkerVisitorTest {

    private ValidQueryPropertyMarkerVisitor.Validation validation;

    // Verify that a query with no markers is not invalid.
    @Test
    public void testNoMarkers() throws ParseException {
        givenQuery("FOO == 'a' && BAR == 'b'");
        assertIsValid();
    }

    // Verify that a query with a marker that has a singular unwrapped source is not invalid.
    @Test
    public void testMarkerWithUnwrappedSingularSource() throws ParseException {
        givenQuery("(( _List_ = true) && FOO == 'a')");
        assertIsValid();
    }

    // Verify that a query with a marker that has a singular wrapped source is not invalid.
    @Test
    public void testMarkerWithWrappedSingularSource() throws ParseException {
        givenQuery("(( _List_ = true) && (FOO == 'a'))");
        assertIsValid();
    }

    // Verify that a query with a marker that has multiple source nodes is invalid.
    @Test
    public void testMultipleSources() throws ParseException {
        givenQuery("(( _List_ = true) && FOO == 'a' && FOO == 'b' && FOO == 'c')");
        assertIsInvalid();
    }

    // Verify that a query with a nested marker that has multiple source nodes is invalid.
    @Test
    public void testMultipleSourcesInNestedMarker() throws ParseException {
        givenQuery("BAR == 'a' && (NAME == 'b' || (( _List_ = true) && FOO == 'a' && FOO == 'b' && FOO == 'c'))");
        assertIsInvalid();
    }

    // Verify that a query with multiple markers with multiple source nodes is invalid.
    @Test
    public void testMultipleMarkersWithMultipleSources() throws ParseException {
        givenQuery("((_Bounded_ = true) && FOO > 5 && FOO < 10) && ((_List_ = true) && FOO == 'a' && FOO == 'b') && ((_Term_ = true) && BAR =~ 'a*' && BAR =~ 'b*')");
        assertIsInvalid();
    }

    @Test
    public void testUnwrappedMarker() throws ParseException {
        givenQuery("(_Bounded_ = true) && (FOO > 5 && FOO < 10)");
        assertIsInvalid();
    }

    private void givenQuery(String query) throws ParseException {
        ASTJexlScript queryScript = JexlASTHelper.parseJexlQuery(query);
        validation = ValidQueryPropertyMarkerVisitor.validate(queryScript);
    }

    private void assertIsValid() {
        if (!validation.isValid()) {
            fail("Expected validation to pass, but failed for reason: " + validation.getReason());
        }
    }

    private void assertIsInvalid() {
        assertFalse(validation.isValid());
    }
}
