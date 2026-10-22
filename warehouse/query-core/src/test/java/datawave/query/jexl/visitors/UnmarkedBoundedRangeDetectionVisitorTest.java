package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ValidateBoundedRangeVisitor;

public class UnmarkedBoundedRangeDetectionVisitorTest {

    @Test
    public void testBoundedMarkerWithValidRange() {
        String query = "((_Bounded_ = true) && (FIELD >= '2' && FIELD <= '3'))";
        test(false, query);
    }

    /**
     * This illegal range is detected by the {@link ValidateBoundedRangeVisitor}
     */
    @Test
    public void testBoundedMarkerWithInvalidRange() {
        String query = "((_Bounded_ = true) && (FIELD >= '5' && FIELD <= '3'))";
        test(false, query);
    }

    @Test
    public void testBoundedMarkerWithInvalidSource() {
        String query = "((_Bounded_ = true) && (FIELD == '5' && FIELD == '3'))";
        // unmarked bounded range will throw an exception
        assertThrows(DatawaveFatalQueryException.class, () -> test(true, query));
    }

    @Test
    public void testNoMarkerValidSource() {
        String query = "FIELD >= '2' && FIELD <= '3'";
        test(true, query);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FIELD >= '2' && FIELD <= '3' && OTHER == 'x'", "FIELD <= '3' && FIELD >= '2' && OTHER == 'x'",
            "FIELD >= '2' && FIELD <= '3' && OTHER == 'x' && THIRD == 'y'", "(FIELD >= '2' && FIELD <= '3' && OTHER == 'x') || OTHER == 'y'"})
    public void testLeadingUnmarkedRangeInWideJunction(String query) {
        test(true, query);
    }

    @ParameterizedTest
    @ValueSource(strings = {"OTHER == 'x' && FIELD >= '2' && FIELD <= '3'", "FIELD >= '2' && OTHER <= '3' && THIRD == 'y'",
            "FIELD >= '2' && FIELD >= '3' && OTHER == 'x'"})
    public void testDoesNotIntroduceNewRangeBoundaries(String query) {
        test(false, query);
    }

    @Test
    public void testNoMarkerInvalidSource() {
        String query = "FIELD == '2' && FIELD == '3'";
        test(false, query);
    }

    private void test(boolean containsUnmarked, String query) {
        ASTJexlScript script = parse(query);
        boolean result = UnmarkedBoundedRangeDetectionVisitor.findUnmarkedBoundedRanges(script);
        assertEquals(containsUnmarked, result);
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query, e);
            throw new RuntimeException(e);
        }
    }

}
