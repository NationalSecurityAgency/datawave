package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

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
