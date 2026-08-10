package datawave.query.jexl.visitors.validate;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;

public class ValidateBoundedRangeVisitorTest {

    @Test
    public void testBoundedMarkerWithValidRange() {
        String query = "((_Bounded_ = true) && (FIELD >= '2' && FIELD <= '3'))";
        test(query);
    }

    @Test
    public void testBoundedMarkerWithInvalidRange() {
        String query = "((_Bounded_ = true) && (FIELD >= '5' && FIELD <= '3'))";
        assertThrows(IllegalStateException.class, () -> test(query));
    }

    @Test
    public void testBoundedMarkerWithInvalidSource() {
        String query = "((_Bounded_ = true) && (FIELD == '5' && FIELD == '3'))";
        // unmarked bounded range will throw an exception
        assertThrows(DatawaveFatalQueryException.class, () -> test(query));
    }

    @Test
    public void testNoMarker() {
        // 'valid' source node for a bounded range
        String query = "FIELD >= '2' && FIELD <= '3'";
        test(query);

        // 'invalid' source for a bounded range,
        query = "FIELD == '2' && FIELD == '3'";
        test(query);
    }

    private void test(String query) {
        ASTJexlScript script = parse(query);
        ValidateBoundedRangeVisitor.validate(script);
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
