package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;

class IndexedTermCountingVisitorTest {

    @Test
    void testSimpleCount() {
        String query = "F == '1' || F == '2' || (F == '3' && F == '4')";
        test(query, 4);
    }

    @Test
    void testNoCount() {
        String query = "A == '1' && B == '2'";
        test(query, 0);
    }

    @Test
    void testPartialCount() {
        String query = "A == '1' && B == '2' && F == 'zee'";
        test(query, 1);
    }

    private void test(String query, int expectedCount) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            int count = IndexedTermCountingVisitor.countTerms(script, Collections.singleton("F"));
            assertEquals(expectedCount, count);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
    }

}
