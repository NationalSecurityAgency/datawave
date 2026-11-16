package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;

public class NullCheckOnlyFieldsVisitorTest {

    /**
     * Verify we find no fields in a query with no null checks.
     */
    @Test
    public void testNoNullChecks() throws ParseException {
        test("FOO == 'bar' && BAR == 'baz'", Set.of());
    }

    /**
     * Verify we find a field in the case of {@code FIELD == null}.
     */
    @Test
    public void testEqNull() throws ParseException {
        test("FOO == 'bar' && FIELD == null", Set.of("FIELD"));
    }

    /**
     * Verify we find a field in the case of {@code FIELD != null}.
     */
    @Test
    public void testNeNull() throws ParseException {
        test("FOO == 'bar' && FIELD != null", Set.of("FIELD"));
    }

    /**
     * Verify we find a field in the case of {@code !(FIELD == null}).
     */
    @Test
    public void testNegatedEqNull() throws ParseException {
        test("FOO == 'bar' && !(FIELD == null)", Set.of("FIELD"));
    }

    /**
     * Verify we find a field in the case of {@code null == FIELD}.
     */
    @Test
    public void testNullLiteralFirst() throws ParseException {
        test("FOO == 'bar' && null == FIELD", Set.of("FIELD"));
    }

    /**
     * Verify we find all fields in multiple cases of null-only checks.
     */
    @Test
    public void testMultipleNullChecks() throws ParseException {
        test("FOO == 'bar' && FIELD == null && !(OTHER == null)", Set.of("FIELD", "OTHER"));
    }

    /**
     * Verify we do not find a field if it is used in a null check and also in a value term.
     */
    @Test
    public void testFieldAlsoUsedInValueTerm() throws ParseException {
        test("FOO == 'bar' && FIELD == null && FIELD != 'x'", Set.of());
        test("FOO == 'bar' && !(FIELD == null) || FIELD == 'x'", Set.of());
    }

    /**
     * Verify we do not find a field if it is used in a null check and also in a function.
     */
    @Test
    public void testFieldAlsoUsedInFunction() throws ParseException {
        test("FOO == 'bar' && FIELD == null && filter:includeRegex(FIELD, 'x.*')", Set.of());
    }

    /**
     * Verify we do not find a field if it is used in a null check and also in a regex match.
     */
    @Test
    public void testFieldAlsoUsedInRegex() throws ParseException {
        test("FOO == 'bar' && FIELD == null && FIELD =~ 'x.*'", Set.of());
    }

    /**
     * Verify we find a field if it is used only in a null check has identifier escaping.
     */
    @Test
    public void testIdentifierEscaping() throws ParseException {
        test("FOO == 'bar' && $1FIELD == null", Set.of("1FIELD"));
    }

    /**
     * Verify fields are not filtered on given a null collection of fields of interest.
     */
    @Test
    void testNullFieldsOfInterest() throws ParseException {
        test("FOO == 'bar' && FIELD == null && !(OTHER == null)", null, Set.of("FIELD", "OTHER"));
    }

    /**
     * Verify fields are not filtered on given an empty collection of fields of interest.
     */
    @Test
    void testEmptyFieldsOfInterest() throws ParseException {
        test("FOO == 'bar' && FIELD == null && !(OTHER == null)", Set.of(), Set.of("FIELD", "OTHER"));
    }

    /**
     * Verify fields are filtered on given an non-empty collection of fields of interest.
     */
    @Test
    void testNonEmptyFieldsOfInterest() throws ParseException {
        // Matching fields should be filtered.
        test("FOO == 'bar' && FIELD == null && !(OTHER == null)", Set.of("FIELD"), Set.of("FIELD"));
        // Non-matching fields should not throw an error.
        test("FOO == 'bar' && FIELD == null && !(OTHER == null)", Set.of("FIELD", "OTHER", "OTHER2"), Set.of("FIELD", "OTHER"));
        // No matching fields should result in an empty set.
        test("FOO == 'bar' && FIELD == null && !(OTHER == null)", Set.of("OTHER2"), Set.of());
    }

    private void test(String query, Set<String> expected) throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        assertEquals(expected, NullCheckOnlyFieldsVisitor.getFields(node));
    }

    private void test(String query, Set<String> fieldsOfInterest, Set<String> expected) throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        assertEquals(expected, NullCheckOnlyFieldsVisitor.getFields(node, fieldsOfInterest));
    }
}
