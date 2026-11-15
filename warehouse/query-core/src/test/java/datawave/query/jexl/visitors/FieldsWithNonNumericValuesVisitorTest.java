package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.jexl.JexlASTHelper;

class FieldsWithNonNumericValuesVisitorTest {

    private String query;
    private final Set<String> expectedFields = new LinkedHashSet<>();

    @AfterEach
    void tearDown() {
        query = null;
        expectedFields.clear();
    }

    /**
     * Test the various field operators with non-numeric string values.
     * 
     * @param operator
     *            The operator
     */
    @ParameterizedTest
    @ValueSource(strings = {"==", "!=", "<", ">", "<=", ">="})
    void testOperatorsWithTextValue(String operator) throws ParseException {
        givenQuery("FOO " + operator + " 'abc'");
        expectFields("FOO");
        assertResult();
    }

    /**
     * Test the various field operators with numeric literal values.
     *
     * @param operator
     *            The operator
     */
    @ParameterizedTest
    @ValueSource(strings = {"==", "!=", "<", ">", "<=", ">="})
    void testOperatorsWithNumericLiteral(String operator) throws ParseException {
        givenQuery("FOO " + operator + " 1");

        // Do not expect any fields.
        assertResult();
    }

    /**
     * Test string literals that are valid numbers.
     */
    @ParameterizedTest
    @ValueSource(strings = {"'1'", "'2.0'", "'-5'", "'1e3'"})
    void testFieldWithStringThatIsNumeric(String literal) throws ParseException {
        givenQuery("FOO == " + literal);

        // Do not expect any fields.
        assertResult();
    }

    /**
     * Test null literals are ignored.
     */
    @Test
    void testNullLiteralIgnored() throws ParseException {
        givenQuery("FOO == null");

        // Do not expect any fields.
        assertResult();
    }

    /**
     * Test a mix of numeric and non-numeric literals across several fields.
     */
    @Test
    void testMultipleFieldsWithMixedValues() throws ParseException {
        givenQuery("FOO == 'abc' && BAR != '>60' || HAT > 3 || BAT < '5' || HEN == 'xyz'");
        expectFields("FOO", "BAR", "HEN");
        assertResult();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expectFields(String... fields) {
        this.expectedFields.addAll(List.of(fields));
    }

    private void assertResult() throws ParseException {
        ASTJexlScript script =JexlASTHelper.parseJexlQuery(query);
        Set<String> actual = FieldsWithNonNumericValuesVisitor.getFields(script);
        assertEquals(expectedFields, actual);
    }
}