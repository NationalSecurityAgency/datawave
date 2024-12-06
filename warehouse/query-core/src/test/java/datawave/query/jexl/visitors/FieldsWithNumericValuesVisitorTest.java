package datawave.query.jexl.visitors;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.jexl.JexlASTHelper;

class FieldsWithNumericValuesVisitorTest {

    private String query;
    private final Set<String> expectedFields = new LinkedHashSet<>();

    @AfterEach
    void tearDown() {
        query = null;
        expectedFields.clear();
    }

    /**
     * Test the various field operators with string values.
     *
     * @param operator
     *            the operator
     */
    @ParameterizedTest
    @ValueSource(strings = {"==", "!=", "<", ">", "<=", ">="})
    void testOperatorsWithTextValue(String operator) throws ParseException {
        givenQuery("FOO " + operator + " 'abc'");

        // Do not expect any fields.
        assertResult();
    }

    /**
     * Test the various field operators with boolean values.
     *
     * @param operator
     *            the operator
     */
    @ParameterizedTest
    @ValueSource(strings = {"==", "!=", "<", ">", "<=", ">="})
    void testOperatorsWithBooleanValue(String operator) throws ParseException {
        givenQuery("FOO " + operator + " true");

        // Do not expect any fields.
        assertResult();
    }

    /**
     * Test the various field operators with numeric values.
     *
     * @param operator
     *            the operator
     */
    @ParameterizedTest
    @ValueSource(strings = {"==", "!=", "<", ">", "<=", ">="})
    void testOperatorsWithNumericValue(String operator) throws ParseException {
        givenQuery("FOO " + operator + " 1");
        expectFields("FOO");
        assertResult();
    }

    /**
     * Test multiple fields with numeric values.
     */
    @Test
    void testMultipleFieldsWithNumericValues() throws ParseException {
        givenQuery("FOO == 'abc' && BAR != true || HAT > 3 || BAT < 5 || HEN <= 15 || VEE >= 20");
        expectFields("HAT", "BAT", "HEN", "VEE");
        assertResult();
    }

    /**
     * Test string literals that are valid numbers.
     */
    @Test
    void testFieldWithStringThatIsNumeric() throws ParseException {
        givenQuery("FOO == '1' && BAR == '2.0' && HAT == 'def'");
        expectFields("FOO", "BAR");
        assertResult();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expectFields(String... fields) {
        this.expectedFields.addAll(List.of(fields));
    }

    private void assertResult() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Set<String> actual = FieldsWithNumericValuesVisitor.getFields(script);
        Assertions.assertEquals(expectedFields, actual);
    }
}
