package datawave.query.rules;

import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IncludeExcludeArgsRuleTest extends ShardQueryRuleTest {

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test a query that does not have the INCLUDE or EXCLUDE function.
     */
    @Test
    public void testQueryWithNoIncludeOrExcludeFunction() throws Exception {
        givenQuery("FOO:'abc'");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single field and value.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithSingleFieldAndValue(String name) throws Exception {
        givenQuery("#" + name + "(FOO,'abc')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single field and value.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithMultipleFieldAndValue(String name) throws Exception {
        givenQuery("#" + name + "(FOO,'abc',BAR,'def')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single field and value after an OR boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithSingleFieldAndValueAfterOR(String name) throws Exception {
        givenQuery("#" + name + "(OR,FOO,'abc')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single field and value after an AND boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithSingleFieldAndValueAfterAND(String name) throws Exception {
        givenQuery("#" + name + "(AND,FOO,'abc')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with multiple fields and values after an OR boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithMultipleFieldsAndValuesAfterOR(String name) throws Exception {
        givenQuery("#" + name + "(OR,FOO,'abc',BAR,'def')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with multiple fields and values after an AND boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithMultipleFieldsAndValuesAfterAND(String name) throws Exception {
        givenQuery("#" + name + "(AND,FOO,'abc',BAR,'def')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with just a single arg.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithSingleArg(String name) throws Exception {
        givenQuery("#" + name + "(FIELD)");
        expectMessage("Function #" + name + " supplied with uneven number of arguments. Must supply field/value pairs, e.g. #" + name + "(FIELD, 'value') or "
                        + "#" + name + "(FIELD1, 'value1', FIELD2, 'value2').");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with an uneven number of arguments greater than one without a boolean arg.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithUnevenArgs(String name) throws Exception {
        givenQuery("#" + name + "(FIELD1,'value',FIELD2)");
        expectMessage("Function #" + name + " supplied with uneven number of arguments. Must supply field/value pairs, e.g. #" + name + "(FIELD, 'value') or "
                        + "#" + name + "(FIELD1, 'value1', FIELD2, 'value2').");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with no arguments after an OR boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithNoArgsAfterOR(String name) throws Exception {
        givenQuery("#" + name + "(OR)");
        expectMessage("Function #" + name
                        + " supplied with no arguments after the first boolean arg OR. Must supply at least a field and value after the first "
                        + "boolean arg, e.g. #" + name + "(OR, FIELD, 'value').");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with no arguments after an AND boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithNoArgsAfterAND(String name) throws Exception {
        givenQuery("#" + name + "(AND)");
        expectMessage("Function #" + name + " supplied with no arguments after the first boolean arg AND. Must supply at least a field and value after the "
                        + "first boolean arg, e.g. #" + name + "(AND, FIELD, 'value').");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single argument after an OR boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithSingleArgsAfterOR(String name) throws Exception {
        givenQuery("#" + name + "(OR,'value')");
        expectMessage("Function #" + name + " supplied with uneven number of arguments after the first boolean arg OR. Must supply field/value after the "
                        + "boolean, e.g. #" + name + "(OR, FIELD, 'value') or #" + name + "(OR, FIELD1, 'value1',' FIELD2, 'value2').");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single argument after an AND boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithSingleArgsAfterAND(String name) throws Exception {
        givenQuery("#" + name + "(AND,'value')");
        expectMessage("Function #" + name + " supplied with uneven number of arguments after the first boolean arg AND. Must supply field/value after the "
                        + "boolean, e.g. #" + name + "(AND, FIELD, 'value') or #" + name + "(AND, FIELD1, 'value1',' FIELD2, 'value2').");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with an uneven number of arguments greater than one after an OR boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithUnevenArgsAfterOR(String name) throws Exception {
        givenQuery("#" + name + "(OR,FIELD1,'value',FIELD2)");
        expectMessage("Function #" + name + " supplied with uneven number of arguments after the first boolean arg OR. Must supply field/value after the "
                        + "boolean, e.g. #" + name + "(OR, FIELD, 'value') or #" + name + "(OR, FIELD1, 'value1',' FIELD2, 'value2').");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with an uneven number of arguments greater than one after an AND boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithUnevenArgsAfterAND(String name) throws Exception {
        givenQuery("#" + name + "(AND,FIELD1,'value',FIELD2)");
        expectMessage("Function #" + name + " supplied with uneven number of arguments after the first boolean arg AND. Must supply field/value after the "
                        + "boolean, e.g. #" + name + "(AND, FIELD, 'value') or #" + name + "(AND, FIELD1, 'value1',' FIELD2, 'value2').");
        assertResult();
    }

    /**
     * Verify that when an exception is thrown, it is captured in the result.
     */
    @Test
    void testExceptionThrown() {
        ShardQueryValidationConfiguration configuration = EasyMock.mock(ShardQueryValidationConfiguration.class);
        Exception exception = new IllegalArgumentException("Failed to get query");
        EasyMock.expect(configuration.getParsedQuery()).andThrow(exception);
        EasyMock.replay(configuration);

    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToLucene();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new IncludeExcludeArgsRule(ruleName);
    }
}
