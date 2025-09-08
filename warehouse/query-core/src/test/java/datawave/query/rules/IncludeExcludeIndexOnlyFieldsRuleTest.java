package datawave.query.rules;

import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.util.MockMetadataHelper;

class IncludeExcludeIndexOnlyFieldsRuleTest extends ShardQueryRuleTest {

    private static final MockMetadataHelper metadataHelper = new MockMetadataHelper();

    @BeforeAll
    static void beforeAll() {
        metadataHelper.setIndexOnlyFields(Set.of("INDEXED1", "INDEXED2"));
    }

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        givenMetadataHelper(metadataHelper);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test a query without functions.
     */
    @Test
    void testQueryWithoutFunctions() throws Exception {
        givenQuery("FOO == 'abc'");

        // Do not expect any messages.
        assertResult();
    }

    /**
     * Test versions of the includeRegex and excludeRegex functions without index only fields.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"includeRegex", "excludeRegex"})
    void testFunctionWithoutIndexedOnlyField(String name) throws Exception {
        givenQuery("filter:" + name + "(FOO,'value')");

        // Do not expect any messages.
        assertResult();
    }

    /**
     * Test versions of the includeRegex and excludeRegex functions with an index only field.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"includeRegex", "excludeRegex"})
    void testFunctionWithSingleIndexOnlyField(String name) throws Exception {
        givenQuery("filter:" + name + "(INDEXED1,'value')");
        expectMessage("Index Only fields found within the filter function filter:" + name + ": INDEXED1 -- The field(s) cannot be used in this manner.");

        assertResult();
    }

    /**
     * Test a query with both the includeRegex and excludeRegex functions with indexed fields.
     */
    @Test
    void testMultipleFunctionWithIndexOnlyField() throws Exception {
        givenQuery("filter:includeRegex(INDEXED1,'value') && filter:excludeRegex(INDEXED2, 'value')");
        expectMessage("Index Only fields found within the filter function filter:includeRegex: INDEXED1 -- The field(s) cannot be used in this manner.");
        expectMessage("Index Only fields found within the filter function filter:excludeRegex: INDEXED2 -- The field(s) cannot be used in this manner.");

        assertResult();
    }

    /**
     * Test a query with both the includeRegex and excludeRegex functions without indexed fields.
     */
    @Test
    void testMultipleFunctionWithoutIndexOnlyField() throws Exception {
        givenQuery("filter:includeRegex(FOO,'value') && filter:excludeRegex(BAR, 'value')");

        // Do not expect any messages.
        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToJexl();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new IncludeExcludeIndexOnlyFieldsRule(ruleName);
    }
}
