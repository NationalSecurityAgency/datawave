package datawave.query.rules;

import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.util.MockMetadataHelper;

class IncludeExcludeIndexFieldsRuleTest extends ShardQueryRuleTest {

    private static final MockMetadataHelper metadataHelper = new MockMetadataHelper();

    @BeforeAll
    static void beforeAll() {
        metadataHelper.setIndexedFields(Set.of("INDEXED1", "INDEXED2"));
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
     * Test versions of the includeRegex and excludeRegex functions without indexed fields.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"includeRegex", "excludeRegex"})
    void testFunctionWithoutIndexedField(String name) throws Exception {
        givenQuery("filter:" + name + "(FOO,'value')");

        // Do not expect any messages.
        assertResult();
    }

    /**
     * Test versions of the includeRegex and excludeRegex functions with an indexed field.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"includeRegex", "excludeRegex"})
    void testFunctionWithSingleIndexedField(String name) throws Exception {
        givenQuery("filter:" + name + "(INDEXED1,'value')");
        expectMessage("Indexed fields found within the function filter:" + name + ": INDEXED1");

        assertResult();
    }

    /**
     * Test a query with both the includeRegex and excludeRegex functions with indexed fields.
     */
    @Test
    void testMultipleFunctionWithIndexedField() throws Exception {
        givenQuery("filter:includeRegex(INDEXED1,'value') && filter:excludeRegex(INDEXED2, 'value')");
        expectMessage("Indexed fields found within the function filter:includeRegex: INDEXED1");
        expectMessage("Indexed fields found within the function filter:excludeRegex: INDEXED2");

        assertResult();
    }

    /**
     * Test a query with both the includeRegex and excludeRegex functions without indexed fields.
     */
    @Test
    void testMultipleFunctionWithoutIndexedField() throws Exception {
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
        return new IncludeExcludeIndexFieldsRule(ruleName);
    }
}
