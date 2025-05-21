package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AmbiguousNotRuleTest extends ShardQueryRuleTest {

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test a query that does not contain a NOT.
     */
    @Test
    void testQueryWithoutNOT() throws Exception {
        givenQuery("FOO:123");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a NOT with a single unwrapped term preceding the NOT.
     */
    @Test
    void testNOTWithSingleUnwrappedPrecedingTerms() throws Exception {
        givenQuery("FIELD1:abc NOT FIELD:def");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a NOT with a single wrapped term preceding the NOT.
     */
    @Test
    void testNOTWithSingleWrappedPrecedingTerms() throws Exception {
        givenQuery("(FIELD1:abc) NOT FIELD:def");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a NOT with multiple wrapped terms preceding the NOT.
     */
    @ParameterizedTest()
    @ValueSource(strings = {"OR", "AND"})
    void testNOTWithWrappedMultiplePrecedingTerms(String junction) throws Exception {
        givenQuery("(FIELD1:abc " + junction + " FIELD2:def) NOT FIELD:ghi");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a NOT with multiple unwrapped terms preceding the NOT.
     */
    @ParameterizedTest()
    @ValueSource(strings = {"OR", "AND"})
    void testNOTWithUnwrappedMultiplePrecedingTerms(String junction) throws Exception {
        givenQuery("FIELD1:abc " + junction + " FIELD2:def NOT FIELD:ghi");

        expectMessage("Ambiguous usage of NOT detected with multiple unwrapped preceding terms: \"FIELD1:abc " + junction
                        + " FIELD2:def NOT\" should be \"(FIELD1:abc " + junction + " FIELD2:def) NOT\".");

        assertResult();
    }

    /**
     * Test a query with a NOT with multiple unwrapped terms preceding the NOT that will be automatically ANDed.
     */
    @Test
    void testNOTWithUnwrappedAutomaticallyAndedPreceedingTerms() throws Exception {
        givenQuery("FIELD1:abc FIELD2:def NOT FIELD:ghi");

        expectMessage("Ambiguous usage of NOT detected with multiple unwrapped preceding terms: \"FIELD1:abc AND FIELD2:def NOT\" should be "
                        + "\"(FIELD1:abc AND FIELD2:def) NOT\".");

        assertResult();
    }

    /**
     * Test a query with a NOT with multiple wrapped terms preceding the NOT that will be automatically ANDed.
     */
    @Test
    void testNOTWithWrappedAutomaticallyAndedPreceedingTerms() throws Exception {
        givenQuery("(FIELD1:abc FIELD2:def) NOT FIELD:ghi");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query that does not consist entirely of a NOT.
     */
    @Test
    void testQueryWithTermThatIsNotPartOfNOT() throws Exception {
        givenQuery("FIELD1:abc OR (FIELD2:abc FIELD3:def NOT FIELD4:ghi)");

        expectMessage("Ambiguous usage of NOT detected with multiple unwrapped preceding terms: \"FIELD2:abc AND FIELD3:def NOT\" should be "
                        + "\"(FIELD2:abc AND FIELD3:def) NOT\".");

        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToLucene();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new AmbiguousNotRule(ruleName);
    }
}
