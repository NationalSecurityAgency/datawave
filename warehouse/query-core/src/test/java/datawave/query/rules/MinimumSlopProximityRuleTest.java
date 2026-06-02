package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MinimumSlopProximityRuleTest extends ShardQueryRuleTest {

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test a query without a slop value.
     */
    @Test
    public void testQueryWithNoSlop() throws Exception {
        givenQuery("FIELD:'abc'");
        assertResult();
    }

    /**
     * Test a query with a slop operator but no numeric value.
     */
    @Test
    public void testQueryWithSlopOperatorNoValue() throws Exception {
        givenQuery("FIELD:\"term1 term2 term3\"~");
        assertResult();
    }

    /**
     * Test a proximity query with a single term and insufficient slop value.
     */
    @Test
    public void testSingleTermProximityLessThanMin() throws Exception {
        givenQuery("FIELD:\"term1\"~0");
        expectMessage("Invalid slop proximity, the " + 0 + " should be " + 1 + " or greater: FIELD:\"term1\"~" + 0);
        assertResult();
    }

    /**
     * Test a proximity query with multiple terms and insufficient slop value.
     */
    @Test
    public void testMultiTermProximityLessThanMin() throws Exception {
        givenQuery("FIELD:\"term1 term2\"~1");
        expectMessage("Invalid slop proximity, the " + 1 + " should be " + 2 + " or greater: FIELD:\"term1 term2\"~" + 1);
        assertResult();
    }

    /**
     * Test a proximity query with a single term and slop value equal to the minimum allowed.
     */
    @Test
    public void testSingleTermProximityEqualToMin() throws Exception {
        givenQuery("FIELD:\"term1\"~1");
        assertResult();
    }

    /**
     * Test a proximity query with multiple terms and slop value equal to the minimum allowed.
     */
    @Test
    public void testMultiTermProximityEqualToMin() throws Exception {
        givenQuery("FIELD:\"term1 term2\"~2");
        assertResult();
    }

    /**
     * Test a proximity query with a single term and slop value greater than the minimum allowed.
     */
    @Test
    public void testSingleTermProximityGreaterThanMin() throws Exception {
        givenQuery("FIELD:\"term1\"~2");
        assertResult();
    }

    /**
     * Test a proximity query with multiple terms and slop value greater than the minimum allowed.
     */
    @Test
    public void testMultiTermProximityGreaterThanMin() throws Exception {
        givenQuery("FIELD:\"term1 term2\"~3");
        assertResult();
    }

    /**
     * Test a proximity query with padded white space on the left.
     */
    @Test
    public void testValidWithPaddedWhiteSpaceLeft() throws Exception {
        givenQuery("FIELD:\"                  term1 term2 term3\"~3");
        assertResult();
    }

    /**
     * Test a proximity query with padded white space on the right.
     */
    @Test
    public void testValidWithPaddedWhiteSpaceRight() throws Exception {
        givenQuery("FIELD:\"term1 term2 term3                  \"~3");
        assertResult();
    }

    /**
     * Test a proximity query with padded white space between terms.
     */
    @Test
    public void testValidWithPaddedWhiteSpaceBetween() throws Exception {
        givenQuery("FIELD:\"term1             term2       term3\"~3");
        assertResult();
    }

    /**
     * Test a proximity query with padded white space on both left and right sides.
     */
    @Test
    public void testValidWithPaddedWhiteSpaceBothSides() throws Exception {
        givenQuery("FIELD:\"            term1 term2 term3      \"~3");
        assertResult();
    }

    /**
     * Test an invalid proximity query with padded white space on the left and insufficient slop value.
     */
    @Test
    public void testInvalidWithPaddedWhiteSpaceLeft() throws Exception {
        givenQuery("FIELD:\"                  term1 term2 term3\"~2");
        expectMessage("Invalid slop proximity, the " + 2 + " should be " + 3 + " or greater: FIELD:\"                  term1 term2 term3\"~" + 2);
        assertResult();
    }

    /**
     * Test an invalid proximity query with padded white space on the right and insufficient slop value.
     */
    @Test
    public void testInvalidWithPaddedWhiteSpaceRight() throws Exception {
        givenQuery("FIELD:\"term1 term2 term3                  \"~2");
        expectMessage("Invalid slop proximity, the " + 2 + " should be " + 3 + " or greater: FIELD:\"term1 term2 term3                  \"~" + 2);
        assertResult();
    }

    /**
     * Test an invalid proximity query with padded white space between terms and insufficient slop value.
     */
    @Test
    public void testInvalidWithPaddedWhiteSpaceBetween() throws Exception {
        givenQuery("FIELD:\"term1             term2       term3\"~2");
        expectMessage("Invalid slop proximity, the " + 2 + " should be " + 3 + " or greater: FIELD:\"term1             term2       term3\"~" + 2);
        assertResult();
    }

    /**
     * Test an invalid proximity query with padded white space on both sides and insufficient slop value.
     */
    @Test
    public void testInvalidWithPaddedWhiteSpaceBothSides() throws Exception {
        givenQuery("FIELD:\"            term1 term2 term3      \"~2");
        expectMessage("Invalid slop proximity, the " + 2 + " should be " + 3 + " or greater: FIELD:\"            term1 term2 term3      \"~" + 2);
        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToLucene();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new MinimumSlopProximityRule(ruleName);
    }
}
