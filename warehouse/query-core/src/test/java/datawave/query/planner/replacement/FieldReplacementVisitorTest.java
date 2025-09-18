package datawave.query.planner.replacement;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.planner.replacement.rules.DirectFieldReplacementRule;
import datawave.query.planner.replacement.rules.FieldReplacementRule;
import datawave.query.planner.replacement.rules.RangeFieldReplacementRule;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class FieldReplacementVisitorTest {
    private static final Logger log = Logger.getLogger(FieldReplacementVisitorTest.class);
    private static final DirectFieldReplacementRule dfrRule = new DirectFieldReplacementRule("HAN", "SOLO");
    private static final Map<String, String> rangeMap = Map.of("R2", "D2", "C3", "PO");
    private static final RangeFieldReplacementRule rfrRule = new RangeFieldReplacementRule(rangeMap);

    private void testReplacement(String original, String expected, List<FieldReplacementRule> rules, boolean checkRange) throws Exception {
        // create a query tree
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);

        // apply the visitor
        ASTJexlScript resultScript = FieldReplacementVisitor.apply(originalScript, rules);

        // Verify the script is as expected, and has a valid lineage.
        assertScriptEquality(resultScript, expected);
        assertLineage(resultScript);

        // Verify the original script was not modified, and still has a valid lineage.
        assertScriptEquality(originalScript, original);
        assertLineage(originalScript);

        if (checkRange) {
            RangeVerificationVisitor resultVisitor = new RangeVerificationVisitor();
            resultScript.jjtAccept(resultVisitor, null);

            ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
            RangeVerificationVisitor expectedVistor = new RangeVerificationVisitor();
            expectedScript.jjtAccept(expectedVistor, null);

            Assert.assertEquals(expectedVistor.getRangesFound(), resultVisitor.getRangesFound());
            Assert.assertTrue(resultVisitor.getRangesFound() > 0);
        }

    }

    private void assertScriptEquality(ASTJexlScript actualScript, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, actualScript);
        if (!comparison.isEqual()) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actualScript));
        }
        assertTrue(comparison.getReason(), comparison.isEqual());
    }

    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }

    @Test
    public void rangeFieldReplacementTest() throws Exception {
        // @formatter:off
        String query = "(_Bounded_ = true) && (R2 >= '2' && R2 <= '4')";
        String expected = "((_Eval_ = true) && ((_Bounded_ = true) && (R2 >= '2' && R2 <= '4'))) && " +
                "((_Bounded_ = true) && (D2 >= '2' && D2 <= '4'))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), true);
    }

    @Test
    public void rangeFieldReplacementWithDecimalsTest() throws Exception {
        // @formatter:off
        String query = "(_Bounded_ = true) && (R2 >= '2.12' && R2 <= '2.24')";
        String expected = "((_Eval_ = true) && ((_Bounded_ = true) && (R2 >= '2.12' && R2 <= '2.24'))) &&" +
                "((_Bounded_ = true) && (D2 >= '2.12' && D2 <= '2.24'))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), true);
    }

    @Test
    public void rangeFieldReplacementInLargerQueryTest() throws Exception {
        // @formatter:off
        String query = "(R2 == '6') || ((_Bounded_ = true) && (R2 >= '2' && R2 <= '4'))";
        String expected = "(R2 == '6') || " +
                "(((_Eval_ = true) && ((_Bounded_ = true) && (R2 >= '2' && R2 <= '4'))) && " +
                "((_Bounded_ = true) && (D2 >= '2' && D2 <= '4')))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), true);
    }

    @Test
    public void rangeFieldReplacementWithMultipleRangesTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (C3 >= '2' && C3 <= '4')) || ((_Bounded_ = true) && (R2 >= '2' && R2 <= '4'))";
        String expected = "(((_Eval_ = true) && ((_Bounded_ = true) && (C3 >= '2' && C3 <= '4'))) && " +
                "((_Bounded_ = true) && (PO >= '2' && PO <= '4'))) || " +
                "(((_Eval_ = true) && ((_Bounded_ = true) && (R2 >= '2' && R2 <= '4'))) && " +
                "((_Bounded_ = true) && (D2 >= '2' && D2 <= '4')))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), true);
    }

    @Test
    public void directFieldReplacementTest() throws Exception {
        // @formatter:off
        String query = "HAN == 'x'";
        String expected = "SOLO == 'x'" ;
        // @formatter:on
        testReplacement(query, expected, List.of(dfrRule), false);
    }

    @Test
    public void multiRuleReplacementTest() throws Exception {
        // @formatter:off
        String query = "(HAN = 6) || ((_Bounded_ = true) && (R2 >= '2' && R2 <= '4'))";
        String expected = "(SOLO = 6) || " +
                "(((_Eval_ = true) && ((_Bounded_ = true) && (R2 >= '2' && R2 <= '4'))) && " +
                "((_Bounded_ = true) && (D2 >= '2' && D2 <= '4')))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule, dfrRule), true);
    }

    @Test
    public void onlyExactStringsAreReplacedTest() throws Exception {
        // @formatter:off
        String query = "HAND == 'x'";
        String expected = "HAND == 'x'" ;
        // @formatter:on
        testReplacement(query, expected, List.of(dfrRule), false);

        // @formatter:off
        query = "THAN == 'x'";
        expected = "THAN == 'x'" ;
        // @formatter:on
        testReplacement(query, expected, List.of(dfrRule), false);

        // @formatter:off
        query = "(_Bounded_ = true) && (R21 >= '2' && R21 <= '4')";
        expected = "(_Bounded_ = true) && (R21 >= '2' && R21 <= '4')" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), false);

        // @formatter:off
        query = "(_Bounded_ = true) && (RR2 >= '2' && RR2 <= '4')";
        expected = "(_Bounded_ = true) && (RR2 >= '2' && RR2 <= '4')" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), false);
    }

    @Test
    public void onlyBoundedRangesAreReplacedTest() throws Exception {
        // @formatter:off
        String query = "(R2 >= '2' && R2 <= '4')";
        String expected = "(R2 >= '2' && R2 <= '4')" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), false);
    }

    public class RangeVerificationVisitor extends BaseVisitor {
        private int rangesFound = 0;

        @Override
        public Object visit(ASTAndNode node, Object data) {
            if (JexlASTHelper.findRange().isRange(node)) {
                rangesFound++;
            }
            return node;
        }

        public int getRangesFound() {
            return rangesFound;
        }
    }
}
