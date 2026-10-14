package datawave.query.planner.replacement;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.planner.replacement.rules.DirectFieldReplacementRule;
import datawave.query.planner.replacement.rules.FieldReplacementRule;
import datawave.query.planner.replacement.rules.RangeFieldReplacementRule;

public class FieldReplacementVisitorTest {
    private static final Logger log = Logger.getLogger(FieldReplacementVisitorTest.class);
    private static final DirectFieldReplacementRule dfrRule = new DirectFieldReplacementRule("ABC", "XYZ");
    private static final Map<String,String> rangeMap = Map.of("AA", "BB", "CC", "DD");
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
        String query = "(_Bounded_ = true) && (AA >= '2' && AA <= '4')";
        String expected = "((_Eval_ = true) && ((_Bounded_ = true) && (AA >= '2' && AA <= '4'))) && " +
                "((_Bounded_ = true) && (BB >= '2' && BB <= '4'))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), true);
    }

    @Test
    public void rangeFieldReplacementWithDecimalsTest() throws Exception {
        // @formatter:off
        String query = "(_Bounded_ = true) && (AA >= '2.12' && AA <= '2.24')";
        String expected = "((_Eval_ = true) && ((_Bounded_ = true) && (AA >= '2.12' && AA <= '2.24'))) &&" +
                "((_Bounded_ = true) && (BB >= '2.12' && BB <= '2.24'))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), true);
    }

    @Test
    public void rangeFieldReplacementInLargerQueryTest() throws Exception {
        // @formatter:off
        String query = "(AA == '6') || ((_Bounded_ = true) && (AA >= '2' && AA <= '4'))";
        String expected = "(AA == '6') || " +
                "(((_Eval_ = true) && ((_Bounded_ = true) && (AA >= '2' && AA <= '4'))) && " +
                "((_Bounded_ = true) && (BB >= '2' && BB <= '4')))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), true);
    }

    @Test
    public void rangeFieldReplacementWithMultipleRangesTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (CC >= '2' && CC <= '4')) || ((_Bounded_ = true) && (AA >= '2' && AA <= '4'))";
        String expected = "(((_Eval_ = true) && ((_Bounded_ = true) && (CC >= '2' && CC <= '4'))) && " +
                "((_Bounded_ = true) && (DD >= '2' && DD <= '4'))) || " +
                "(((_Eval_ = true) && ((_Bounded_ = true) && (AA >= '2' && AA <= '4'))) && " +
                "((_Bounded_ = true) && (BB >= '2' && BB <= '4')))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), true);
    }

    @Test
    public void directFieldReplacementTest() throws Exception {
        // @formatter:off
        String query = "ABC == 'x'";
        String expected = "XYZ == 'x'" ;
        // @formatter:on
        testReplacement(query, expected, List.of(dfrRule), false);
    }

    @Test
    public void multiRuleReplacementTest() throws Exception {
        // @formatter:off
        String query = "(ABC = 6) || ((_Bounded_ = true) && (AA >= '2' && AA <= '4'))";
        String expected = "(XYZ = 6) || " +
                "(((_Eval_ = true) && ((_Bounded_ = true) && (AA >= '2' && AA <= '4'))) && " +
                "((_Bounded_ = true) && (BB >= '2' && BB <= '4')))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule, dfrRule), true);
    }

    @Test
    public void onlyExactStringsAreReplacedTest() throws Exception {
        // @formatter:off
        String query = "ABCD == 'x'";
        String expected = "ABCD == 'x'" ;
        // @formatter:on
        testReplacement(query, expected, List.of(dfrRule), false);

        // @formatter:off
        query = "TABC == 'x'";
        expected = "TABC == 'x'" ;
        // @formatter:on
        testReplacement(query, expected, List.of(dfrRule), false);

        // @formatter:off
        query = "(_Bounded_ = true) && (AA1 >= '2' && AA1 <= '4')";
        expected = "(_Bounded_ = true) && (AA1 >= '2' && AA1 <= '4')" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), false);

        // @formatter:off
        query = "(_Bounded_ = true) && (RAA >= '2' && RAA <= '4')";
        expected = "(_Bounded_ = true) && (RAA >= '2' && RAA <= '4')" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), false);
    }

    @Test
    public void onlyBoundedRangesAreReplacedTest() throws Exception {
        // @formatter:off
        String query = "(AA >= '2' && AA <= '4')";
        String expected = "(AA >= '2' && AA <= '4')" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule), false);
    }

    public class RangeVerificationVisitor extends RebuildingVisitor {
        private int rangesFound = 0;

        @Override
        public Object visit(ASTAndNode node, Object data) {
            if (JexlASTHelper.findRange().isRange(node)) {
                rangesFound++;
            }
            return super.visit(node, data);
        }

        public int getRangesFound() {
            return rangesFound;
        }
    }
}
