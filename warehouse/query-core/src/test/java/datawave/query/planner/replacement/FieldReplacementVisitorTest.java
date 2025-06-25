package datawave.query.planner.replacement;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.planner.replacement.rules.DirectFieldReplacementRule;
import datawave.query.planner.replacement.rules.FieldReplacementRule;
import datawave.query.planner.replacement.rules.RangeFieldReplacementRule;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class FieldReplacementVisitorTest {
    private static final Logger log = Logger.getLogger(FieldReplacementVisitorTest.class);
    private static final DirectFieldReplacementRule dfrRule = new DirectFieldReplacementRule("HAN", "SOLO");
    private static final Map<String, String> rangeMap = Map.of("R2", "D2", "STAR", "DESTROYER");
    private static final RangeFieldReplacementRule rfrRule = new RangeFieldReplacementRule(rangeMap);


    private void testReplacement(String original, String expected, List<FieldReplacementRule> rules) throws Exception {
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
    public void regexPushdowqnTransformRuleTest() throws Exception {
        // @formatter:off
        String query = "R2 = 2 && ((_Bounded_ = true) && (R2 >= '2' && R2 <= '3'))";
        String expected = "R2 = 2 && ((_Bounded_ = true) && (D2 >= '2' && D2 <= '3'))" ;
        // @formatter:on
        testReplacement(query, expected, List.of(rfrRule));
    }

    @Test
    public void regexPushdownTransformRuleTest() throws Exception {
        // @formatter:off
        String query = "HAN == 'x'";
        String expected = "SOLO == 'x'" ;
        // @formatter:on
        testReplacement(query, expected, List.of(dfrRule));
    }

//    @Test
//    public void regexPushdownAnyfieldTransformRuleTest() {
//        // @formatter:off
//        String query = "BLA == 'x' && " +
//                "BLA =~ 'ab.*' && " +
//                "BLA =~ 'a.*' && " +
//                "BLA =~ 'okregex' && " +
//                "_ANYFIELD_ =~ '.*<bla>'";
//        String expected = "BLA == 'x' && " +
//                "BLA =~ 'ab.*' && " +
//                "((_Eval_ = true) && (BLA =~ 'a.*')) && " +
//                "BLA =~ 'okregex' && " +
//                "((_Eval_ = true) && (_ANYFIELD_ =~ '.*<bla>'))";
//        // @formatter:on
//        try {
//            testPushdown(query, expected);
//            fail("Expected anyfield regex pushdown to fail");
//        } catch (Exception e) {
//            // ok
//        }
//    }
//
//    @Test
//    public void regexSimplifierTransformRuleTest() throws Exception {
//        // @formatter:off
//        String query = "BLA == '.*?.*?x' && " +
//                "BLA =~ 'ab.*.*' && " +
//                "BLA !~ 'a.*.*.*.*?.*?' && " +
//                "BLA =~ '.*?.*?.*bla.*?.*?blabla' && " +
//                "_ANYFIELD_ =~ '.*.*?.*?<bla>' && " +
//                "filter:excludeRegex(BLA, '.*?.*?.*bla.*?.*?blabla') && " +
//                "filter:includeRegex(BLA, '.*?.*?.*bla.*?.*?blabla')";
//        String expected = "BLA == '.*?.*?x' && " +
//                "BLA =~ 'ab.*?' && " +
//                "BLA !~ 'a.*?' && " +
//                "BLA =~ '.*?bla.*?blabla' && " +
//                "_ANYFIELD_ =~ '.*?<bla>' && " +
//                "filter:excludeRegex(BLA, '.*?bla.*?blabla') && " +
//                "filter:includeRegex(BLA, '.*?bla.*?blabla')";
//        // @formatter:on
//        testSimplify(query, expected);
//    }
//
//    @Test
//    public void regexDotAllTransformRuleTest() throws Exception {
//        // @formatter:off
//        String query = "BLA == '(\\s|.)*' && " +
//                "BLA !~ '(.|\\s)*' && " +
//                "BLA =~ '(\\s|.)*word(.|\\s)*' &&" +
//                "filter:excludeRegex(BLA, '(\\s|.)*word(.|\\s)*') && " +
//                "filter:includeRegex(BLA, '(\\s|.)*word(.|\\s)*')";
//        String expected = "BLA == '(\\s|.)*' && " +
//                "BLA !~ '.*' && " +
//                "BLA =~ '.*word.*' &&" +
//                "filter:excludeRegex(BLA, '.*word.*') && " +
//                "filter:includeRegex(BLA, '.*word.*')";
//        // @formatter:on
//        testDotall(query, expected);
//    }
//
//    @Test
//    public void skipQueryMarkersTest() throws Exception {
//        // @formatter:off
//        String query = "BLA == 'x' && " +
//                "BLA =~ 'ab.*' && (" +
//                "(_Value_ = true) && (BLA =~ 'a.*')) && " +
//                "((_Value_ = true) && (BLA =~ 'okregex')) && " +
//                "BLA =~ '.*<bla>'";
//        String expected = "BLA == 'x' && " +
//                "BLA =~ 'ab.*' && " +
//                "((_Value_ = true) && (BLA =~ 'a.*')) && " +
//                "((_Value_ = true) && (BLA =~ 'okregex')) && " +
//                "((_Eval_ = true) && (BLA =~ '.*<bla>'))";
//        // @formatter:on
//        testPushdown(query, expected);
//    }
//
//    @Test
//    public void depthTest() throws Exception {
//        // @formatter:off
//        String query = "(((BLA == 'x' && " +
//                "BLA =~ 'ab.*' && " +
//                "BLA =~ 'a.*') && " +
//                "((BLA =~ 'okregex'))) && " +
//                "BLA =~ '.*<bla>')";
//        String expected = "(((_Eval_ = true) && (BLA =~ '.*<bla>')) && " +
//                "(((BLA =~ 'okregex')) && " +
//                "(((_Eval_ = true) && (BLA =~ 'a.*')) && " +
//                "BLA =~ 'ab.*' && " +
//                "BLA == 'x')))";
//        // @formatter:on
//        testPushdown(query, expected, newArrayList(regexPushdownRule, reverseAndRule));
//    }
//
//    @Test
//    public void testANDNodeTransform() throws Exception {
//        // @formatter:off
//        String query = "BLA == 'x' && " +
//                "BLA =~ 'ab.*' && " +
//                "BLA =~ 'a.*' && " +
//                "BLA =~ 'okregex' && " +
//                "BLA =~ '.*<bla>'";
//        String expected = "((_Eval_ = true) && (BLA =~ '.*<bla>')) && " +
//                "BLA =~ 'okregex' && " +
//                "((_Eval_ = true) && (BLA =~ 'a.*')) && " +
//                "BLA =~ 'ab.*' && " +
//                "BLA == 'x'";
//        // @formatter:on
//        testPushdown(query, expected, newArrayList(regexPushdownRule, reverseAndRule));
//    }
//
//    @Test
//    public void testTransformOrder() throws Exception {
//        // @formatter:off
//        String query = "BLA == 'x' && " +
//                "BLA =~ 'ab.*' && " +
//                "BLA =~ 'a.*' && " +
//                "BLA =~ 'okregex' && " +
//                "BLA =~ '.*<bla>'";
//        String expected1 = "BLA =~ '.*<bla>' && " +
//                "BLA =~ 'okregex' && " +
//                "BLA =~ 'a.*' && " +
//                "BLA =~ 'ab.*' && " +
//                "BLA == 'x'";
//        String expected2 = "((_Eval_ = true) && (BLA =~ '.*<bla>')) && " +
//                "BLA =~ 'okregex' && " +
//                "((_Eval_ = true) && (BLA =~ 'a.*')) && " +
//                "BLA =~ 'ab.*' && " +
//                "BLA == 'x'";
//        // @formatter:on
//        testPushdown(query, expected1, newArrayList(regexPushdownRule, reverseAndRule, pullUpRule));
//        testPushdown(query, expected2, newArrayList(pullUpRule, reverseAndRule, regexPushdownRule));
//    }
}
