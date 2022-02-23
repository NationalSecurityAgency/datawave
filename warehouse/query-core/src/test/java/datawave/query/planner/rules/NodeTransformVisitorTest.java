package datawave.query.planner.rules;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NodeTransformVisitorTest {
    
    private static final Logger log = Logger.getLogger(NodeTransformVisitorTest.class);
    private static final RegexPushdownTransformRule regexPushdownRule = new RegexPushdownTransformRule();
    private static final RegexSimplifierTransformRule regexSimplifier = new RegexSimplifierTransformRule();
    private static final RegexDotallTransformRule regexDotall = new RegexDotallTransformRule();
    private static final NodeTransformRule reverseAndRule = new NodeTransformRule() {
        @Override
        public JexlNode apply(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
            if (node instanceof ASTAndNode) {
                // reverse the children
                ArrayList<JexlNode> children = newArrayList();
                children.addAll(Arrays.asList(children(node)));
                Collections.reverse(children);
                return children(node, children.toArray(new JexlNode[0]));
            }
            return node;
        }
    };
    private static final NodeTransformRule pullUpRule = new NodeTransformRule() {
        @Override
        public JexlNode apply(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
            QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
            if (instance.isAnyType()) {
                return instance.getSource();
            }
            return node;
        }
    };
    
    @Before
    public void beforeTest() {
        regexPushdownRule.setRegexPatterns(Arrays.asList(".\\.\\*", "\\.\\*.", "\\.\\*<[^<>]+>"));
    }
    
    private void testPushdown(String query, String expected) throws Exception {
        testPushdown(query, expected, Collections.singletonList(regexPushdownRule));
    }
    
    private void testSimplify(String query, String expected) throws Exception {
        testPushdown(query, expected, Collections.singletonList(regexSimplifier));
    }
    
    private void testDotall(String query, String expected) throws Exception {
        testPushdown(query, expected, Collections.singletonList(regexDotall));
    }
    
    private void testPushdown(String original, String expected, List<NodeTransformRule> rules) throws Exception {
        // create a query tree
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        MockMetadataHelper helper = new MockMetadataHelper();
        
        // apply the visitor
        ASTJexlScript resultScript = NodeTransformVisitor.transform(originalScript, rules, new ShardQueryConfiguration(), helper);
        
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
    public void regexPushdownTransformRuleTest() throws Exception {
        // @formatter:off
        String query = "BLA == 'x' && " +
                "BLA =~ 'ab.*' && " +
                "BLA =~ 'a.*' && " +
                "BLA =~ 'okregex' && " +
                "BLA =~ '.*<bla>'";
        String expected = "BLA == 'x' && " +
                "BLA =~ 'ab.*' && " +
                "((_Eval_ = true) && (BLA =~ 'a.*')) && " +
                "BLA =~ 'okregex' && " +
                "((_Eval_ = true) && (BLA =~ '.*<bla>'))";
        // @formatter:on
        testPushdown(query, expected);
    }
    
    @Test
    public void regexPushdownAnyfieldTransformRuleTest() {
        // @formatter:off
        String query = "BLA == 'x' && " +
                "BLA =~ 'ab.*' && " +
                "BLA =~ 'a.*' && " +
                "BLA =~ 'okregex' && " +
                "_ANYFIELD_ =~ '.*<bla>'";
        String expected = "BLA == 'x' && " +
                "BLA =~ 'ab.*' && " +
                "((_Eval_ = true) && (BLA =~ 'a.*')) && " +
                "BLA =~ 'okregex' && " +
                "((_Eval_ = true) && (_ANYFIELD_ =~ '.*<bla>'))";
        // @formatter:on
        try {
            testPushdown(query, expected);
            fail("Expected anyfield regex pushdown to fail");
        } catch (Exception e) {
            // ok
        }
    }
    
    @Test
    public void regexSimplifierTransformRuleTest() throws Exception {
        // @formatter:off
        String query = "BLA == '.*?.*?x' && " +
                "BLA =~ 'ab.*.*' && " +
                "BLA !~ 'a.*.*.*.*?.*?' && " +
                "BLA =~ '.*?.*?.*bla.*?.*?blabla' && " +
                "_ANYFIELD_ =~ '.*.*?.*?<bla>' && " +
                "filter:excludeRegex(BLA, '.*?.*?.*bla.*?.*?blabla') && " +
                "filter:includeRegex(BLA, '.*?.*?.*bla.*?.*?blabla')";
        String expected = "BLA == '.*?.*?x' && " +
                "BLA =~ 'ab.*?' && " +
                "BLA !~ 'a.*?' && " +
                "BLA =~ '.*?bla.*?blabla' && " +
                "_ANYFIELD_ =~ '.*?<bla>' && " +
                "filter:excludeRegex(BLA, '.*?bla.*?blabla') && " +
                "filter:includeRegex(BLA, '.*?bla.*?blabla')";
        // @formatter:on
        testSimplify(query, expected);
    }
    
    @Test
    public void regexDotAllTransformRuleTest() throws Exception {
        // @formatter:off
        String query = "BLA == '(\\s|.)*' && " +
                "BLA !~ '(.|\\s)*' && " +
                "BLA =~ '(\\s|.)*word(.|\\s)*' &&" +
                "filter:excludeRegex(BLA, '(\\s|.)*word(.|\\s)*') && " +
                "filter:includeRegex(BLA, '(\\s|.)*word(.|\\s)*')";
        String expected = "BLA == '(\\s|.)*' && " +
                "BLA !~ '.*' && " +
                "BLA =~ '.*word.*' &&" +
                "filter:excludeRegex(BLA, '.*word.*') && " +
                "filter:includeRegex(BLA, '.*word.*')";
        // @formatter:on
        testDotall(query, expected);
    }
    
    @Test
    public void skipQueryMarkersTest() throws Exception {
        // @formatter:off
        String query = "BLA == 'x' && " +
                "BLA =~ 'ab.*' && (" +
                "(_Value_ = true) && (BLA =~ 'a.*')) && " +
                "((_Value_ = true) && (BLA =~ 'okregex')) && " +
                "BLA =~ '.*<bla>'";
        String expected = "BLA == 'x' && " +
                "BLA =~ 'ab.*' && " +
                "((_Value_ = true) && (BLA =~ 'a.*')) && " +
                "((_Value_ = true) && (BLA =~ 'okregex')) && " +
                "((_Eval_ = true) && (BLA =~ '.*<bla>'))";
        // @formatter:on
        testPushdown(query, expected);
    }
    
    @Test
    public void depthTest() throws Exception {
        // @formatter:off
        String query = "(((BLA == 'x' && " +
                "BLA =~ 'ab.*' && " +
                "BLA =~ 'a.*') && " +
                "((BLA =~ 'okregex'))) && " +
                "BLA =~ '.*<bla>')";
        String expected = "(((_Eval_ = true) && (BLA =~ '.*<bla>')) && " +
                "(((BLA =~ 'okregex')) && " +
                "(((_Eval_ = true) && (BLA =~ 'a.*')) && " +
                "BLA =~ 'ab.*' && " +
                "BLA == 'x')))";
        // @formatter:on
        testPushdown(query, expected, newArrayList(regexPushdownRule, reverseAndRule));
    }
    
    @Test
    public void testANDNodeTransform() throws Exception {
        // @formatter:off
        String query = "BLA == 'x' && " +
                "BLA =~ 'ab.*' && " +
                "BLA =~ 'a.*' && " +
                "BLA =~ 'okregex' && " +
                "BLA =~ '.*<bla>'";
        String expected = "((_Eval_ = true) && (BLA =~ '.*<bla>')) && " +
                "BLA =~ 'okregex' && " +
                "((_Eval_ = true) && (BLA =~ 'a.*')) && " +
                "BLA =~ 'ab.*' && " +
                "BLA == 'x'";
        // @formatter:on
        testPushdown(query, expected, newArrayList(regexPushdownRule, reverseAndRule));
    }
    
    @Test
    public void testTransformOrder() throws Exception {
        // @formatter:off
        String query = "BLA == 'x' && " +
                "BLA =~ 'ab.*' && " +
                "BLA =~ 'a.*' && " +
                "BLA =~ 'okregex' && " +
                "BLA =~ '.*<bla>'";
        String expected1 = "BLA =~ '.*<bla>' && " +
                "BLA =~ 'okregex' && " +
                "BLA =~ 'a.*' && " +
                "BLA =~ 'ab.*' && " +
                "BLA == 'x'";
        String expected2 = "((_Eval_ = true) && (BLA =~ '.*<bla>')) && " +
                "BLA =~ 'okregex' && " +
                "((_Eval_ = true) && (BLA =~ 'a.*')) && " +
                "BLA =~ 'ab.*' && " +
                "BLA == 'x'";
        // @formatter:on
        testPushdown(query, expected1, newArrayList(regexPushdownRule, reverseAndRule, pullUpRule));
        testPushdown(query, expected2, newArrayList(pullUpRule, reverseAndRule, regexPushdownRule));
    }
}
