package datawave.query.planner.rules;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NodeTransformVisitorTest {
    
    private static final List<String> PATTERNS = Arrays.asList(new String[] {".\\.\\*", "\\.\\*.", "\\.\\*<[^<>]+>"});
    private static final RegexPushdownTransformRule regexPushdownRule = new RegexPushdownTransformRule();
    private static final RegexSimplifierTransformRule regexSimplifier = new RegexSimplifierTransformRule();
    private static final NodeTransformRule reverseAndRule = new NodeTransformRule() {
        @Override
        public JexlNode apply(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
            if (node instanceof ASTAndNode) {
                // reverse the children
                ArrayList<JexlNode> children = newArrayList();
                for (JexlNode child : children(node)) {
                    children.add(child);
                }
                Collections.reverse(children);
                return children(node, children.toArray(new JexlNode[0]));
            }
            return node;
        }
    };
    private static final NodeTransformRule pullUpRule = new NodeTransformRule() {
        @Override
        public JexlNode apply(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
            if (QueryPropertyMarker.instanceOf(node, null)) {
                return QueryPropertyMarker.getQueryPropertySource(node, null);
            }
            return node;
        }
    };
    
    @Before
    public void beforeTest() {
        regexPushdownRule.setRegexPatterns(Arrays.asList(new String[] {".\\.\\*", "\\.\\*.", "\\.\\*<[^<>]+>"}));
    }
    
    private void testPushdown(String query, String expected) throws Exception {
        testPushdown(query, expected, Collections.singletonList(regexPushdownRule));
    }
    
    private void testSimplify(String query, String expected) throws Exception {
        testPushdown(query, expected, Collections.singletonList(regexSimplifier));
    }
    
    private void testPushdown(String query, String expected, List<NodeTransformRule> rules) throws Exception {
        // create a query tree
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        MockMetadataHelper helper = new MockMetadataHelper();
        
        // apply the visitor
        script = NodeTransformVisitor.transform(script, rules, new ShardQueryConfiguration(), helper);
        
        // test the query tree
        String result = JexlStringBuildingVisitor.buildQuery(script);
        assertEquals("Unexpected transform", expected, result);
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
    public void regexPushdownAnyfieldTransformRuleTest() throws Exception {
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
                "BLA =~ 'a.*.*.*.*?.*?' && " +
                "BLA =~ '.*?.*?.*bla.*?.*?blabla' && " +
                "_ANYFIELD_ =~ '.*.*?.*?<bla>'";
        String expected = "BLA == '.*?.*?x' && " +
                "BLA =~ 'ab.*?' && " +
                "BLA =~ 'a.*?' && " +
                "BLA =~ '.*?bla.*?blabla' && " +
                "_ANYFIELD_ =~ '.*?<bla>'";
        // @formatter:on
        testSimplify(query, expected);
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
