package datawave.query.rewrite.jexl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.query.rewrite.jexl.JexlNodeFactory.ContainerType;
import datawave.query.rewrite.jexl.visitors.PrintingVisitor;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JexlASTHelperTest {
    
    private static final Logger log = Logger.getLogger(JexlASTHelperTest.class);
    
    @Test
    public void test() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == 'bar' and (FOO == 'bar' and FOO == 'bar')");
        List<ASTEQNode> eqNodes = JexlASTHelper.getEQNodes(query);
        
        for (JexlNode eqNode : eqNodes) {
            Assert.assertFalse(JexlASTHelper.isWithinOr(eqNode));
        }
    }
    
    @Test
    public void test1() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == '1' and (FOO == '2' and (FOO == '3' or FOO == '4'))");
        List<ASTEQNode> eqNodes = JexlASTHelper.getEQNodes(query);
        
        Map<String,Boolean> expectations = Maps.newHashMap();
        expectations.put("1", false);
        expectations.put("2", false);
        expectations.put("3", true);
        expectations.put("4", true);
        
        for (JexlNode eqNode : eqNodes) {
            String value = JexlASTHelper.getLiteralValue(eqNode).toString();
            Assert.assertTrue(expectations.containsKey(value));
            
            Assert.assertEquals(expectations.get(value), JexlASTHelper.isWithinOr(eqNode));
        }
    }
    
    @Test
    public void test2() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == '1' and (FOO == '2' or (FOO == '3' and FOO == '4'))");
        List<ASTEQNode> eqNodes = JexlASTHelper.getEQNodes(query);
        
        Map<String,Boolean> expectations = Maps.newHashMap();
        expectations.put("1", false);
        expectations.put("2", true);
        expectations.put("3", true);
        expectations.put("4", true);
        
        for (JexlNode eqNode : eqNodes) {
            String value = JexlASTHelper.getLiteralValue(eqNode).toString();
            Assert.assertTrue(expectations.containsKey(value));
            
            Assert.assertEquals(expectations.get(value), JexlASTHelper.isWithinOr(eqNode));
        }
    }
    
    @Test
    public void test3() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == '1'");
        List<ASTEQNode> eqNodes = JexlASTHelper.getEQNodes(query);
        
        Map<String,Boolean> expectations = Maps.newHashMap();
        expectations.put("1", false);
        
        for (JexlNode eqNode : eqNodes) {
            String value = JexlASTHelper.getLiteralValue(eqNode).toString();
            Assert.assertTrue(expectations.containsKey(value));
            
            Assert.assertEquals(expectations.get(value), JexlASTHelper.isWithinOr(eqNode));
        }
    }
    
    @Test
    public void test4() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO =~ '1' and (FOO == '2' or (FOO =~ '3' and FOO == '4'))");
        
        List<ASTEQNode> eqNodes = JexlASTHelper.getEQNodes(query);
        
        Map<String,Boolean> expectations = Maps.newHashMap();
        expectations.put("2", true);
        expectations.put("4", true);
        
        for (JexlNode eqNode : eqNodes) {
            String value = JexlASTHelper.getLiteralValue(eqNode).toString();
            Assert.assertTrue(expectations.containsKey(value));
            
            Assert.assertEquals(expectations.get(value), JexlASTHelper.isWithinOr(eqNode));
        }
        
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(query);
        
        expectations = Maps.newHashMap();
        expectations.put("1", false);
        expectations.put("3", true);
        
        for (JexlNode erNode : erNodes) {
            String value = JexlASTHelper.getLiteralValue(erNode).toString();
            Assert.assertTrue(expectations.containsKey(value));
            
            Assert.assertEquals(expectations.get(value), JexlASTHelper.isWithinOr(erNode));
        }
    }
    
    @Test
    public void sameJexlNodeEquality() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == '1'");
        
        Assert.assertTrue(JexlASTHelper.equals(query, query));
    }
    
    @Test
    public void sameParsedJexlNodeEquality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1'");
        ASTJexlScript two = JexlASTHelper.parseJexlQuery("FOO == '1'");
        
        Assert.assertTrue(JexlASTHelper.equals(one, two));
    }
    
    @Test
    public void jexlNodeInequality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1'");
        ASTJexlScript two = JexlASTHelper.parseJexlQuery("'1' == '1'");
        
        Assert.assertFalse(JexlASTHelper.equals(one, two));
    }
    
    @Test
    public void nullJexlNodeEquality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1'");
        ASTJexlScript two = null;
        
        Assert.assertFalse(JexlASTHelper.equals(one, two));
        
        Assert.assertFalse(JexlASTHelper.equals(two, one));
    }
    
    @Test
    public void jexlNodeOrderInequality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1'");
        ASTJexlScript two = JexlASTHelper.parseJexlQuery("'1' == FOO");
        
        Assert.assertFalse(JexlASTHelper.equals(one, two));
    }
    
    @Test
    public void nestedJexlNodeOrderEquality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1' && (BAR == 'bar' || BAR == 'baz')");
        ASTJexlScript two = JexlASTHelper.parseJexlQuery("FOO == '1' && (BAR == 'bar' || BAR == 'baz')");
        
        Assert.assertTrue(JexlASTHelper.equals(one, two));
    }
    
    @Test
    public void nestedJexlNodeOrderInequality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1' && (BAR == 'bar' || BAR == 'baz')");
        ASTJexlScript two = JexlASTHelper.parseJexlQuery("FOO == '1' && (BAR == 'baz' || BAR == 'bar')");
        
        Assert.assertFalse(JexlASTHelper.equals(one, two));
        
        ASTJexlScript three = JexlASTHelper.parseJexlQuery("(BAR == 'bar' || BAR == 'baz') && FOO == '1'");
        
        Assert.assertFalse(JexlASTHelper.equals(two, three));
    }
    
    @Test
    public void manualNestedJexlNodeOrderEquality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("(FOO == '1' && (BAR == 'bar' || BAR == 'baz'))");
        
        JexlNode or = JexlNodeFactory.createNodeTreeFromFieldValues(ContainerType.OR_NODE, new ASTEQNode(ParserTreeConstants.JJTEQNODE), new ASTEQNode(
                        ParserTreeConstants.JJTEQNODE), "BAR", Lists.newArrayList("bar", "baz"));
        JexlNode and = JexlNodeFactory.createAndNode(Lists.newArrayList(JexlNodeFactory.buildEQNode("FOO", "1"), or));
        
        ASTJexlScript two = JexlNodeFactory.createScript(and);
        
        Assert.assertTrue(JexlASTHelper.equals(one, two));
    }
    
    @Test
    public void testNormalizeLiteral() throws Throwable {
        LcNoDiacriticsType normalizer = new LcNoDiacriticsType();
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("F == 'aSTrInG'");
        if (log.isDebugEnabled()) {
            PrintingVisitor.printQuery(script);
        }
        JexlNode literal = script.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0);
        ASTReference ref = JexlASTHelper.normalizeLiteral(literal, normalizer);
        Assert.assertEquals("astring", ref.jjtGetChild(0).image);
    }
    
    @Test
    public void testFindLiteral() throws Throwable {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("i == 10");
        if (log.isDebugEnabled()) {
            PrintingVisitor.printQuery(script);
        }
        JexlNode literal = JexlASTHelper.findLiteral(script);
        Assert.assertTrue(literal instanceof ASTNumberLiteral);
    }
    
    @Test
    public void testApplyNormalization() throws Throwable {
        {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery("F == 'aSTrInG'");
            if (log.isDebugEnabled()) {
                PrintingVisitor.printQuery(script);
            }
            JexlASTHelper.applyNormalization(script.jjtGetChild(0), new LcNoDiacriticsType());
            if (log.isDebugEnabled()) {
                PrintingVisitor.printQuery(script);
            }
        }
        
        {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery("F == 7");
            if (log.isDebugEnabled()) {
                PrintingVisitor.printQuery(script);
            }
            JexlASTHelper.applyNormalization(script.jjtGetChild(0), new NumberType());
            if (log.isDebugEnabled()) {
                PrintingVisitor.printQuery(script);
            }
        }
    }
    
    @Test
    public void testNonRangeNodeNegation() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A < 'b' && A > 'a' && !(FOO == 'bar')");
        
        List<JexlNode> nonRangeChildNodes = new ArrayList<>();
        Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper
                        .getBoundedRangesIndexAgnostic((ASTAndNode) (script.jjtGetChild(0)), nonRangeChildNodes, true);
        
        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(2, ranges.values().iterator().next().size());
        Assert.assertEquals(1, nonRangeChildNodes.size());
        Assert.assertEquals(ASTNotNode.class, nonRangeChildNodes.get(0).getClass());
        
        script = JexlASTHelper.parseJexlQuery("A < 5 && A > 1 && !(FOO == 'bar' && !(BAR == 'foo') && BAR != 'foo')");
        
        nonRangeChildNodes.clear();
        ranges = JexlASTHelper.getBoundedRangesIndexAgnostic((ASTAndNode) (script.jjtGetChild(0)), nonRangeChildNodes, true);
        
        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(2, ranges.values().iterator().next().size());
        Assert.assertEquals(1, nonRangeChildNodes.size());
        Assert.assertEquals(ASTNotNode.class, nonRangeChildNodes.get(0).getClass());
        
        script = JexlASTHelper
                        .parseJexlQuery("A < 5 && A > 1 && !(FOO == 'term' && !(BAR =~ 'regex') && BAR !~ 'regex' && (BAR != 'term') && ! (BAR == 'term') && FOO =~ 'regex')");
        
        nonRangeChildNodes.clear();
        ranges = JexlASTHelper.getBoundedRangesIndexAgnostic((ASTAndNode) (script.jjtGetChild(0)), nonRangeChildNodes, true);
        
        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(2, ranges.values().iterator().next().size());
        Assert.assertEquals(1, nonRangeChildNodes.size());
        Assert.assertEquals(ASTNotNode.class, nonRangeChildNodes.get(0).getClass());
    }
    
}
