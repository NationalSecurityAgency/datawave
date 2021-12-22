package datawave.query.jexl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.query.jexl.JexlNodeFactory.ContainerType;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static junit.framework.TestCase.assertNull;

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
    public void testGetLiteralValueSafely() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == FOO2");
        assertNull(JexlASTHelper.getLiteralValueSafely(query));
    }
    
    @Test(expected = NoSuchElementException.class)
    public void testGetLiteralValueThrowsNSEE() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == FOO2");
        assertNull(JexlASTHelper.getLiteralValue(query));
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
    public void testDereferenceIntersection() throws ParseException {
        String query = "(FOO == 'a' && FOO == 'b')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode child = script.jjtGetChild(0);
        
        JexlNode test = JexlASTHelper.dereference(child);
        Assert.assertEquals("FOO == 'a' && FOO == 'b'", JexlStringBuildingVisitor.buildQueryWithoutParse(test));
    }
    
    @Test
    public void testDereferenceUnion() throws ParseException {
        String query = "(FOO == 'a' || FOO == 'b')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode child = script.jjtGetChild(0);
        
        JexlNode test = JexlASTHelper.dereference(child);
        Assert.assertEquals("FOO == 'a' || FOO == 'b'", JexlStringBuildingVisitor.buildQueryWithoutParse(test));
    }
    
    @Test
    public void testDereferenceMarkerNode() throws ParseException {
        String query = "(((((_Value_ = true) && (FOO =~ 'a.*')))))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode child = script.jjtGetChild(0);
        
        JexlNode test = JexlASTHelper.dereference(child);
        Assert.assertEquals("(_Value_ = true) && (FOO =~ 'a.*')", JexlStringBuildingVisitor.buildQueryWithoutParse(test));
        // Note: this is bad. In a larger intersection with other terms, we have now effectively lost which term is marked
        // Example: (_Value_ = true) && (FOO =~ 'a.*') && FOO2 == 'bar' && FOO3 == 'baz'
    }
    
    // dereference marked node while preserving the final wrapper layer
    @Test
    public void testDereferenceMarkerNodeSafely() throws ParseException {
        String query = "(((((_Value_ = true) && (FOO =~ 'a.*')))))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode child = script.jjtGetChild(0);
        
        JexlNode test = JexlASTHelper.dereferenceSafely(child);
        Assert.assertEquals("((_Value_ = true) && (FOO =~ 'a.*'))", JexlStringBuildingVisitor.buildQueryWithoutParse(test));
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
    public void testFindRange() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((_Bounded_ = true) && (A < 'b' && A > 'a')) && !(FOO == 'bar')");
        
        LiteralRange range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));
        
        Assert.assertNull(range);
        
        script = JexlASTHelper.parseJexlQuery("(A < 5 && A > 1)");
        
        range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));
        
        Assert.assertNull(range);
        
        script = JexlASTHelper.parseJexlQuery("((_Bounded_ = true) && (A < 5 && A > 1))");
        
        range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));
        
        Assert.assertNotNull(range);
        Assert.assertNotNull(range.getLowerNode());
        Assert.assertNotNull(range.getUpperNode());
        Assert.assertEquals(1, range.getLower());
        Assert.assertEquals(5, range.getUpper());
        Assert.assertFalse(range.isLowerInclusive());
        Assert.assertFalse(range.isUpperInclusive());
        
        script = JexlASTHelper.parseJexlQuery("((_Bounded_ = true) && (A <= 5 && A >= 1))");
        
        range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));
        
        Assert.assertNotNull(range);
        Assert.assertNotNull(range.getLowerNode());
        Assert.assertNotNull(range.getUpperNode());
        Assert.assertEquals(1, range.getLower());
        Assert.assertEquals(5, range.getUpper());
        Assert.assertTrue(range.isLowerInclusive());
        Assert.assertTrue(range.isUpperInclusive());
    }
    
    @Test
    public void testFindDelayedRange() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((_Delayed_ = true) && ((_Bounded_ = true) && (A < 'b' && A > 'a'))) && !(FOO == 'bar')");
        
        LiteralRange range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));
        
        Assert.assertNull(range);
        
        script = JexlASTHelper.parseJexlQuery("((_Delayed_ = true) && ((_Bounded_ = true) && (A < 'b' && A > 'a')))");
        
        range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));
        
        Assert.assertNotNull(range);
        Assert.assertNotNull(range.getLowerNode());
        Assert.assertNotNull(range.getUpperNode());
        Assert.assertEquals("a", range.getLower());
        Assert.assertEquals("b", range.getUpper());
        Assert.assertFalse(range.isLowerInclusive());
        Assert.assertFalse(range.isUpperInclusive());
    }
    
    @Test
    public void testFindNotDelayedRange() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((_Delayed_ = true) && ((_Bounded_ = true) && (A < 'b' && A > 'a')))");
        
        LiteralRange range = JexlASTHelper.findRange().notDelayed().getRange(script.jjtGetChild(0));
        
        Assert.assertNull(range);
        
        script = JexlASTHelper.parseJexlQuery("((_Bounded_ = true) && (A < 5 && A > 1))");
        
        range = JexlASTHelper.findRange().notDelayed().getRange(script.jjtGetChild(0));
        
        Assert.assertNotNull(range);
        Assert.assertNotNull(range.getLowerNode());
        Assert.assertNotNull(range.getUpperNode());
        Assert.assertEquals(1, range.getLower());
        Assert.assertEquals(5, range.getUpper());
        Assert.assertFalse(range.isLowerInclusive());
        Assert.assertFalse(range.isUpperInclusive());
    }
    
    @Test
    public void testFindIndexedRange() throws Exception {
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(Collections.singleton("A"));
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((_Bounded_ = true) && (A < 'b' && A > 'a'))");
        
        LiteralRange range = JexlASTHelper.findRange().indexedOnly(null, helper).getRange(script.jjtGetChild(0));
        
        Assert.assertNotNull(range);
        Assert.assertNotNull(range.getLowerNode());
        Assert.assertNotNull(range.getUpperNode());
        Assert.assertEquals("a", range.getLower());
        Assert.assertEquals("b", range.getUpper());
        Assert.assertFalse(range.isLowerInclusive());
        Assert.assertFalse(range.isUpperInclusive());
        
        script = JexlASTHelper.parseJexlQuery("B < 5 && B > 1");
        
        range = JexlASTHelper.findRange().indexedOnly(null, helper).getRange(script.jjtGetChild(0));
        
        Assert.assertNull(range);
    }
    
    @Test
    public void parse1TrailingBackslashEquals() throws Exception {
        String query = "CITY == 'city\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse1TrailingBackslashRegex() throws Exception {
        String query = "CITY =~ 'city\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse2TrailingBackslashesEquals() throws Exception {
        String query = "CITY == 'city\\\\\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse2TrailingBackslashesRegex() throws Exception {
        String query = "CITY =~ 'city\\\\\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse3TrailingBackslashesEquals() throws Exception {
        String query = "CITY == 'city\\\\\\\\\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse3TrailingBackslashesRegex() throws Exception {
        String query = "CITY =~ 'city\\\\\\\\\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse1LeadingBackslashEquals() throws Exception {
        String query = "CITY == '\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse1LeadingBackslashRegex() throws Exception {
        String query = "CITY =~ '\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse2LeadingBackslashesEquals() throws Exception {
        String query = "CITY == '\\\\\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse2LeadingBackslashesRegex() throws Exception {
        String query = "CITY =~ '\\\\\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse3LeadingBackslashesEquals() throws Exception {
        String query = "CITY == '\\\\\\\\\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse3LeadingBackslashesRegex() throws Exception {
        String query = "CITY =~ '\\\\\\\\\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse1InteriorBackslashEquals() throws Exception {
        String query = "CITY == 'ci\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse1InteriorBackslashRegex() throws Exception {
        String query = "CITY =~ 'ci\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse2InteriorBackslashesEquals() throws Exception {
        String query = "CITY == 'ci\\\\\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse2InteriorBackslashesRegex() throws Exception {
        String query = "CITY =~ 'ci\\\\\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse3InteriorBackslashesEquals() throws Exception {
        String query = "CITY == 'ci\\\\\\\\\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    @Test
    public void parse3InteriorBackslashesRegex() throws Exception {
        String query = "CITY =~ 'ci\\\\\\\\\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    // This test is here to ensure that we can freely convert between a jexl tree and
    // a query string, without impact to the string literal for the regex node.
    // WEB QUERY: CITY =~ 'ci\\\\\\ty\.blah'
    // StringLiteral.image: "ci\\\\\\ty\.blah"
    @Test
    public void transitiveRegexParseTest() throws Exception {
        String query = "CITY =~ 'ci\\\\\\\\\\\\ty\\.blah'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        JexlNode newNode = JexlASTHelper.parseJexlQuery(interpretedQuery);
        String reinterpretedQuery = JexlStringBuildingVisitor.buildQuery(newNode);
        Assert.assertEquals("CITY =~ 'ci\\\\\\\\\\\\ty\\.blah'", interpretedQuery);
        Assert.assertEquals(reinterpretedQuery, interpretedQuery);
        Assert.assertEquals("ci\\\\\\\\\\\\ty\\.blah", node.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image);
        Assert.assertEquals(node.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image, newNode.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image);
    }
    
    // This test is here to ensure that we can freely convert between a jexl tree and
    // a query string, without impact to the string literal for the regex node.
    // This also shows that an unescaped backslash (the one before '.blah') will be preserved between conversions.
    // WEB QUERY: CITY == 'ci\\\\\\ty\.blah'
    // StringLiteral.image: "ci\\\ty\.blah"
    @Test
    public void transitiveEqualsParseWithEscapedRegexTest() throws Exception {
        String query = "CITY == 'ci\\\\\\\\\\\\ty\\.blah'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        JexlNode newNode = JexlASTHelper.parseJexlQuery(interpretedQuery);
        String reinterpretedQuery = JexlStringBuildingVisitor.buildQuery(newNode);
        // note: while this is different from the original query, it produces the same string literal
        Assert.assertEquals("CITY == 'ci\\\\\\\\\\\\ty\\\\.blah'", interpretedQuery);
        Assert.assertEquals(reinterpretedQuery, interpretedQuery);
        Assert.assertEquals("ci\\\\\\ty\\.blah", node.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image);
        Assert.assertEquals(node.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image, newNode.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image);
    }
    
    // This is similar to the last test, but shows the usage of an explicit, escaped backslash before '.blah'
    // WEB QUERY: CITY == 'ci\\\\\\ty\\.blah'
    // StringLiteral.image: "ci\\\ty\.blah"
    @Test
    public void transitiveEqualsParseTest() throws Exception {
        String query = "CITY == 'ci\\\\\\\\\\\\ty\\\\.blah'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        JexlNode newNode = JexlASTHelper.parseJexlQuery(interpretedQuery);
        String reinterpretedQuery = JexlStringBuildingVisitor.buildQuery(newNode);
        // note: while this is different from the original query, it produces the same string literal
        Assert.assertEquals("CITY == 'ci\\\\\\\\\\\\ty\\\\.blah'", interpretedQuery);
        Assert.assertEquals(reinterpretedQuery, interpretedQuery);
        Assert.assertEquals("ci\\\\\\ty\\.blah", node.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image);
        Assert.assertEquals(node.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image, newNode.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image);
    }
    
    // This test ensures that the literal value of a regex node preserves the full number of backslashes as present in the query.
    // This is also testing that an escaped single quote is handled correctly for a regex node.
    // WEB QUERY: CITY =~ 'ci\\\\\\ty\\.bl\'ah'
    // StringLiteral.image: "ci\\\\\\ty\\.bl'ah"
    @Test
    public void parseRegexWithEscapedQuoteTest() throws Exception {
        String query = "CITY =~ 'ci\\\\\\\\\\\\ty\\\\.bl\\'ah'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        Assert.assertEquals("ci\\\\\\\\\\\\ty\\\\.bl'ah", node.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    // This test is is similar to the previous one, but with multiple backslashes before the embedded single quote.
    // WEB QUERY: CITY =~ 'ci\\\\\\ty\\.bl\\\'ah'
    // StringLiteral.image: "ci\\\\\\ty\\.bl\\'ah"
    @Test
    public void parseRegexWithEscapedQuoteAndBackslashesTest() throws Exception {
        String query = "CITY =~ 'ci\\\\\\\\\\\\ty\\\\.bl\\\\\\'ah'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        Assert.assertEquals("ci\\\\\\\\\\\\ty\\\\.bl\\\\'ah", node.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    // This test ensures that the literal value of an equals node has had the escape characters removed for each backslash.
    // This is also testing that an escaped single quote is handled correctly for an equals node.
    // WEB QUERY: CITY == 'ci\\\\\\ty\\.bl\'ah'
    // StringLiteral.image: "ci\\\ty\.bl'ah"
    @Test
    public void parseEqualsWithEscapedQuoteTest() throws Exception {
        String query = "CITY == 'ci\\\\\\\\\\\\ty\\\\.bl\\'ah'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        Assert.assertEquals("ci\\\\\\ty\\.bl'ah", node.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image);
        Assert.assertEquals(query, interpretedQuery);
    }
    
    // This test is is similar to the previous one, but with multiple backslashes before the embedded single quote.
    // WEB QUERY: CITY == 'ci\\\\\\ty\\.bl\\\'ah'
    // StringLiteral.image: "ci\\\ty\.bl\'ah"
    @Test
    public void parseEqualsWithEscapedQuoteAndBackslashesTest() throws Exception {
        String query = "CITY == 'ci\\\\\\\\\\\\ty\\\\.bl\\\\\\'ah'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        Assert.assertEquals("ci\\\\\\ty\\.bl\\'ah", node.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).image);
        Assert.assertEquals(query, interpretedQuery);
    }
}
