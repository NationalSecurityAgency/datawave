package datawave.query.jexl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.jexl.JexlNodeFactory.ContainerType;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.util.MockMetadataHelper;
import datawave.query.util.Tuple3;

public class JexlASTHelperTest {

    private static final Logger log = Logger.getLogger(JexlASTHelperTest.class);

    @Test
    public void test() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == 'bar' and (FOO == 'bar' and FOO == 'bar')");
        List<ASTEQNode> eqNodes = JexlASTHelper.getEQNodes(query);

        for (JexlNode eqNode : eqNodes) {
            assertFalse(JexlASTHelper.isDescendantOfOr(eqNode));
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
            assertTrue(expectations.containsKey(value));

            assertEquals(expectations.get(value), JexlASTHelper.isDescendantOfOr(eqNode));
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
            assertTrue(expectations.containsKey(value));

            assertEquals(expectations.get(value), JexlASTHelper.isDescendantOfOr(eqNode));
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
            assertTrue(expectations.containsKey(value));

            assertEquals(expectations.get(value), JexlASTHelper.isDescendantOfOr(eqNode));
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
            assertTrue(expectations.containsKey(value));

            assertEquals(expectations.get(value), JexlASTHelper.isDescendantOfOr(eqNode));
        }

        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(query);

        expectations = Maps.newHashMap();
        expectations.put("1", false);
        expectations.put("3", true);

        for (JexlNode erNode : erNodes) {
            String value = JexlASTHelper.getLiteralValue(erNode).toString();
            assertTrue(expectations.containsKey(value));
            assertEquals(expectations.get(value), JexlASTHelper.isDescendantOfOr(erNode));
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

        assertTrue(JexlASTHelper.equals(query, query));
    }

    @Test
    public void sameParsedJexlNodeEquality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1'");
        ASTJexlScript two = JexlASTHelper.parseJexlQuery("FOO == '1'");

        assertTrue(JexlASTHelper.equals(one, two));
    }

    @Test
    public void jexlNodeInequality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1'");
        ASTJexlScript two = JexlASTHelper.parseJexlQuery("'1' == '1'");

        assertFalse(JexlASTHelper.equals(one, two));
    }

    @Test
    public void nullJexlNodeEquality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1'");
        ASTJexlScript two = null;

        assertFalse(JexlASTHelper.equals(one, two));

        assertFalse(JexlASTHelper.equals(two, one));
    }

    @Test
    public void jexlNodeOrderInequality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1'");
        ASTJexlScript two = JexlASTHelper.parseJexlQuery("'1' == FOO");

        assertFalse(JexlASTHelper.equals(one, two));
    }

    @Test
    public void nestedJexlNodeOrderEquality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1' && (BAR == 'bar' || BAR == 'baz')");
        ASTJexlScript two = JexlASTHelper.parseJexlQuery("FOO == '1' && (BAR == 'bar' || BAR == 'baz')");

        assertTrue(JexlASTHelper.equals(one, two));
    }

    @Test
    public void nestedJexlNodeOrderInequality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("FOO == '1' && (BAR == 'bar' || BAR == 'baz')");
        ASTJexlScript two = JexlASTHelper.parseJexlQuery("FOO == '1' && (BAR == 'baz' || BAR == 'bar')");

        assertFalse(JexlASTHelper.equals(one, two));

        ASTJexlScript three = JexlASTHelper.parseJexlQuery("(BAR == 'bar' || BAR == 'baz') && FOO == '1'");

        assertFalse(JexlASTHelper.equals(two, three));
    }

    @Test
    public void manualNestedJexlNodeOrderEquality() throws Exception {
        ASTJexlScript one = JexlASTHelper.parseJexlQuery("(FOO == '1' && (BAR == 'bar' || BAR == 'baz'))");

        JexlNode or = JexlNodeFactory.createNodeTreeFromFieldValues(ContainerType.OR_NODE, new ASTEQNode(ParserTreeConstants.JJTEQNODE),
                        new ASTEQNode(ParserTreeConstants.JJTEQNODE), "BAR", Lists.newArrayList("bar", "baz"));
        JexlNode and = JexlNodeFactory.createAndNode(Lists.newArrayList(JexlNodeFactory.buildEQNode("FOO", "1"), or));

        ASTJexlScript two = JexlNodeFactory.createScript(and);

        assertTrue(JexlASTHelper.equals(one, two));
    }

    @Test
    public void testDereferenceIntersection() throws ParseException {
        String query = "(FOO == 'a' && FOO == 'b')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode child = script.jjtGetChild(0);

        JexlNode test = JexlASTHelper.dereference(child);
        assertEquals("FOO == 'a' && FOO == 'b'", JexlStringBuildingVisitor.buildQueryWithoutParse(test));
    }

    @Test
    public void testDereferenceUnion() throws ParseException {
        String query = "(FOO == 'a' || FOO == 'b')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode child = script.jjtGetChild(0);

        JexlNode test = JexlASTHelper.dereference(child);
        assertEquals("FOO == 'a' || FOO == 'b'", JexlStringBuildingVisitor.buildQueryWithoutParse(test));
    }

    @Test
    public void testDereferenceMarkerNode() throws ParseException {
        String query = "(((((_Value_ = true) && (FOO =~ 'a.*')))))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode child = script.jjtGetChild(0);

        JexlNode test = JexlASTHelper.dereference(child);
        assertEquals("(_Value_ = true) && (FOO =~ 'a.*')", JexlStringBuildingVisitor.buildQueryWithoutParse(test));
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
        assertEquals("((_Value_ = true) && (FOO =~ 'a.*'))", JexlStringBuildingVisitor.buildQueryWithoutParse(test));
    }

    @Test
    public void testNormalizeLiteral() throws Throwable {
        LcNoDiacriticsType normalizer = new LcNoDiacriticsType();
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("F == 'aSTrInG'");
        if (log.isDebugEnabled()) {
            PrintingVisitor.printQuery(script);
        }
        JexlNode literal = script.jjtGetChild(0).jjtGetChild(1);
        ASTStringLiteral stringLiteral = JexlASTHelper.normalizeLiteral(literal, normalizer);
        assertEquals("astring", stringLiteral.getLiteral());
    }

    @Test
    public void testFindLiteral() throws Throwable {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("i == 10");
        if (log.isDebugEnabled()) {
            PrintingVisitor.printQuery(script);
        }
        JexlNode literal = JexlASTHelper.findLiteral(script);
        assertTrue(literal instanceof ASTNumberLiteral);
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

        assertNull(range);

        script = JexlASTHelper.parseJexlQuery("(A < 5 && A > 1)");

        range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));

        assertNull(range);

        script = JexlASTHelper.parseJexlQuery("((_Bounded_ = true) && (A < 5 && A > 1))");

        range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));

        Assert.assertNotNull(range);
        Assert.assertNotNull(range.getLowerNode());
        Assert.assertNotNull(range.getUpperNode());
        assertEquals(1, range.getLower());
        assertEquals(5, range.getUpper());
        assertFalse(range.isLowerInclusive());
        assertFalse(range.isUpperInclusive());

        script = JexlASTHelper.parseJexlQuery("((_Bounded_ = true) && (A <= 5 && A >= 1))");

        range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));

        Assert.assertNotNull(range);
        Assert.assertNotNull(range.getLowerNode());
        Assert.assertNotNull(range.getUpperNode());
        assertEquals(1, range.getLower());
        assertEquals(5, range.getUpper());
        assertTrue(range.isLowerInclusive());
        assertTrue(range.isUpperInclusive());
    }

    @Test
    public void testFindDelayedRange() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((_Delayed_ = true) && ((_Bounded_ = true) && (A < 'b' && A > 'a'))) && !(FOO == 'bar')");

        LiteralRange range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));

        assertNull(range);

        script = JexlASTHelper.parseJexlQuery("((_Delayed_ = true) && ((_Bounded_ = true) && (A < 'b' && A > 'a')))");

        range = JexlASTHelper.findRange().getRange(script.jjtGetChild(0));

        Assert.assertNotNull(range);
        Assert.assertNotNull(range.getLowerNode());
        Assert.assertNotNull(range.getUpperNode());
        assertEquals("a", range.getLower());
        assertEquals("b", range.getUpper());
        assertFalse(range.isLowerInclusive());
        assertFalse(range.isUpperInclusive());
    }

    @Test
    public void testFindNotDelayedRange() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((_Delayed_ = true) && ((_Bounded_ = true) && (A < 'b' && A > 'a')))");

        LiteralRange range = JexlASTHelper.findRange().notDelayed().getRange(script.jjtGetChild(0));

        assertNull(range);

        script = JexlASTHelper.parseJexlQuery("((_Bounded_ = true) && (A < 5 && A > 1))");

        range = JexlASTHelper.findRange().notDelayed().getRange(script.jjtGetChild(0));

        Assert.assertNotNull(range);
        Assert.assertNotNull(range.getLowerNode());
        Assert.assertNotNull(range.getUpperNode());
        assertEquals(1, range.getLower());
        assertEquals(5, range.getUpper());
        assertFalse(range.isLowerInclusive());
        assertFalse(range.isUpperInclusive());
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
        assertEquals("a", range.getLower());
        assertEquals("b", range.getUpper());
        assertFalse(range.isLowerInclusive());
        assertFalse(range.isUpperInclusive());

        script = JexlASTHelper.parseJexlQuery("B < 5 && B > 1");

        range = JexlASTHelper.findRange().indexedOnly(null, helper).getRange(script.jjtGetChild(0));

        assertNull(range);
    }

    @Test
    public void parse1TrailingBackslashEquals() throws Exception {
        String query = "CITY == 'city\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse1TrailingBackslashRegex() throws Exception {
        String query = "CITY =~ 'city\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse2TrailingBackslashesEquals() throws Exception {
        String query = "CITY == 'city\\\\\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse2TrailingBackslashesRegex() throws Exception {
        String query = "CITY =~ 'city\\\\\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse3TrailingBackslashesEquals() throws Exception {
        String query = "CITY == 'city\\\\\\\\\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse3TrailingBackslashesRegex() throws Exception {
        String query = "CITY =~ 'city\\\\\\\\\\\\'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse1LeadingBackslashEquals() throws Exception {
        String query = "CITY == '\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse1LeadingBackslashRegex() throws Exception {
        String query = "CITY =~ '\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse2LeadingBackslashesEquals() throws Exception {
        String query = "CITY == '\\\\\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse2LeadingBackslashesRegex() throws Exception {
        String query = "CITY =~ '\\\\\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse3LeadingBackslashesEquals() throws Exception {
        String query = "CITY == '\\\\\\\\\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse3LeadingBackslashesRegex() throws Exception {
        String query = "CITY =~ '\\\\\\\\\\\\city'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse1InteriorBackslashEquals() throws Exception {
        String query = "CITY == 'ci\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse1InteriorBackslashRegex() throws Exception {
        String query = "CITY =~ 'ci\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse2InteriorBackslashesEquals() throws Exception {
        String query = "CITY == 'ci\\\\\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse2InteriorBackslashesRegex() throws Exception {
        String query = "CITY =~ 'ci\\\\\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse3InteriorBackslashesEquals() throws Exception {
        String query = "CITY == 'ci\\\\\\\\\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
    }

    @Test
    public void parse3InteriorBackslashesRegex() throws Exception {
        String query = "CITY =~ 'ci\\\\\\\\\\\\ty'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(query, interpretedQuery);
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
        assertEquals("CITY =~ 'ci\\\\\\\\\\\\ty\\.blah'", interpretedQuery);
        assertEquals(reinterpretedQuery, interpretedQuery);
        assertEquals("ci\\\\\\\\\\\\ty\\.blah", JexlNodes.getIdentifierOrLiteral(node.jjtGetChild(0).jjtGetChild(1)));
        assertEquals(JexlNodes.getIdentifierOrLiteral(node.jjtGetChild(0).jjtGetChild(1)),
                        JexlNodes.getIdentifierOrLiteral(newNode.jjtGetChild(0).jjtGetChild(1)));
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
        assertEquals("CITY == 'ci\\\\\\\\\\\\ty\\\\.blah'", interpretedQuery);
        assertEquals(reinterpretedQuery, interpretedQuery);
        assertEquals("ci\\\\\\ty\\.blah", JexlNodes.getIdentifierOrLiteral(node.jjtGetChild(0).jjtGetChild(1)));
        assertEquals(JexlNodes.getIdentifierOrLiteral(node.jjtGetChild(0).jjtGetChild(1)),
                        JexlNodes.getIdentifierOrLiteral(newNode.jjtGetChild(0).jjtGetChild(1)));
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
        assertEquals("CITY == 'ci\\\\\\\\\\\\ty\\\\.blah'", interpretedQuery);
        assertEquals(reinterpretedQuery, interpretedQuery);
        assertEquals("ci\\\\\\ty\\.blah", JexlNodes.getIdentifierOrLiteral(node.jjtGetChild(0).jjtGetChild(1)));
        assertEquals(JexlNodes.getIdentifierOrLiteral(node.jjtGetChild(0).jjtGetChild(1)),
                        JexlNodes.getIdentifierOrLiteral(newNode.jjtGetChild(0).jjtGetChild(1)));
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
        assertEquals("ci\\\\\\\\\\\\ty\\\\.bl'ah", JexlNodes.getIdentifierOrLiteral(node.jjtGetChild(0).jjtGetChild(1)));
        assertEquals(query, interpretedQuery);
    }

    // This test is is similar to the previous one, but with multiple backslashes before the embedded single quote.
    // WEB QUERY: CITY =~ 'ci\\\\\\ty\\.bl\\\'ah'
    // StringLiteral.image: "ci\\\\\\ty\\.bl\\'ah"
    @Test
    public void parseRegexWithEscapedQuoteAndBackslashesTest() throws Exception {
        String query = "CITY =~ 'ci\\\\\\\\\\\\ty\\\\.bl\\\\\\'ah'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        assertEquals("ci\\\\\\\\\\\\ty\\\\.bl\\\\'ah", JexlNodes.getIdentifierOrLiteral(node.jjtGetChild(0).jjtGetChild(1)));
        assertEquals(query, interpretedQuery);
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
        assertEquals("ci\\\\\\ty\\.bl'ah", JexlNodes.getIdentifierOrLiteral(node.jjtGetChild(0).jjtGetChild(1)));
        assertEquals(query, interpretedQuery);
    }

    // This test is is similar to the previous one, but with multiple backslashes before the embedded single quote.
    // WEB QUERY: CITY == 'ci\\\\\\ty\\.bl\\\'ah'
    // StringLiteral.image: "ci\\\ty\.bl\'ah"
    @Test
    public void parseEqualsWithEscapedQuoteAndBackslashesTest() throws Exception {
        String query = "CITY == 'ci\\\\\\\\\\\\ty\\\\.bl\\\\\\'ah'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String interpretedQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        assertEquals("ci\\\\\\ty\\.bl\\'ah", JexlNodes.getIdentifierOrLiteral(node.jjtGetChild(0).jjtGetChild(1)));
        assertEquals(query, interpretedQuery);
    }

    // Verify that true is returned for a query with valid junctions.
    @Test
    public void validateJunctionChildrenWithValidTree() throws ParseException {
        String query = "FOO == 'bar' && FOO == 'baz'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        assertTrue(JexlASTHelper.validateJunctionChildren(node));
        assertTrue(JexlASTHelper.validateJunctionChildren(node, false));
    }

    // Verify that false is returned for a query with invalid junctions when failHard == false.
    @Test
    public void validateJunctionChildrenWithInvalidTree() {
        ASTEQNode eqNode = (ASTEQNode) JexlNodeFactory.buildEQNode("FOO", "bar");
        ASTAndNode conjunction = (ASTAndNode) JexlNodeFactory.createAndNode(Collections.singletonList(eqNode));
        assertFalse(JexlASTHelper.validateJunctionChildren(conjunction));
        assertFalse(JexlASTHelper.validateJunctionChildren(conjunction, false));
    }

    // Verify that an exception is thrown for a query with invalid junctions when failHard == true.
    @Test
    public void validateJunctionChildrenWithInvalidTreeAndFailHard() {
        ASTEQNode eqNode = (ASTEQNode) JexlNodeFactory.buildEQNode("FOO", "bar");
        ASTAndNode conjunction = (ASTAndNode) JexlNodeFactory.createAndNode(Collections.singletonList(eqNode));
        RuntimeException exception = Assert.assertThrows(RuntimeException.class, () -> JexlASTHelper.validateJunctionChildren(conjunction, true));
        assertEquals("Could not process node. Instance of AND/OR node found with less than 2 children", exception.getCause().getMessage());
    }

    @Test
    public void testGetFunctionIdentifiers() {
        // single term
        String query = "AGE > '+bE1'";
        testIdentifierParse(query, Sets.newHashSet("AGE"));

        // union of two terms
        query = "(AGE > '+bE1' || ETA > '+bE1')";
        testIdentifierParse(query, Sets.newHashSet("AGE", "ETA"));

        // complex function
        query = "(grouping:getGroupsForMatchesInGroup((NOME || NAME), 'MEADOW', (GENERE || GENDER), 'FEMALE')) == MAGIC";
        testIdentifierParse(query, Sets.newHashSet("GENDER", "GENERE", "MAGIC", "NAME", "NOME"));

        // function output feeds method
        query = "((AGE || ETA).getValuesForGroups(grouping:getGroupsForMatchesInGroup((NOME || NAME), 'MEADOW', (GENERE || GENDER), 'FEMALE')) == MAGIC)";
        testIdentifierParse(query, Sets.newHashSet("AGE", "ETA", "GENDER", "GENERE", "MAGIC", "NAME", "NOME"));

        // original full query
        query = "(AGE > '+bE1' || ETA > '+bE1') && (AGE < '+cE1' || ETA < '+cE1') && ((_Eval_ = true) && ((AGE || ETA).getValuesForGroups(grouping:getGroupsForMatchesInGroup((NOME || NAME), 'MEADOW', (GENERE || GENDER), 'FEMALE')) == MAGIC))";
        testIdentifierParse(query, Sets.newHashSet("AGE", "ETA", "GENDER", "GENERE", "MAGIC", "NAME", "NOME"));

        query = "content:phrase(termOffsetMap, 'bar', 'baz')";
        testIdentifierParse(query, Collections.singleton("termOffsetMap"));

        query = "content:phrase(FOO, termOffsetMap, 'bar', 'baz')";
        testIdentifierParse(query, Sets.newHashSet("FOO", "termOffsetMap"));
    }

    private void testIdentifierParse(String query, Set<String> expectedIdentifiers) {
        try {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            Set<String> fields = JexlASTHelper.getIdentifierNames(script);
            assertEquals("Expected fields but was", expectedIdentifiers, fields);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testIdentifierDeconstructionWithGroupings() {
        // no prefix or grouping
        testDeconstructionGroupingTrue("FIELD", "FIELD");

        // has a simple grouping, does not have a prefix
        testDeconstructionGroupingTrue("FIELD.1", "FIELD.1");

        // has a complex grouping, does not have a prefix
        testDeconstructionGroupingTrue("FIELD.1.2", "FIELD.1.2");

        // has a prefix, does not have a grouping
        testDeconstructionGroupingTrue("FIELD", "$FIELD");

        // has a prefix, has a simple grouping
        testDeconstructionGroupingTrue("FIELD.1", "$FIELD.1");

        // has a prefix, has a complex grouping
        testDeconstructionGroupingTrue("FIELD.1.2", "$FIELD.1.2");

        // empty string should return untouched
        testDeconstructionGroupingTrue("", "");
    }

    @Test
    public void testIdentifierDeconstructionWithoutGroupings() {
        // no prefix or grouping
        testDeconstructionGroupingFalse("FIELD", "FIELD");

        // has a simple grouping, does not have a prefix
        testDeconstructionGroupingFalse("FIELD", "FIELD.1");

        // has a complex grouping, does not have a prefix
        testDeconstructionGroupingFalse("FIELD", "FIELD.1.2");

        // has a prefix, does not have a grouping
        testDeconstructionGroupingFalse("FIELD", "$FIELD");

        // has a prefix, has a simple grouping
        testDeconstructionGroupingFalse("FIELD", "$FIELD.1");

        // has a prefix, has a complex grouping
        testDeconstructionGroupingFalse("FIELD", "$FIELD.1.2");

        // empty string should remain untouched
        testDeconstructionGroupingFalse("", "");
    }

    private void testDeconstructionGroupingTrue(String expected, String input) {
        String actual = JexlASTHelper.deconstructIdentifier(input, true);
        assertEquals(expected, actual);
    }

    private void testDeconstructionGroupingFalse(String expected, String input) {
        String actual = JexlASTHelper.deconstructIdentifier(input, false);
        assertEquals(expected, actual);
    }

    // Verify the nesting order between 'or' and 'and' in a Jexl query.
    @Test
    public void testArtificialParenthesisTreeOrAnd() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == '1' or FOO == '2' and BAR == '3'");
        // Parenthesis artificially added in should be: "((FOO == '1') or ((FOO == '2') and (BAR == '3')))"

        List<ASTOrNode> orNodes = JexlASTHelper.getNodesOfType(query, ASTOrNode.class);
        List<ASTAndNode> andNodes = JexlASTHelper.getNodesOfType(query, ASTAndNode.class);

        for (JexlNode orNode : orNodes) {
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTAndNode.class));
        }

        for (JexlNode andNode : andNodes) {
            assertTrue(JexlASTHelper.isDescendantOfOr(andNode));
        }
    }

    // Verify the nesting order between 'and' and 'not' in a Jexl query.
    @Test
    public void testArtificialParenthesisTreeAndNot() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("not (FOO == '1') and BAR == '3'");
        // Parenthesis artificially added in should be: "((not (FOO == '1')) and (BAR == '3'))"

        List<ASTAndNode> andNodes = JexlASTHelper.getNodesOfType(query, ASTAndNode.class);
        List<ASTNotNode> notNodes = JexlASTHelper.getNodesOfType(query, ASTNotNode.class);

        for (JexlNode andNode : andNodes) {
            assertTrue(JexlASTHelper.descendantsContainNodeType(andNode, ASTNotNode.class));
        }

        for (JexlNode notNode : notNodes) {
            assertTrue(JexlASTHelper.isDescendantOfNodeType(notNode, ASTAndNode.class));
        }
    }

    // Verify the nesting order between 'or' and 'not' in a Jexl query.
    @Test
    public void testArtificialParenthesisTreeOrNot() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("not (FOO == '1') or BAR == '3'");
        // Parenthesis artificially added in should be: "((not (FOO == '1')) or (BAR == '3'))"

        List<ASTOrNode> orNodes = JexlASTHelper.getNodesOfType(query, ASTOrNode.class);
        List<ASTNotNode> notNodes = JexlASTHelper.getNodesOfType(query, ASTNotNode.class);

        for (JexlNode orNode : orNodes) {
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTNotNode.class));
        }

        for (JexlNode notNode : notNodes) {
            assertTrue(JexlASTHelper.isDescendantOfNodeType(notNode, ASTOrNode.class));
        }
    }

    // Verify the nesting order when 'or', 'and', and 'not' are all present in a Jexl query.
    @Test
    public void testArtificialParenthesisTreeOrAndNot() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == '1' or not (BAR == '3') and FOO == '2'");
        // Parenthesis artificially added in should be: "((FOO == '1') or ((not (BAR == '3')) and (FOO == '2')))"

        List<ASTOrNode> orNodes = JexlASTHelper.getNodesOfType(query, ASTOrNode.class);
        List<ASTAndNode> andNodes = JexlASTHelper.getNodesOfType(query, ASTAndNode.class);
        List<ASTNotNode> notNodes = JexlASTHelper.getNodesOfType(query, ASTNotNode.class);

        for (JexlNode orNode : orNodes) {
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTAndNode.class));
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTNotNode.class));
        }

        for (JexlNode andNode : andNodes) {
            assertTrue(JexlASTHelper.isDescendantOfNodeType(andNode, ASTOrNode.class));
            assertTrue(JexlASTHelper.descendantsContainNodeType(andNode, ASTNotNode.class));
        }

        for (JexlNode notNode : notNodes) {
            assertTrue(JexlASTHelper.isDescendantOfNodeType(notNode, ASTOrNode.class));
            assertTrue(JexlASTHelper.isDescendantOfNodeType(notNode, ASTAndNode.class));
        }
    }

    @Test
    public void testChildrenContainNodeType() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("not FOO == '1' or (FOO == '2' and BAR == '3')");

        List<ASTOrNode> orNodes = JexlASTHelper.getNodesOfType(query, ASTOrNode.class);
        List<ASTAndNode> andNodes = JexlASTHelper.getNodesOfType(query, ASTAndNode.class);

        for (JexlNode orNode : orNodes) {
            assertFalse(JexlASTHelper.descendantsContainNodeType(orNode, ASTOrNode.class));
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTAndNode.class));
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTNotNode.class));
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTEQNode.class));
        }

        for (JexlNode andNode : andNodes) {
            assertFalse(JexlASTHelper.descendantsContainNodeType(andNode, ASTOrNode.class));
            assertTrue(JexlASTHelper.descendantsContainNodeType(andNode, ASTEQNode.class));
        }
    }

    @Test
    public void testArtificialParenthesisQueryMeaningChange() throws Exception {
        // Query with parenthesis
        String parenthesisQueryString = "(FOO == '1' or FOO == '2') and BAR == '3'";

        // Query without parenthesis
        String noParenthesisQueryString = "FOO == '1' or FOO == '2' and BAR == '3'";
        // Parenthesis artificially added in should be: "((FOO == '1') or ((FOO == '2') and (BAR == '3')))"

        DatawaveJexlContext djc = new DatawaveJexlContext();
        djc.set("FOO", 1);
        djc.set("BAR", 2);

        Tuple3<Key,Document,DatawaveJexlContext> evalArgs = new Tuple3<>(new Key(), new Document(), djc);

        JexlEvaluation eval = new JexlEvaluation(parenthesisQueryString);
        assertFalse(eval.apply(evalArgs));

        JexlEvaluation evalBad = new JexlEvaluation(noParenthesisQueryString);
        assertTrue(evalBad.apply(evalArgs));
    }

    // Verify the nesting order between 'or' and 'and' in a Lucene query.
    @Test
    public void testLuceneArtificialParenthesisTreeOrAnd() throws Exception {
        String queryString = "FOO:1 OR FOO:2 AND BAR:3";
        ASTJexlScript query = JexlASTHelper.parseJexlQuery(new LuceneToJexlQueryParser().convertToJexlNode(queryString).toString());
        // Parenthesis artificially added in should be: "((FOO:1) OR ((FOO:2) AND (BAR:3)))"

        List<ASTOrNode> orNodes = JexlASTHelper.getNodesOfType(query, ASTOrNode.class);
        List<ASTAndNode> andNodes = JexlASTHelper.getNodesOfType(query, ASTAndNode.class);

        for (JexlNode orNode : orNodes) {
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTAndNode.class));
        }

        for (JexlNode andNode : andNodes) {
            assertTrue(JexlASTHelper.isDescendantOfOr(andNode));
        }
    }

    // Verify the nesting order between 'AND' and 'NOT' in a Lucene query. #EXCLUDE() utilizes ASTNotNode.
    @Test
    public void testLuceneArtificialParenthesisTreeAndNot() throws Exception {
        String queryString = "#EXCLUDE(FOO) AND BAR:3";
        ASTJexlScript query = JexlASTHelper.parseJexlQuery(new LuceneToJexlQueryParser().convertToJexlNode(queryString).toString());
        // Parenthesis artificially added in should be: "((#EXCLUDE(FOO)) AND (BAR:3))"

        List<ASTAndNode> andNodes = JexlASTHelper.getNodesOfType(query, ASTAndNode.class);
        List<ASTNotNode> notNodes = JexlASTHelper.getNodesOfType(query, ASTNotNode.class);

        for (JexlNode andNode : andNodes) {
            assertTrue(JexlASTHelper.descendantsContainNodeType(andNode, ASTNotNode.class));
        }

        for (JexlNode notNode : notNodes) {
            assertTrue(JexlASTHelper.isDescendantOfNodeType(notNode, ASTAndNode.class));
        }
    }

    // Verify the nesting order between 'OR' and 'NOT' in a Lucene query. #EXCLUDE() utilizes ASTNotNode.
    @Test
    public void testLuceneArtificialParenthesisTreeOrNot() throws Exception {
        String queryString = "#EXCLUDE(FOO) OR BAR:3";
        ASTJexlScript query = JexlASTHelper.parseJexlQuery(new LuceneToJexlQueryParser().convertToJexlNode(queryString).toString());
        // Parenthesis artificially added in should be: "((#EXCLUDE(FOO)) OR (BAR:3))"

        List<ASTOrNode> orNodes = JexlASTHelper.getNodesOfType(query, ASTOrNode.class);
        List<ASTNotNode> notNodes = JexlASTHelper.getNodesOfType(query, ASTNotNode.class);

        for (JexlNode orNode : orNodes) {
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTNotNode.class));
        }

        for (JexlNode notNode : notNodes) {
            assertTrue(JexlASTHelper.isDescendantOfNodeType(notNode, ASTOrNode.class));
        }
    }

    // Verify the nesting order when 'OR', 'AND', and 'NOT' are all present in a Lucene query. 'NOT' becomes 'AND NOT'.
    @Test
    public void testLuceneArtificialParenthesisTreeOrAndNot() throws Exception {
        String queryString = "BAR:3 OR FOO:1 NOT FOO:2";
        ASTJexlScript query = JexlASTHelper.parseJexlQuery(new LuceneToJexlQueryParser().convertToJexlNode(queryString).toString());
        // Parenthesis artificially added in should be: "((BAR:3) OR ((FOO:1) AND (NOT (FOO:2))))"

        List<ASTOrNode> orNodes = JexlASTHelper.getNodesOfType(query, ASTOrNode.class);
        List<ASTAndNode> andNodes = JexlASTHelper.getNodesOfType(query, ASTAndNode.class);
        List<ASTNotNode> notNodes = JexlASTHelper.getNodesOfType(query, ASTNotNode.class);

        for (JexlNode orNode : orNodes) {
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTAndNode.class));
            assertTrue(JexlASTHelper.descendantsContainNodeType(orNode, ASTNotNode.class));
        }

        for (JexlNode andNode : andNodes) {
            assertTrue(JexlASTHelper.isDescendantOfNodeType(andNode, ASTOrNode.class));
            assertTrue(JexlASTHelper.descendantsContainNodeType(andNode, ASTNotNode.class));
        }

        for (JexlNode notNode : notNodes) {
            assertTrue(JexlASTHelper.isDescendantOfNodeType(notNode, ASTOrNode.class));
            assertTrue(JexlASTHelper.isDescendantOfNodeType(notNode, ASTAndNode.class));
        }
    }

}
