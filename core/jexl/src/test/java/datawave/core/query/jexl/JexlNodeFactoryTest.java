package datawave.core.query.jexl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.core.query.jexl.lookups.IndexLookupMap;
import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.exceptions.DatawaveFatalQueryException;

public class JexlNodeFactoryTest {

    // It's worth noting that 'expandFields' is almost always set to true. This test is a contrived case that exercises a code path.
    @Test
    public void testNoFieldExpansion() throws ParseException {
        IndexLookupMap indexLookupMap = new IndexLookupMap(10, 10);
        indexLookupMap.putAll("FOO", Sets.newHashSet("6", "9"));
        indexLookupMap.putAll("BAR", Sets.newHashSet("7", "8"));

        JexlNode originalNode = JexlASTHelper.parseJexlQuery("((_Bounded_ = true) && (FOO > 5 && FOO < 10))");
        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, false, originalNode, indexLookupMap, false,
                        true, false);

        assertEquals("(FOO == '6' || FOO == '7' || FOO == '8' || FOO == '9')", JexlStringBuildingVisitor.buildQuery(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }

    @Test
    public void testOrFromFieldNames() throws ParseException {
        JexlNode original = JexlASTHelper.parseJexlQuery("_ANYFIELD_ =~ 'bar'");

        // let's get the literal...
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(original);
        assertEquals("Expected 1 ER node, but got " + erNodes.size(), 1, erNodes.size());
        ASTERNode erNode = erNodes.get(0);
        Object literal = JexlASTHelper.getLiteralValue(erNode);

        List<String> fieldNames = Lists.newArrayList("FOO1", "FOO2", "FOO3");
        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldNames(JexlNodeFactory.ContainerType.OR_NODE, new ASTEQNode(ParserTreeConstants.JJTEQNODE),
                        literal, fieldNames);

        assertEquals("(FOO1 == 'bar' || FOO2 == 'bar' || FOO3 == 'bar')", JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }

    @Test
    public void testOrFromFieldValues() throws ParseException {
        JexlNode original = JexlASTHelper.parseJexlQuery("FOO =~ 'bar.*'");
        // and get the erNode...
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(original);
        assertEquals("Expected 1 ERNode but got " + erNodes.size(), 1, erNodes.size());
        ASTERNode erNode = erNodes.get(0);

        List<String> fieldValues = Lists.newArrayList("bar1", "bar2", "bar3");

        // Convert FOO to a conjunction of FOO[1,3]
        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldValues(JexlNodeFactory.ContainerType.OR_NODE, new ASTEQNode(ParserTreeConstants.JJTORNODE),
                        erNode, "FOO", fieldValues);

        assertEquals("(FOO == 'bar1' || FOO == 'bar2' || FOO == 'bar3')", JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }

    // an AndNode is created when expanding a negation
    @Test
    public void testAndFromFieldValues() throws ParseException {
        JexlNode original = JexlASTHelper.parseJexlQuery("!(FOO =~ 'bar.*')");
        // and get the erNode...
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(original);
        assertEquals("Expected 1 ERNode but got " + erNodes.size(), 1, erNodes.size());
        ASTERNode erNode = erNodes.get(0);

        List<String> fieldValues = Lists.newArrayList("bar1", "bar2", "bar3");

        // Convert FOO to a conjunction of FOO[1,3]
        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldValues(JexlNodeFactory.ContainerType.AND_NODE, new ASTEQNode(ParserTreeConstants.JJTANDNODE),
                        erNode, "FOO", fieldValues);

        // we only assert the expansion inside the negated branch
        assertEquals("(FOO == 'bar1' && FOO == 'bar2' && FOO == 'bar3')", JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }

    @Test
    public void testOrFromFieldsToValues() throws ParseException {
        JexlNode original = JexlASTHelper.parseJexlQuery("FOO =~ 'bar.*'");
        // and get the erNode...
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(original);
        assertEquals("Expected 1 ERNode but got " + erNodes.size(), 1, erNodes.size());
        ASTERNode erNode = erNodes.get(0);

        IndexLookupMap lookupMap = new IndexLookupMap(500, 5000); // defaults from ShardQueryConfig
        lookupMap.put("FOO", "bar1");
        lookupMap.put("FOO", "bar2");
        lookupMap.put("FOO", "bar3");

        // Convert FOO to a conjunction of FOO[1,3]
        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, false, erNode, lookupMap, true, true, false);

        assertEquals("(FOO == 'bar1' || FOO == 'bar2' || FOO == 'bar3')", JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }

    @Test(expected = DatawaveFatalQueryException.class)
    public void testOrFromFieldsToValues_maxUnfieldedExpansion() throws ParseException {
        JexlNode original = JexlASTHelper.parseJexlQuery("_ANYFIELD_ =~ 'bar.*'");
        // and get the erNode...
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(original);
        assertEquals("Expected 1 ERNode but got " + erNodes.size(), 1, erNodes.size());
        ASTERNode erNode = erNodes.get(0);

        IndexLookupMap lookupMap = new IndexLookupMap(1, 5000); // defaults from ShardQueryConfig
        lookupMap.put("FOO1", "bar1");
        lookupMap.put("FOO2", "bar2");
        assertTrue(lookupMap.isKeyThresholdExceeded());

        // Convert FOO to a conjunction of FOO[1,3]
        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, false, erNode, lookupMap, true, true, false);

        assertEquals("((_Term_ = true) && (_ANYFIELD_ =~ 'bar.*'))", JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }

    @Test
    public void testOrFromFieldsToValues_maxRegexExpansion() throws ParseException {
        JexlNode original = JexlASTHelper.parseJexlQuery("_ANYFIELD_ =~ 'bar.*'");
        // and get the erNode...
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(original);
        assertEquals("Expected 1 ERNode but got " + erNodes.size(), 1, erNodes.size());
        ASTERNode erNode = erNodes.get(0);

        IndexLookupMap lookupMap = new IndexLookupMap(500, 1); // defaults from ShardQueryConfig
        lookupMap.put("FOO1", "bar1");
        lookupMap.put("FOO1", "bar2");
        lookupMap.put("FOO2", "bar1");
        lookupMap.put("FOO2", "bar2");
        lookupMap.put("FOO3", "bar1");
        assertFalse(lookupMap.isKeyThresholdExceeded());
        assertTrue(lookupMap.get("FOO1").isThresholdExceeded());
        assertTrue(lookupMap.get("FOO2").isThresholdExceeded());
        assertFalse(lookupMap.get("FOO3").isThresholdExceeded());

        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, false, erNode, lookupMap, true, true, false);

        String expected = "(((_Value_ = true) && (FOO1 =~ 'bar.*')) || FOO3 == 'bar1' || ((_Value_ = true) && (FOO2 =~ 'bar.*')))";
        assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }

    @Test
    public void testRangeExpansionToMaxExpansion() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO >= 'bar1' && FOO <= 'bar4'");
        JexlNode range = script.jjtGetChild(0);

        IndexLookupMap lookupMap = new IndexLookupMap(500, 1); // defaults from ShardQueryConfig
        lookupMap.put("FOO", "bar1");
        lookupMap.put("FOO", "bar2");
        lookupMap.put("FOO", "bar3");
        lookupMap.put("FOO", "bar4");
        assertFalse(lookupMap.isKeyThresholdExceeded());
        assertTrue(lookupMap.get("FOO").isThresholdExceeded());

        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, false, range, lookupMap, true, true, false);

        String expected = "((_Value_ = true) && (FOO >= 'bar1' && FOO <= 'bar4'))";
        assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }

    @Test
    public void testEnsureRangeExpansionDoesNotDuplicateNodes() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO >= 'bar1' && FOO <= 'bar4') && BAR == 'foo'");

        IndexLookupMap lookupMap = new IndexLookupMap(500, 100); // defaults from ShardQueryConfig
        lookupMap.put("FOO", "bar1");
        lookupMap.put("FOO", "bar2");
        lookupMap.put("FOO", "bar3");
        lookupMap.put("FOO", "bar4");
        assertFalse(lookupMap.isKeyThresholdExceeded());
        assertFalse(lookupMap.get("FOO").isThresholdExceeded());

        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, false, script, lookupMap, true, true, false);

        String expected = "(FOO == 'bar1' || FOO == 'bar2' || FOO == 'bar3' || FOO == 'bar4')";
        assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }

    // preserve negated regex nodes that find nothing during index expansion
    @Test
    public void testNegatedRegexFoundNothing() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO !~ 'ba.*'");

        IndexLookupMap lookupMap = new IndexLookupMap(500, 100); // defaults from ShardQueryConfig
        assertFalse(lookupMap.isKeyThresholdExceeded());

        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, true, script, lookupMap, true, true, false);

        assertEquals("FOO !~ 'ba.*'", JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }
}
