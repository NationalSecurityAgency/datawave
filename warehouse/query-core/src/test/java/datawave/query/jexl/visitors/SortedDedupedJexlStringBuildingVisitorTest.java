package datawave.query.jexl.visitors;

import com.google.common.collect.Lists;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static datawave.query.jexl.JexlNodeFactory.buildEQNode;

public class SortedDedupedJexlStringBuildingVisitorTest {
    
    @Test
    public void testSimpleEquality() throws Exception {
        String queryA = "FOO == 'abc'";
        String queryB = "FOO == 'abc'";
        Assertions.assertEquals(nodeToKey(JexlASTHelper.parseJexlQuery(queryA)), nodeToKey(JexlASTHelper.parseJexlQuery(queryB)));
        
    }
    
    @Test
    public void testSubExpression() throws Exception {
        String queryA = "(FOO == 'abc' || CAR == 'carA')";
        String queryB = "(CAR == 'carA' || FOO == 'abc')";
        Assertions.assertEquals(nodeToKey(JexlASTHelper.parseJexlQuery(queryA)), nodeToKey(JexlASTHelper.parseJexlQuery(queryB)));
        
    }
    
    @Test
    public void testExpressionDelayed() throws Exception {
        JexlNode queryA = JexlASTHelper.parseJexlQuery("FOO == 'abc'");
        JexlNode queryB = ASTDelayedPredicate.create(JexlASTHelper.parseJexlQuery("FOO == 'abc'"));
        Assertions.assertNotEquals(nodeToKey(queryA), nodeToKey(queryB));
        
    }
    
    @Test
    public void testRange() throws Exception {
        JexlNode queryA = JexlASTHelper.parseJexlQuery("FOO >= 'abc' && FOO >= 'bcd'");
        JexlNode queryB = JexlASTHelper.parseJexlQuery("FOO >= 'bcd' && FOO >= 'abc'");
        Assertions.assertEquals(nodeToKey(queryA), nodeToKey(queryB));
        
    }
    
    @Test
    public void testRangeFalse() throws Exception {
        JexlNode queryA = JexlASTHelper.parseJexlQuery("(FOO >= 'abc' && FOO >= 'bcd') && CAR=='wagon'");
        JexlNode queryB = JexlASTHelper.parseJexlQuery("FOO >= 'bcd' && FOO >= 'abc'");
        Assertions.assertNotEquals(nodeToKey(queryA), nodeToKey(queryB));
        
    }
    
    @Test
    public void testFunction() throws Exception {
        JexlNode queryA = JexlASTHelper.parseJexlQuery("(FOO == 'blah1' && FOO == 'blah2' && content:phrase(termOffsetMap, 'twisted', 'pair'))");
        JexlNode queryB = JexlASTHelper.parseJexlQuery("(content:phrase(termOffsetMap, 'twisted', 'pair') && FOO == 'blah1' && FOO == 'blah2')");
        Assertions.assertEquals(nodeToKey(queryA), nodeToKey(queryB));
        
    }
    
    @Test
    public void testSimpleEqualityNegative() throws Exception {
        String queryA = "FOO == 'ab2c'";
        String queryB = "FOO == 'abc'";
        Assertions.assertNotEquals(nodeToKey(JexlASTHelper.parseJexlQuery(queryA)), nodeToKey(JexlASTHelper.parseJexlQuery(queryB)));
        
    }
    
    @Test
    public void testDifferentObjectsSameValuesSameHash() {
        JexlNode eq01 = buildEQNode("FOO", "bar");
        JexlNode eq02 = buildEQNode("FOO", "bar");

        Assertions.assertEquals(nodeToKey(eq01), nodeToKey(eq02));
    }
    
    @Test
    public void testOneHasMoreNodes() {
        JexlNode eq01 = buildEQNode("FOO", "bar");
        JexlNode eq02 = buildEQNode("FOO", "baz");
        JexlNode eq03 = buildEQNode("FOO", "byzantium");
        
        JexlNode or01 = JexlNodeFactory.createOrNode(Arrays.asList(eq01, eq02));
        JexlNode or02 = JexlNodeFactory.createOrNode(Arrays.asList(eq01, eq02, eq03));

        Assertions.assertNotEquals(nodeToKey(or01), nodeToKey(or02));
    }
    
    @Test
    public void testRightHasDuplicateNodes() throws ParseException {
        JexlNode or01 = JexlASTHelper.parseJexlQuery("FOO == 'bar' || FOO == 'baz'");
        JexlNode or02 = JexlASTHelper.parseJexlQuery("FOO == 'bar' || FOO == 'baz' || FOO == 'baz'");
        
        // OR nodes with the same, but unordered children WILL match.
        Assertions.assertEquals(nodeToKey(or01), nodeToKey(or02));
    }
    
    @Test
    public void testOrderOfAddition() {
        JexlNode eq01 = buildEQNode("FOO", "bar");
        JexlNode eq02 = buildEQNode("FOO", "baz");
        
        JexlNode or01 = JexlNodeFactory.createOrNode(Lists.newArrayList(eq01, eq02));
        JexlNode or02 = JexlNodeFactory.createOrNode(Lists.newArrayList(eq02, eq01));
        
        // Same ORNode should match
        Assertions.assertEquals(nodeToKey(or01), nodeToKey(or02));
        
        JexlNode or03 = JexlNodeFactory.createOrNode(Lists.newArrayList(or01, or02));
        JexlNode or04 = JexlNodeFactory.createOrNode(Lists.newArrayList(or02, or01));
        
        // OR nodes with the same, but unordered children WILL match.
        Assertions.assertEquals(nodeToKey(or03), nodeToKey(or04));
    }
    
    private static String nodeToKey(JexlNode node) {
        return JexlStringBuildingVisitor.buildQueryWithoutParse(TreeFlatteningRebuildingVisitor.flatten(node), true);
    }
    
}
