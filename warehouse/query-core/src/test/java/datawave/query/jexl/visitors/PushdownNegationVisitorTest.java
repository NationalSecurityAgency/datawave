package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class PushdownNegationVisitorTest {
    PushdownNegationVisitor visitor;
    
    @BeforeEach
    public void setup() {
        visitor = new PushdownNegationVisitor();
    }
    
    @Test
    public void testEq() {
        String query = "!(F1 == 'v1')";
        test(query, query);
    }
    
    @Test
    public void testNEq() {
        String query = "!(F1 != 'v1')";
        String expected = "F1 == 'v1'";
        test(query, expected);
    }
    
    @Test
    public void testNR() {
        String query = "!(F1 !~ 'v1')";
        String expected = "F1 =~ 'v1'";
        test(query, expected);
    }
    
    @Test
    public void testER() {
        String query = "!(F1 =~ 'v1')";
        String expected = "!(F1 =~ 'v1')";
        test(query, expected);
    }
    
    @Test
    public void testDoubleNegationEq() {
        String query = "!!(F1 == 'v1')";
        String expected = "F1 == 'v1'";
        test(query, expected);
    }
    
    @Test
    public void testTripleNegationEq() {
        String query = "!!!(F1 == 'v1')";
        String expected = "!(F1 == 'v1')";
        test(query, expected);
    }
    
    @Test
    public void testTripleNestedNegationEq() {
        String query = "!(!(!(F1 == 'v1')))";
        String expected = "!(F1 == 'v1')";
        test(query, expected);
    }
    
    @Test
    public void testTripleNegationNEq() {
        String query = "!!!(F1 != 'v1')";
        String expected = "F1 == 'v1'";
        test(query, expected);
    }
    
    @Test
    public void testAnd() {
        String query = "!(F1 == 'v1' && F2 == 'v2')";
        String expected = "(!(F1 == 'v1') || !(F2 == 'v2'))";
        test(query, expected);
    }
    
    @Test
    public void testAndNE() {
        String query = "!(F1 != 'v1' && F2 != 'v2')";
        String expected = "(F1 == 'v1' || F2 == 'v2')";
        test(query, expected);
    }
    
    @Test
    public void testOr() {
        String query = "!(F1 == 'v1' || F2 == 'v2')";
        String expected = "(!(F1 == 'v1') && !(F2 == 'v2'))";
        test(query, expected);
    }
    
    @Test
    public void testOrNE() {
        String query = "!(F1 != 'v1' || F2 != 'v2')";
        String expected = "(F1 == 'v1' && F2 == 'v2')";
        test(query, expected);
    }
    
    @Test
    public void testNestedAnd() {
        String query = "!(F1 == 'v1' && F2 == 'v2' && (F3 == 'v3' || F4 == 'v4'))";
        String expected = "(!(F1 == 'v1') || !(F2 == 'v2') || ((!(F3 == 'v3') && !(F4 == 'v4'))))";
        test(query, expected);
    }
    
    @Test
    public void testNestedAndMixedCancels() {
        String query = "!(F1 != 'v1' && !F2 == 'v2' && (F3 == 'v3' || F4 == 'v4'))";
        String expected = "(F1 == 'v1' || F2 == 'v2' || ((!(F3 == 'v3') && !(F4 == 'v4'))))";
        test(query, expected);
    }
    
    /**
     * Same as testNestedAnd but validate that the original is not modified
     *
     * @throws ParseException
     */
    @Test
    public void testGuarantees() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 == 'v1' && F2 == 'v2' && (F3 == 'v3' || F4 == 'v4'))");
        String orig = JexlStringBuildingVisitor.buildQuery(query);
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("(!(F1 == 'v1') || !(F2 == 'v2') || ((!(F3 == 'v3') && !(F4 == 'v4'))))", JexlStringBuildingVisitor.buildQuery(result));
        Assertions.assertNotEquals(orig, JexlStringBuildingVisitor.buildQuery(result));
        Assertions.assertEquals(orig, JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testNestedOr() {
        String query = "!(F1 == 'v1' || F2 == 'v2' || (F3 == 'v3' && F4 == 'v4'))";
        String expected = "(!(F1 == 'v1') && !(F2 == 'v2') && ((!(F3 == 'v3') || !(F4 == 'v4'))))";
        test(query, expected);
    }
    
    @Test
    public void testNestedOrMixedCancels() {
        String query = "!(F1 != 'v1' || !F2 == 'v2' || (F3 == 'v3' && F4 == 'v4'))";
        String expected = "(F1 == 'v1' && F2 == 'v2' && ((!(F3 == 'v3') || !(F4 == 'v4'))))";
        test(query, expected);
    }
    
    @Test
    public void testDelayedPropertyMarkerPropagate() {
        String query = "!((_Delayed_ = true) && (F1 == 'v1' || F2 == 'v2'))";
        String expected = "((_Delayed_ = true) && ((!(F1 == 'v1') && !(F2 == 'v2'))))";
        test(query, expected);
    }
    
    @Test
    public void testEvaluationOnlyPropertyMarkerPropagate() {
        String query = "!((_Eval_ = true) && (F1 == 'v1' || F2 == 'v2'))";
        String expected = "((_Eval_ = true) && ((!(F1 == 'v1') && !(F2 == 'v2'))))";
        test(query, expected);
    }
    
    @Test
    public void testExceededOrPropertyMarkerPropagate() {
        String query = "!((_List_ = true) && (F1 == 'v1' || F2 == 'v2'))";
        String expected = "!((_List_ = true) && (F1 == 'v1' || F2 == 'v2'))";
        test(query, expected);
    }
    
    @Test
    public void testExceededValuePropertyMarkerPropagate() {
        String query = "!((_Value_ = true) && (F1 == 'v1' || F2 == 'v2'))";
        String expected = "!((_Value_ = true) && (F1 == 'v1' || F2 == 'v2'))";
        test(query, expected);
    }
    
    @Test
    public void testExceededTermPropertyMarkerPropagate() {
        String query = "!((_Term_ = true) && (F1 == 'v1' || F2 == 'v2'))";
        String expected = "!((_Term_ = true) && (F1 == 'v1' || F2 == 'v2'))";
        test(query, expected);
    }
    
    @Test
    public void testBoundedRangeNoPropagation() {
        String query = "F3 == 'v3' || !((_Bounded_ = true) && (F1 >= 'v1' && F1 <= 'v2'))";
        String expected = "F3 == 'v3' || !((_Bounded_ = true) && (F1 >= 'v1' && F1 <= 'v2'))";
        test(query, expected);
    }
    
    @Test
    public void testPartialBoundedRangePropagation() {
        String query = "F3 == 'v3' || !((_Bounded_ = true) && (F1 >= 'v1' && F2 <= 'v2'))";
        String expected = "F3 == 'v3' || !((_Bounded_ = true) && (F1 >= 'v1' && F2 <= 'v2'))";
        test(query, expected);
    }
    
    @Test
    public void testMixedBoundedRanges() {
        String query = "(F3 == 'v3' || !(((_Bounded_ = true) && (F1 >= 'v1' && F2 <= 'v2')) || !((_Bounded_ = true) && (F1 >= 'v1' && F1 <= 'v2'))))";
        String expected = "(F3 == 'v3' || (!((_Bounded_ = true) && (F1 >= 'v1' && F2 <= 'v2')) && ((_Bounded_ = true) && (F1 >= 'v1' && F1 <= 'v2'))))";
        test(query, expected);
    }
    
    @Test
    public void testMixedMarkers() {
        String query = "!((_Delayed_ = true) && (F1 == 'v1' && ((_Term_ = true) && (F2 == 'v2'))))";
        String expected = "((_Delayed_ = true) && ((!(F1 == 'v1') || !((_Term_ = true) && (F2 == 'v2')))))";
        test(query, expected);
    }
    
    @Test
    public void testMixedMarkersInverted() {
        String query = "!((_Term_ = true) && (F1 == 'v1' && ((_Delayed_ = true) && (F2 == 'v2'))))";
        String expected = "!((_Term_ = true) && (F1 == 'v1' && ((_Delayed_ = true) && (F2 == 'v2'))))";
        test(query, expected);
    }
    
    @Test
    public void testNegatedUnionOfMarkerNodes() {
        String query = "!( ((_Delayed_ = true) && (FOO =~ 'ba.*')) && ((_Delayed_ = true) && (FOO =~ 'xy.*')) )";
        String expected = "(!((_Delayed_ = true) && (FOO =~ 'ba.*')) || !((_Delayed_ = true) && (FOO =~ 'xy.*')))";
        test(query, expected);
    }
    
    @Test
    public void testNegateMarkedNode() throws ParseException {
        // source it
        String source = "FOO =~ 'ba.*'";
        JexlNode sourceNode = JexlASTHelper.parseJexlQuery(source);
        Assertions.assertEquals(source, JexlStringBuildingVisitor.buildQuery(sourceNode));
        
        // mark it
        String markedSource = "((_Delayed_ = true) && (FOO =~ 'ba.*'))";
        JexlNode marked = ASTDelayedPredicate.create(sourceNode);
        Assertions.assertEquals(markedSource, JexlStringBuildingVisitor.buildQueryWithoutParse(marked));
        
        // negate
        String negatedMarkedSource = "!((_Delayed_ = true) && (FOO =~ 'ba.*'))";
        JexlNode negated = JexlNodes.negate(marked);
        Assertions.assertEquals(negatedMarkedSource, JexlStringBuildingVisitor.buildQueryWithoutParse(negated));
        
        // validate it
        test(negatedMarkedSource, negatedMarkedSource);
        
        // technologic
    }
    
    @Test
    public void testFunction() {
        String query = "!filter:includeRegex(F1, '.*')";
        test(query, query);
    }
    
    @Test
    public void testExpandedFunction() {
        String query = "!filter:includeRegex((F1 || F2), '.*')";
        test(query, query);
    }
    
    @Test
    public void testNENotNegated() {
        String query = "!(F1 == 'v1') && F2 != 'v2'";
        String expected = "!(F1 == 'v1') && F2 != 'v2'";
        test(query, expected);
    }
    
    @Test
    public void testApplyDeMorgansNull() {
        assertThrows(IllegalArgumentException.class, () -> PushdownNegationVisitor.applyDeMorgans(null, false));
    }
    
    @Test
    public void testApplyDeMorgansNoChildren() {
        assertThrows(IllegalStateException.class, () -> PushdownNegationVisitor.applyDeMorgans(new ASTAndNode(1), false));
    }
    
    @Test
    public void testApplyDeMorgansNotAndOrNode() {
        JexlNode node = JexlNodeFactory.buildEQNode("field", "value");
        assertThrows(IllegalArgumentException.class, () -> PushdownNegationVisitor.applyDeMorgans(node, false));
    }
    
    @Test
    public void testApplyDeMorgansNegateAndRoot() {
        JexlNode child1 = JexlNodeFactory.buildEQNode("f1", "v1");
        JexlNode child2 = JexlNodeFactory.buildEQNode("f2", "v2");
        List<JexlNode> children = new ArrayList<>();
        children.add(child1);
        children.add(child2);
        JexlNode and = JexlNodeFactory.createUnwrappedAndNode(children);
        JexlNode result = PushdownNegationVisitor.applyDeMorgans(and, true);
        Assertions.assertEquals("!((!(f1 == 'v1') || !(f2 == 'v2')))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testApplyDeMorgansNegateOrRoot() {
        JexlNode child1 = JexlNodeFactory.buildEQNode("f1", "v1");
        JexlNode child2 = JexlNodeFactory.buildEQNode("f2", "v2");
        List<JexlNode> children = new ArrayList<>();
        children.add(child1);
        children.add(child2);
        JexlNode or = JexlNodeFactory.createUnwrappedOrNode(children);
        JexlNode result = PushdownNegationVisitor.applyDeMorgans(or, true);
        Assertions.assertEquals("!((!(f1 == 'v1') && !(f2 == 'v2')))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    private void test(String query, String expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            ASTJexlScript pushed = (ASTJexlScript) PushdownNegationVisitor.pushdownNegations(script);
            
            Assertions.assertEquals(expected, JexlStringBuildingVisitor.buildQuery(pushed));
            Assertions.assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseAndFlattenJexlQuery(expected), pushed));
            JexlNodeAssert.assertThat(pushed).hasValidLineage();
        } catch (ParseException e) {
            e.printStackTrace();
            Assertions.fail("Failed to complete test: " + e.getMessage());
        }
    }
}
