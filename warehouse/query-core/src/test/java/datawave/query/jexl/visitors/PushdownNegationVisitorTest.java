package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
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
    public void testEq() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 == 'v1')");
        String base = JexlStringBuildingVisitor.buildQuery(query);
        visitor.visit(query, null);
        Assertions.assertEquals(base, JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testNEq() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 != 'v1')");
        visitor.visit(query, null);
        Assertions.assertEquals("(F1 == 'v1')", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testNR() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 !~ 'v1')");
        visitor.visit(query, null);
        Assertions.assertEquals("(F1 =~ 'v1')", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testER() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 =~ 'v1')");
        visitor.visit(query, null);
        Assertions.assertEquals("!(F1 =~ 'v1')", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testDoubleNegationEq() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!!(F1 == 'v1')");
        visitor.visit(query, null);
        Assertions.assertEquals("(F1 == 'v1')", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testTripleNegationEq() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!!!(F1 == 'v1')");
        visitor.visit(query, null);
        Assertions.assertEquals("!(F1 == 'v1')", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testTripleNestedNegationEq() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(!(!(F1 == 'v1')))");
        visitor.visit(query, null);
        Assertions.assertEquals("((!(F1 == 'v1')))", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testTripleNegationNEq() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!!!(F1 != 'v1')");
        visitor.visit(query, null);
        Assertions.assertEquals("(F1 == 'v1')", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testAnd() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 == 'v1' && F2 == 'v2')");
        visitor.visit(query, null);
        Assertions.assertEquals("((!(F1 == 'v1') || !(F2 == 'v2')))", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testAndNE() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 != 'v1' && F2 != 'v2')");
        visitor.visit(query, null);
        Assertions.assertEquals("(((F1 == 'v1') || (F2 == 'v2')))", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testOr() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 == 'v1' || F2 == 'v2')");
        visitor.visit(query, null);
        Assertions.assertEquals("((!(F1 == 'v1') && !(F2 == 'v2')))", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testOrNE() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 != 'v1' || F2 != 'v2')");
        visitor.visit(query, null);
        Assertions.assertEquals("(((F1 == 'v1') && (F2 == 'v2')))", JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testNestedAnd() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 == 'v1' && F2 == 'v2' && (F3 == 'v3' || F4 == 'v4'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("((!(F1 == 'v1') || !(F2 == 'v2') || (((!(F3 == 'v3') && !(F4 == 'v4'))))))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testNestedAndMixedCancels() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 != 'v1' && !F2 == 'v2' && (F3 == 'v3' || F4 == 'v4'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("(((F1 == 'v1') || (F2 == 'v2') || (((!(F3 == 'v3') && !(F4 == 'v4'))))))", JexlStringBuildingVisitor.buildQuery(result));
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
        Assertions.assertEquals("((!(F1 == 'v1') || !(F2 == 'v2') || (((!(F3 == 'v3') && !(F4 == 'v4'))))))", JexlStringBuildingVisitor.buildQuery(result));
        Assertions.assertNotEquals(orig, JexlStringBuildingVisitor.buildQuery(result));
        Assertions.assertEquals(orig, JexlStringBuildingVisitor.buildQuery(query));
    }
    
    @Test
    public void testNestedOr() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 == 'v1' || F2 == 'v2' || (F3 == 'v3' && F4 == 'v4'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("((!(F1 == 'v1') && !(F2 == 'v2') && (((!(F3 == 'v3') || !(F4 == 'v4'))))))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testNestedOrMixedCancels() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 != 'v1' || !F2 == 'v2' || (F3 == 'v3' && F4 == 'v4'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("(((F1 == 'v1') && (F2 == 'v2') && (((!(F3 == 'v3') || !(F4 == 'v4'))))))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testDelayedPropertyMarkerPropagate() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!((_Delayed_ = true) && (F1 == 'v1' || F2 == 'v2'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("((_Delayed_ = true) && ((!(F1 == 'v1') && !(F2 == 'v2'))))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testEvaluationOnlyPropertyMarkerPropagate() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!((_Eval_ = true) && (F1 == 'v1' || F2 == 'v2'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("((_Eval_ = true) && ((!(F1 == 'v1') && !(F2 == 'v2'))))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testExceededOrPropertyMarkerPropagate() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!((_List_ = true) && (F1 == 'v1' || F2 == 'v2'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("!((_List_ = true) && (F1 == 'v1' || F2 == 'v2'))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testExceededValuePropertyMarkerPropagate() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!((_Value_ = true) && (F1 == 'v1' || F2 == 'v2'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("!((_Value_ = true) && (F1 == 'v1' || F2 == 'v2'))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testExceededTermPropertyMarkerPropagate() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!((_Term_ = true) && (F1 == 'v1' || F2 == 'v2'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("!((_Term_ = true) && (F1 == 'v1' || F2 == 'v2'))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testBoundedRangeNoPropagation() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("F3 == 'v3' || !((_Bounded_ = true) && (F1 >= 'v1' && F1 <= 'v2'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("F3 == 'v3' || !((_Bounded_ = true) && (F1 >= 'v1' && F1 <= 'v2'))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testPartialBoundedRangePropagation() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("F3 == 'v3' || !((_Bounded_ = true) && (F1 >= 'v1' && F2 <= 'v2'))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("F3 == 'v3' || !((_Bounded_ = true) && (F1 >= 'v1' && F2 <= 'v2'))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testMixedBoundedRanges() throws ParseException {
        ASTJexlScript query = JexlASTHelper
                        .parseJexlQuery("(F3 == 'v3' || !(((_Bounded_ = true) && (F1 >= 'v1' && F2 <= 'v2')) || !((_Bounded_ = true) && (F1 >= 'v1' && F1 <= 'v2'))))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals(
                        "(F3 == 'v3' || ((!(((_Bounded_ = true) && (F1 >= 'v1' && F2 <= 'v2'))) && (((_Bounded_ = true) && (F1 >= 'v1' && F1 <= 'v2'))))))",
                        JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testMixedMarkers() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!((_Delayed_ = true) && (F1 == 'v1' && ((_Term_ = true) && (F2 == 'v2'))))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("((_Delayed_ = true) && ((!(F1 == 'v1') || !(((_Term_ = true) && (F2 == 'v2'))))))",
                        JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testMixedMarkersInverted() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!((_Term_ = true) && (F1 == 'v1' && ((_Delayed_ = true) && (F2 == 'v2'))))");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("!((_Term_ = true) && (F1 == 'v1' && ((_Delayed_ = true) && (F2 == 'v2'))))", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testFunction() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!filter:includeRegex(F1, '.*')");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals(JexlStringBuildingVisitor.buildQuery(query), JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testExpandedFunction() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!filter:includeRegex(F1 || F2, '.*')");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals(JexlStringBuildingVisitor.buildQuery(query), JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testNENotNegated() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("!(F1 == 'v1') && F2 != 'v2'");
        JexlNode result = PushdownNegationVisitor.pushdownNegations(query);
        Assertions.assertEquals("!(F1 == 'v1') && F2 != 'v2'", JexlStringBuildingVisitor.buildQuery(result));
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
}
