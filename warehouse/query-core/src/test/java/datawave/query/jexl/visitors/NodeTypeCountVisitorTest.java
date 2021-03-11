package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.NodeTypeCount;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NodeTypeCountVisitorTest {
    
    @Test
    public void testSimpleNode() {
        NodeTypeCount count = (NodeTypeCount) new SimpleNode(ParserTreeConstants.JJTREFERENCE).jjtAccept(new NodeTypeCountVisitor(), null);
        assertEquals(1, count.getTotal(SimpleNode.class));
    }
    
    @Test
    public void testASTJexlScript() {
        assertEquals(1, count(new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT)).getTotal(ASTJexlScript.class));
    }
    
    @Test
    public void testASTBlock() {
        assertEquals(1, count(new ASTBlock(ParserTreeConstants.JJTBLOCK)).getTotal(ASTBlock.class));
    }
    
    @Test
    public void testASTAmbiguous() {
        assertEquals(1, count(new ASTAmbiguous(ParserTreeConstants.JJTAMBIGUOUS)).getTotal(ASTAmbiguous.class));
    }
    
    @Test
    public void testASTIfStatement() {
        assertEquals(1, count(new ASTIfStatement(ParserTreeConstants.JJTIFSTATEMENT)).getTotal(ASTIfStatement.class));
    }
    
    @Test
    public void testASTForeachStatement() {
        assertEquals(1, count(new ASTWhileStatement(ParserTreeConstants.JJTWHILESTATEMENT)).getTotal(ASTWhileStatement.class));
    }
    
    @Test
    public void testASTReturnStatement() {
        assertEquals(1, count(new ASTReturnStatement(ParserTreeConstants.JJTRETURNSTATEMENT)).getTotal(ASTReturnStatement.class));
    }
    
    @Test
    public void testASTAssignment() {
        assertEquals(1, count(new ASTAssignment(ParserTreeConstants.JJTASSIGNMENT)).getTotal(ASTAssignment.class));
    }
    
    @Test
    public void testASTVar() {
        assertEquals(1, count(new ASTVar(ParserTreeConstants.JJTVAR)).getTotal(ASTVar.class));
    }
    
    @Test
    public void testASTReference() {
        assertEquals(1, count(new ASTReference(ParserTreeConstants.JJTREFERENCE)).getTotal(ASTReference.class));
    }
    
    @Test
    public void testASTTernaryNode() {
        assertEquals(1, count(new ASTTernaryNode(ParserTreeConstants.JJTTERNARYNODE)).getTotal(ASTTernaryNode.class));
    }
    
    @Test
    public void testASTOrNode() {
        assertEquals(1, count(new ASTOrNode(ParserTreeConstants.JJTORNODE)).getTotal(ASTOrNode.class));
    }
    
    @Test
    public void testASTAndNode() {
        assertEquals(1, count(new ASTAndNode(ParserTreeConstants.JJTANDNODE)).getTotal(ASTAndNode.class));
    }
    
    @Test
    public void testASTBitwiseOrNode() {
        assertEquals(1, count(new ASTBitwiseOrNode(ParserTreeConstants.JJTBITWISEORNODE)).getTotal(ASTBitwiseOrNode.class));
    }
    
    @Test
    public void testASTBitwiseXorNodee() {
        assertEquals(1, count(new ASTBitwiseXorNode(ParserTreeConstants.JJTBITWISEXORNODE)).getTotal(ASTBitwiseXorNode.class));
    }
    
    @Test
    public void testASTBitwiseAndNode() {
        assertEquals(1, count(new ASTBitwiseAndNode(ParserTreeConstants.JJTBITWISEANDNODE)).getTotal(ASTBitwiseAndNode.class));
    }
    
    @Test
    public void testASTEQNode() {
        assertEquals(1, count(new ASTEQNode(ParserTreeConstants.JJTEQNODE)).getTotal(ASTEQNode.class));
    }
    
    @Test
    public void testASTNENode() {
        assertEquals(1, count(new ASTNENode(ParserTreeConstants.JJTNENODE)).getTotal(ASTNENode.class));
    }
    
    @Test
    public void testASTLTNode() {
        assertEquals(1, count(new ASTLTNode(ParserTreeConstants.JJTLTNODE)).getTotal(ASTLTNode.class));
    }
    
    @Test
    public void testASTGTNode() {
        assertEquals(1, count(new ASTGTNode(ParserTreeConstants.JJTGTNODE)).getTotal(ASTGTNode.class));
    }
    
    @Test
    public void testASTLENode() {
        assertEquals(1, count(new ASTLENode(ParserTreeConstants.JJTLENODE)).getTotal(ASTLENode.class));
    }
    
    @Test
    public void testASTGENode() {
        assertEquals(1, count(new ASTGENode(ParserTreeConstants.JJTGENODE)).getTotal(ASTGENode.class));
    }
    
    @Test
    public void testASTERNode() {
        assertEquals(1, count(new ASTERNode(ParserTreeConstants.JJTERNODE)).getTotal(ASTERNode.class));
    }
    
    @Test
    public void testASTNRNode() {
        assertEquals(1, count(new ASTNRNode(ParserTreeConstants.JJTNRNODE)).getTotal(ASTNRNode.class));
    }
    
    @Test
    public void testASTAdditiveNode() {
        assertEquals(1, count(new ASTAdditiveNode(ParserTreeConstants.JJTADDITIVENODE)).getTotal(ASTAdditiveNode.class));
    }
    
    @Test
    public void testASTAdditiveOperator() {
        assertEquals(1, count(new ASTAdditiveOperator(ParserTreeConstants.JJTADDITIVEOPERATOR)).getTotal(ASTAdditiveOperator.class));
    }
    
    @Test
    public void testASTMulNode() {
        assertEquals(1, count(new ASTMulNode(ParserTreeConstants.JJTMULNODE)).getTotal(ASTMulNode.class));
    }
    
    @Test
    public void testASTDivNode() {
        assertEquals(1, count(new ASTDivNode(ParserTreeConstants.JJTDIVNODE)).getTotal(ASTDivNode.class));
    }
    
    @Test
    public void testASTModNode() {
        assertEquals(1, count(new ASTModNode(ParserTreeConstants.JJTMODNODE)).getTotal(ASTModNode.class));
    }
    
    @Test
    public void testASTUnaryMinusNode() {
        assertEquals(1, count(new ASTUnaryMinusNode(ParserTreeConstants.JJTUNARYMINUSNODE)).getTotal(ASTUnaryMinusNode.class));
    }
    
    @Test
    public void testASTBitwiseComplNode() {
        assertEquals(1, count(new ASTBitwiseComplNode(ParserTreeConstants.JJTBITWISECOMPLNODE)).getTotal(ASTBitwiseComplNode.class));
    }
    
    @Test
    public void testASTNotNode() {
        assertEquals(1, count(new ASTNotNode(ParserTreeConstants.JJTNOTNODE)).getTotal(ASTNotNode.class));
    }
    
    @Test
    public void testASTIdentifier() {
        assertEquals(1, count(new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER)).getTotal(ASTIdentifier.class));
    }
    
    @Test
    public void testASTNullLiteral() {
        assertEquals(1, count(new ASTNullLiteral(ParserTreeConstants.JJTNULLLITERAL)).getTotal(ASTNullLiteral.class));
    }
    
    @Test
    public void testASTTrueNode() {
        assertEquals(1, count(new ASTTrueNode(ParserTreeConstants.JJTTRUENODE)).getTotal(ASTTrueNode.class));
    }
    
    @Test
    public void testASTFalseNode() {
        assertEquals(1, count(new ASTFalseNode(ParserTreeConstants.JJTFALSENODE)).getTotal(ASTFalseNode.class));
    }
    
    @Test
    public void testASTNumberLiteral() {
        assertEquals(1, count(new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL)).getTotal(ASTNumberLiteral.class));
    }
    
    @Test
    public void testASTStringLiteral() {
        assertEquals(1, count(new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL)).getTotal(ASTStringLiteral.class));
    }
    
    @Test
    public void testASTArrayLiteral() throws ParseException {
        assertEquals(1, count("[1, 2, 3]").getTotal(ASTArrayLiteral.class));
    }
    
    @Test
    public void testASTMapLiteral() throws ParseException {
        assertEquals(1, count("{'one':1, 'two':2, 'three':3}").getTotal(ASTMapLiteral.class));
    }
    
    @Test
    public void testASTMapEntry() {
        assertEquals(1, count(new ASTMapEntry(ParserTreeConstants.JJTMAPENTRY)).getTotal(ASTMapEntry.class));
    }
    
    @Test
    public void testASTEmptyFunction() {
        assertEquals(1, count(new ASTEmptyFunction(ParserTreeConstants.JJTEMPTYFUNCTION)).getTotal(ASTEmptyFunction.class));
    }
    
    @Test
    public void testASTSizeFunction() {
        assertEquals(1, count(new ASTSizeFunction(ParserTreeConstants.JJTSIZEFUNCTION)).getTotal(ASTSizeFunction.class));
    }
    
    @Test
    public void testASTFunctionNode() {
        assertEquals(1, count(new ASTFunctionNode(ParserTreeConstants.JJTFUNCTIONNODE)).getTotal(ASTFunctionNode.class));
    }
    
    @Test
    public void testASTMethodNode() {
        assertEquals(1, count(new ASTMethodNode(ParserTreeConstants.JJTMETHODNODE)).getTotal(ASTMethodNode.class));
    }
    
    @Test
    public void testASTSizeMethod() {
        assertEquals(1, count(new ASTSizeMethod(ParserTreeConstants.JJTSIZEMETHOD)).getTotal(ASTSizeMethod.class));
    }
    
    @Test
    public void testASTConstructorNode() {
        assertEquals(1, count(new ASTConstructorNode(ParserTreeConstants.JJTCONSTRUCTORNODE)).getTotal(ASTConstructorNode.class));
    }
    
    @Test
    public void testASTArrayAccess() {
        assertEquals(1, count(new ASTArrayAccess(ParserTreeConstants.JJTARRAYACCESS)).getTotal(ASTArrayAccess.class));
    }
    
    @Test
    public void testASTReferenceExpression() {
        assertEquals(1, count(new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION)).getTotal(ASTReferenceExpression.class));
    }
    
    @Test
    public void testASTDelayedPredicate() throws ParseException {
        assertEquals(1, count("((_Delayed_ = true) && (FOO == 1 && FOO == 3))").getTotal(ASTDelayedPredicate.class));
    }
    
    @Test
    public void testASTEvaluationOnly() throws ParseException {
        assertEquals(1, count("((_Eval_ = true) && (FOO == 1 && FOO == 3))").getTotal(ASTEvaluationOnly.class));
    }
    
    @Test
    public void testBoundedRange() throws ParseException {
        assertEquals(1, count("((_Bounded_ = true) && (FOO > 1 && FOO < 5))").getTotal(BoundedRange.class));
    }
    
    @Test
    public void testExceededOrThresholdMarkerJexlNode() throws ParseException {
        assertEquals(1, count("((_List_ = true) && (FOO == 1 && FOO == 3))").getTotal(ExceededOrThresholdMarkerJexlNode.class));
    }
    
    @Test
    public void testExceededTermThresholdMarkerJexlNode() throws ParseException {
        assertEquals(1, count("((_Term_ = true) && (FOO == 1 && FOO == 3))").getTotal(ExceededTermThresholdMarkerJexlNode.class));
    }
    
    @Test
    public void testExceededValueThresholdMarkerJexlNode() throws ParseException {
        assertEquals(1, count("((_Value_ = true) && (FOO == 1 && FOO == 3))").getTotal(ExceededValueThresholdMarkerJexlNode.class));
    }
    
    @Test
    public void testIndexHoleMarkerJexlNode() throws ParseException {
        assertEquals(1, count("((_Hole_ = true) && (FOO == 1 && FOO == 3))").getTotal(IndexHoleMarkerJexlNode.class));
    }
    
    private NodeTypeCount count(String query) throws ParseException {
        return count(JexlASTHelper.parseJexlQuery(query));
    }
    
    private NodeTypeCount count(JexlNode script) {
        return NodeTypeCountVisitor.countNodes(script);
    }
}
