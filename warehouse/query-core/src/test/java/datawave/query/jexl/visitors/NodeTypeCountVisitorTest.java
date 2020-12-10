package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
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
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeTypeCountVisitorTest {
    
    // Verify the correct internal count is incremented for each node type.
    @Test
    public void testCountForEachType() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
        assertEquals(1, NodeTypeCountVisitor.countNodes(script).getTotalASTJexlScriptNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT)).getTotalASTJexlScriptNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTBlock(ParserTreeConstants.JJTBLOCK)).getTotalASTBlockNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTAmbiguous(ParserTreeConstants.JJTAMBIGUOUS)).getTotalASTAmbiguousNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTIfStatement(ParserTreeConstants.JJTIFSTATEMENT)).getTotalASTIfStatementNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTWhileStatement(ParserTreeConstants.JJTWHILESTATEMENT)).getTotalASTWhileStatementNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTForeachStatement(ParserTreeConstants.JJTFOREACHSTATEMENT)).getTotalASTForeachStatementNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTReturnStatement(ParserTreeConstants.JJTRETURNSTATEMENT)).getTotalASTReturnStatementNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTAssignment(ParserTreeConstants.JJTASSIGNMENT)).getTotalASTAssignmentNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTVar(ParserTreeConstants.JJTVAR)).getTotalASTVarNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTReference(ParserTreeConstants.JJTREFERENCE)).getTotalASTReferenceNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTTernaryNode(ParserTreeConstants.JJTTERNARYNODE)).getTotalASTTernaryNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTOrNode(ParserTreeConstants.JJTORNODE)).getTotalASTOrNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTAndNode(ParserTreeConstants.JJTANDNODE)).getTotalASTAndNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTBitwiseOrNode(ParserTreeConstants.JJTBITWISEORNODE)).getTotalASTBitwiseOrNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTBitwiseXorNode(ParserTreeConstants.JJTBITWISEXORNODE)).getTotalASTBitwiseXorNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTBitwiseAndNode(ParserTreeConstants.JJTBITWISEANDNODE)).getTotalASTBitwiseAndNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTEQNode(ParserTreeConstants.JJTEQNODE)).getTotalASTEQNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTNENode(ParserTreeConstants.JJTNENODE)).getTotalASTNENodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTLTNode(ParserTreeConstants.JJTLTNODE)).getTotalASTLTNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTGTNode(ParserTreeConstants.JJTGTNODE)).getTotalASTGTNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTLENode(ParserTreeConstants.JJTLENODE)).getTotalASTLENodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTGENode(ParserTreeConstants.JJTGENODE)).getTotalASTGENodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTERNode(ParserTreeConstants.JJTERNODE)).getTotalASTERNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTNRNode(ParserTreeConstants.JJTNRNODE)).getTotalASTNRNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTAdditiveNode(ParserTreeConstants.JJTADDITIVENODE)).getTotalASTAdditiveNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTAdditiveOperator(ParserTreeConstants.JJTADDITIVEOPERATOR)).getTotalASTAdditiveOperatorNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTMulNode(ParserTreeConstants.JJTMULNODE)).getTotalASTMulNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTDivNode(ParserTreeConstants.JJTDIVNODE)).getTotalASTDivNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTModNode(ParserTreeConstants.JJTMODNODE)).getTotalASTModNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTUnaryMinusNode(ParserTreeConstants.JJTUNARYMINUSNODE)).getTotalASTUnaryMinusNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTBitwiseComplNode(ParserTreeConstants.JJTBITWISECOMPLNODE)).getTotalASTBitwiseComplNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTNotNode(ParserTreeConstants.JJTNOTNODE)).getTotalASTNotNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER)).getTotalASTIdentifiersNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTNullLiteral(ParserTreeConstants.JJTNULLLITERAL)).getTotalASTNullLiteralNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTTrueNode(ParserTreeConstants.JJTTRUENODE)).getTotalASTTrueNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTFalseNode(ParserTreeConstants.JJTFALSENODE)).getTotalASTFalseNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL)).getTotalASTNumberLiteralNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL)).getTotalASTStringLiteralNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("[1, 2, 3]")).getTotalASTArrayLiteralNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("{'one':1, 'two':2, 'three':3}")).getTotalASTMapLiteralNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTMapEntry(ParserTreeConstants.JJTMAPENTRY)).getTotalASTMapEntryNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTEmptyFunction(ParserTreeConstants.JJTEMPTYFUNCTION)).getTotalASTEmptyFunctionNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTSizeFunction(ParserTreeConstants.JJTSIZEFUNCTION)).getTotalASTSizeFunctionNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTFunctionNode(ParserTreeConstants.JJTFUNCTIONNODE)).getTotalASTFunctionNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTMethodNode(ParserTreeConstants.JJTMETHODNODE)).getTotalASTMethodNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTSizeMethod(ParserTreeConstants.JJTSIZEMETHOD)).getTotalASTSizeMethodNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTConstructorNode(ParserTreeConstants.JJTCONSTRUCTORNODE)).getTotalASTConstructorNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTArrayAccess(ParserTreeConstants.JJTARRAYACCESS)).getTotalASTArrayAccessNodes());
        assertEquals(1, NodeTypeCountVisitor.countNodes(new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION))
                        .getTotalASTReferenceExpressionNodes());
    }
    
    @Test
    public void testHasRegexNodes() throws ParseException {
        assertFalse(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("FOO == 'bar'")).hasRegexNodes()); // No regex present.
        assertTrue(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("FOO =~ '*value*'")).hasRegexNodes()); // ER regex present.
        assertTrue(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("FOO !~ '*value*'")).hasRegexNodes()); // NR regex present.
    }
    
    @Test
    public void testHasPossibleBoundedRange() throws ParseException {
        assertFalse(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("FOO > 1 && BAR >= 2")).hasPossibleBoundedRange()); // Does not have LT or LE.
        assertFalse(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("FOO < 1 && BAR <= 2")).hasPossibleBoundedRange()); // Does not have GT or GE.
        assertTrue(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("FOO < 1 && BAR > 2")).hasPossibleBoundedRange()); // Has LT and GT.
        assertTrue(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("FOO < 1 && BAR >= 2")).hasPossibleBoundedRange()); // Has LT and GE.
        assertTrue(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("FOO <= 1 && BAR > 2")).hasPossibleBoundedRange()); // Has LE and GT.
        assertTrue(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("FOO <= 1 && BAR >= 2")).hasPossibleBoundedRange()); // Has LE and GE.
    }
    
    @Test
    public void testHasFunctionNodes() throws ParseException {
        assertFalse(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("FOO == 'bar'")).hasFunctionNodes()); // No function present.
        assertTrue(NodeTypeCountVisitor.countNodes(JexlASTHelper.parseJexlQuery("ns:expand(FOO == 'bar')")).hasFunctionNodes()); // Function present.
    }
}
