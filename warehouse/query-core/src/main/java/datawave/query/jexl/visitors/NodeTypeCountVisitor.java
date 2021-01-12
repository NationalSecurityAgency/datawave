package datawave.query.jexl.visitors;

import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
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
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;

/**
 * Counts the total number of nodes for each node type present in a query tree.
 */
public class NodeTypeCountVisitor implements ParserVisitor {
    
    public static NodeTypeCountVisitor countNodes(JexlNode node) {
        NodeTypeCountVisitor visitor = new NodeTypeCountVisitor();
        node.jjtAccept(visitor, null);
        return visitor;
    }
    
    private int totalSimpleNodes;
    private int totalASTJexlScriptNodes;
    private int totalASTBlockNodes;
    private int totalASTAmbiguousNodes;
    private int totalASTIfStatementNodes;
    private int totalASTWhileStatementNodes;
    private int totalASTForeachStatementNodes;
    private int totalASTReturnStatementNodes;
    private int totalASTAssignmentNodes;
    private int totalASTVarNodes;
    private int totalASTReferenceNodes;
    private int totalASTTernaryNodes;
    private int totalASTOrNodes;
    private int totalASTAndNodes;
    private int totalASTBitwiseOrNodes;
    private int totalASTBitwiseXorNodes;
    private int totalASTBitwiseAndNodes;
    private int totalASTEQNodes;
    private int totalASTNENodes;
    private int totalASTLTNodes;
    private int totalASTGTNodes;
    private int totalASTLENodes;
    private int totalASTGENodes;
    private int totalASTERNodes;
    private int totalASTNRNodes;
    private int totalASTAdditiveNodes;
    private int totalASTAdditiveOperatorNodes;
    private int totalASTMulNodes;
    private int totalASTDivNodes;
    private int totalASTModNodes;
    private int totalASTUnaryMinusNodes;
    private int totalASTBitwiseComplNodes;
    private int totalASTNotNodes;
    private int totalASTIdentifiersNodes;
    private int totalASTNullLiteralNodes;
    private int totalASTTrueNodes;
    private int totalASTFalseNodes;
    private int totalASTNumberLiteralNodes;
    private int totalASTStringLiteralNodes;
    private int totalASTArrayLiteralNodes;
    private int totalASTMapLiteralNodes;
    private int totalASTMapEntryNodes;
    private int totalASTEmptyFunctionNodes;
    private int totalASTSizeFunctionNodes;
    private int totalASTFunctionNodes;
    private int totalASTMethodNodes;
    private int totalASTSizeMethodNodes;
    private int totalASTConstructorNodes;
    private int totalASTArrayAccessNodes;
    private int totalASTReferenceExpressionNodes;
    private int totalASTDelayedPredicateNodes;
    private int totalASTEvaluationOnlyNodes;
    private int totalBoundedRangeNodes;
    private int totalExceededOrThresholdMarkerJexlNode;
    private int totalExceededTermThresholdMarkerJexlNodes;
    private int totalExceededValueThresholdMarkerJexlNodes;
    private int totalIndexHoleMarkerJexlNodes;
    
    @Override
    public Object visit(SimpleNode node, Object data) {
        totalSimpleNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        totalASTJexlScriptNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        totalASTBlockNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        totalASTAmbiguousNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        totalASTIfStatementNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        totalASTWhileStatementNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        totalASTForeachStatementNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        totalASTReturnStatementNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        totalASTAssignmentNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        totalASTVarNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        // Check if this is a specialized reference type.
        if (node instanceof QueryPropertyMarker) {
            if (node instanceof ASTDelayedPredicate) {
                totalASTDelayedPredicateNodes++;
            } else if (node instanceof ASTEvaluationOnly) {
                totalASTEvaluationOnlyNodes++;
            } else if (node instanceof BoundedRange) {
                totalBoundedRangeNodes++;
            } else if (node instanceof ExceededOrThresholdMarkerJexlNode) {
                totalExceededOrThresholdMarkerJexlNode++;
            } else if (node instanceof ExceededTermThresholdMarkerJexlNode) {
                totalExceededTermThresholdMarkerJexlNodes++;
            } else if (node instanceof ExceededValueThresholdMarkerJexlNode) {
                totalExceededValueThresholdMarkerJexlNodes++;
            } else if (node instanceof IndexHoleMarkerJexlNode) {
                totalIndexHoleMarkerJexlNodes++;
            }
        } else {
            totalASTReferenceNodes++;
        }
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        totalASTTernaryNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        totalASTOrNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        totalASTAndNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        totalASTBitwiseOrNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        totalASTBitwiseXorNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        totalASTBitwiseAndNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        totalASTEQNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        totalASTNENodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        totalASTLTNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        totalASTGTNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        totalASTLENodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        totalASTGENodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        totalASTERNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        totalASTNRNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        totalASTAdditiveNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        totalASTAdditiveOperatorNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        totalASTMulNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        totalASTDivNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        totalASTModNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        totalASTUnaryMinusNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        totalASTBitwiseComplNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        totalASTNotNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        totalASTIdentifiersNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        totalASTNullLiteralNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        totalASTTrueNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        totalASTFalseNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        totalASTNumberLiteralNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        totalASTStringLiteralNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        totalASTArrayLiteralNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        totalASTMapLiteralNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        totalASTMapEntryNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        totalASTEmptyFunctionNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        totalASTSizeFunctionNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        totalASTFunctionNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        totalASTMethodNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        totalASTSizeMethodNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        totalASTConstructorNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        totalASTArrayAccessNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        totalASTReferenceExpressionNodes++;
        node.childrenAccept(this, data);
        return null;
    }
    
    /**
     * Return the total {@link SimpleNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalSimpleNodes() {
        return totalSimpleNodes;
    }
    
    /**
     * Return the total {@link ASTJexlScript} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTJexlScriptNodes() {
        return totalASTJexlScriptNodes;
    }
    
    /**
     * Return the total {@link ASTBlock} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTBlockNodes() {
        return totalASTBlockNodes;
    }
    
    /**
     * Return the total {@link ASTAmbiguous} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTAmbiguousNodes() {
        return totalASTAmbiguousNodes;
    }
    
    /**
     * Return the total {@link ASTIfStatement} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTIfStatementNodes() {
        return totalASTIfStatementNodes;
    }
    
    /**
     * Return the total {@link ASTWhileStatement} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTWhileStatementNodes() {
        return totalASTWhileStatementNodes;
    }
    
    /**
     * Return the total {@link ASTForeachStatement} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTForeachStatementNodes() {
        return totalASTForeachStatementNodes;
    }
    
    /**
     * Return the total {@link ASTReturnStatement} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTReturnStatementNodes() {
        return totalASTReturnStatementNodes;
    }
    
    /**
     * Return the total {@link ASTAssignment} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTAssignmentNodes() {
        return totalASTAssignmentNodes;
    }
    
    /**
     * Return the total {@link ASTVar} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTVarNodes() {
        return totalASTVarNodes;
    }
    
    /**
     * Return the total {@link ASTReference} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTReferenceNodes() {
        return totalASTReferenceNodes;
    }
    
    /**
     * Return the total {@link ASTTernaryNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTTernaryNodes() {
        return totalASTTernaryNodes;
    }
    
    /**
     * Return the total {@link ASTOrNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTOrNodes() {
        return totalASTOrNodes;
    }
    
    /**
     * Return the total {@link ASTAndNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTAndNodes() {
        return totalASTAndNodes;
    }
    
    /**
     * Return the total {@link ASTBitwiseOrNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTBitwiseOrNodes() {
        return totalASTBitwiseOrNodes;
    }
    
    /**
     * Return the total {@link ASTBitwiseXorNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTBitwiseXorNodes() {
        return totalASTBitwiseXorNodes;
    }
    
    /**
     * Return the total {@link ASTBitwiseAndNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTBitwiseAndNodes() {
        return totalASTBitwiseAndNodes;
    }
    
    /**
     * Return the total {@link ASTEQNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTEQNodes() {
        return totalASTEQNodes;
    }
    
    /**
     * Return the total {@link ASTNENode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTNENodes() {
        return totalASTNENodes;
    }
    
    /**
     * Return the total {@link ASTLTNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTLTNodes() {
        return totalASTLTNodes;
    }
    
    /**
     * Return the total {@link ASTGTNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTGTNodes() {
        return totalASTGTNodes;
    }
    
    /**
     * Return the total {@link ASTLENode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTLENodes() {
        return totalASTLENodes;
    }
    
    /**
     * Return the total {@link ASTGENode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTGENodes() {
        return totalASTGENodes;
    }
    
    /**
     * Return the total {@link ASTERNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTERNodes() {
        return totalASTERNodes;
    }
    
    /**
     * Return the total {@link ASTNRNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTNRNodes() {
        return totalASTNRNodes;
    }
    
    /**
     * Return the total {@link ASTAdditiveNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTAdditiveNodes() {
        return totalASTAdditiveNodes;
    }
    
    /**
     * Return the total {@link ASTAdditiveOperator} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTAdditiveOperatorNodes() {
        return totalASTAdditiveOperatorNodes;
    }
    
    /**
     * Return the total {@link ASTMulNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTMulNodes() {
        return totalASTMulNodes;
    }
    
    /**
     * Return the total {@link ASTDivNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTDivNodes() {
        return totalASTDivNodes;
    }
    
    /**
     * Return the total {@link ASTModNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTModNodes() {
        return totalASTModNodes;
    }
    
    /**
     * Return the total {@link ASTUnaryMinusNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTUnaryMinusNodes() {
        return totalASTUnaryMinusNodes;
    }
    
    /**
     * Return the total {@link ASTBitwiseComplNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTBitwiseComplNodes() {
        return totalASTBitwiseComplNodes;
    }
    
    /**
     * Return the total {@link ASTNotNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTNotNodes() {
        return totalASTNotNodes;
    }
    
    /**
     * Return the total {@link ASTIdentifier} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTIdentifiersNodes() {
        return totalASTIdentifiersNodes;
    }
    
    /**
     * Return the total {@link ASTNullLiteral} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTNullLiteralNodes() {
        return totalASTNullLiteralNodes;
    }
    
    /**
     * Return the total {@link ASTTrueNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTTrueNodes() {
        return totalASTTrueNodes;
    }
    
    /**
     * Return the total {@link ASTFalseNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTFalseNodes() {
        return totalASTFalseNodes;
    }
    
    /**
     * Return the total {@link ASTNumberLiteral} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTNumberLiteralNodes() {
        return totalASTNumberLiteralNodes;
    }
    
    /**
     * Return the total {@link ASTStringLiteral} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTStringLiteralNodes() {
        return totalASTStringLiteralNodes;
    }
    
    /**
     * Return the total {@link ASTArrayLiteral} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTArrayLiteralNodes() {
        return totalASTArrayLiteralNodes;
    }
    
    /**
     * Return the total {@link ASTMapLiteral} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTMapLiteralNodes() {
        return totalASTMapLiteralNodes;
    }
    
    /**
     * Return the total {@link ASTMapEntry} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTMapEntryNodes() {
        return totalASTMapEntryNodes;
    }
    
    /**
     * Return the total {@link ASTEmptyFunction} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTEmptyFunctionNodes() {
        return totalASTEmptyFunctionNodes;
    }
    
    /**
     * Return the total {@link ASTSizeFunction} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTSizeFunctionNodes() {
        return totalASTSizeFunctionNodes;
    }
    
    /**
     * Return the total {@link ASTFunctionNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTFunctionNodes() {
        return totalASTFunctionNodes;
    }
    
    /**
     * Return the total {@link ASTMethodNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTMethodNodes() {
        return totalASTMethodNodes;
    }
    
    /**
     * Return the total {@link ASTSizeMethod} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTSizeMethodNodes() {
        return totalASTSizeMethodNodes;
    }
    
    /**
     * Return the total {@link ASTConstructorNode} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTConstructorNodes() {
        return totalASTConstructorNodes;
    }
    
    /**
     * Return the total {@link ASTArrayAccess} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTArrayAccessNodes() {
        return totalASTArrayAccessNodes;
    }
    
    /**
     * Return the total {@link ASTReferenceExpression} nodes found.
     * 
     * @return the total count
     */
    public int getTotalASTReferenceExpressionNodes() {
        return totalASTReferenceExpressionNodes;
    }
    
    /**
     * Return the total {@link org.apache.commons.jexl2.parser.ASTDelayedPredicate} nodes found.
     *
     * @return the total count
     */
    public int getTotalASTDelayedPredicateNodes() {
        return totalASTDelayedPredicateNodes;
    }
    
    /**
     * Return the total {@link org.apache.commons.jexl2.parser.ASTEvaluationOnly} nodes found.
     *
     * @return the total count
     */
    public int getTotalASTEvaluationOnlyNodes() {
        return totalASTEvaluationOnlyNodes;
    }
    
    /**
     * Return the total {@link datawave.query.jexl.nodes.BoundedRange} nodes found.
     *
     * @return the total count
     */
    public int getTotalBoundedRangeNodes() {
        return totalBoundedRangeNodes;
    }
    
    /**
     * Return the total {@link datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode} nodes found.
     *
     * @return the total count
     */
    public int getTotalExceededOrThresholdMarkerJexlNode() {
        return totalExceededOrThresholdMarkerJexlNode;
    }
    
    /**
     * Return the total {@link datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode} nodes found.
     *
     * @return the total count
     */
    public int getTotalExceededTermThresholdMarkerJexlNodes() {
        return totalExceededTermThresholdMarkerJexlNodes;
    }
    
    /**
     * Return the total {@link datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode} nodes found.
     *
     * @return the total count
     */
    public int getTotalExceededValueThresholdMarkerJexlNodes() {
        return totalExceededValueThresholdMarkerJexlNodes;
    }
    
    /**
     * Return the total {@link datawave.query.jexl.nodes.IndexHoleMarkerJexlNode} nodes found.
     *
     * @return the total count
     */
    public int getTotalIndexHoleMarkerJexlNodes() {
        return totalIndexHoleMarkerJexlNodes;
    }
    
    /**
     * Return whether or not any regex nodes were found.
     *
     * @return true if at least one regex node was found, or false otherwise
     */
    public boolean hasRegexNodes() {
        return totalASTNRNodes > 0 || totalASTERNodes > 0;
    }
    
    /**
     * Return whether or not if a node indicating a bounded range was found.
     *
     * @return true if a bounded range was found, or false otherwise
     */
    public boolean hasBoundedRange() {
        return totalBoundedRangeNodes > 0;
    }
    
    /**
     * Return whether or not any function nodes were found.
     *
     * @return true if at least one function node was found, or false otherwise.
     */
    public boolean hasFunctionNodes() {
        return totalASTFunctionNodes > 0;
    }
}
