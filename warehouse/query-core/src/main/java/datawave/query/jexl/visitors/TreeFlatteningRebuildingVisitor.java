package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAmbiguous;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTUnknownFieldERNode;
import org.apache.commons.jexl3.parser.ASTUnsatisfiableERNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;

/**
 * This is a wrapper for the TreeFlatteningRebuilder which allows the TreeFlatteningRebuilder to be run as a Jexl node visitor.
 */
public class TreeFlatteningRebuildingVisitor extends RebuildingVisitor {
    
    final private TreeFlatteningRebuilder treeFlatteningRebuilder;
    
    public TreeFlatteningRebuildingVisitor(boolean simplifyReferenceExpressions) {
        this.treeFlatteningRebuilder = new TreeFlatteningRebuilder(simplifyReferenceExpressions);
    }
    
    /**
     * This will flatten ands and ors.
     * 
     * @param node
     *            the node to flatten
     * @param <T>
     *            type of the node
     * @return the flattened copy
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T flatten(T node) {
        return TreeFlatteningRebuilder.flatten(node);
    }
    
    /**
     * This will flatten ands, ors, and reference expressions NOTE: If you remove reference expressions, this may adversely affect the evaluation of the query
     * (true in the index query logic case: bug?).
     * 
     * @param node
     *            the node to flatten
     * @param <T>
     *            type of the node
     * @return the flattened copy
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T flattenAll(T node) {
        return TreeFlatteningRebuilder.flattenAll(node);
    }
    
    @Override
    public ASTJexlScript apply(ASTJexlScript input) {
        return treeFlatteningRebuilder.flattenTree(input);
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTAddNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTSubNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        return treeFlatteningRebuilder.flattenTree(node);
    }
}
