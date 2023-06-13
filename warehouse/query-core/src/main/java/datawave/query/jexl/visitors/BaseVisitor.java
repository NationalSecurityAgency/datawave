package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAnnotatedStatement;
import org.apache.commons.jexl3.parser.ASTAnnotation;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTBreak;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTContinue;
import org.apache.commons.jexl3.parser.ASTDecrementGetNode;
import org.apache.commons.jexl3.parser.ASTDefineVars;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTDoWhileStatement;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEWNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTExtendedLiteral;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTGetDecrementNode;
import org.apache.commons.jexl3.parser.ASTGetIncrementNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIdentifierAccess;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTIncrementGetNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTJxltLiteral;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNEWNode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNSWNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNullpNode;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTQualifiedIdentifier;
import org.apache.commons.jexl3.parser.ASTRangeNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTRegexLiteral;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSWNode;
import org.apache.commons.jexl3.parser.ASTSetAddNode;
import org.apache.commons.jexl3.parser.ASTSetAndNode;
import org.apache.commons.jexl3.parser.ASTSetDivNode;
import org.apache.commons.jexl3.parser.ASTSetLiteral;
import org.apache.commons.jexl3.parser.ASTSetModNode;
import org.apache.commons.jexl3.parser.ASTSetMultNode;
import org.apache.commons.jexl3.parser.ASTSetOrNode;
import org.apache.commons.jexl3.parser.ASTSetShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSetSubNode;
import org.apache.commons.jexl3.parser.ASTSetXorNode;
import org.apache.commons.jexl3.parser.ASTShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTShiftRightNode;
import org.apache.commons.jexl3.parser.ASTShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTUnaryPlusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.ParserVisitor;

/**
 * A common base for a visitor that calls <code>childrenAccept(this, data)</code> on every node visited.
 *
 */
public class BaseVisitor extends ParserVisitor {
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTDoWhileStatement node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTContinue node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTBreak node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTNullpNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTShiftLeftNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTShiftRightNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTShiftRightUnsignedNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTAddNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSWNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTNSWNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTEWNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTNEWNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSubNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTUnaryPlusNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTRegexLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTExtendedLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTRangeNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTIdentifierAccess node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTArguments node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTDefineVars node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetAddNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetSubNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetMultNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetDivNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetModNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetAndNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetXorNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetShiftLeftNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetShiftRightNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTSetShiftRightUnsignedNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTGetDecrementNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTGetIncrementNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTDecrementGetNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTIncrementGetNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTJxltLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTAnnotation node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTAnnotatedStatement node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    protected Object visit(ASTQualifiedIdentifier node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
}
