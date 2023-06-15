package datawave.query.jexl.visitors;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.jexl3.parser.JexlNodes.children;
import static org.apache.commons.jexl3.parser.JexlNodes.newInstanceOfType;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

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
import org.apache.commons.jexl3.parser.ASTNamespaceIdentifier;
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
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import com.google.common.base.Function;

/**
 * Base Visitor class that returns a new AST. Each visit method should return a copy of the visited node.
 *
 */
public class RebuildingVisitor extends BaseVisitor implements Function<ASTJexlScript,ASTJexlScript> {
    
    protected QueryStopwatch timers = null;
    protected String description = "Configurable script";
    protected String scriptName = "";
    
    public RebuildingVisitor(final QueryStopwatch timers, final String scriptName) {
        this.timers = timers;
        this.scriptName = scriptName;
    }
    
    public RebuildingVisitor() {
        
    }
    
    /**
     * Helper method to return a copy of the tree denoted by the given root
     *
     * @param root
     *            the root node
     * @return a copy of the tree
     */
    public static JexlNode copy(JexlNode root) {
        RebuildingVisitor visitor = new RebuildingVisitor();
        JexlNode copy = (JexlNode) root.jjtAccept(visitor, null);
        copy.jjtSetParent(root.jjtGetParent());
        return copy;
    }
    
    public static JexlNode copyInto(JexlNode root, JexlNode target) {
        RebuildingVisitor visitor = new RebuildingVisitor();
        
        JexlNode copyRoot = (JexlNode) root.jjtAccept(visitor, null);
        
        if (null != copyRoot) {
            target.jjtSetParent(copyRoot.jjtGetParent());
            for (int i = 0; i < copyRoot.jjtGetNumChildren(); i++) {
                JexlNode child = copyRoot.jjtGetChild(i);
                target.jjtAddChild(child, i);
                child.jjtSetParent(target);
            }
        }
        
        return target;
    }
    
    private <T extends JexlNode> T copy(T node, Object data) {
        T newNode = newInstanceOfType(node);
        ArrayList<JexlNode> children = newArrayList();
        for (JexlNode child : children(node)) {
            JexlNode copiedChild = (JexlNode) child.jjtAccept(this, data);
            if (copiedChild != null) {
                children.add(copiedChild);
            }
        }
        return children(newNode, children.toArray(new JexlNode[children.size()]));
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        ASTIdentifier copy = copy(node, data);
        
        JexlNodes.setIdentifier(copy, node.getName());
        
        copy.setRedefined(node.isRedefined());
        copy.setShaded(node.isShaded());
        copy.setCaptured(node.isCaptured());
        copy.setLexical(node.isLexical());
        copy.setConstant(node.isConstant());
        
        if (copy instanceof ASTNamespaceIdentifier) {
            ((ASTNamespaceIdentifier) copy).setNamespace(node.getNamespace(), node.getName());
        }
        
        return copy;
    }
    
    @Override
    protected Object visit(ASTArguments node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        ASTStringLiteral copy = copy(node, data);
        JexlNodes.setLiteral(copy, node.getLiteral());
        return copy;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        ASTNumberLiteral copy = copy(node, data);
        
        if (!JexlNodes.setLiteral(copy, node.getLiteral())) {
            QueryException qe = new QueryException(DatawaveErrorCode.ASTNUMBERLITERAL_TYPE_ASCERTAIN_ERROR, MessageFormat.format("Node: {0}", node));
            throw new DatawaveFatalQueryException(qe);
        }
        
        return copy;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTDoWhileStatement node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTContinue node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTBreak node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTNullpNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTShiftLeftNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTShiftRightNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTShiftRightUnsignedNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSWNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTNSWNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTEWNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTNEWNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTAddNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSubNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTUnaryPlusNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTRegexLiteral node, Object data) {
        ASTRegexLiteral copy = copy(node, data);
        JexlNodes.setLiteral(copy, node.getLiteral().pattern());
        return copy;
    }
    
    @Override
    protected Object visit(ASTSetLiteral node, Object data) {
        ASTSetLiteral copy = copy(node, data);
        copy.jjtClose();
        return copy;
    }
    
    @Override
    protected Object visit(ASTExtendedLiteral node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTRangeNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTIdentifierAccess node, Object data) {
        ASTIdentifierAccess copy = copy(node, data);
        JexlNodes.setIdentifierAccess(copy, node.getName());
        return copy;
    }
    
    @Override
    protected Object visit(ASTDefineVars node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetAddNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetSubNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetMultNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetDivNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetModNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetAndNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetOrNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetXorNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetShiftLeftNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetShiftRightNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTSetShiftRightUnsignedNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTGetDecrementNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTGetIncrementNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTDecrementGetNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTIncrementGetNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTJxltLiteral node, Object data) {
        ASTJxltLiteral copy = copy(node, data);
        JexlNodes.setLiteral(copy, node.getLiteral());
        return copy;
    }
    
    @Override
    protected Object visit(ASTAnnotation node, Object data) {
        ASTAnnotation copy = copy(node, data);
        JexlNodes.setAnnotation(copy, node.getName());
        return copy;
    }
    
    @Override
    protected Object visit(ASTAnnotatedStatement node, Object data) {
        return copy(node, data);
    }
    
    @Override
    protected Object visit(ASTQualifiedIdentifier node, Object data) {
        ASTQualifiedIdentifier copy = copy(node, data);
        JexlNodes.setQualifiedIdentifier(copy, node.getName());
        return copy;
    }
    
    /**
     * Base setup so that we can eventually move to a more functional model
     * 
     * @param input
     *            the input script
     * @return the script with applied models
     */
    @Override
    public ASTJexlScript apply(final ASTJexlScript input) {
        TraceStopwatch stopwatch = null;
        if (null != timers) {
            stopwatch = timers.newStartedStopwatch(scriptName + " - " + description);
        }
        ASTJexlScript script = (ASTJexlScript) input.jjtAccept(this, null);
        if (null != stopwatch)
            stopwatch.stop();
        return script;
    }
    
    public RebuildingVisitor setDescription(String description) {
        this.description = description;
        return this;
    }
    
}
