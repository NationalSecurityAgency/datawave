package datawave.query.jexl.visitors;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
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
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
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
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.jexl2.parser.SimpleNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Visitor can be used to identify and prune children of a JexlNode that will always be TRUE or FALSE. Our use of _NOFIELD_
 */
public class QueryPruningVisitor extends BaseVisitor {
    public enum TRUTH_STATE {
        UNKNOWN, TRUE, FALSE
    }
    
    /**
     * Given a JexlNode, determine if any children or even the node itself can be replaced by a TrueNode or FalseNode, For any rewritten sub-trees, create an
     * AND node which combines an assignment showing the section of the pruned tree, and the replacement ASTTrue/ASTFalse result of the prune
     * 
     * @param node
     *            a non-null query node
     * @return a rewritten query tree for node
     */
    public static JexlNode reduce(JexlNode node) {
        QueryPruningVisitor visitor = new QueryPruningVisitor(true);
        
        // flatten the tree and create a copy
        JexlNode copy = TreeFlatteningRebuildingVisitor.flatten(node);
        
        copy.jjtAccept(visitor, null);
        
        return copy;
    }
    
    /**
     * Given a JexlNode determine if the query the node represents is TRUE/FALSE/UNKNOWN
     * 
     * @param node
     *            non-null JexlNode
     * @return TRUE if evaluation would always return true, FALSE if it will always return false, otherwise UNKNOWN
     */
    public static TRUTH_STATE getState(JexlNode node) {
        QueryPruningVisitor visitor = new QueryPruningVisitor(false);
        return (TRUTH_STATE) node.jjtAccept(visitor, null);
    }
    
    private boolean rewrite;
    
    private QueryPruningVisitor(boolean rewrite) {
        this.rewrite = rewrite;
    }
    
    // base cases find a _NOFIELD_ replace it with a FALSE, otherwise unknown
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return identifyAndReplace(node);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return identifyAndReplace(node);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return identifyAndReplace(node);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return identifyAndReplace(node);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return identifyAndReplace(node);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return identifyAndReplace(node);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return identifyAndReplace(node);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return identifyAndReplace(node);
    }
    
    // handle merge ups and replacements
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        Set<TRUTH_STATE> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((TRUTH_STATE) (node.jjtGetChild(i).jjtAccept(this, data)));
        }
        
        if (states.contains(TRUTH_STATE.UNKNOWN) || states.size() > 1) {
            return TRUTH_STATE.UNKNOWN;
        } else {
            if (states.iterator().next() == TRUTH_STATE.TRUE) {
                replaceAndAssign(node, new ASTFalseNode(ParserTreeConstants.JJTFALSENODE));
                return TRUTH_STATE.FALSE;
            } else {
                replaceAndAssign(node, new ASTTrueNode(ParserTreeConstants.JJTTRUENODE));
                return TRUTH_STATE.TRUE;
            }
        }
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        Set<TRUTH_STATE> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((TRUTH_STATE) (node.jjtGetChild(i).jjtAccept(this, data)));
        }
        
        if (states.contains(TRUTH_STATE.TRUE)) {
            replaceAndAssign(node, new ASTTrueNode(ParserTreeConstants.JJTTRUENODE));
            return TRUTH_STATE.TRUE;
        } else if (states.contains(TRUTH_STATE.UNKNOWN)) {
            return TRUTH_STATE.UNKNOWN;
        } else {
            replaceAndAssign(node, new ASTFalseNode(ParserTreeConstants.JJTFALSENODE));
            return TRUTH_STATE.FALSE;
        }
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        Set<TRUTH_STATE> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((TRUTH_STATE) (node.jjtGetChild(i).jjtAccept(this, data)));
        }
        
        if (states.contains(TRUTH_STATE.FALSE)) {
            replaceAndAssign(node, new ASTFalseNode(ParserTreeConstants.JJTFALSENODE));
            return TRUTH_STATE.FALSE;
        } else if (states.contains(TRUTH_STATE.UNKNOWN)) {
            return TRUTH_STATE.UNKNOWN;
        } else {
            replaceAndAssign(node, new ASTTrueNode(ParserTreeConstants.JJTTRUENODE));
            return TRUTH_STATE.TRUE;
        }
    }
    
    // deal with wrappers
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        // attempt to reduce
        Set<TRUTH_STATE> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((TRUTH_STATE) (node.jjtGetChild(i).jjtAccept(this, data)));
        }
        
        if (states.size() == 1) {
            return states.iterator().next();
        } else {
            return TRUTH_STATE.UNKNOWN;
        }
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        Set<TRUTH_STATE> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((TRUTH_STATE) (node.jjtGetChild(i).jjtAccept(this, data)));
        }
        
        if (states.size() == 1) {
            return states.iterator().next();
        } else {
            return TRUTH_STATE.UNKNOWN;
        }
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        Set<TRUTH_STATE> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((TRUTH_STATE) (node.jjtGetChild(i).jjtAccept(this, data)));
        }
        
        if (states.size() == 1) {
            return states.iterator().next();
        } else {
            return TRUTH_STATE.UNKNOWN;
        }
    }
    
    // true/false for respective nodes
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return TRUTH_STATE.TRUE;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return TRUTH_STATE.FALSE;
    }
    
    // propagate unknown for all other things we may encounter
    @Override
    public Object visit(SimpleNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTIntegerLiteral node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTFloatLiteral node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return TRUTH_STATE.UNKNOWN;
    }
    
    /**
     * Simple case processing only. If a candidate node has exactly 1 identifier and literal node and it is _NO_FIELD_ we know the truth state will always be
     * FALSE
     * 
     * @param node
     * @return TRUTH_STATE.FALSE if the the sole identifier is _NOFIELD_, otherwise TRUTH_STATE.UNKNOWN
     */
    private TRUTH_STATE identifyAndReplace(JexlNode node) {
        try {
            if (Constants.NO_FIELD.equals(JexlASTHelper.getIdentifier(node))) {
                return TRUTH_STATE.FALSE;
            }
        } catch (NoSuchElementException e) {
            // no-op
        }
        
        return TRUTH_STATE.UNKNOWN;
    }
    
    /**
     * Prune a tree with an assignment showing the portion of the tree that was pruned. This will be more verbose, but will clearly identify what was removed.
     * Only remove a node if rewrite is enabled
     * 
     * @param toReplace
     * @param baseReplacement
     */
    private void replaceAndAssign(JexlNode toReplace, JexlNode baseReplacement) {
        if (rewrite && toReplace != null) {
            JexlNode parent = toReplace.jjtGetParent();
            if (parent != null && parent != toReplace) {
                JexlNode assignment = JexlNodeFactory.createAssignment("pruned", JexlStringBuildingVisitor.buildQuery(toReplace));
                List<JexlNode> children = new ArrayList<>(2);
                children.add(assignment);
                children.add(baseReplacement);
                JexlNodes.swap(parent, toReplace, JexlNodeFactory.createAndNode(children));
            }
        }
    }
    
    private boolean replace(JexlNode node, JexlNode replacement) {
        if (rewrite) {
            JexlNode parent = node.jjtGetParent();
            if (parent != null && parent != node) {
                JexlNodes.swap(parent, node, replacement);
                return true;
            }
        }
        return false;
        
    }
}
