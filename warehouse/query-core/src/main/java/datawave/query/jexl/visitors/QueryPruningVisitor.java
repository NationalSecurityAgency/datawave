package datawave.query.jexl.visitors;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl3.parser.ASTAddNode;
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
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Visitor can be used to identify and prune children of a JexlNode that will always be TRUE or FALSE. When present, _NOFIELD_ indicates that a criteria can
 * never be fulfilled. Using this visitor two questions can be answered about any JexlNode. What is the JexlNode's current state? And what does the JexlNode's
 * tree look like if we reduce it based on everything we know to be TRUE/FALSE about the tree?
 */
public class QueryPruningVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(QueryPruningVisitor.class);
    
    public enum TruthState {
        UNKNOWN, TRUE, FALSE
    }
    
    /**
     * Given a JexlNode, determine if any children or even the node itself can be replaced by a TrueNode or FalseNode, For any rewritten sub-trees, log the
     * pruned tree, and the replace with ASTTrue/ASTFalse.
     * 
     * @param node
     *            a non-null query node
     * @param showPrune
     *            if set to true, debug lines will be written showing what was pruned out of each tree
     * @return a rewritten query tree for node
     */
    public static JexlNode reduce(JexlNode node, boolean showPrune) {
        QueryPruningVisitor visitor = new QueryPruningVisitor(true, showPrune);
        
        String before = null;
        String after;
        
        if (showPrune) {
            before = JexlStringBuildingVisitor.buildQuery(node);
        }
        
        // flatten the tree and create a copy
        JexlNode copy = TreeFlatteningRebuildingVisitor.flatten(node);
        
        copy.jjtAccept(visitor, null);
        
        // Now since we could have removed children within AND/OR nodes,
        // reflatten to remove boolean operators with single children
        copy = TreeFlatteningRebuildingVisitor.flatten(copy);
        
        if (showPrune) {
            after = JexlStringBuildingVisitor.buildQuery(copy);
            if (StringUtils.equals(before, after)) {
                log.debug("Query was not pruned");
            } else {
                log.debug("Query before prune: " + before + "\nQuery after prune: " + after);
            }
        }
        
        return copy;
    }
    
    /**
     * Given a JexlNode determine if the query the node represents is TRUE/FALSE/UNKNOWN
     * 
     * @param node
     *            non-null JexlNode
     * @return TRUE if evaluation would always return true, FALSE if it will always return false, otherwise UNKNOWN
     */
    public static TruthState getState(JexlNode node) {
        QueryPruningVisitor visitor = new QueryPruningVisitor(false);
        return (TruthState) node.jjtAccept(visitor, null);
    }
    
    final private boolean rewrite;
    final private boolean debugPrune;
    
    private QueryPruningVisitor(boolean rewrite) {
        this(rewrite, false);
    }
    
    private QueryPruningVisitor(boolean rewrite, boolean debugPrune) {
        this.rewrite = rewrite;
        this.debugPrune = debugPrune;
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
        if (node.jjtGetNumChildren() != 1) {
            throw new IllegalStateException("ASTNotNode must only have one child: " + node);
        }
        // grab the node string before recursion so the original is intact
        String originalString = null;
        if (rewrite && debugPrune) {
            originalString = JexlStringBuildingVisitor.buildQuery(node);
        }
        
        TruthState state = (TruthState) node.jjtGetChild(0).jjtAccept(this, data);
        
        if (state == TruthState.TRUE) {
            replaceAndAssign(node, originalString, new ASTFalseNode(ParserTreeConstants.JJTFALSENODE));
            return TruthState.FALSE;
        } else if (state == TruthState.FALSE) {
            replaceAndAssign(node, originalString, new ASTTrueNode(ParserTreeConstants.JJTTRUENODE));
            return TruthState.TRUE;
        } else {
            return TruthState.UNKNOWN;
        }
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        // grab the node string before recursion so the original is intact
        String originalString = null;
        if (rewrite && debugPrune) {
            originalString = JexlStringBuildingVisitor.buildQuery(node);
        }
        
        Set<TruthState> states = new HashSet<>();
        for (int i = node.jjtGetNumChildren() - 1; i >= 0; i--) {
            JexlNode child = node.jjtGetChild(i);
            String originalChild = null;
            if (rewrite && debugPrune) {
                originalChild = JexlStringBuildingVisitor.buildQuery(child);
            }
            TruthState state = (TruthState) (child.jjtAccept(this, data));
            if (state == TruthState.TRUE) {
                // short circuit
                replaceAndAssign(node, originalString, new ASTTrueNode(ParserTreeConstants.JJTTRUENODE));
                return TruthState.TRUE;
            } else if (state == TruthState.FALSE) {
                // drop this node from the OR it can never be satisfied
                // if the child was a junction that was pruned, then the child object is a ref node but the child
                // at index i is not. reassign for consistency.
                child = node.jjtGetChild(i);
                replaceAndAssign(child, null, null);
                if (debugPrune) {
                    log.debug("Pruning " + originalChild + " from " + originalString);
                }
            }
            states.add(state);
        }
        
        // the node is either UNKNOWN or FALSE at this point since TRUE breaks evaluation early
        if (states.contains(TruthState.UNKNOWN)) {
            return TruthState.UNKNOWN;
        } else {
            replaceAndAssign(node, originalString, new ASTFalseNode(ParserTreeConstants.JJTFALSENODE));
            return TruthState.FALSE;
        }
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // grab the node string before recursion so the original is intact
        String originalString = null;
        if (rewrite && debugPrune) {
            originalString = JexlStringBuildingVisitor.buildQuery(node);
        }
        
        Set<TruthState> states = new HashSet<>();
        for (int i = node.jjtGetNumChildren() - 1; i >= 0; i--) {
            JexlNode child = node.jjtGetChild(i);
            String originalChild = null;
            if (rewrite && debugPrune) {
                originalChild = JexlStringBuildingVisitor.buildQuery(child);
            }
            TruthState state = (TruthState) (child.jjtAccept(this, data));
            if (state == TruthState.FALSE) {
                // short circuit
                replaceAndAssign(node, originalString, new ASTFalseNode(ParserTreeConstants.JJTFALSENODE));
                return TruthState.FALSE;
            } else if (state == TruthState.TRUE) {
                // drop this node from the AND it will always be satisfied
                // if the child was a junction that was pruned, then the child object is a ref node but the child
                // at index i is not. reassign for consistency.
                child = node.jjtGetChild(i);
                replaceAndAssign(child, null, null);
                if (debugPrune) {
                    log.debug("Pruning " + originalChild + " from " + originalString);
                }
            }
            states.add(state);
        }
        
        // the node is either UNKNOWN or TRUE at this point since FALSE breaks evaluation early
        if (states.contains(TruthState.UNKNOWN)) {
            return TruthState.UNKNOWN;
        } else {
            replaceAndAssign(node, originalString, new ASTTrueNode(ParserTreeConstants.JJTTRUENODE));
            return TruthState.TRUE;
        }
    }
    
    // deal with wrappers
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        // if there are multiple children here there are multiple statements/queries process this as an implicit OR do not reduce across them, but reduce each
        // individually
        Set<TruthState> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((TruthState) node.jjtGetChild(i).jjtAccept(this, data));
        }
        
        if (states.contains(TruthState.TRUE)) {
            return TruthState.TRUE;
        } else if (states.contains(TruthState.UNKNOWN)) {
            return TruthState.UNKNOWN;
        } else {
            return TruthState.FALSE;
        }
    }
    
    /**
     * When a junction is pruned to a single child also prune that junction and the surrounding reference expression
     * <p>
     * <code>parent - refExpr - and/or - single child</code>
     * <p>
     * prunes to
     * <p>
     * <code>parent - single child</code>
     *
     * @param node
     *            a JexlNode
     */
    private void pruneJunctionWithSingleChild(JexlNode node) {
        boolean isRefExp = node.getClass().isAssignableFrom(ASTReferenceExpression.class);
        if (isRefExp && node.jjtGetNumChildren() == 1) {
            JexlNode junction = node.jjtGetChild(0);
            if (junction.jjtGetNumChildren() == 1 && (junction instanceof ASTOrNode || junction instanceof ASTAndNode)) {
                JexlNode parent = node.jjtGetParent();
                JexlNode source = junction.jjtGetChild(0);
                JexlNodes.swap(parent, node, source);
            }
        }
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        if (node.jjtGetNumChildren() != 1) {
            throw new IllegalStateException("ASTReferenceExpression must only have one child: " + node);
        }
        Object o = node.jjtGetChild(0).jjtAccept(this, data);
        
        pruneJunctionWithSingleChild(node);
        
        return o;
    }
    
    // true/false for respective nodes
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return TruthState.TRUE;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return TruthState.FALSE;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTAddNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTSubNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return TruthState.UNKNOWN;
    }
    
    /**
     * Simple case processing only. If a candidate node has exactly 1 identifier and literal node and it is _NO_FIELD_ we know the truth state will always be
     * FALSE
     * 
     * @param node
     *            the jexl node
     * @return TruthState.FALSE if the the sole identifier is _NOFIELD_, otherwise TruthState.UNKNOWN
     */
    private TruthState identifyAndReplace(JexlNode node) {
        try {
            if (Constants.NO_FIELD.equals(JexlASTHelper.getIdentifier(node))) {
                return TruthState.FALSE;
            }
        } catch (NoSuchElementException e) {
            // no-op
        }
        
        return TruthState.UNKNOWN;
    }
    
    /**
     * Prune a tree, optionally with an assignment showing the portion of the tree that was pruned. This will be more verbose, but will clearly identify what
     * was removed. Only remove a node if rewrite is enabled, only write an assignment node showing the pruned tree is queryString is not null
     * 
     * @param toReplace
     *            the node to replace in the tree with an assignment
     * @param queryString
     *            the string to use if building an assignment node for the portion of the tree that was pruned, may be null
     * @param baseReplacement
     *            what to replace toReplace with
     */
    private void replaceAndAssign(JexlNode toReplace, String queryString, JexlNode baseReplacement) {
        if (rewrite && toReplace != null) {
            JexlNode parent = toReplace.jjtGetParent();
            if (parent != null && parent != toReplace) {
                if (queryString != null && log.isDebugEnabled() && this.debugPrune && baseReplacement != null) {
                    log.debug("Pruning " + queryString + " to " + (baseReplacement instanceof ASTTrueNode ? "true" : "false"));
                }
                
                if (baseReplacement != null) {
                    JexlNodes.swap(parent, toReplace, baseReplacement);
                } else {
                    // remove the node entirely
                    List<JexlNode> children = new ArrayList<>(parent.jjtGetNumChildren() - 1);
                    for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
                        JexlNode child = parent.jjtGetChild(i);
                        if (child != toReplace) {
                            children.add(child);
                        } else {
                            // clear the old nodes parentage
                            child.jjtSetParent(null);
                        }
                    }
                    
                    JexlNodes.children(parent, children.toArray(new JexlNode[children.size()]));
                }
            }
            
            // if a junction was completely pruned it is replaced with the last child. In this case, also check
            // for extra reference expressions and prune those.
            if ((toReplace instanceof ASTOrNode || toReplace instanceof ASTAndNode) && (parent != null && parent.jjtGetNumChildren() == 1)) {
                if (parent instanceof ASTReferenceExpression && parent.jjtGetParent() != null) {
                    JexlNodes.swap(parent.jjtGetParent(), parent, parent.jjtGetChild(0));
                }
            }
        }
    }
}
