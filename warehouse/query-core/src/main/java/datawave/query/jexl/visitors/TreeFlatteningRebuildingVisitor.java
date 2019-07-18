package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This will flatten ands and ors. If requested this will also remove reference expressions and references where possible. NOTE: If you remove reference
 * expressions and references, this will adversely affect the jexl evaluation of the query.
 */
public class TreeFlatteningRebuildingVisitor extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(TreeFlatteningRebuildingVisitor.class);
    
    private final boolean removeReferences;
    
    public TreeFlatteningRebuildingVisitor(boolean removeReferences) {
        this.removeReferences = removeReferences;
    }
    
    /**
     * This will flatten ands and ors.
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T flatten(T node) {
        return flatten(node, false);
    }
    
    /**
     * This will flatten ands, ors, and references and references expressions NOTE: If you remove reference expressions and references, this may adversely
     * affect the evaluation of the query (true in the index query logic case: bug?).
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T flattenAll(T node) {
        return flatten(node, true);
    }
    
    /**
     * This will flatten ands and ors. If requested this will also remove reference expressions and references where possible. NOTE: If you remove reference
     * expressions and references, this may adversely affect the evaluation of the query (true in the index query logic case: bug?).
     */
    @SuppressWarnings("unchecked")
    private static <T extends JexlNode> T flatten(T node, boolean removeReferences) {
        TreeFlatteningRebuildingVisitor visitor = new TreeFlatteningRebuildingVisitor(removeReferences);
        
        return (T) node.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        ASTOrNode orNode = JexlNodes.newInstanceOfType(node);
        orNode.image = node.image;
        flattenTree(node, orNode);
        return orNode;
    }
    
    private static boolean isBoundedRange(ASTAndNode node) {
        List<JexlNode> otherNodes = new ArrayList<>();
        Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic(node, otherNodes, false);
        return ranges.size() == 1 && otherNodes.isEmpty();
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (ExceededValueThresholdMarkerJexlNode.instanceOf(node) || ExceededTermThresholdMarkerJexlNode.instanceOf(node)
                        || ExceededOrThresholdMarkerJexlNode.instanceOf(node) || ASTDelayedPredicate.instanceOf(node) || ASTEvaluationOnly.instanceOf(node)) {
            return super.visit(node, data);
        }
        
        ASTAndNode andNode = JexlNodes.newInstanceOfType(node);
        andNode.image = node.image;
        flattenTree(node, andNode);
        return andNode;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (!removeReferences) {
            return super.visit(node, data);
        }
        if (ASTDelayedPredicate.instanceOf(node) || IndexHoleMarkerJexlNode.instanceOf(node) || ASTEvaluationOnly.instanceOf(node)) {
            return super.visit(node, data);
        }
        if (ExceededValueThresholdMarkerJexlNode.instanceOf(node) || ExceededTermThresholdMarkerJexlNode.instanceOf(node)
                        || ExceededOrThresholdMarkerJexlNode.instanceOf(node)) {
            return super.visit(node, data);
        }
        if (JexlASTHelper.dereference(node) instanceof ASTAssignment) {
            return super.visit(node, data);
        }
        if (node.jjtGetParent() instanceof ASTAndNode) {
            ASTAndNode andNode = JexlNodes.newInstanceOfType(((ASTAndNode) (node.jjtGetParent())));
            andNode.image = node.jjtGetParent().image;
            
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
                newNode.jjtSetParent(andNode);
                andNode.jjtAddChild(newNode, andNode.jjtGetNumChildren());
            }
            
            return andNode;
        }
        if (node.jjtGetParent() instanceof ASTOrNode) {
            ASTOrNode orNode = JexlNodes.newInstanceOfType(((ASTOrNode) (node.jjtGetParent())));
            orNode.image = node.jjtGetParent().image;
            
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
                newNode.jjtSetParent(orNode);
                orNode.jjtAddChild(newNode, orNode.jjtGetNumChildren());
            }
            
            return orNode;
            
        }
        if (node.jjtGetParent() instanceof ASTNotNode) {
            // ensure we keep negated expressions
            return super.visit(node, data);
        }
        
        JexlNode newNode = (JexlNode) super.visit(node, data);
        JexlNode childNode = null;
        /**
         * Explore the possibility that we have an unnecessary top level ASTReference expression. Could walk up the tree, but this will be less work as we're
         * checking if we're the root, then advance to see if we've within a Reference expression.
         */
        if (null == newNode.jjtGetParent() && (childNode = advanceReferenceExpression(newNode)) != null) {
            if (childNode.jjtGetNumChildren() == 1) {
                return childNode.jjtGetChild(0);
            }
        }
        return newNode;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        ASTReferenceExpression newExpressive = null;
        if (!removeReferences) {
            return super.visit(node, data);
        }
        if (ExceededValueThresholdMarkerJexlNode.instanceOf(node) || ExceededTermThresholdMarkerJexlNode.instanceOf(node)
                        || ExceededOrThresholdMarkerJexlNode.instanceOf(node)) {
            return super.visit(node, data);
        }
        if (JexlASTHelper.dereference(node) instanceof ASTAssignment) {
            return super.visit(node, data);
        }
        if (node.jjtGetParent() instanceof ASTAndNode) {
            ASTAndNode andNode = JexlNodes.newInstanceOfType(((ASTAndNode) (node.jjtGetParent())));
            andNode.image = node.jjtGetParent().image;
            
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
                newNode.jjtSetParent(andNode);
                andNode.jjtAddChild(newNode, andNode.jjtGetNumChildren());
            }
            
            return andNode;
        }
        if (node.jjtGetParent() instanceof ASTOrNode) {
            ASTOrNode orNode = JexlNodes.newInstanceOfType(((ASTOrNode) (node.jjtGetParent())));
            orNode.image = node.jjtGetParent().image;
            
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
                newNode.jjtSetParent(orNode);
                orNode.jjtAddChild(newNode, orNode.jjtGetNumChildren());
            }
            
            return orNode;
        }
        if (node.jjtGetParent() instanceof ASTNotNode || node.jjtGetParent() instanceof ASTReference) {
            // ensure we keep negated expressions
            return super.visit(node, data);
        }
        if (node.jjtGetNumChildren() == 1 && (newExpressive = advanceReferenceExpression(node.jjtGetChild(0))) != null) {
            return visit(newExpressive, data);
        }
        
        return super.visit(node, data);
    }
    
    /**
     * Advances a child reference expression, if one is embedded Ex. {@code Ref RefExpr <-- this way we at Ref RefExpr}
     * 
     * will become
     * 
     * {@code Ref RefExpr <-- this still way we at}
     * 
     * @param jexlNode
     *            Incoming JexlNode
     * @return
     */
    protected ASTReferenceExpression advanceReferenceExpression(JexlNode jexlNode) {
        if (jexlNode instanceof ASTReference) {
            if (jexlNode.jjtGetNumChildren() == 1 && jexlNode.jjtGetChild(0) instanceof ASTReferenceExpression) {
                ASTReferenceExpression expression = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
                if (null != jexlNode.jjtGetParent()) {
                    expression.image = jexlNode.jjtGetParent().image;
                } else {
                    expression.image = null;
                }
                
                JexlNode node = jexlNode.jjtGetChild(0);
                
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
                    newNode.jjtSetParent(expression);
                    expression.jjtAddChild(newNode, expression.jjtGetNumChildren());
                }
                
                return expression;
            }
        }
        return null;
    }
    
    protected void flattenTree(JexlNode currentNode, JexlNode newNode) {
        if (!currentNode.getClass().equals(newNode.getClass())) {
            log.warn("newNode is not the same type as currentNode ... something has probably gone horribly wrong");
        }
        
        final LinkedList<JexlNode> toCombine = new LinkedList<>();
        do {
            for (int i = 0; i < currentNode.jjtGetNumChildren(); i++) {
                final JexlNode child = currentNode.jjtGetChild(i);
                final JexlNode node;
                final JexlNode dereferenced;
                if (child instanceof ASTOrNode) {
                    node = child;
                    dereferenced = JexlASTHelper.dereference(child);
                } else {
                    node = (JexlNode) child.jjtAccept(this, null);
                    dereferenced = JexlASTHelper.dereference(node);
                }
                if (acceptableNodesToCombine(currentNode, dereferenced, !node.equals(dereferenced))) {
                    toCombine.addFirst(dereferenced);
                } else {
                    newNode.jjtAddChild(node, newNode.jjtGetNumChildren());
                    node.jjtSetParent(newNode);
                }
            }
        } while ((currentNode = toCombine.pollFirst()) != null);
    }
    
    protected static boolean acceptableNodesToCombine(JexlNode currentNode, JexlNode newNode, boolean isWrapped) {
        if (currentNode.getClass().equals(newNode.getClass())) {
            // if this is a bounded range or marker node, then to not combine
            // don't allow combination of a marker node UNLESS it's already unwrapped
            if (newNode instanceof ASTAndNode && (isBoundedRange((ASTAndNode) newNode) || (QueryPropertyMarker.instanceOf(newNode, null) && isWrapped))) {
                return false;
            }
            
            return true;
        }
        
        return false;
    }
    
}
