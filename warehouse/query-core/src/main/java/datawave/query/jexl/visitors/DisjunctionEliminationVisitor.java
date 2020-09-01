package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;

import static org.apache.commons.jexl2.parser.JexlNodes.replaceChild;

public class DisjunctionEliminationVisitor extends RebuildingVisitor {
    
    private static final Logger log = Logger.getLogger(DisjunctionEliminationVisitor.class);
    
    /**
     * Given a JexlNode, determine if any duplicate disjunctions in the node can be removed.
     *
     * @param node
     *            a query node
     * @return a re-written query tree for the node
     */
    public static <T extends JexlNode> T optimize(T node) {
        if (node == null) {
            return null;
        }
        
        // Operate on copy of query tree.
        T copy = (T) copy(node);
        
        // Visit and enforce collapsing redundant nodes within expression.
        DisjunctionEliminationVisitor visitor = new DisjunctionEliminationVisitor();
        copy.jjtAccept(visitor, null);
        
        return copy;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        JexlNode left = getFirstNonASTReference(node.jjtGetChild(0));
        JexlNode right = getFirstNonASTReference(node.jjtGetChild(1));
        try {
            if (isDisjunction(left) && hasDuplicate(left, right)) {
                return copyChildAndUpdateParent(node, right);
            }
            if (isDisjunction(right) && hasDuplicate(right, left)) {
                return copyChildAndUpdateParent(node, left);
            }
        } catch (ParseException e) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to parse child node to check for equivalency", e);
            }
        }
        return node;
    }
    
    // Return the first non-wrapped node.
    private JexlNode getFirstNonASTReference(JexlNode node) {
        if (node instanceof ASTReference || node instanceof ASTReferenceExpression) {
            return getFirstNonASTReference(node.jjtGetChild(0));
        } else {
            return node;
        }
    }
    
    // Return whether or not the given node is an OR.
    private boolean isDisjunction(JexlNode node) {
        return node instanceof ASTOrNode;
    }
    
    // Return true if the disjunction node contains a duplicate of the provided node.
    private boolean hasDuplicate(JexlNode conjunction, JexlNode otherNode) throws ParseException {
        int totalChildren = conjunction.jjtGetNumChildren();
        ASTJexlScript script = getScript(otherNode);
        for (int i = 0; i < totalChildren; i++) {
            JexlNode child = conjunction.jjtGetChild(i);
            if (isEquivalent(child, script)) {
                return true;
            }
        }
        return false;
    }
    
    // Copy the child node and replace the original node with it in the original node's parent.
    private JexlNode copyChildAndUpdateParent(JexlNode original, JexlNode child) {
        JexlNode copy = copy(child);
        copy.image = original.image;
        replaceChild(original.jjtGetParent(), original, copy);
        return copy;
    }
    
    // Return whether or not the two JEXL queries are equivalent.
    private boolean isEquivalent(JexlNode node, ASTJexlScript script) throws ParseException {
        ASTJexlScript nodeScript = getScript(node);
        return TreeEqualityVisitor.isEqual(nodeScript, script, new TreeEqualityVisitor.Reason());
    }
    
    // Return the Jexl node as a script.
    private ASTJexlScript getScript(JexlNode node) throws ParseException {
        return JexlASTHelper.parseJexlQuery(JexlStringBuildingVisitor.buildQuery(node));
    }
}
