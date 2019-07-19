package datawave.query.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.jexl2.parser.JexlNodes.replaceChild;
import static org.apache.commons.jexl2.parser.JexlNodes.swap;
import static org.apache.commons.jexl2.parser.ParserTreeConstants.JJTANDNODE;
import static org.apache.commons.jexl2.parser.ParserTreeConstants.JJTORNODE;

/**
 * Visitor that enforces node uniqueness within AND or OR expressions. Nodes can be single nodes or subtrees.
 * 
 * <pre>
 * For example:
 * (A || A) => (A)
 * (A && A) => (A)
 * </pre>
 * 
 * This visitor returns a copy of the original query tree, and flattens the copy via the {@link TreeFlatteningRebuildingVisitor}
 * <p>
 * Node traversal is post order.
 */
public class UniqueExpressionTermsVisitor extends RebuildingVisitor {
    
    private int iterations = 0;
    private int duplicates = 0;
    
    private static final Logger log = Logger.getLogger(UniqueExpressionTermsVisitor.class);
    
    /**
     * Apply this visitor until the query tree stops changing or the maximum number of iterations is exceeded.
     *
     * @param node
     *            - the root node for a query tree
     * @param <T>
     * @return
     */
    public static <T extends JexlNode> T enforce(T node, int maxIterations) {
        if (node == null)
            return null;
        
        // Operate on copy of query tree
        T copy = (T) copy(node);
        
        UniqueExpressionTermsVisitor visitor = new UniqueExpressionTermsVisitor();
        boolean changesMade = true;
        
        while (visitor.iterations < maxIterations && changesMade) {
            visitor.iterations++;
            
            copy = TreeFlatteningRebuildingVisitor.flatten(copy);
            copy.jjtAccept(visitor, null);
            
            // Changes were made if the two trees are not equals
            changesMade = !TreeEqualityVisitor.isEqual((ASTJexlScript) node, (ASTJexlScript) copy, new TreeEqualityVisitor.Reason());
            node = copy;
        }
        
        // Log if we stopped visiting due to exceeding the max iterations
        if (visitor.iterations >= maxIterations && visitor.iterations > 0) {
            if (log.isDebugEnabled()) {
                log.debug("UniqueExpressionTermsVisitor exceeded maximum iteration (" + maxIterations + ").");
            }
        }
        
        if (log.isDebugEnabled()) {
            log.debug("UniqueExpressionTermsVisitor took " + visitor.iterations + " iterations to fully process query tree.");
            log.debug("UniqueExpressionTermsVisitor removed " + visitor.duplicates + " duplicate terms");
        }
        return node;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        
        // Post-order traversal
        node.childrenAccept(this, data);
        
        return removeDuplicateNodes(node, JJTORNODE);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        
        // Post-order traversal
        node.childrenAccept(this, data);
        
        return removeDuplicateNodes(node, JJTANDNODE);
    }
    
    private JexlNode removeDuplicateNodes(JexlNode node, int nodeId) {
        
        List<JexlNode> children = removeDuplicateChildNodes(node);
        
        if (children.size() == 1) {
            // If only one child remains, swap child node for current node.
            JexlNode child = children.get(0);
            swap(node.jjtGetParent(), node, child);
            return child;
        } else {
            // If two or more children remain, replace current children with new children
            JexlNode copy;
            if (nodeId == JJTORNODE) {
                copy = new ASTOrNode(JJTORNODE);
            } else if (nodeId == JJTANDNODE) {
                copy = new ASTAndNode(JJTANDNODE);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Tried to remove duplicates from a node that was not an AND/OR node.");
                }
                return node;
            }
            
            copy.image = node.image;
            copy.jjtSetParent(node.jjtGetParent());
            for (int ii = 0; ii < children.size(); ii++) {
                copy.jjtAddChild(children.get(ii), ii);
            }
            replaceChild(node.jjtGetParent(), node, copy);
            node = copy;
        }
        return node;
    }
    
    /**
     * Enforce uniqueness among child nodes.
     *
     * @param node
     *            - an ASTOrNode or ASTAndNode
     */
    private List<JexlNode> removeDuplicateChildNodes(JexlNode node) {
        
        Set<String> childKeys = new HashSet<>();
        List<JexlNode> children = new ArrayList<>();
        for (int ii = 0; ii < node.jjtGetNumChildren(); ii++) {
            JexlNode child = node.jjtGetChild(ii);
            String childKey = TreeHashVisitor.getNodeHash(child).toString();
            if (childKeys.add(childKey)) {
                children.add(child);
            } else {
                this.duplicates++;
            }
        }
        return children;
    }
}
